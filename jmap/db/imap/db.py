from datetime import datetime
import re

from imapclient import IMAPClient, imap_utf7
from imapclient.exceptions import IMAPClientError
from jmap import errors, parse
from ..base import BaseDB
from .message import ImapMessage, EmailState, FIELDS_MAP, format_message_id, parse_message_id, keyword2flag
from .mailbox import ImapMailbox


class ImapDB(BaseDB):
    def __init__(self, username, password='h', host='localhost', port=143, *args, **kwargs):
        super().__init__(username, *args, **kwargs)
        self.imap = IMAPClient(host, port, use_uid=True, ssl=False)
        self.imap.login(username, password)
        self.imap.enable("UTF8=ACCEPT")
        self.imap.enable("QRESYNC")

        self.mailbox_state = now_state()
        self.mailboxes = {}
        self.byimapname = {}
        self.messages = {}

        self.sync_mailboxes()
        self.imap.select_folder('virtual/All')

    
    def get_email_state(self):
        "Return newest Mailbox state"
        res = self.imap.folder_status('virtual/All', [b'UIDNEXT', b'HIGHESTMODSEQ'])
        return str(EmailState(res[b'UIDNEXT'], res[b'HIGHESTMODSEQ']))

    def get_mailbox_state(self):
        "Return newest Mailbox state"
        res = self.sync_mailboxes()
        return self.mailbox_state

    def get_cached_messaged(self, properties=(), id__in=()):
        messages = []
        if not self.messages:
            return messages, id__in, properties
        fetch_props = set()
        fetch_ids = set(id__in)
        for id in id__in:
            msg = self.messages.get(id, None)
            if msg:
                found = True
                for prop in properties:
                    try:
                        msg[prop]
                    except (KeyError, AttributeError):
                        found = False
                        fetch_props.add(prop)
                if found:
                    fetch_ids.remove(id)
                    messages.append(msg)
        # if one messages is missing, need to fetch all properties
        if len(messages) < len(id__in):
            fetch_props = properties
        return messages, fetch_ids, fetch_props


    def get_messages(self, properties=(), sort={}, id__in=None, threadId__in=None, updated__gt=None, **criteria):
        """
        updated__gt: str "{uid},{modseq}"
        criteria: dict of JMAP criteria
        """
        # XXX: id == threadId for now
        if id__in is None and threadId__in is not None:
            id__in = [id[1:] for id in threadId__in]
        if id__in is None:
            messages = []
        else:
            # try get everything from cache
            messages, id__in, properties = self.get_cached_messaged(properties, id__in=id__in)

        fetch_fields = {f for prop, f in FIELDS_MAP.items() if prop in properties}
        if b'RFC822' in fetch_fields:
            # remove redundand fields
            fetch_fields.discard(b'RFC822.HEADER')
            fetch_fields.discard(b'RFC822.SIZE')

        sort_criteria = as_imap_sort(sort) or '' if sort else None

        if id__in is not None:
            if len(id__in) == 0:
                return messages  # no messages matches empty ids
            if not fetch_fields and not sort_criteria:
                # when we don't need anything new from IMAP, create empty messages
                # useful when requested conditions can be calculated from id (threadId)
                messages.extend(self.messages.get(id, 0) or ImapMessage(id=id) for id in id__in)
                return messages
            uids = []
            for id in id__in:
                try:
                    uids.append(parse_message_id(id))
                except ValueError:
                    # not parsable == not found
                    continue
        else:
            uids = None
        # now uids is None or not empty

        search_criteria = self.as_imap_search(criteria)
        if sort_criteria:
            if uids:
                search_criteria += b' UID ' + encode_seqset(uids)
            uids = self.imap.sort(sort_criteria, bytes(search_criteria))
        elif search_criteria:
            if uids:
                search_criteria += b' UID ' + encode_seqset(uids)
            uids = self.imap.search(bytes(search_criteria))

        fetch_fields.add('X-MAILBOX')
        if updated__gt:
            state = EmailState.from_str(updated__gt)
            modifiers = 'changedsince %s vanished' % state.modseq
            if uids:
                uids = [u for u in uids if u >= state.uid]
            elif uids is None:
                uids = b'%d:*' % state.uid
            fetches = self.imap.fetch(uids, fetch_fields, modifiers)
        else:
            if uids is None:
                uids = b'1:*'
            fetches = self.imap.fetch(uids, fetch_fields)

        for uid, data in fetches.items():
            id = format_message_id(uid)
            msg = self.messages.get(id, None)
            if not msg:
                imapname = imap_utf7.decode(data[b'X-MAILBOX'])
                msg = ImapMessage(id=id, mailboxIds=[self.byimapname[imapname]['id']])
                self.messages[id] = msg
            msg.update(data)
            messages.append(msg)

        return messages


    def create_message(self, mailboxIds=None, **data):
        if not mailboxIds:
            raise errors.invalidArguments('mailboxIds is required when creating email')
        msg = ImapMessage(**data)
        try:
            mailboxid, = mailboxIds
        except ValueError:
            raise errors.tooManyMailboxes('Only 1 mailbox allowed in this implementation')
        mailbox = self.mailboxes.get(mailboxid, None)
        if not mailbox or mailbox['deleted']:
            raise errors.notFound(f"Mailbox {mailboxid} not found")
        res = self.imap.append(mailbox['imapname'], msg['RFC822'], flags=msg['flags'])
        # TODO parse res, get uid, fetch x-guid, return id
        match = re.search(r'\[APPENDUID=(\d+)\]', res)
        return format_message_id(int(match.group(1)))


    def update_message(self, id, ifInState=None, **update):
        uid = parse_message_id(id)
        flags_add = []
        flags_del = []
        mids_add = []
        mids_del = []
        for path in sorted(update.keys()):
            try:
                prop, key = path.split('/')
                if prop == 'keywords':
                    if update[path]:
                        flags_add.append(keyword2flag(key))
                    else:
                        flags_del.append(keyword2flag(key))
                elif prop == 'mailboxId':
                    if update[path]:
                        mids_add.append(key)
                    else:
                        mids_del.append(key)
                else:
                    raise errors.invalidArguments(f"Unknown update {path}")
            except ValueError:
                items = update[path].items()
                if path == 'keywords':
                    flags_add = [keyword2flag(k) for k,v in items if v]
                    flags_del = [keyword2flag(k) for k,v in items if not v]
                elif path == 'mailboxIds':
                    mids_add = [k for k, v in items if v]
                    mids_del = [k for k, v in items if not v]
                else:
                    raise errors.invalidArguments(f"Unknown update {path}")

        if flags_add:
            self.imap.add_flags(uid, flags_add)
        if flags_del:
            self.imap.remove_flags(uid, flags_del)
        
        if mids_add or mids_del:
            if flags_add or flags_del:
                error = errors.serverPartialFail
            else:
                error = errors.tooManyMailboxes
            raise error("This implementation don't support Email/set mailboxIds, use Email/copy")


    def destroy_message(self, id):
        uid = parse_message_id(id)
        try:
            self.imap.add_flags(uid, b'\\Deleted')
            # remove from cache
            self.messages.pop(id, None)
            # if you want to remove this expunge,
            # then you need to work with NOT DELETED messages elsewhere
            self.imap.expunge(uid)
        except IMAPClientError:
            raise errors.notFound()


    def get_mailboxes(self, fields=None, updated__gt=None, deleted=None, **criteria):
        self.sync_mailboxes(fields)
        mailboxes = self.mailboxes.values()

        if deleted is not None:
            if deleted:
                mailboxes = (m for m in mailboxes if m['deleted'])
            else:
                mailboxes = (m for m in mailboxes if not m['deleted'])

        if updated__gt is not None:
            mailboxes = (m for m in mailboxes if m['updated'] > updated__gt)

        return mailboxes


    def sync_mailboxes(self, fields=None):
        # TODO: LIST "" % RETURN (STATUS (MESSAGES UIDVALIDITY HIGHESTMODSEQ X-GUID))
        deleted_ids = set(self.mailboxes.keys())
        if fields is None:
            fields = {'totalEmails','unreadEmails','totalThreads','unreadThreads'}
        new_state = now_state()
        for flags, sep, imapname in self.imap.list_folders():
            if b'\\Noselect' in flags:
                continue
            status = self.imap.folder_status(imapname, (['MESSAGES', 'UIDVALIDITY', 'UIDNEXT', 'X-GUID']))
            id = status[b'X-GUID'].decode()
            mailbox = self.mailboxes.get(id, None)
            if mailbox:
                deleted_ids.remove(id)
            else:
                mailbox = ImapMailbox(id=id, db=self)
                self.byimapname[imapname] = mailbox
                self.mailboxes[id] = mailbox
            data = {
                'totalEmails': status[b'MESSAGES'],
                'imapname': imapname,
                'sep': sep.decode(),
                'flags': flags,
                'uidvalidity': status[b'UIDVALIDITY'],
                'uidnext': status[b'UIDNEXT'],
            }
            if 'unreadEmails' in fields:
                uids = self.imap.search(b'UNSEEN UNDRAFT X-MAILBOX %s' % imap_utf7.encode(imapname))
                data['unreadEmails'] = len(uids)
            for key, val in data.items():
                if mailbox[key] != val:
                    if key not in ('totalEmails', 'unreadEmails', 'totalThreads', 'unreadThreads'):
                        mailbox['updatedNonCounts'] = new_state
                        if key == 'imapname':
                            mailbox.pop('name', None)
                            mailbox.pop('parentId', None)
                    mailbox['updated'] = new_state
                    mailbox[key] = val

        for id in deleted_ids:
            mailbox = self.mailboxes[id]
            if not mailbox['deleted']:
                mailbox['deleted'] = new_state
    

    def mailbox_imapname(self, parentId, name):
        parent = self.mailboxes.get(parentId, None)
        if not parent or parent['deleted']:
            raise errors.notFound('parent folder not found')
        return parent['imapname'] + parent['sep'] + name


    def create_mailbox(self, name=None, parentId=None, isSubscribed=True, **kwargs):
        if not name:
            raise errors.invalidProperties('name is required')
        imapname = self.mailbox_imapname(parentId, name)
        # TODO: parse returned MAILBOXID
        try:
            res = self.imap.create_folder(imapname)
        except IMAPClientError as e:
            desc = str(e)
            if '[ALREADYEXISTS]' in desc:
                raise errors.invalidArguments(desc)
        except Exception:
            raise errors.serverFail(res.decode())

        if not isSubscribed:
            self.imap.unsubscribe_folder(imapname)

        status = self.imap.folder_status(imapname, ['X-GUID'])
        self.sync_mailboxes()
        return status[b'X-GUID'].decode()


    def update_mailbox(self, id, name=None, parentId=None, isSubscribed=None, sortOrder=None, **update):
        mailbox = self.mailboxes.get(id, None)
        if not mailbox or mailbox['deleted']:
            raise errors.notFound('mailbox not found')
        imapname = mailbox['imapname']

        if (name is not None and name != mailbox['name']) or \
           (parentId is not None and parentId != mailbox['parentId']):
            if not name:
                raise errors.invalidProperties('name is required')
            newimapname = self.mailbox_imapname(parentId, name)
            res = self.imap.rename_folder(imapname, newimapname)
            if b'NO' in res or b'BAD' in res:
                raise errors.serverFail(res.encode())

        if isSubscribed is not None and isSubscribed != mailbox['isSubscribed']:
            if isSubscribed:
                res = self.imap.subscribe_folder(imapname)
            else:
                res = self.imap.unsubscribe_folder(imapname)
            if b'NO' in res or b'BAD' in res:
                raise errors.serverFail(res.encode())

        if sortOrder is not None and sortOrder != mailbox['sortOrder']:
            # TODO: store in persistent storage
            mailbox['sortOrder'] = sortOrder
        self.sync_mailboxes()


    def destroy_mailbox(self, id):
        mailbox = self.mailboxes.get(id, None)
        if not mailbox or mailbox['deleted']:
            raise errors.notFound('mailbox not found')
        res = self.imap.delete_folder(mailbox['imapname'])
        if b'NO' in res or b'BAD' in res:
            raise errors.serverFail(res.encode())
        self.sync_mailboxes()


    def as_imap_search(self, criteria):
        out = bytearray()
        operator = criteria.get('operator', None)
        if operator:
            conds = criteria['conds']
            if operator == 'NOT' or operator == 'OR':
                if len(conds) > 0:
                    if operator == 'NOT':
                        out += b' NOT'
                    lastcond = len(conds) - 1
                    for i, cond in enumerate(conds):
                        # OR needs 2 args, we can omit last OR
                        if i < lastcond:
                            out += b' OR'
                        out += self.as_imap_search(cond)
                    return out
                else:
                    raise errors.unsupportedFilter(f"Empty filter conditions")
            elif operator == 'AND':
                for c in conds:
                    out += self.as_imap_search(c)
                return out
            raise errors.unsupportedFilter(f"Invalid operator {operator}")

        for crit, value in criteria.items():
            search, func = SEARCH_MAP.get(crit, (None, None))
            if search:
                out += search
                out += func(value) if func else value
            elif 'deleted' == crit:
                if not value:
                    out += b' NOT'
                out += b' DELETED'
            elif 'header' == crit:
                out += b' HEADER '
                out += value[0].encode()
                out += b' '
                out += value[1].encode()
            elif 'hasAttachment' == crit:
                if not value:
                    out += b' NOT'
                # needs Dovecot flag attachments on save
                out += b' KEYWORD $HasAttachment'
                # or out += b'MIMEPART (DISPOSITION TYPE attachment)')
            elif 'inMailbox' == crit:
                out += b' X-MAILBOX '
                try:
                    out += imap_utf7.encode(self.mailboxes[value]["imapname"])
                except KeyError:
                    raise errors.notFound(f"Mailbox {value} not found")
            elif 'inMailboxOtherThan' == crit:
                try:
                    for id in value:
                        out += b' NOT X-MAILBOX '
                        out += imap_utf7.encode(self.mailboxes[id]["imapname"])
                except KeyError:
                    raise errors.notFound(f"Mailbox {value} not found")
            else:
                raise UserWarning(f'Filter {crit} not supported')
        return out

def int2bytes(i):
    return b'%d' % i

SEARCH_MAP = {
    'blobId': (b'X-GUID', bytes),
    'minSize': (b'NOT SMALLER', int2bytes),
    'maxSize': (b'NOT LARGER', int2bytes),
    'hasKeyword': (b'KEYWORD', keyword2flag),
    'notKeyword': (b'UNKEYWORD', keyword2flag),
    'allInThreadHaveKeyword': (b'NOT INTHREAD UNKEYWORD', keyword2flag),
    'someInThreadHaveKeyword': (b'INTHREAD KEYWORD', keyword2flag),
    'noneInThreadHaveKeyword': (b'NOT INTHREAD KEYWORD', keyword2flag),
    'before': (b'BEFORE', bytes),  # TODO: consider time, not only date
    'after': (b'AFTER', bytes),
    'subject': (b'SUBJECT', bytes),
    'text': (b'TEXT', bytes),
    'body': (b'BODY', bytes),
    'from': (b'FROM', bytes),
    'to': (b'TO', bytes),
    'cc': (b'CC', bytes),
    'bcc': (b'BCC', bytes),
}

SORT_MAP = {
    'receivedAt': 'ARRIVAL',
    'sentAt': 'DATE',
    'size': 'SIZE',
    'subject': 'SUBJECT',
    'from': 'FROM',
    'to': 'TO',
    'cc': 'CC',
}

def as_imap_sort(sort):
    criteria = []
    for crit in sort:
        if not crit.get('isAscending', True):
            criteria.append('REVERSE')
        try:
            criteria.append(SORT_MAP[crit['property']])
        except KeyError:
            raise errors.unsupportedSort(f"Property {crit['property']} is not sortable")
    return criteria


def find_type(message, part):
    if message.get('id', '') == part:
        return message['type']
    
    for sub in message['attachments']:
        typ = find_type(sub, part)
        if type:
            return type
    return None


def encode_seqset(seq):
    "Compress sequence of intergers to bytearray for IMAP"
    out = bytearray()
    last = None
    skipped = False
    for i in sorted(seq):
        if i - 1 == last:
            skipped = True
        elif last is None:
            out += b'%d' % i
        elif i - 1 > last:
            if skipped:
                out += b':%d,%d' % (last, i)
                skipped = False
            else:
                out += b',%d' % (i)
        last = i
    if skipped:
        out += b':%d' % last
    return out


def now_state():
    return str(int(datetime.now().timestamp()))
