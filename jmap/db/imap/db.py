from imapclient import IMAPClient
from imapclient.exceptions import IMAPClientError

from jmap import errors
from ..base import BaseDB
from .message import ImapMessage, FIELDS_MAP, format_message_id, parse_message_id
from .mailbox import ImapMailbox


class ImapDB(BaseDB):
    def __init__(self, username, password='h', host='localhost', port=143, *args, **kwargs):
        super().__init__(username, *args, **kwargs)
        self.imap = IMAPClient(host, port, use_uid=True, ssl=False)
        res = self.imap.login(username, password)
        self.cursor.execute("SELECT lowModSeq,highModSeq,highModSeqMailbox,highModSeqThread,highModSeqEmail FROM account LIMIT 1")
        row = self.cursor.fetchone()
        self.lastfoldersync = 0
        if row:
            self.lowModSeq,
            self.highModSeq,
            self.highModSeqMailbox,
            self.highModSeqThread,
            self.highModSeqEmail = row
        else:
            self.lowModSeq = 0
            self.highModSeq = 1
            self.highModSeqMailbox = 1
            self.highModSeqThread = 1
            self.highModSeqEmail = 1

        # (imapname, readonly)
        self.selected_folder = (None, False)
        self.mailboxes = {}
        self.messages = {}
        self.sync_mailboxes()


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

    def get_messages(self, properties=(), sort={}, inMailbox=None, inMailboxOtherThan=(), id__in=None, threadId__in=None, **criteria):
        # XXX: id == threadId for now
        if id__in is None and threadId__in is not None:
            id__in = [id[1:] for id in threadId__in]
        if id__in is None:
            messages = []
        else:
            # try get everything from cache
            messages, id__in, properties = self.get_cached_messaged(properties, id__in=id__in)

        fetch_fields = {f for prop, f in FIELDS_MAP.items() if prop in properties}
        if 'RFC822' in fetch_fields:
            # remove redundand fields
            fetch_fields.discard('RFC822.HEADER')
            fetch_fields.discard('RFC822.SIZE')

        if inMailbox:
            mailbox = self.mailboxes.get(inMailbox, None)
            if not mailbox:
                raise errors.notFound(f'Mailbox {inMailbox} not found')
            mailboxes = [mailbox]
        elif inMailboxOtherThan:
            mailboxes = [m for m in self.mailboxes.values() if m['id'] not in inMailboxOtherThan]
        else:
            mailboxes = self.mailboxes.values()

        search_criteria = as_imap_search(criteria)
        sort_criteria = as_imap_sort(sort) or '' if sort else None

        mailbox_uids = {}
        if id__in is not None:
            if len(id__in) == 0:
                return messages  # no messages matches empty ids
            if not fetch_fields and not sort_criteria:
                # when we don't need anything new from IMAP, create empty messages
                # useful when requested conditions can be calculated from id (threadId)
                messages.extend(self.messages.get(id, 0) or ImapMessage(id=id) for id in id__in)
                return messages
            
            for id in id__in:
                # TODO: check uidvalidity
                try:
                    mailboxid, uidvalidity, uid = parse_message_id(id)
                except ValueError:
                    # not parsable == not found
                    continue
                uids = mailbox_uids.get(mailboxid, [])
                if not uids:
                    mailbox_uids[mailboxid] = uids
                uids.append(uid)
            # filter out unnecessary mailboxes
            mailboxes = [m for m in mailboxes if m['id'] in mailbox_uids]

        for mailbox in mailboxes:
            imapname = mailbox['imapname']
            if self.selected_folder[0] != imapname:
                self.imap.select_folder(imapname, readonly=True)
                self.selected_folder = (imapname, True)

            uids = mailbox_uids.get(mailbox['id'], None)
            # uids are now None or not empty
            # fetch all
            if sort_criteria:
                if uids:
                    search = f'{",".join(map(str, uids))} {search_criteria}'
                else:
                    search = search_criteria or 'ALL'
                uids = self.imap.sort(sort_criteria, search)
            elif search_criteria:
                if uids:
                    search = f'{",".join(map(str, uids))} {search_criteria}'
                uids = self.imap.search(search)
            if uids is None:
                uids = '1:*'
            fetch_fields.add('UID')
            fetches = self.imap.fetch(uids, fetch_fields)

            for uid, data in fetches.items():
                id = format_message_id(mailbox['id'], mailbox['uidvalidity'], uid)
                msg = self.messages.get(id, None)
                if not msg:
                    msg = ImapMessage(id=id, mailboxIds=[mailbox['id']])
                    self.messages[id] = msg
                for k, v in data.items():
                    msg[k.decode()] = v
                messages.append(msg)
        return messages

    def get_raw_message(self, msgid, part=None):
        self.cursor.execute('SELECT imapname,uidvalidity,uid FROM ifolders JOIN imessages USING (ifolderid) WHERE msgid=?', [msgid])
        imapname, uidvalidity, uid = self.cursor.fetchone()
        if not imapname:
            return None
        typ = 'message/rfc822'
        if part:
            parsed = self.fill_messages([msgid])
            typ = find_type(parsed[msgid], part)


        res = self.imap.getpart(imapname, uidvalidity, uid, part)
        return typ, res['data']


    def get_mailboxes(self, fields=None, **criteria):
        self.sync_mailboxes()
        return self.mailboxes.values()

    def sync_mailboxes(self):
        byimapname = {}
        # TODO: LIST "" % RETURN (STATUS (MESSAGES UIDVALIDITY HIGHESTMODSEQ X-GUID))
        for flags, sep, imapname in self.imap.list_folders():
            if b'\\Noselect' in flags:
                continue
            status = self.imap.folder_status(imapname, (['MESSAGES', 'UIDVALIDITY', 'UIDNEXT', 'HIGHESTMODSEQ', 'X-GUID']))
            byimapname[imapname] = ImapMailbox(
                id=status[b'X-GUID'].decode(),
                totalEmails=status[b'MESSAGES'],
                imapname=imapname,
                sep=sep.decode(),
                flags=flags,
                uidvalidity=status[b'UIDVALIDITY'],
                uidnext=status[b'UIDNEXT'],
                highestmodseq=status[b'HIGHESTMODSEQ'],
                byimapname=byimapname
            )
        # update cache
        self.mailboxes = {mbox['id']: mbox for mbox in byimapname.values()}
    

    def mailbox_imapname(self, parentId, name):
        parent = self.mailboxes.get(parentId, None)
        if not parent:
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
        if not mailbox:
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
            # TODO: update in persistent storage
            mailbox['sortOrder'] = sortOrder
        self.sync_mailboxes()


    def destroy_mailbox(self, id):
        mailbox = self.mailboxes.get(id, None)
        if not mailbox:
            raise errors.notFound('mailbox not found')
        res = self.imap.delete_folder(mailbox['imapname'])
        if b'NO' in res or b'BAD' in res:
            raise errors.serverFail(res.encode())
        self.sync_mailboxes()


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


SEARCH_MAP = {
    'blobId': 'X-GUID',
    'minSize': 'NOT SMALLER',
    'maxSize': 'NOT LARGER',
    'hasKeyword': 'KEYWORD',
    'notKeyword': 'UNKEYWORD',
    'allInThreadHaveKeyword': 'NOT INTHREAD UNKEYWORD',
    'someInThreadHaveKeyword': 'INTHREAD KEYWORD',
    'noneInThreadHaveKeyword': 'NOT INTHREAD KEYWORD',
    'before': 'BEFORE',  # TODO: consider time, not only date
    'after': 'AFTER',
    'subject': 'SUBJECT',
    'text': 'TEXT',
    'body': 'BODY',
    'from': 'FROM',
    'to': 'TO',
    'cc': 'CC',
    'bcc': 'BCC',
}

def as_imap_search(criteria):
    operator = criteria.get('operator', None)
    if operator:
        conds = criteria['conds']
        if operator == 'NOT' or operator == 'OR':
            if len(conds) > 0:
                out = []
                if operator == 'NOT':
                    out.append('NOT')
                for cond in conds:
                    out.append('OR')
                    out.append(as_imap_search(cond))
                # OR needs 2 args, we can delete last OR
                del out[-3]
                return ' '.join(out)
            else:
                raise errors.unsupportedFilter(f"Empty filter conditions")
        elif operator == 'AND':
            return ' '.join([as_imap_search(c) for c in conds])
        raise errors.unsupportedFilter(f"Invalid operator {operator}")

    out = []

    if 'hasAttachment' in criteria:
        if not criteria['hasAttachment']:
            out.append('NOT')
        # needs Dovecot flag attachments on save
        out.append('KEYWORD $HasAttachment')
        # or out.append('MIMEPART (DISPOSITION TYPE attachment)')

    if 'header' in criteria:
        out.append('HEADER')
        criteria = dict(criteria)  # make a copy
        out.extend(criteria.pop('header'))

    for crit, value in criteria.items():
        try:
            out.append(SEARCH_MAP[crit])
            out.append(value)
        except KeyError:
            raise UserWarning(f'Filter {crit} not supported')

    return ' '.join(out)


def find_type(message, part):
    if message.get('id', '') == part:
        return message['type']
    
    for sub in message['attachments']:
        typ = find_type(sub, part)
        if type:
            return type
    return None
