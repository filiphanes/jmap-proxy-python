import asyncio
from datetime import datetime
import re

from jmap import errors
from .aioimaplib import IMAP4, parse_list_status, parse_esearch, parse_status, parse_fetch, iter_messageset, \
    AioImapException, encode_messageset
from .imap_utf7 import imap_utf7_encode, imap_utf7_decode
from .message import ImapMessage, EmailState, FIELDS_MAP, format_message_id, parse_message_id, keyword2flag
from .mailbox import ImapMailbox


class ImapDB:
    """
    Class with DB interface inspired by Django ORM.
    It should handle only data access manipulation, not any JMAP specific syntax or rules.
    It should be replacable with SQL/CQL database class with same interface.
    get_messages()
    get_mailboxes()
    ...
    raises JMAP errors
    """
    @classmethod
    async def init(cls, username, password='h', host='localhost', port=143, loop=None):
        self = cls()
        self.username = username
        self.password = password
        self._mailbox_state = now_state()
        self._mailbox_state_low = self._mailbox_state
        self.mailboxes = {}
        self.byimapname = {}
        self.messages = {}
        self.loop = loop or asyncio.get_running_loop()
        self.imap = IMAP4(host, port, loop=self.loop, timeout=600)
        await self.imap.wait_hello_from_server()
        await self.imap.login(self.username, self.password)
        await self.imap.enable("UTF8=ACCEPT"),
        await self.imap.enable("QRESYNC"),
        await self.imap.select('virtual/All'),
        await self.sync_mailboxes(),
        return self

    async def email_state(self):
        "Return current Mailbox state"
        ok, lines = await self.imap.status('virtual/All', '(UIDNEXT HIGHESTMODSEQ)')
        status = parse_status(lines)
        return str(EmailState(int(status['UIDNEXT']), int(status['HIGHESTMODSEQ'])))

    async def email_state_low(self):
        return '1'

    async def mailbox_state(self):
        "Return current Mailbox state"
        await self.sync_mailboxes()
        return self._mailbox_state

    async def mailbox_state_low(self):
        return self._mailbox_state_low

    async def thread_state(self):
        "Return current Thread state"
        await self.email_state()

    async def thread_state_low(self):
        await self.email_state_low()


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


    async def get_messages(self, properties=(), sort={}, id__in=None, threadId__in=None, updated__gt=None, **criteria):
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
        if 'RFC822' in fetch_fields:
            # remove redundand fields
            fetch_fields.discard('RFC822.HEADER')
            fetch_fields.discard('RFC822.SIZE')

        sort_criteria = as_imap_sort(sort) if sort else None

        if id__in is not None:
            if len(id__in) == 0:
                return messages  # no messages matches empty ids
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
                search_criteria += b' UID ' + encode_messageset(uids)
            ok, lines = await self.imap.uid_sort(sort_criteria.decode(), search_criteria.decode(), ret='ALL')
            uids = parse_esearch(lines)['ALL']
        elif search_criteria:
            if uids:
                search_criteria += b' UID ' + encode_messageset(uids)
            ok, lines = await self.imap.uid_search(search_criteria.decode(), ret='ALL')
            uids = parse_esearch(lines)['ALL']

        # optimization: don't fetch X-MAILBOX when we filter by mailbox
        inMailbox = criteria.get('inMailbox', None)
        if inMailbox:
            mailboxIds = [inMailbox]
        else:
            fetch_fields.add('X-MAILBOX')

        if updated__gt:
            state = EmailState.from_str(updated__gt)
            modifiers = '(CHANGEDSINCE %s VANISHED)' % state.modseq
            if uids:
                uids = [u for u in uids if u >= state.uid]
            elif uids is None:
                uids = '%d:*' % state.uid
                fetch_fields.add('UID')
        else:
            if uids is None:
                uids = '1:*'
                fetch_fields.add('UID')
            modifiers = None

        if fetch_fields:
            fetch_fields.add('UID')
            ok, lines = await self.imap.uid_fetch(uids, f"({' '.join(fetch_fields)})", modifiers)
            if modifiers and lines[0].startswith('(EARLIER) '):
                for uid in iter_messageset(lines[0][10:]):
                    id = format_message_id(uid)
                    messages.append(ImapMessage(id=id, deleted=True))
                lines = lines[1:]
            for seq, data in parse_fetch(lines[:-1]):
                id = format_message_id(data['UID'])
                msg = self.messages.get(id, None)
                if not msg:
                    if not inMailbox:
                        mailboxIds = [self.byimapname[data['X-MAILBOX']]['id']]
                    msg = ImapMessage(id=id, mailboxIds=mailboxIds)
                    self.messages[id] = msg
                msg.update(data)
                messages.append(msg)
        else:
            # messages only from uids
            for uid in iter_messageset(uids):
                id = format_message_id(uid)
                msg = self.messages.get(id, None)
                if not msg:
                    msg = ImapMessage(id=id, mailboxIds=mailboxIds)
                    self.messages[id] = msg
                messages.append(msg)

        return messages


    async def create_message(self, mailboxIds=None, **data):
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
        res = await self.imap.append(mailbox['imapname'], msg['RFC822'], flags=msg['flags'])
        # TODO parse res, get uid, fetch x-guid, return id
        match = re.search(r'\[APPENDUID=(\d+)\]', res)
        return format_message_id(int(match.group(1)))


    async def update_message(self, id, ifInState=None, **update):
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

        # uid = parse_message_id(id) # following lines uses directly id
        if flags_add:
            await self.imap.uid_store(id, '+FLAGS', f"({' '.join(flags_add)})")
        if flags_del:
            await self.imap.uid_store(id, '-FLAGS', f"({' '.join(flags_del)})")
        
        if mids_add or mids_del:
            if flags_add or flags_del:
                error = errors.serverPartialFail
            else:
                error = errors.tooManyMailboxes
            raise error("This implementation don't support Email/set mailboxIds, use Email/copy")


    async def destroy_message(self, id):
        # uid = parse_message_id(id) # following lines uses directly id
        try:
            await self.imap.uid_store(id, '+FLAGS', '(\\Deleted)')
            # remove from cache
            self.messages.pop(id, None)
            # if you want to remove this expunge,
            # then you need to work with NOT DELETED messages elsewhere
            await self.imap.uid_expunge(id)
        except AioImapException:
            raise errors.notFound()


    async def get_mailboxes(self, fields=None, updated__gt=None, deleted=None, **criteria):
        await self.sync_mailboxes(fields)
        mailboxes = self.mailboxes.values()

        if deleted is not None:
            if deleted:
                mailboxes = [m for m in mailboxes if m['deleted']]
            else:
                mailboxes = [m for m in mailboxes if not m['deleted']]

        if updated__gt is not None:
            mailboxes = [m for m in mailboxes if m['updated'] > updated__gt]

        return mailboxes


    async def sync_mailboxes(self, fields=None):
        deleted_ids = set(self.mailboxes.keys())
        if fields is None:
            fields = {'totalEmails','unreadEmails','totalThreads','unreadThreads'}
        new_state = now_state()
        ok, lines = await self.imap.list(ret='SPECIAL-USE SUBSCRIBED STATUS (MESSAGES X-GUID)')
        for flags, sep, imapname, status in parse_list_status(lines):
            flags = set(f.lower() for f in flags)
            if '\\noselect' in flags:
                continue
            id = status['X-GUID']
            mailbox = self.mailboxes.get(id, None)
            if mailbox:
                deleted_ids.remove(id)
            else:
                mailbox = ImapMailbox(id=id, imapname=None, sep=None, flags=None)
                mailbox.db = self
                self.byimapname[imapname] = mailbox
                self.mailboxes[id] = mailbox
            data = {
                'totalEmails': int(status['MESSAGES']),
                'imapname': imapname,
                'sep': sep,
                'flags': flags,
            }
            if 'unreadEmails' in fields:
                # TODO: run all esearches concurrently
                ok, lines = await self.imap.uid_search('UNSEEN UNDRAFT X-MAILBOX %s' % imapname, ret='COUNT')
                search = parse_esearch(lines)
                data['unreadEmails'] = int(search['COUNT'])

            # set updated state
            for key, val in data.items():
                if mailbox[key] != val:
                    if key not in {'totalEmails', 'unreadEmails', 'totalThreads', 'unreadThreads'}:
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


    async def create_mailbox(self, name=None, parentId=None, isSubscribed=True, **kwargs):
        if not name:
            raise errors.invalidProperties('name is required')
        imapname = self.mailbox_imapname(parentId, name)
        # TODO: parse returned MAILBOXID
        ok, lines = await self.imap.create(imapname)
        if ok != 'OK':
            if '[ALREADYEXISTS]' in lines[0]:
                raise errors.invalidArguments(lines[0])
            else:
                raise errors.serverFail(lines[0])

        if not isSubscribed:
            await self.imap.unsubscribe(imapname)

        await self.sync_mailboxes()
        return self.byimapname[imapname]['id']


    async def update_mailbox(self, id, name=None, parentId=None, isSubscribed=None, sortOrder=None, **update):
        mailbox = self.mailboxes.get(id, None)
        if not mailbox or mailbox['deleted']:
            raise errors.notFound('mailbox not found')
        imapname = mailbox['imapname']

        if (name is not None and name != mailbox['name']) or \
           (parentId is not None and parentId != mailbox['parentId']):
            if not name:
                raise errors.invalidProperties('name is required')
            newimapname = self.mailbox_imapname(parentId, name)
            ok, lines = await self.imap.rename(imapname, newimapname)
            if ok != 'OK':
                raise errors.serverFail(lines[0])

        if isSubscribed is not None and isSubscribed != mailbox['isSubscribed']:
            if isSubscribed:
                ok, lines = await self.imap.subscribe(imapname)
            else:
                ok, lines = await self.imap.unsubscribe(imapname)
            if ok != 'OK':
                raise errors.serverFail(lines[0])

        if sortOrder is not None and sortOrder != mailbox['sortOrder']:
            # TODO: store in persistent storage
            mailbox['sortOrder'] = sortOrder
        await self.sync_mailboxes()


    async def destroy_mailbox(self, id):
        mailbox = self.mailboxes.get(id, None)
        if not mailbox or mailbox['deleted']:
            raise errors.notFound('mailbox not found')
        ok, lines = await self.imap.delete(mailbox['imapname'])
        if ok != 'OK':
            raise errors.serverFail(lines[0])
        await self.sync_mailboxes()


    def as_imap_search(self, criteria):
        out = bytearray()
        operator = criteria.get('operator', None)
        if operator:
            conds = criteria['conds']
            if operator == 'NOT' or operator == 'OR':
                if len(conds) > 0:
                    if operator == 'NOT':
                        out += b'NOT '
                    lastcond = len(conds) - 1
                    for i, cond in enumerate(conds):
                        # OR needs 2 args, we can omit last OR
                        if i < lastcond:
                            out += b'OR '
                        out += self.as_imap_search(cond)
                        out += b' '
                    return out
                else:
                    raise errors.unsupportedFilter(f"Empty filter conditions")
            elif operator == 'AND':
                for c in conds:
                    out += self.as_imap_search(c)
                    out += b' '
                return out
            raise errors.unsupportedFilter(f"Invalid operator {operator}")

        for crit, value in criteria.items():
            search, func = SEARCH_MAP.get(crit, (None, None))
            if search:
                out += search
                out += func(value) if func else value
            elif 'deleted' == crit:
                if not value:
                    out += b'NOT '
                out += b'DELETED '
            elif 'header' == crit:
                out += b'HEADER '
                out += value[0].encode()
                out += b' '
                out += value[1].encode()
                out += b' '
            elif 'hasAttachment' == crit:
                if not value:
                    out += b'NOT '
                # needs Dovecot flag attachments on save
                out += b'KEYWORD $HasAttachment '
                # or out += b'MIMEPART (DISPOSITION TYPE attachment)')
            elif 'inMailbox' == crit:
                out += b'X-MAILBOX '
                try:
                    out += imap_utf7_encode(self.mailboxes[value]["imapname"])
                    out += b' '
                except KeyError:
                    raise errors.notFound(f"Mailbox {value} not found")
            elif 'inMailboxOtherThan' == crit:
                try:
                    for id in value:
                        out += b'NOT X-MAILBOX '
                        out += imap_utf7_encode(self.mailboxes[id]["imapname"])
                        out += b' '
                except KeyError:
                    raise errors.notFound(f"Mailbox {value} not found")
            else:
                raise UserWarning(f'Filter {crit} not supported')
        if out:
            out.pop()
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
    'receivedAt': b'ARRIVAL',
    'sentAt':     b'DATE',
    'subject':    b'SUBJECT',
    'size':       b'SIZE',
    'from':       b'FROM',
    'to':         b'TO',
    'cc':         b'CC',
}

def as_imap_sort(sort):
    out = bytearray()
    for crit in sort:
        if not crit.get('isAscending', True):
            out += b'REVERSE '
        try:
            out += SORT_MAP[crit['property']]
            out += b' '
        except KeyError:
            raise errors.unsupportedSort(f"Property {crit['property']} is not sortable")
    out.pop()
    return out


def find_type(message, part):
    if message.get('id', '') == part:
        return message['type']
    
    for sub in message['attachments']:
        typ = find_type(sub, part)
        if type:
            return type
    return None


def now_state():
    return str(int(datetime.now().timestamp()))
