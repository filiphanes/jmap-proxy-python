import asyncio
import itertools
from datetime import datetime
import re

from jmap import errors
from jmap.core import MAX_OBJECTS_IN_GET
from jmap.parse import asAddresses, asDate, asGroupedAddresses, asMessageIds, asRaw, asText, asURLs, htmltotext
from .aioimaplib import IMAP4, parse_list_status, parse_esearch, parse_status, parse_fetch, iter_messageset, \
    encode_messageset, parse_thread, unquoted, quoted
from .email import ImapEmail, EmailState, format_email_id, parse_email_id, keyword2flag
from .mailbox import ImapMailbox

ALL_MAILBOX_PROPERTIES = {
    'id', 'name', 'parentId', 'role', 'sortOrder', 'isSubscribed',
    'totalEmails', 'unreadEmails', 'totalThreads', 'unreadThreads',
    'myRights',
}

ALL_PROPERTIES = {
    'id', 'blobId', 'threadId', 'mailboxIds',
    'hasAttachemnt', 'keywords', 'subject',
    'sentAt', 'receivedAt', 'size',
    'from', 'to', 'cc', 'bcc', 'replyTo',
    'messageId', 'inReplyTo', 'references', 'sender',
    'attachments', 'hasAttachment', 'headers', 'preview',
    'textBody', 'htmlBody', 'bodyValues', 'references',
    # 'body'
}
ALL_BODY_PROPERTIES = {
    "partId", "blobId", "size", "name", "type",
    "charset", "disposition", "cid", "language", "location",
}

FIELDS_MAP = {
    'id':           'UID',
    'blobId':       'EMAILID',  # OBJECTID imap extension
    'threadId':     'EMAILID',
    'mailboxIds':   'X-MAILBOX',  # Dovecot
    'hasAttachment': 'FLAGS',
    # 'hasAttachment':'RFC822',  # when server don't set $HasAttachment flag
    'keywords':     'FLAGS',
    'preview':      'PREVIEW',
    'receivedAt':   'INTERNALDATE',
    'size':         'RFC822.SIZE',
    'attachments':  'RFC822',
    'bodyStructure':'RFC822',
    'bodyValues':   'RFC822',
    'textBody':     'RFC822',
    'htmlBody':     'RFC822',
    'messageId':    'RFC822.HEADER',
    'headers':      'RFC822.HEADER',
    'sender':       'RFC822.HEADER',
    'subject':      'RFC822.HEADER',
    'from':         'RFC822.HEADER',
    'to':           'RFC822.HEADER',
    'cc':           'RFC822.HEADER',
    'bcc':          'RFC822.HEADER',
    'replyTo':      'RFC822.HEADER',
    'inReplyTo':    'RFC822.HEADER',
    'sentAt':       'RFC822.HEADER',
    'references':   'RFC822.HEADER',
    'created':      'UID',
    'updated':      'MODSEQ',
    'deleted':      'MODSEQ',
}


header_prop_re = re.compile(r'^header:([^:]+)(?::as(\w+))?(:all)?')

HEADER_FORMS = {
    None: asRaw,
    'Raw': asRaw,
    'Date': asDate,
    'Text': asText,
    'URLs': asURLs,
    'Addresses': asAddresses,
    'GroupedAddresses': asGroupedAddresses,
    'MessageIds': asMessageIds,
}

class ImapAccount:
    """
    JMAP Account which uses IMAP as backend
    """

    def __init__(self, username, password='h', host='localhost', port=143, loop=None):
        self.id = username
        self.name = username
        self.capabilities = {
            "urn:ietf:params:jmap:vacationresponse": {},
            "urn:ietf:params:jmap:submission": {
                "submissionExtensions": [],
                "maxDelayedSend": 44236800  # 512 days
            },
            "urn:ietf:params:jmap:mail": {
                "maxSizeMailboxName": 490,
                "maxSizeAttachmentsPerEmail": 50000000,
                "mayCreateTopLevelMailbox": True,
                "maxMailboxesPerEmail": 1,  # IMAP implementation
                "maxMailboxDepth": None,
                "emailQuerySortOptions": [
                    "receivedAt",
                    # "from",
                    # "to",
                    "subject",
                    "size",
                    # "header.x-spam-score"
                ]
            }
        }
        self.username = username
        self.password = password
        self._mailbox_state = now_state()
        self._mailbox_state_low = self._mailbox_state
        self.mailboxes = {}
        self.byimapname = {}
        self.emails = {}
        self.blobs = {}
        self.loop = loop or asyncio.get_running_loop()
        self.imap = IMAP4(host, port, loop=self.loop, timeout=600)

    async def ainit(self):
        """Asynchronously connects to imap class"""
        await self.imap.wait_hello_from_server()
        await self.imap.login(self.username, self.password)
        await self.imap.enable("UTF8=ACCEPT")
        await self.imap.enable("QRESYNC")
        await self.imap.select('virtual/All')
        await self.sync_mailboxes()

    async def mailbox_get(self, idmap, ids=None, properties=None):
        """
        https://jmap.io/spec-mail.html#mailboxget
        https://jmap.io/spec-core.html#get
        """
        if properties is None:
            properties = ALL_MAILBOX_PROPERTIES
        else:
            properties = set(properties)
            if properties - ALL_MAILBOX_PROPERTIES:
                raise errors.invalidArguments('Invalid argument requested')
            properties.add('id')
        await self.sync_mailboxes(properties)

        notFound = []
        if ids:
            lst = []
            for id in (idmap.get(i) for i in ids):
                mbox = self.mailboxes.get(id, None)
                if mbox and not mbox['deleted']:
                    lst.append({k: mbox[k] for k in properties})
                else:
                    notFound.append(id)
        else:
            lst = [{k: mbox[k] for k in properties}
                   for mbox in self.mailboxes.values()
                   if not mbox['deleted']]

        return {
            'accountId': self.id,
            'state': await self.mailbox_state(),
            'list': lst,
            'notFound': notFound,
        }

    async def mailbox_set(self, idmap, ifInState=None, create=None, update=None, destroy=None,
                          onDestroyRemoveEmails=False):
        """
        https://jmap.io/spec-mail.html#mailboxset
        https://jmap.io/spec-core.html#set
        """
        oldState = await self.mailbox_state()
        if ifInState is not None and ifInState != oldState:
            raise errors.stateMismatch()

        # CREATE
        created = {}
        notCreated = {}
        created_imapnames = {}
        for cid, mailbox in (create or {}).items():
            try:
                mbox = ImapMailbox(mailbox)
                mbox.db = self
                imapname = mbox['imapname']
                ok, lines = await self.imap.create(imapname)
                if ok != 'OK':
                    if '[ALREADYEXISTS]' in lines[0]:
                        raise errors.invalidArguments(lines[0])
                    else:
                        raise errors.serverFail(lines[0])
                match = re.search(r'\[MAILBOXID \(([^)]+)\)\]', lines[0])
                if match:
                    id = match.group(1)
                    mbox[id] = id
                    mbox['sep'] = '/'
                    mbox['flags'] = set()
                    self.mailboxes[id] = mbox
                    self.byimapname[imapname] = mbox
                    created[cid] = {'id': id}
                else:
                    # set created[cid] after sync_mailboxes()
                    created_imapnames[cid] = imapname
                if not mailbox.get('isSubscribed', True):
                    ok, lines = await self.imap.unsubscribe(imapname)
                    # TODO: handle failed unsubscribe
            except KeyError:
                notCreated[cid] = errors.invalidArguments().to_dict()
            except errors.JmapError as e:
                notCreated[cid] = e.to_dict()

        # UPDATE
        updated = {}
        notUpdated = {}
        for id, update in (update or {}).items():
            try:
                mailbox = self.mailboxes.get(id, None)
                if not mailbox or mailbox['deleted']:
                    raise errors.notFound(f'Mailbox {id} not found')
                await self.update_mailbox(mailbox, update)
                updated[id] = update
            except errors.JmapError as e:
                notUpdated[id] = e.to_dict()

        # DESTROY
        destroyed = []
        notDestroyed = {}
        for id in destroy or ():
            try:
                mailbox = self.mailboxes.get(id, None)
                if not mailbox or mailbox['deleted']:
                    raise errors.notFound('mailbox not found')
                ok, lines = await self.imap.delete(mailbox['imapname'])
                if ok != 'OK':
                    raise errors.serverFail(lines[0])
                mailbox['deleted'] = True
                destroyed.append(id)
            except errors.JmapError as e:
                notDestroyed[id] = e.to_dict()

        await self.sync_mailboxes()
        for cid, imapname in created_imapnames.values():
            mbox = self.byimapname.get(imapname, None)
            if mbox:
                created[cid] = {'id': mbox['id']}
                idmap.set(cid, mbox['id'])
            else:
                notCreated[cid] = errors.serverFail().to_dict()

        return {
            'accountId': self.id,
            'oldState': oldState,
            'newState': await self.mailbox_state(),
            'created': created,
            'notCreated': notCreated,
            'updated': updated,
            'notUpdated': notUpdated,
            'destroyed': destroyed,
            'notDestroyed': notDestroyed,
        }

    async def mailbox_query(self, sort=None, filter=None, position=0, anchor=None,
                            anchorOffset=0, limit=None):
        """
        https://jmap.io/spec-mail.html#mailboxquery
        https://jmap.io/spec-core.html#get
        """
        mailboxes = (mbox for mbox in self.mailboxes if not mbox['deleted'])
        if filter:
            mailboxes = [mbox for mbox in mailboxes if _mailbox_match(mbox, filter)]
        if sort:
            mailboxes = _mailbox_sort(mailboxes, sort, {'data': mailboxes})

        start = position
        if anchor:
            # need to calculate the position
            for i, x in enumerate(mailboxes):
                if x['id'] == anchor:
                    start = i + anchorOffset
                    break
            else:
                raise errors.anchorNotFound()

        if limit:
            end = start + limit - 1
        else:
            end = len(mailboxes)

        return {
            'accountId': self.id,
            'filter': filter,
            'sort': sort,
            'queryState': await self.mailbox_state(),
            'canCalculateChanges': False,
            'position': start,
            'total': len(mailboxes),
            'ids': [x['id'] for x in mailboxes[start:end]],
        }

    async def mailbox_changes(self, sinceState, maxChanges=None):
        """
        https://jmap.io/spec-mail.html#mailboxchanges
        https://jmap.io/spec-core.html#changes
        """
        new_state = await self.mailbox_state()
        if sinceState <= await self.mailbox_state_low():
            raise errors.cannotCalculateChanges({'new_state': new_state})

        removed = []
        created = []
        updated = []
        only_counts = True
        for mbox in self.mailboxes:
            if mbox['updated'] > sinceState:
                if mbox['deleted']:
                    # don't append created and deleted
                    if mbox['created'] <= sinceState:
                        removed.append(mbox['id'])
                elif mbox['created'] > sinceState:
                    created.append(mbox['id'])
                else:
                    if mbox['updatedNonCounts'] > sinceState:
                        only_counts = False
                    updated.append(mbox['id'])

        if len(removed) + len(created) + len(updated) > maxChanges:
            raise errors.cannotCalculateChanges({'new_state': new_state})

        return {
            'accountId': self.id,
            'oldState': sinceState,
            'newState': new_state,
            'hasMoreChanges': False,
            'created': created,
            'updated': updated,
            'removed': removed,
            'changedProperties': ["totalEmails", "unreadEmails", "totalThreads",
                                  "unreadThreads"] if only_counts else None,
        }

    async def email_query(self, sort={}, filter={},
                          position=None, anchor=None, anchorOffset=None, limit: int = 10000,
                          collapseThreads=False, calculateTotal=False):
        start = position or 0
        if anchor:
            if position is not None:
                raise errors.invalidArguments("anchor and position can't ")
        elif anchorOffset is not None:
            raise errors.invalidArguments("anchorOffset need anchor")

        sort_criteria = as_imap_sort(sort) if sort else None
        search_criteria = self.as_imap_search(filter)
        if collapseThreads:
            ok, lines = await self.imap.uid_thread('REFS', search_criteria.decode())
            threads = parse_thread(lines[:1])
            # TODO flatten threads
            if threads:
                search_criteria += b' UID %s' % encode_messageset((int(t[0]) for t in threads))
        if sort_criteria:
            ok, lines = await self.imap.uid_sort(sort_criteria.decode(), search_criteria.decode(), ret='ALL')
        elif search_criteria:
            ok, lines = await self.imap.uid_search(search_criteria.decode(), ret='ALL COUNT')
        uids = parse_esearch(lines).get('ALL', '')

        ids = [format_email_id(uid) for uid in iter_messageset(uids)]
        if anchor:
            # need to calculate position
            try:
                start = ids.index(anchor) + (anchorOffset or 0)
                if start < 0:
                    start = 0
            except ValueError:
                raise errors.anchorNotFound()

        end = start + limit
        if start < 0 <= end:
            end = len(ids)

        return {
            'accountId': self.id,
            'filter': filter,
            'sort': sort,
            'collapseThreads': collapseThreads,
            'queryState': await self.email_state(),
            'canCalculateChanges': False,
            'position': start,
            'ids': ids[start:end],
            'total': len(ids),
        }

    async def email_get(self, idmap,
                        ids=None,
                        properties=None,
                        bodyProperties=None,
                        fetchTextBodyValues=False,
                        fetchHTMLBodyValues=False,
                        fetchAllBodyValues=False,
                        maxBodyValueBytes=0,
                        _prefetch=(),
                    ):
        """
        https://jmap.io/spec-mail.html#emailget
        https://jmap.io/spec-core.html#get
        """
        lst = []
        notFound = []
        simple_props = set()
        header_props = set()
        if properties:
            for prop in properties:
                m = header_prop_re.match(prop)
                if m:
                    header_props.add(m.group(0, 1, 2, 3))
                    simple_props.add('headers')
                else:
                    simple_props.add(prop)
            for prop in _prefetch:
                if header_prop_re.match(prop):
                    simple_props.add('headers')
                else:
                    simple_props.add(prop)
            if 'body' in simple_props:
                simple_props.remove('body')
                simple_props.add('textBody')
                simple_props.add('htmlBody')
        else:
            properties = ALL_PROPERTIES

        if bodyProperties is None:
            bodyProperties = ALL_BODY_PROPERTIES

        if header_props and 'headers' not in properties:
            simple_props.remove('headers')

        if ids is None:
            # get MAX_OBJECTS_IN_GET
            ok, lines = await self.imap.search('ALL', ret='ALL')
            messageset = parse_esearch(lines).get('ALL', '')
            ids = map(format_email_id, iter_messageset(messageset))
            ids = tuple(itertools.islice(ids, MAX_OBJECTS_IN_GET))
        elif len(ids) > MAX_OBJECTS_IN_GET:
            raise errors.tooLarge('Requested more than {MAX_OBJECTS_IN_GET} ids')

        await self.fill_emails(simple_props, ids)

        for id in ids:
            try:
                msg = self.emails[id]
            except KeyError:
                notFound.append(id)
                continue

            # Fill most of msg properties except header:*
            data = {prop: msg[prop] for prop in simple_props}
            data['id'] = msg['id']
            if 'textBody' in msg and 'htmlBody' not in msg and not msg['textBody']:
                data['textBody'] = htmltotext(msg['htmlBody'])
            if 'bodyValues' in properties:
                # if fetchHTMLBodyValues:
                #     data['bodyValues'] = {k: v for k, v in msg['bodyValues'].items() if v['type'] == 'text/html'}
                # elif fetchTextBodyValues:
                #     data['bodyValues'] = {k: v for k, v in msg['bodyValues'].items() if v['type'] == 'text/plain'}
                # elif fetchAllBodyValues:
                #     data['bodyValues'] = msg['bodyValues']
                # jmap-demo-webmail needs all bodyValues even when fetchHTMLBodyValues=True
                data['bodyValues'] = msg['bodyValues']
                if maxBodyValueBytes:
                    for k, bodyValue in data['bodyValues'].items():
                        if len(bodyValue['value']) > maxBodyValueBytes:
                            bodyValue = {k: v for k, v in bodyValue.items()}
                            bodyValue['value'] = bodyValue['value'][:maxBodyValueBytes]
                            bodyValue['isTruncated'] = True,
                            data['bodyValues'][k] = bodyValue

            for prop, name, form, getall in header_props:
                try:
                    func = HEADER_FORMS[form]
                except KeyError:
                    raise errors.invalidProperties(f'Unknown header-form {form} in {prop}')

                name = name.lower()
                if getall:
                    data[prop] = [func(h['value'])
                                  for h in msg['headers'] if h['name'].lower() == name]
                else:
                    data[prop] = func(msg.get_header(name))

            lst.append(data)

        return {
            'accountId': self.id,
            'list': lst,
            'state': await self.email_state(),
            'notFound': list(notFound),
        }

    async def email_set(self, idmap, ifInState=None, create={}, update={}, destroy=()):
        oldState = await self.email_state()
        if ifInState is not None and ifInState != oldState:
            raise errors.stateMismatch()

        # CREATE
        created = {}
        notCreated = {}
        if create:
            for cid, data in create.items():
                try:
                    try:
                        mailboxid, = data['mailboxIds']
                    except KeyError:
                        raise errors.invalidArguments('mailboxIds is required when creating email')
                    except ValueError:
                        raise errors.tooManyMailboxes('Only 1 mailbox allowed in this implementation')
                    mailbox = self.mailboxes.get(mailboxid, None)
                    if not mailbox or mailbox['deleted']:
                        raise errors.notFound(f"Mailbox {mailboxid} not found")
                    msg = ImapEmail(**data)
                    body = msg['RFC822']
                    flags = "(%s)" % (''.join(msg['FLAGS']))
                    ok, lines = await self.imap.append(body, mailbox['imapname'], flags)
                    match = re.search(r'\[APPENDUID (\d+) (\d+)\]', lines[0])
                    # TODO: FETCH UID and EMAILID
                    id = format_email_id(int(match.group(2)))
                    created[cid] = {'id': id, 'blobId': msg['blobId']}
                    idmap.set(cid, id)
                except errors.JmapError as e:
                    notCreated[cid] = e.to_dict()

        # UPDATE
        updated = {}
        notUpdated = {}
        if update:
            ids = [idmap.get(id) for id in update.keys()]
            await self.fill_emails(('keywords', 'mailboxIds'), ids)
            for id in ids:
                try:
                    msg = self.emails[id]
                except KeyError:
                    notUpdated[id] = errors.notFound().to_dict()
                    continue
                try:
                    await self.update_email(msg, update[id], ifInState)
                    updated[id] = update[id]
                except errors.JmapError as e:
                    notUpdated[id] = e.to_dict()

        # DESTROY
        destroyed = []
        notDestroyed = {}
        if destroy:
            uids = []
            ids = [idmap.get(id) for id in update.keys()]
            for id in ids:
                try:
                    uids.append(parse_email_id(ids))
                except ValueError:
                    notDestroyed[id] = errors.notFound().to_dict()

            messageset = encode_messageset(uids).encode()
            await self.imap.uid_store(messageset, '+FLAGS', '(\\Deleted)')
            await self.imap.uid_expunge(messageset)
            for id in ids:
                self.emails.pop(id, None)
                destroyed.append(id)
                # TODO: notDestroyed[id] = errors.notFound().to_dict()

        return {
            'accountId': self.id,
            'oldState': oldState,
            'newState': await self.email_state(),
            'created': created,
            'notCreated': notCreated,
            'updated': updated,
            'notUpdated': notUpdated,
            'destroyed': destroyed,
            'notDestroyed': notDestroyed,
        }

    async def email_changes(self, sinceState, maxChanges=None):
        newState = await self.email_state()

        if sinceState <= await self.email_state_low():
            raise errors.cannotCalculateChanges({'new_state': newState})

        state = EmailState.from_str(sinceState)
        ok, lines = await self.imap.uid_fetch(
            '%d:*' % state.uid,
            "(UID)",
            '(CHANGEDSINCE %s VANISHED)' % state.modseq
        )
        if lines[0].startswith('(EARLIER) '):
            removed = [format_email_id(uid)
                       for uid in iter_messageset(lines[0][10:])]
            lines = lines[1:]
        else:
            removed = []

        created = []
        updated = []
        for seq, data in parse_fetch(lines[:-1]):
            uid = int(data['UID'])
            if uid > state.uid:
                created.append(format_email_id(uid))
            else:
                updated.append(format_email_id(uid))

        # TODO: create intermediate state
        if maxChanges and len(removed) + len(created) + len(updated) > maxChanges:
            raise errors.cannotCalculateChanges({'new_state': newState})

        return {
            'accountId': self.id,
            'oldState': sinceState,
            'newState': newState,
            'hasMoreChanges': False,
            'created': created,
            'updated': updated,
            'removed': removed,
        }

    async def thread_get(self, idmap, ids=None):
        lst = []
        notFound = []

        if ids is None:
            ok, lines = await self.imap.uid_search('ALL', ret='ALL')
            messageset = parse_esearch(lines).get('ALL', '')
        else:
            ids = [idmap.get(id) for id in ids]
            search = self.as_imap_search({'threadIds': ids}).decode()
            ok, lines = await self.imap.uid_search(search, ret='ALL')
            messageset = parse_esearch(lines).get('ALL', '')
        if messageset:
            # ok, lines = await self.imap.uid_thread('REFS', 'UID %s' % messageset)
            # threads = parse_thread(lines)
            # await self.fill_emails(['blobId'], [t[0] for t in threads])
            ids = [format_email_id(uid) for uid in iter_messageset(messageset)]
            await self.fill_emails(['blobId'], ids)
            for id in ids:
                try:
                    msg = self.emails[id]
                except KeyError:
                    continue
                lst.append({'id': msg['blobId'], 'emailIds': [id]})

        return {
            'accountId': self.id,
            'list': lst,
            'state': await self.thread_state(),
            'notFound': notFound,
        }

    async def download(self, blobId):
        search = self.as_imap_search({'blobId': blobId})
        ok, lines = self.imap.uid_search(search, ret='ALL')
        messageset = parse_esearch(lines)
        for uid in messageset:
            ok, lines = self.imap.uid_fetch(str(uid), '(RFC822)')
            for seq, data in parse_fetch(lines[:-1]):
                return data['RFC822']
        raise errors.notFound(f"Blob {blobId} not found")


    async def email_import(self, ifInState=None, emails=()):
        oldState = await self.thread_state()
        if ifInState and ifInState != oldState:
            raise errors.stateMismatch({'newState': oldState})

        created = {}
        notCreated = {}
        for id, email in emails.items():
            try:
                blobId = email.get('blobId', None)
                if not blobId:
                    raise errors.invalidArguments()
                body = await self.download(blobId)
                mailboxIds = email.get('mailboxIds', None)
                if not mailboxIds:
                    raise errors.invalidArguments('mailboxIds are required')
                elif len(mailboxIds) > 1:
                    raise errors.invalidArguments('Max 1 mailboxIds allowed')
                try:
                    imapname = self.mailboxes[mailboxIds[0]]['imapname']
                except KeyError:
                    raise errors.notFound(f"mailboxId {mailboxIds[0]} not found")
                flags = "(%s)" % \
                        (' '.join(keyword2flag(kw) for kw in email['keywords']))
                if 'receivedAt' in email:
                    date = email['receivedAt']
                else:
                    # TODO: most recent Received header
                    date = datetime.now()
                ok, lines = self.imap.append(body, imapname, flags, date)
                match = re.search(r'\[APPENDUID (\d+) (\d+)\]', lines[0])
                created[id] = format_email_id(int(match.group(2)))
            except errors.JmapError as e:
                notCreated[id] = e.to_dict()
            except Exception as e:
                notCreated[id] = errors.serverPartialFail(str(e))

        return {
            'accountId': self.id,
            'oldState': oldState,
            'newState': await self.email_state(),
            'created': created,
            'notCreated': notCreated,
        }


    async def thread_changes(self, sinceState, maxChanges=None):
        # TODO: threadIds
        return await self.email_changes(sinceState, maxChanges)

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

    async def fill_emails(self, properties=(), ids=None):
        """Fills self.emails with required properties"""

        try:
            fields = {FIELDS_MAP[prop] for prop in properties}
        except KeyError as e:
            raise errors.invalidArguments(f'Property not recognized: {e}')
        if 'RFC822' in fields:
            # remove redundand fields
            fields.discard('RFC822.HEADER')
            fields.discard('RFC822.SIZE')

        fetch_uids = set()
        fetch_fields = set()
        for id in ids:
            try:
                uid = parse_email_id(id)
            except ValueError:
                continue
            msg = self.emails.get(id, None)
            if msg is not None:
                missing = fields - msg.keys()
                if missing:
                    fetch_uids.add(uid)
                    fetch_fields.update(missing)
            else:
                fetch_uids.add(uid)
                fetch_fields = fields

        if not fetch_fields:
            return
        fetch_fields.add('UID')
        fetch_uids = encode_messageset(fetch_uids).decode()
        ok, lines = await self.imap.uid_fetch(fetch_uids, "(%s)" % (' '.join(fetch_fields)))
        for seq, data in parse_fetch(lines[:-1]):
            id = data['UID']  # format_email_id
            msg = self.emails.get(id, None)
            if not msg:
                msg = ImapEmail(id=id)
                self.emails[id] = msg
            if 'mailboxIds' in properties:
                imapname = unquoted(data['X-MAILBOX'])
                msg['mailboxIds'] = [self.byimapname[imapname]['id']]
            msg.update(data)

    async def update_email(self, msg, update, ifInState=None):
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
                        msg['keywords'][key] = True
                    else:
                        flags_del.append(keyword2flag(key))
                        msg['keywords'].pop(key, None)
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
                    flags_add = [keyword2flag(k) for k, v in items if v]
                    flags_del = [keyword2flag(k) for k, v in items if not v]
                elif path == 'mailboxIds':
                    mids_add = [k for k, v in items if v]
                    mids_del = [k for k, v in items if not v]
                else:
                    raise errors.invalidArguments(f"Unknown update {path}")

        # uid = parse_email_id(id) # following lines uses directly id
        if flags_add:
            await self.imap.uid_store(msg['id'], '+FLAGS', f"({' '.join(flags_add)})")
        if flags_del:
            await self.imap.uid_store(msg['id'], '-FLAGS', f"({' '.join(flags_del)})")

        if mids_add or mids_del:
            if flags_add or flags_del:
                error = errors.serverPartialFail
            else:
                error = errors.tooManyMailboxes
            raise error("This implementation don't support Email/set mailboxIds, use Email/copy")


    async def sync_mailboxes(self, fields=None):
        deleted_ids = set(self.mailboxes.keys())
        if fields is None:
            fields = {'totalEmails', 'unreadEmails', 'totalThreads', 'unreadThreads'}
        new_state = now_state()
        ok, lines = await self.imap.list(ret='SPECIAL-USE SUBSCRIBED STATUS (MESSAGES MAILBOXID)')
        for flags, sep, imapname, status in parse_list_status(lines):
            imapname = unquoted(imapname)
            flags = set(f.lower() for f in flags)
            if '\\noselect' in flags:
                continue
            id = status['MAILBOXID'][1:-1]
            mailbox = self.mailboxes.get(id, None)
            if mailbox:
                deleted_ids.remove(id)
            else:
                mailbox = ImapMailbox(id=id, imapname=imapname, sep=sep, flags=flags)
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

    async def update_mailbox(self, mailbox, update):
        renamefrom = None
        subscribe = None

        for key, value in update.items():
            if value != mailbox[key]:
                if key in ('name', 'parentId'):
                    renamefrom = mailbox.pop('imapname')
                elif key == 'isSubscribed':
                    subscribe = bool(value)
                elif key == 'sortOrder':
                    # TODO: store in persistent storage
                    pass
                mailbox[key] = value

        if renamefrom:
            renameto = mailbox['imapname']
            ok, lines = await self.imap.rename(renamefrom, renameto)
            if ok != 'OK':
                raise errors.serverFail(lines[0])

        if subscribe is not None:
            if subscribe:
                ok, lines = await self.imap.subscribe(mailbox['imapname'])
            else:
                ok, lines = await self.imap.unsubscribe(mailbox['imapname'])
            if ok != 'OK':
                if renamefrom:
                    raise errors.serverPartialFail(lines[0])
                else:
                    raise errors.serverFail(lines[0])

    def as_imap_search(self, criteria):
        out = bytearray()
        if 'operator' in criteria:
            operator = criteria['operator']
            conds = criteria['conditions']
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
                    out += quoted(self.mailboxes[value]["imapname"].encode())
                    out += b' '
                except KeyError:
                    raise errors.notFound(f"Mailbox {value} not found")
            elif 'threadIds' == crit:
                if value:
                    out += b'INTHREAD REFS'
                    i = len(value)
                    for id in value:
                        if i > 1:
                            out += b' OR'
                        out += b' EMAILID '
                        out += id.encode()
                        i -= 1
                    out += b' '
            elif 'inMailboxOtherThan' == crit:
                try:
                    for id in value:
                        out += b'NOT X-MAILBOX '
                        out += quoted(self.mailboxes[id]["imapname"].encode())
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

def quoted_bytes(s):
    return quoted(bytes(s))


SEARCH_MAP = {
    'blobId': (b'EMAILID', bytes),
    'minSize': (b'NOT SMALLER', int2bytes),
    'maxSize': (b'NOT LARGER', int2bytes),
    'hasKeyword': (b'KEYWORD', keyword2flag),
    'notKeyword': (b'UNKEYWORD', keyword2flag),
    'allInThreadHaveKeyword': (b'NOT INTHREAD UNKEYWORD', keyword2flag),
    'someInThreadHaveKeyword': (b'INTHREAD KEYWORD', keyword2flag),
    'noneInThreadHaveKeyword': (b'NOT INTHREAD KEYWORD', keyword2flag),
    'before': (b'BEFORE', bytes),  # TODO: consider time, not only date
    'after': (b'AFTER', bytes),
    'subject': (b'SUBJECT', quoted_bytes),
    'text': (b'TEXT', quoted_bytes),
    'body': (b'BODY', quoted_bytes),
    'from': (b'FROM', quoted_bytes),
    'to': (b'TO', quoted_bytes),
    'cc': (b'CC', quoted_bytes),
    'bcc': (b'BCC', quoted_bytes),
}

SORT_MAP = {
    'receivedAt': b'ARRIVAL',
    'sentAt': b'DATE',
    'subject': b'SUBJECT',
    'size': b'SIZE',
    'from': b'FROM',
    'to': b'TO',
    'cc': b'CC',
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


def now_state():
    return str(int(datetime.now().timestamp()))


def _mailbox_match(mbox, filter):
    if 'hasRole' in filter and \
            bool(filter['hasRole']) != bool(mbox['role']):
        return False
    if 'isSubscribed' in filter and \
            bool(filter['isSubscribed']) != bool(mbox['isSubscribed']):
        return False
    if 'parentId' in filter and \
            filter['parentId'] != mbox['parentId']:
        return False
    return True

def _mailbox_sort(data, sortargs, storage):
    return data
