import asyncio
import itertools
from datetime import datetime
import re
from operator import itemgetter

from jmap import errors
from jmap.core import MAX_OBJECTS_IN_GET
from jmap.parse import asAddresses, asDate, asGroupedAddresses, asMessageIds, asRaw, asText, asURLs, htmltotext
from .aioimaplib import IMAP4, parse_list_status, parse_esearch, parse_status, parse_fetch, iter_messageset, \
    encode_messageset, parse_thread, unquoted, quoted, parse_metadata
from .email import ImapEmail, EmailState, keyword2flag
from .mailbox import ImapMailbox


class ImapAccount:
    """JMAP user Account using IMAP as backend"""

    def __init__(self, username, password='h', host='localhost', port=143, loop=None):
        self.capabilities = {
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
        self.id = username
        self.name = username
        self.username = username
        self.password = password

        self.mailboxes = {}
        self.byimapname = {}
        self._mailbox_state = now_state()
        self._mailbox_state_low = self._mailbox_state
        self.emails = {}
        self.blobs = {}

        self.imap = IMAP4(host, port, timeout=600, loop=loop)
        self.imapname_all = 'virtual/All'

    async def ainit(self):
        """Asynchronously connects to imap class"""
        await self.imap.wait_hello_from_server()
        await self.imap.login(self.username, self.password)
        await self.imap.enable("UTF8=ACCEPT")
        await self.imap.enable("QRESYNC")
        await self.sync_mailboxes({'imapname'})
        # find \All mailbox
        for mailbox in self.mailboxes.values():
            if '\\all' in mailbox['flags']:
                self.imapname_all = quoted(mailbox['imapname'])
                break

        ok, lines = await self.imap.select(self.imapname_all)
        if ok != 'OK':
            raise Exception(f"Mailbox {self.imapname_all} needs to be selectable.")
        for line in lines:
            match = uidvalidity_re.search(line)
            if match:
                self.uidvalidity = int(match.group(1))
                break
        else:
            raise Exception('UIDVALIDITY for virtual/All not found.')

    async def mailbox_get(self, idmap, ids=None, properties=None):
        """https://jmap.io/spec-mail.html#mailboxget"""
        if properties is None:
            properties = ALL_MAILBOX_PROPERTIES
        else:
            properties = set(properties)
            if not properties.issubset(ALL_MAILBOX_PROPERTIES):
                raise errors.invalidArguments('Invalid argument requested')
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
        """https://jmap.io/spec-mail.html#mailboxset"""
        oldState = await self.mailbox_state()
        if ifInState is not None and ifInState != oldState:
            raise errors.stateMismatch()

        # CREATE
        created = {}
        notCreated = {}
        created_imapnames = {}
        for cid, mailbox in (create or {}).items():
            mbox = ImapMailbox(mailbox)
            mbox.db = self
            try:
                imapname = mbox['imapname']
            except KeyError:
                raise errors.notFound("Parent mailbox not found")
            try:
                ok, lines = await self.imap.create(quoted(imapname))
                if ok != 'OK':
                    if '[ALREADYEXISTS]' in lines[0]:
                        raise errors.invalidArguments(lines[0])
                    else:
                        raise errors.serverFail(lines[0])
                # OBJECTID extension returns MAILBOXID on create
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
                ok, lines = await self.imap.delete(quoted(mailbox['imapname']))
                if ok != 'OK':
                    raise errors.serverFail(lines[0])
                mailbox['deleted'] = True
                destroyed.append(id)
            except errors.JmapError as e:
                notDestroyed[id] = e.to_dict()

        await self.sync_mailboxes({'id','imapname'})
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

    async def mailbox_query(self, sort=None, filter=None, position=None, anchor=None,
                            anchorOffset=0, limit=None, calculateTotal=False):
        """https://jmap.io/spec-mail.html#mailboxquery"""
        position, limit2, sort, filter = _validate_query(position, limit, sort, filter)

        mailboxes = [b for b in self.mailboxes.values() if not b['deleted']]
        total = len(mailboxes)

        for prop, value in filter.items():
            # TODO: FilterOperator AND, OR, NOT
            try:
                filter_func = MAILBOX_FILTERS[prop]
            except KeyError:
                raise errors.unsupportedFilter(f"Unknown filter {prop}")
            mailboxes = (b for b in mailboxes if filter_func(b, value))

        # python sort is stable so we can make consecutive
        # sorts in reversed order to get complex sort
        for key in reversed(sort):
            mailboxes = sorted(
                mailboxes,
                key=itemgetter(key['property']),
                reverse=not key.get('isAscending', True),
            )

        ids = [b['id'] for b in mailboxes]
        if anchor:
            # need to calculate the position
            for position, id in enumerate(mailboxes):
                if id == anchor:
                    position = max(position + anchorOffset, 0)
                    break
            else:
                raise errors.anchorNotFound()
        elif position < 0:
            position = max(position + total, 0)

        ids = ids[position:position + limit2]

        out = {
            'accountId': self.id,
            'queryState': await self.mailbox_state(),
            'canCalculateChanges': False,
            'position': position,
            'ids': ids,
        }
        if calculateTotal:
            out['total'] = total
        if limit != limit2:
            out['limit'] = limit2
        return out

    async def mailbox_changes(self, sinceState, maxChanges=None):
        """https://jmap.io/spec-mail.html#mailboxchanges"""
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

    async def email_query(self, sort=None, filter=None,
                          position=None, limit=None,
                          anchor=None, anchorOffset=None,
                          collapseThreads=False, calculateTotal=False):
        position, limit2, sort, filter = _validate_query(position, limit, sort, filter)

        search_criteria = self.as_imap_search(filter)
        sort_criteria = as_imap_sort(sort)
        if collapseThreads:
            ok, lines = await self.imap.uid_thread('REFS', search_criteria.decode())
            threads = parse_thread(lines[:1])
            # TODO flatten threads
            if threads:
                search_criteria += b' UID %s' % encode_messageset((int(t[0]) for t in threads))
        if sort_criteria:
            ok, lines = await self.imap.uid_sort(sort_criteria.decode(), search_criteria.decode(), ret='ALL COUNT')
        elif search_criteria:
            ok, lines = await self.imap.uid_search(search_criteria.decode(), ret='ALL COUNT')
        result = parse_esearch(lines)
        uidset = result.get('ALL', '')
        total = int(result.get('COUNT', 0))

        if anchor:
            # need to calculate position
            try:
                anchor_uid = self.parse_email_id(anchor)
            except ValueError:
                raise errors.anchorNotFound()
            for position, uid in enumerate(iter_messageset(uidset)):
                if uid == anchor_uid:
                    if type(anchorOffset) is int:
                        position = max(position + anchorOffset, 0)
                    elif anchorOffset is not None:
                        raise errors.invalidArguments('anchorOffset is not int')
                    break
            else:
                raise errors.anchorNotFound()
        elif position < 0:
            position = max(position + total, 0)

        if position < total:
            uids = iter_messageset(uidset)
            uids = itertools.islice(uids, position, position + limit)
            ids = list(self.format_email_id(uid) for uid in uids)
        else:
            ids = []

        out = {
            'accountId': self.id,
            'queryState': await self.email_state(),
            'canCalculateChanges': False,
            'position': position,
            'ids': ids,
            'collapseThreads': collapseThreads,
        }
        if calculateTotal:
            out['total'] = total
        if limit != limit2:
            out['limit'] = limit2
        return out

    async def email_get(self, idmap, ids=None, properties=None, bodyProperties=None,
                        fetchTextBodyValues=False, fetchHTMLBodyValues=False,
                        fetchAllBodyValues=False, maxBodyValueBytes=0):
        """https://jmap.io/spec-mail.html#emailget"""
        lst = []
        notFound = []
        fill_props = set()
        header_props = set()
        if properties:
            for prop in properties:
                m = header_prop_re.match(prop)
                if m is None:
                    fill_props.add(prop)
                else:
                    header_props.add(m.group(0, 1, 2, 3))
                    fill_props.add('headers')
            if 'body' in fill_props:
                fill_props.remove('body')
                fill_props.update(('textBody', 'htmlBody'))
        else:
            properties = ALL_PROPERTIES
            fill_props = set(properties)

        if bodyProperties is None:
            bodyProperties = ALL_BODY_PROPERTIES

        if header_props and 'headers' not in properties:
            fill_props.remove('headers')

        if ids is None:
            # get MAX_OBJECTS_IN_GET
            ok, lines = await self.imap.search('ALL', ret='ALL')
            if ok != 'OK':
                raise errors.serverFail('\n'.join(lines))
            uids = iter_messageset(parse_esearch(lines).get('ALL', ''))
            ids = (self.format_email_id(uid) for uid in uids)
            ids = tuple(itertools.islice(ids, MAX_OBJECTS_IN_GET))
        elif len(ids) > MAX_OBJECTS_IN_GET:
            raise errors.tooLarge('Requested more than {MAX_OBJECTS_IN_GET} ids')
        else:
            ids = [idmap.get(id) for id in ids]

        await self.fill_emails(fill_props, ids)

        for id in ids:
            try:
                msg = self.emails[id]
            except KeyError:
                notFound.append(id)
                continue

            # Fill most of msg properties except header:*
            data = {prop: msg[prop] for prop in fill_props}
            data['id'] = msg['id']
            if 'textBody' in msg and 'htmlBody' not in msg and not msg['textBody']:
                data['textBody'] = htmltotext(msg['htmlBody'])
            if 'bodyValues' in properties:
                # bug: jmap-demo-webmail needs all bodyValues even when fetchHTMLBodyValues=True
                if fetchHTMLBodyValues:
                    data['bodyValues'] = {k: v for k, v in msg['bodyValues'].items() if v['type'] == 'text/html'}
                elif fetchTextBodyValues:
                    data['bodyValues'] = {k: v for k, v in msg['bodyValues'].items() if v['type'] == 'text/plain'}
                elif fetchAllBodyValues:
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

    async def create_email(self, data):
        try:
            mailboxid, = data['mailboxIds']
        except KeyError:
            raise errors.invalidArguments('mailboxIds is required when creating email')
        except ValueError:
            raise errors.tooManyMailboxes('Max mailboxIds is 1')

        mailbox = self.mailboxes.get(mailboxid, None)
        if not mailbox or mailbox['deleted']:
            raise errors.notFound(f"Mailbox {mailboxid} not found")
        msg = ImapEmail(data)
        blobs = {}
        for attachment in data.get('attachments', ()):
            blobId = attachment.get('blobId', None)
            if blobId is not None:
                blobs[blobId] = await self.download(blobId)
        body = msg.make_body(blobs)
        flags = "(%s)" % (''.join(msg['FLAGS']))
        imapname = mailbox['imapname']
        ok, lines = await self.imap.append(body, imapname, flags)
        match = re.search(r'\[APPENDUID (\d+) (\d+)\]', lines[-1])
        ok, lines = await self.imap.noop()
        ok, lines = await self.imap.search(f"X-REAL-UID {match[2]} X-MAILBOX {imapname}", ret='ALL')
        search = parse_esearch(lines)
        ok, lines = await self.imap.uid_fetch(search['ALL'], "(UID X-GUID)")
        for seq, fetch in parse_fetch(lines[:-1]):
            id = self.format_email_id(int(fetch['UID']))
            msg['id'] = id
            msg['X-GUID'] = fetch['X-GUID']
            self.emails[id] = msg
            return {
                'id': id,
                'blobId': msg['blobId'],
            }

    async def create_emails(self, idmap, create):
        created, notCreated = {}, {}
        for cid, data in create.items():
            try:
                created[cid] = await self.create_email(data)
                idmap.set(cid, created[cid]['id'])
            except errors.JmapError as e:
                notCreated[cid] = e.to_dict()
        return created, notCreated

    async def update_email(self, msg, update):
        values = update.pop('keywords', {})
        store = {True: [keyword2flag(k) for k, v in values.items() if v],
                False: [keyword2flag(k) for k, v in values.items() if not v]}
        values = update.pop('mailboxIds', {})
        mids = {True: [k for k, v in values.items() if v],
               False: [k for k, v in values.items() if not v]}

        for path, value in update.items():
            prop, _, key = path.partition('/')
            if prop == 'keywords':
                store[bool(value)].append(keyword2flag(key))
            elif prop == 'mailboxIds':
                mids[bool(value)].append(key)
            else:
                raise errors.invalidArguments(f"Unknown update {path}")

        uid = str(self.parse_email_id(msg['id']))
        for add, flags in store.items():
            flags = f"({' '.join(flags)})"
            ok, lines = await self.imap.uid_store(uid, '+FLAGS' if add else '-FLAGS', flags)
            if ok != 'OK':
                raise errors.serverFail('\n'.join(lines))
            for seq, data in parse_fetch(lines[:-1]):
                if uid == data['UID']:
                    msg['FLAGS'] = data['FLAGS']
                    msg.pop('keywords', None)

        if mids[True] or mids[False]:
            if len(mids[True]) > 1 or \
               len(mids[False]) > 1 or \
               msg['mailboxIds'] != mids[False]:
                raise errors.tooManyMailboxes("Email must be always in exactly 1 mailbox")
            try:
                mailbox_to = self.mailboxes[mids[True][0]]
            except KeyError:
                raise errors.notFound('Mailbox not found')
            ok, lines = self.imap.uid_move(uid, quoted(mailbox_to['imapname']))
            if ok != 'OK':
                raise errors.serverFail('\n'.join(lines))

    async def update_emails(self, update):
        updated = {}
        notUpdated = {}
        await self.fill_emails(('keywords', 'mailboxIds'), update.keys())
        for id, patch in update.items():
            try:
                updated[id] = await self.update_email(self.emails[id], patch)
            except KeyError:
                notUpdated[id] = errors.notFound().to_dict()
            except errors.JmapError as e:
                notUpdated[id] = e.to_dict()
        return updated, notUpdated

    async def destroy_emails(self, ids):
        destroyed = []
        notDestroyed = {}
        uids = []
        for id in ids:
            try:
                uids.append(self.parse_email_id(id))
                self.emails.pop(id, None)
                destroyed.append(id)
            except ValueError:
                notDestroyed[id] = errors.notFound().to_dict()
        uidset = encode_messageset(uids).decode()
        await self.imap.uid_store(uidset, '+FLAGS', '(\\Deleted)')
        await self.imap.uid_expunge(uidset)
        # TODO: notDestroyed[id] = errors.notFound().to_dict()
        return destroyed, notDestroyed

    async def email_set(self, idmap, ifInState=None, create=None, update=None, destroy=None):
        oldState = await self.email_state()
        if ifInState is not None and ifInState != oldState:
            raise errors.stateMismatch()

        created, notCreated = await self.create_emails(idmap, create or {})

        update = {idmap.get(id): path for id, path in (update or {})}
        updated, notUpdated = await self.update_emails(update)

        destroy = [idmap.get(id) for id in (destroy or ())]
        destroyed, notDestroyed = await self.destroy_emails(destroy)

        return {
            'accountId': self.id,
            'oldState': oldState,
            'newState': await self.email_state(),
            'created': created or None,
            'notCreated': notCreated or None,
            'updated': updated or None,
            'notUpdated': notUpdated or None,
            'destroyed': destroyed or None,
            'notDestroyed': notDestroyed or None,
        }

    async def email_changes(self, sinceState, maxChanges=None):
        newState = await self.email_state()
        if sinceState <= await self.email_state_low():
            raise errors.cannotCalculateChanges({'new_state': newState})

        state = EmailState.from_string(sinceState)
        ok, lines = await self.imap.uid_fetch(
            '%d:*' % state.uid,
            "(UID)",
            '(CHANGEDSINCE %s VANISHED)' % state.modseq
        )
        if lines[0].startswith('(EARLIER) '):
            removed = [self.format_email_id(uid)
                       for uid in iter_messageset(lines[0][10:])]
            lines = lines[1:]
        else:
            removed = []

        created = []
        updated = []
        for seq, data in parse_fetch(lines[:-1]):
            uid = int(data['UID'])
            id = self.format_email_id(uid)
            if uid > state.uid:
                created.append(id)
            else:
                updated.append(id)

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
            uidset = parse_esearch(lines).get('ALL', '')
        else:
            ids = [idmap.get(id) for id in ids]
            search = self.as_imap_search({'threadIds': ids}).decode()
            ok, lines = await self.imap.uid_search(search, ret='ALL')
            uidset = parse_esearch(lines).get('ALL', '')
        if uidset:
            # ok, lines = await self.imap.uid_thread('REFS', 'UID %s' % uidset)
            # threads = parse_thread(lines)
            # await self.fill_emails(['blobId'], [t[0] for t in threads])
            ids = [self.format_email_id(uid) for uid in iter_messageset(uidset)]
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

    async def upload(self, content, typ=''):
        """Store as email with 1 part containing content"""
        from email.message import EmailMessage
        msg = EmailMessage()
        msg.set_content(content)
        body = msg.as_bytes()
        ok, lines = self.imap.append(body, 'Drafts')
        # TODO: fetch created email['id']
        return {
            'accountId': self.id,
            'blobId': f"G{email['id']}-0",
            'size': len(content),
            'type': typ,
        }

    async def download(self, blobId):
        search = self.as_imap_search({'blobId': blobId})
        ok, lines = self.imap.uid_search(search, ret='ALL')
        uidset = parse_esearch(lines)
        for uid in uidset:
            ok, lines = self.imap.uid_fetch(str(uid), '(BODY.PEEK[])')
            for seq, data in parse_fetch(lines[:-1]):
                return data['BODY[]']
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
                # TODO: use uid from \All mailbox
                created[id] = self.format_email_id(int(match.group(2)))
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
        ok, lines = await self.imap.status(self.imapname_all, '(UIDNEXT HIGHESTMODSEQ)')
        status = parse_status(lines)
        return str(EmailState(self.uidvalidity, int(status['UIDNEXT']), int(status['HIGHESTMODSEQ'])))

    async def email_state_low(self):
        return '1'

    async def mailbox_state(self):
        "Return current Mailbox state"
        await self.sync_mailboxes({'created'})
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

        if 'BODY.PEEK[]' in fields:  # remove redundand fields
            fields.discard('BODY.PEEK[HEADER]')
            fields.discard('RFC822.SIZE')

        fetch_uids = set()
        fetch_fields = set()
        for id in ids:
            try:
                uid = self.parse_email_id(id)
            except ValueError:
                continue
            msg = self.emails.get(id, None)
            if msg is None:
                fetch_uids.add(uid)
                fetch_fields = fields
            else:
                missing = fields - msg.keys()
                if missing:
                    fetch_uids.add(uid)
                    fetch_fields.update(missing)

        if not fetch_fields:
            return
        fetch_fields.add('UID')
        fetch_uids = encode_messageset(fetch_uids).decode()
        ok, lines = await self.imap.uid_fetch(fetch_uids, "(%s)" % (' '.join(fetch_fields)))
        if ok != 'OK':
            raise errors.serverFail(lines[0])
        for seq, data in parse_fetch(lines[:-1]):
            id = self.format_email_id(data['UID'])
            msg = self.emails.get(id, None)
            if not msg:
                msg = ImapEmail(id=id)
                self.emails[id] = msg
            if 'mailboxIds' in properties:
                try:
                    imapname = unquoted(data['X-MAILBOX'])
                except KeyError:
                    # don't know why sometimes Dovecot returns additional
                    # FETCH with duplicate UID with only MODSEQ
                    if msg['mailboxIds']:
                        continue
                msg['mailboxIds'] = [self.byimapname[imapname]['id']]
            msg.update(data)

    async def sync_mailboxes(self, fields=None):
        deleted_ids = set(self.mailboxes.keys())
        if fields is None:
            fields = {'totalEmails', 'unreadEmails', 'totalThreads', 'unreadThreads'}
        new_state = now_state()
        ok, lines = await self.imap.list(ret='SPECIAL-USE SUBSCRIBED STATUS (MESSAGES X-GUID)')
        for flags, sep, imapnameq, status in parse_list_status(lines):
            imapname = unquoted(imapnameq)
            flags = set(f.lower() for f in flags)
            if flags.intersection({'\\noselect', '\\nonexistent'}):
                continue
            id = status['X-GUID']
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
                ok, lines = await self.imap.search('UNSEEN UNDRAFT X-MAILBOX %s' % imapnameq, ret='COUNT')
                search = parse_esearch(lines)
                data['unreadEmails'] = int(search['COUNT'])

            if 'sortOrder' in fields:
                ok, lines = await self.imap.getmetadata(imapnameq, '(/private/sortorder)')
                for box, metadata in parse_metadata(lines[:-1]):
                    if unquoted(box) == imapname:
                        try:
                            data['sortOrder'] = int(metadata['/private/sortorder'])
                        except ValueError:  # got NIL or wrong value
                            pass

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

    async def update_mailbox(self, mailbox, update):
        fail = errors.serverFail
        imapnameq = quoted(mailbox['imapname'])

        if 'name' in update or 'parentId' in update:
            mailbox.pop('imapname')
            mailbox['name'] = update.get('name', mailbox['name'])
            mailbox['parentId'] = update.get('parentId', mailbox['parentId'])
            renameto = mailbox['imapname']
            ok, lines = await self.imap.rename(imapnameq, quoted(renameto))
            if ok != 'OK':
                raise fail('\n'.join(lines))
            fail = errors.serverPartialFail

        if 'isSubscribed' in update:
            if update['isSubscribed']:
                ok, lines = await self.imap.subscribe(imapnameq)
            else:
                ok, lines = await self.imap.unsubscribe(imapnameq)
            if ok != 'OK':
                raise fail('\n'.join(lines))
            fail = errors.serverPartialFail

        if 'sortOrder' in update:
            ok, lines = await self.imap.setmetadata(
                imapnameq, f"(/private/sortorder {int(update['sortOrder'])})")
            if ok != 'OK':
                raise fail('\n'.join(lines))

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
                        out += b' X-GUID '
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

    def parse_email_id(self, id):
        try:
            uidvalidity, uid = map(int, id.split('-'))
        except AttributeError:
            raise ValueError()
        if uidvalidity != self.uidvalidity or not 2**32 > uid > 0:
            raise ValueError()
        return uid

    def format_email_id(self, uid):
        return f'{self.uidvalidity}-{uid}'


def _validate_query(position, limit, sort, filter):
    if position is None:
        position = 0
    elif type(position) is not int:
        raise errors.invalidArguments('position is not int or null')
    if limit is None:
        limit = 1000
    elif type(limit) is not int or limit < 0:
        raise errors.invalidArguments('limit is not unsigned int or null')
    elif limit > 1000:
        limit = 1000
    if sort is None:
        sort = ()
    elif type(sort) is not list:
        raise errors.invalidArguments('sort MUST be list or null')
    if filter is None:
        filter = {}
    elif type(filter) is not dict:
        raise errors.invalidArguments('filter MUST be list or null')
    return position, limit, sort, filter


header_prop_re = re.compile(r'^header:([^:]+)(?::as(\w+))?(:all)?')
uidvalidity_re = re.compile(r'\[UIDVALIDITY ([0-9]+)\]', re.I)

ALL_MAILBOX_PROPERTIES = {
    'id', 'name', 'parentId', 'role', 'sortOrder', 'isSubscribed',
    'totalEmails', 'unreadEmails', 'totalThreads', 'unreadThreads',
    'myRights',
}

ALL_PROPERTIES = {
    'id', 'blobId', 'threadId', 'mailboxIds',
    'hasAttachment', 'keywords', 'subject',
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

    # OBJECTID extension
    # 'blobId':       'EMAILID',
    # 'threadId':     'THREADID',

    # only Dovecot
    'blobId':       'X-GUID',
    'threadId':     'X-GUID',
    'mailboxIds':   'X-MAILBOX',

    # when server sets $HasAttachment flag
    'hasAttachment': 'FLAGS',
    # 'hasAttachment':'BODY.PEEK[]',

    'keywords':     'FLAGS',

    # PREVIEW=* extension https://datatracker.ietf.org/doc/draft-ietf-extra-imap-fetch-preview/
    'preview':      'PREVIEW',
    # 'preview':      'BODY.PEEK[]',

    'receivedAt':   'INTERNALDATE',
    'size':         'RFC822.SIZE',
    'attachments':  'BODY.PEEK[]',
    'bodyStructure':'BODY.PEEK[]',
    'bodyValues':   'BODY.PEEK[]',
    'textBody':     'BODY.PEEK[]',
    'htmlBody':     'BODY.PEEK[]',
    'messageId':    'BODY.PEEK[HEADER]',
    'headers':      'BODY.PEEK[HEADER]',
    'sender':       'BODY.PEEK[HEADER]',
    'subject':      'BODY.PEEK[HEADER]',
    'from':         'BODY.PEEK[HEADER]',
    'to':           'BODY.PEEK[HEADER]',
    'cc':           'BODY.PEEK[HEADER]',
    'bcc':          'BODY.PEEK[HEADER]',
    'replyTo':      'BODY.PEEK[HEADER]',
    'inReplyTo':    'BODY.PEEK[HEADER]',
    'sentAt':       'BODY.PEEK[HEADER]',
    'references':   'BODY.PEEK[HEADER]',
    'created':      'UID',
    'updated':      'MODSEQ',
    'deleted':      'MODSEQ',
}

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

def int2bytes(i):
    return b'%d' % i

def quoted_bytes(s):
    return quoted(bytes(s))


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
    if out:
        out.pop()
    return out


def now_state():
    return str(int(datetime.now().timestamp()))


MAILBOX_FILTERS = {
    'hasAnyRole':   lambda mbox, val: bool(mbox['role']) == val,
    'isSubscribed': lambda mbox, val: mbox['isSubscribed'] == val,
    'name':         lambda mbox, val: val in mbox['name'],
    'parentId':     lambda mbox, val: mbox['parentId'] == val,
    'role':         lambda mbox, val: mbox['role'] == val,
}
