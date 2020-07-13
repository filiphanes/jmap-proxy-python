from binascii import a2b_base64, b2a_base64
from collections import defaultdict
from datetime import datetime
import email
from email.policy import default
import hashlib
import re
import uuid

try:
    import orjson as json
except ImportError:
    import json
from imapclient import IMAPClient
from imapclient.exceptions import IMAPClientError
from imapclient.response_types import Envelope

from jmap import errors, parse
from jmap.parse import asAddresses, asDate, asMessageIds, asText, bodystructure, htmltotext, parseStructure

from .base import BaseDB


KNOWN_SPECIALS = set(b'\\HasChildren \\HasNoChildren \\NoSelect \\NoInferiors \\UnMarked'.lower().split())

# special use or name magic
ROLE_MAP = {
  'inbox': 'inbox',

  'drafts': 'drafts',
  'draft': 'drafts',
  'draft messages': 'drafts',

  'bulk': 'junk',
  'bulk mail': 'junk',
  'junk': 'junk',
  'junk mail': 'junk',
  'junk': 'spam',
  'spam mail': 'junk',
  'spam messages': 'junk',

  'archive': 'archive',
  'sent': 'sent',
  'sent items': 'sent',
  'sent messages': 'sent',

  'deleted messages': 'trash',
  'trash': 'trash',

  '\\inbox': 'inbox',
  '\\trash': 'trash',
  '\\sent': 'sent',
  '\\junk': 'junk',
  '\\spam': 'junk',
  '\\archive': 'archive',
  '\\drafts': 'drafts',
}


KEYWORD2FLAG = {
    '$answered': '\\Answered',
    '$flagged': '\\Flagged',
    '$draft': '\\Draft',
    '$seen': '\\Seen',
}
FLAG2KEYWORD = {f.lower().encode(): kw for kw, f in KEYWORD2FLAG.items()}


FIELDS_MAP = {
    'blobId': 'X-GUID',  # Dovecot
    # 'blobId': 'MESSAGEID',  # IMAP extension OBJECTID
    'hasAttachment': 'FLAGS',
    'headers': 'RFC822.HEADER',
    'keywords': 'FLAGS',
    'preview': 'PREVIEW',
    'receivedAt': 'INTERNALDATE',
    'size': 'RFC822.SIZE',
    'attachments': 'RFC822',
    'bodyStructure': 'RFC822',
    'bodyValues': 'RFC822',
    'textBody': 'RFC822',
    'htmlBody': 'RFC822',
    'subject': 'RFC822.HEADER',
    'from': 'RFC822.HEADER',
    'to': 'RFC822.HEADER',
    'cc': 'RFC822.HEADER',
    'bcc': 'RFC822.HEADER',
    'replyTo': 'RFC822.HEADER',
    'inReplyTo': 'RFC822.HEADER',
    'sentAt': 'RFC822.HEADER',
    'references': 'RFC822.HEADER',
}

class ImapMessage(dict):
    header_re = re.compile(r'^([\w-]+)\s*:\s*(.+?)\r\n(?=[\w\r])', re.I | re.M | re.DOTALL)

    def __missing__(self, key):
        self[key] = getattr(self, key)()
        return self[key]

    def get_header(self, name: str):
        "Return raw value from last header instance, name needs to be lowercase."
        return self['LASTHEADERS'].get(name, None)

    def EML(self):
        return email.message_from_bytes(self['RFC822'], policy=default)

    def LASTHEADERS(self):
        # make headers dict with only last instance of each header
        # as required by JMAP spec for single header get
        return {name.lower(): raw
            for name, raw in self.header_re.findall(self['DECODEDHEADERS'])}

    def DECODEDHEADERS(self):
        try:
            return self.pop('RFC822.HEADER').decode()
        except KeyError:
            match = re.search(rb'\r\n\r\n', self['RFC822'])
            if match:
                return self['RFC822'][:match.end()].decode()


    def blobId(self):
        return self['X-GUID'].decode()

    def hasAttachment(self):
        # Dovecot with mail_attachment_detection_options = add-flags-on-save
        return '$HasAttachment' in self['keywords']

    def headers(self):
        return [{'name': name, 'value': value}
            for name, value in self.header_re.findall(self['DECODEDHEADERS'])]

    def inReplyTo(self):
        return asMessageIds(self.get_header('in-reply-to'))

    def keywords(self):
        return {FLAG2KEYWORD.get(f.lower(), f.decode()): True for f in self.pop('FLAGS')}

    def messageId(self):
        return asMessageIds(self.get_header('message-id'))

    def mailboxIds(self):
        return [ parse_message_id(self['id'])[0] ]

    def preview(self):
        return self.pop('PREVIEW')[1].decode()

    def receivedAt(self):
        return self.pop('INTERNALDATE')

    def references(self):
        return asMessageIds(self.get_header('references'))

    def replyTo(self):
        return asAddresses(self.get_header('reply-to'))

    def sentAt(self):
        return asDate(self.get_header('date'))

    def size(self):
        try:
            return self.pop('RFC822.SIZE')
        except AttributeError:
            return len(self['RFC822'])

    def subject(self):
        return asText(self.get_header('subject')) or ''

    def threadId(self):
        # TODO: threading
        return f"t{self['id']}"

    def bodyStructure(self):
        self['bodyValues'], bodyStructure \
            = bodystructure(self['id'], self['EML'])
        return bodyStructure

    def bodyValues(self):
        bodyValues, self['bodystructure'] \
            = bodystructure(self['id'], self['EML'])
        return bodyValues

    def textBody(self):
        textBody, self['htmlBody'], self['attachments'] \
            = parseStructure([self['bodyStructure']], 'mixed', False)
        return textBody

    def htmlBody(self):
        self['textBody'], htmlBody, self['attachments'] \
            = parseStructure([self['bodyStructure']], 'mixed', False)
        return htmlBody

    def attachments(self):
        self['textBody'], self['htmlBody'], attachments \
            = parseStructure([self['bodyStructure']], 'mixed', False)
        return attachments


# Define address getters
# "from" is reserved in python, it needs to be defined this way
# others are similar
def address_getter(field):
    def get(self):
        return asAddresses(self.get_header(field)) or []
    return get
for prop in ('from', 'to', 'cc', 'bcc', 'sender'):
    setattr(ImapMessage, prop, address_getter(prop))


# def format_message_id(mailboxid, uid):
#     return f'{mailboxid}_{uid}'

# def parse_message_id(messageid):
#     return messageid.split('_')

def format_message_id(mailboxid, uidvalidity, uid):
    return b2a_base64(
        bytes.fromhex(mailboxid) +
        uidvalidity.to_bytes(4, 'big') + 
        uid.to_bytes(4, 'big'),
        newline=False
    ).replace(b'+', b'-').replace(b'/', b'_').decode()

def parse_message_id(messageid):
    b = a2b_base64(messageid.encode().replace(b'-', b'+').replace(b'_', b'/'))
    return b[:16].hex(), \
           int.from_bytes(b[16:20], 'big'), \
           int.from_bytes(b[20:24], 'big')

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
        self.sync_mailboxes()
        self.messages = {}


    def get_messages_cached(self, properties=(), id__in=()):
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
            messages, id__in, properties = self.get_messages_cached(properties, id__in=id__in)

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
                mailboxid, uidvalidity, uid = parse_message_id(id)
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
    

    def changed_record(self, ifolderid, uid, flags=(), labels=()):
        res = self.dmaybeupdate('imessages', {
            'flags': json.dumps(sorted(flags)),
            'labels': json.dumps(sorted(labels)),
        }, {'ifolderid': ifolderid, 'uid': uid})
        if res:
            msgid = self.dgefield('imessages', {'ifolderid': ifolderid, 'uid': uid}, 'msgid')
            self.mark_sync(msgid)
    
    def import_message(self, rfc822, mailboxIds, keywords):
        folderdata = self.dget('ifolders')
        foldermap = {f['ifolderid']: f for f in folderdata}
        jmailmap = {f['jmailboxid']: f for f in folderdata if f.get('jmailboxid', False)}
        # store to the first named folder - we can use labels on gmail to add to other folders later.
        id, others = mailboxIds
        imapname = jmailmap[id][imapname]
        flags = set(keywords)
        for kw in flags:
            if kw in KEYWORD2FLAG:
                flags.remove(kw)
                flags.add(KEYWORD2FLAG[kw])
        appendres = self.imap.append('imapname', '(' + ' '.join(flags) + ')', datetime.now(), rfc822)
        # TODO: compare appendres[2] with uidvalidity
        uid = appendres[3]
        fdata = jmailmap[mailboxIds[0]]
        self.do_folder(fdata['ifolderid'], fdata['label'])
        ifolderid = fdata['ifolderid']
        msgdata = self.dgetone('imessages', {
            'ifolderid': ifolderid,
            'uid': uid,
        }, 'msgid,thrid,size')
        
        # XXX - did we fail to sync this back?  Annoying
        if not msgdata:
            raise Exception('Failed to get back stored message from imap server')
        # save us having to download it again - drop out of transaction so we don't wait on the parse
        message = parse.parse(rfc822, msgdata['msgid'])
        self.begin()
        self.dinsert('jrawmessage', {
            'msgid': msgdata['msgid'],
            'parsed': json.dumps('message'),
            'hasAttachment': message['hasattachment'],
        })
        self.commit()
        return msgdata
    
    def update_messages(self, changes, idmap):
        if not changes:
            return {}, {}
        
        changed = {}
        notchanged = {}
        map = {}
        msgids = set(changes.keys())
        sql = 'SELECT msgid,ifolderid,uid FROM imessages WHERE msgid IN (' + (('?,' * len(msgids))[:-1]) + ')'
        self.cursor.execute(sql, list(msgids))
        for msgid, ifolderid, uid in self.cursor:
            if not msgid in map:
                map[msgid] = {ifolderid: {uid}}
            elif not ifolderid in map[msgid]:
                map[msgid][ifolderid] = {uid}
            else:
                map[msgid][ifolderid].add(uid)
            msgids.discard(msgid)

        for msgid in msgids:
            notchanged[msgid] = {
                'type': 'notFound',
                'description': 'No such message on server',
            }
        
        folderdata = self.dget('ifolders')
        foldermap = {f['ifolderid']: f for f in folderdata}
        jmailmap = {f['jmailboxid']: f for f in folderdata if 'jmailboxid' in f}
        jmapdata = self.dget('jmailboxes')
        jidmap = {d['jmailboxid']: (d['role'] or '') for d in jmapdata}
        jrolemap = {d['role']: d['jmailboxid'] for d in jmapdata if 'role' in d}

        for msgid in map.keys():
            action = changes[msgid]
            try:
                for ifolderid, uids in map[msgid].items():
                    # TODO: merge similar actions?
                    imapname = foldermap[ifolderid]['imapname']
                    uidvalidity = foldermap[ifolderid]['uidvalidity']
                    if self.selected_folder != (imapname, False):
                        self.imap.select_folder(imapname)
                        self.selected_folder = (imapname, False)
                    if imapname and uidvalidity and 'keywords' in action:
                        flags = set(action['keywords'])
                        for kw in flags:
                            if kw in KEYWORD2FLAG:
                                flags.remove(kw)
                                flags.add(KEYWORD2FLAG[kw])
                        self.imap.set_flags(uids, flags, silent=True)

                if 'mailboxIds' in action:
                    mboxes = [idmap(k) for k in action['mailboxIds'].keys()]
                    # existing ifolderids containing this message
                    # identify a source message to work from
                    ifolderid = sorted(map[msgid])[0]
                    uid = sorted(map[msgid][ifolderid])[0]
                    imapname = foldermap[ifolderid]['imapname']
                    uidvalidity = foldermap[ifolderid]['uidvalidity']

                    # existing ifolderids with this message
                    current = set(map[msgid].keys())
                    # new ifolderids that should contain this message
                    new = set(jmailmap[x]['ifolderid'] for x in mboxes)
                    for ifolderid in new:
                        # unless there's already a matching message in it
                        if current.pop(ifolderid):
                            continue
                        # copy from the existing message
                        newfolder = foldermap[ifolderid]['imapname']
                        self.imap.copy(imapname, uidvalidity, uid, newfolder)
                    for ifolderid in current:
                        # these ifolderids didn't exist in new, so delete all matching UIDs from these folders
                        self.imap.move(
                            foldermap[ifolderid]['imapname'],
                            foldermap[ifolderid]['uidvalidity'],
                            map[msgid][ifolderid],  # uids
                        )
            except Exception as e:
                notchanged[msgid] = {'type': 'error', 'description': str(e)}
                raise e
            else:
                changed[msgid] = None

        return changed, notchanged    

    def destroy_messages(self, ids):
        if not ids:
            return [], {}
        destroymap = defaultdict(dict)
        notdestroyed = {}
        idset = set(ids)
        rows = self.dget('imessages', {'msgid': ('IN', idset)},
                         'msgid,ifolderid,uid')
        for msgid, ifolderid, uid in rows:
            idset.discard(msgid)
            destroymap[ifolderid][uid] = msgid
        for msgid in idset:
            notdestroyed[msgid] = {
                'type': 'notFound',
                'description': "No such message on server",
            }

        folderdata = self.dget('ifolders')
        foldermap = {d['ifolderid']: d for d in folderdata}
        jmailmap = {d['jmailboxid']: d for d in folderdata if 'jmailboxid' in d}
        destroyed = []
        for ifolderid, ifolder in destroymap.items():
            #TODO: merge similar actions?
            if not ifolder['imapname']:
                for msgid in destroymap[ifolderid]:
                    notdestroyed[msgid] = \
                        {'type': 'notFound', 'description': "No folder"}
            self.imap.move(ifolder['imapname'], ifolder['uidvalidity'],
                                   destroymap[ifolderid].keys(), None)
            destroyed.extend(destroymap[ifolderid].values())

        return destroyed, notdestroyed
    
    def deleted_record(self, ifolderid, uid):
        msgid = self.dgetfield('imessages', {'ifolderid': ifolderid, 'uid': uid}, 'msgid')
        if msgid:
            self.ddelete('imessages', {'ifolderid': ifolderid, 'uid': uid})
            self.mark_sync(msgid)

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
        byimapname = {}
        # TODO: LIST "" % RETURN (STATUS (UNSEEN MESSAGES HIGHESTMODSEQ MAILBOXID))
        for flags, sep, imapname in self.imap.list_folders():
            status = self.imap.folder_status(imapname, (['MESSAGES', 'UIDVALIDITY', 'UIDNEXT', 'HIGHESTMODSEQ', 'X-GUID']))
            flags = [f.lower() for f in flags]
            roles = [f for f in flags if f not in KNOWN_SPECIALS]
            label = roles[0].decode() if roles else imapname
            role = ROLE_MAP.get(label.lower(), None)
            can_select = b'\\noselect' not in flags
            byimapname[imapname] = {
                # Dovecot can fetch X-GUID
                'id': status[b'X-GUID'].decode(),
                'parentId': None,
                'name': imapname,
                'role': role,
                'sortOrder': 2 if role else (1 if role == 'inbox' else 3),
                'isSubscribed': True,  # TODO: use LSUB
                'totalEmails': status[b'MESSAGES'],
                'unreadEmails': 0,
                'totalThreads': 0,
                'unreadThreads': 0,
                'myRights': {
                    'mayReadItems': can_select,
                    'mayAddItems': can_select,
                    'mayRemoveItems': can_select,
                    'maySetSeen': can_select,
                    'maySetKeywords': can_select,
                    'mayCreateChild': True,
                    'mayRename': False if role else True,
                    'mayDelete': False if role else True,
                    'maySubmit': can_select,
                },
                'imapname': imapname,
                'sep': sep.decode(),
                'uidvalidity': status[b'UIDVALIDITY'],
                'uidnext': status[b'UIDNEXT'],
                # Data sync properties
                'createdModSeq': status[b'UIDVALIDITY'],  # TODO: persist
                'updatedModSeq': status[b'UIDVALIDITY'],  # TODO: from persistent storage
                'updatedNotCountsModSeq': status[b'UIDVALIDITY'],  # TODO: from persistent storage
                'emailHighestModSeq': status[b'HIGHESTMODSEQ'],
                'deleted': 0,
            }

        # set name and parentId for child folders
        for imapname, mailbox in byimapname.items():
            names = imapname.rsplit(mailbox['sep'], maxsplit=1)
            if len(names) == 2:
                mailbox['parentId'] = byimapname[names[0]]['id']
                mailbox['name'] = names[1]

        # update cache
        self.mailboxes = {mbox['id']: mbox for mbox in byimapname.values()}
        return byimapname.values()


    def sync_mailboxes(self):
        self.get_mailboxes()
    

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

        status = self.imap.folder_status(imapname, ['UIDVALIDITY'])
        self.sync_mailboxes()
        return f"f{status[b'UIDVALIDITY']}"


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
        mailbox['deleted'] = datetime.now().timestamp()
        self.sync_mailboxes()


    def create_submission(self, new, idmap):
        if not new:
            return {}, {}
        
        todo = {}
        createmap = {}
        notcreated = {}
        for cid, sub in new.items():
            msgid = idmap.get(sub['emailId'], sub['emailId'])
            if not msgid:
                notcreated[cid] = {'error': 'nos msgid provided'}
                continue
            thrid = self.dgetfield('jmessages', {'msgid': msgid, 'deleted': 0}, 'thrid')
            if not thrid:
                notcreated[cid] = {'error': 'message does not exist'}
                continue
            id = self.dmake('jsubmission', {
                'sendat': datetime.fromisoformat()(sub['sendAt']).isoformat() if sub['sendAt'] else datetime.now().isoformat(),
                'msgid': msgid,
                'thrid': thrid,
                'envelope': json.dumps(sub['envelope']) if 'envelope' in sub else None,
            })
            createmap[cid] = {'id': id}
            todo[cid] = msgid
        self.commit()

        for cid, sub in todo.items():
            type, rfc822 = self.get_raw_message(todo[cid])
            self.imap.send_mail(rfc822, sub['envelope'])

        return createmap, notcreated


    def update_submission(self, changed, idmap):
        return {}, {x: 'change not supported' for x in changed.keys()}


    def destroy_submission(self, destroy):
        if not destroy:
            return [], {}
        destroyed = []
        notdestroyed = {}
        namemap = {}
        for subid in destroy:
            deleted = self.dgetfield('jsubmission', {'jsubid': subid}, 'deleted')
            if deleted:
                destroy.append(subid)
                self.ddelete('jsubmission', {'jsubid': subid})
            else:
                notdestroyed[subid] = {'type': 'notFound', 'description': 'submission not found'}
        self.commit()
        return destroyed, notdestroyed
    
    def _initdb(self):
        super()._initdb()

        self.dbh.execute("""
            CREATE TABLE IF NOT EXISTS ifolders (
            ifolderid INTEGER PRIMARY KEY NOT NULL,
            jmailboxid INTEGER,
            sep TEXT NOT NULL,
            imapname TEXT NOT NULL,
            label TEXT,
            uidvalidity INTEGER,
            uidfirst INTEGER,
            uidnext INTEGER,
            highestmodseq INTEGER,
            uniqueid TEXT,
            mtime DATE NOT NULL
            )""")
        self.dbh.execute("CREATE INDEX IF NOT EXISTS ifolderj ON ifolders (jmailboxid)");
        self.dbh.execute("CREATE INDEX IF NOT EXISTS ifolderlabel ON ifolders (label)");

        self.dbh.execute("""
            CREATE TABLE IF NOT EXISTS imessages (
            imessageid INTEGER PRIMARY KEY NOT NULL,
            ifolderid INTEGER,
            uid INTEGER,
            internaldate DATE,
            modseq INTEGER,
            flags TEXT,
            labels TEXT,
            thrid TEXT,
            msgid TEXT,
            envelope TEXT,
            bodystructure TEXT,
            size INTEGER,
            mtime DATE NOT NULL
            )""")
        self.dbh.execute("CREATE UNIQUE INDEX IF NOT EXISTS imsgfrom ON imessages (ifolderid, uid)");
        self.dbh.execute("CREATE INDEX IF NOT EXISTS imessageid ON imessages (msgid)");
        self.dbh.execute("CREATE INDEX IF NOT EXISTS imessagethrid ON imessages (thrid)");

        self.dbh.execute("""
            CREATE TABLE IF NOT EXISTS ithread (
            messageid TEXT PRIMARY KEY,
            sortsubject TEXT,
            thrid TEXT
            )""")
        self.dbh.execute("CREATE INDEX IF NOT EXISTS ithrid ON ithread (thrid)");


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
    'before': 'BEFORE',
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


def _normalsubject(subject):
    # Re: and friends
    subject = re.sub(r'^[ \t]*[A-Za-z0-9]+:', subject, '')
    # [LISTNAME] and friends
    sub = re.sub(r'^[ \t]*\\[[^]]+\\]', subject, '')
    # any old whitespace
    sub = re.sub(r'[ \t\r\n]+', subject, '')


# ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz-_.~"
ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+_.~"
def base_encode(n: int, b: int = 64, a=ALPHABET):
    if not n:
        return a[0]
    s = ''
    dm = divmod  # Access to locals is faster.
    while n:
        n, r = dm(n, b)
        s = a[r] + s
    return s

ALPHABET_DICT = {c: v for v, c in enumerate(ALPHABET)}
def base_decode(s:str, b:int=64, d=ALPHABET_DICT):
    n = 0
    for c in s:
        n = n * b + d[c]
    return n
