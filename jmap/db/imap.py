import hashlib
from collections import defaultdict
import re
from datetime import datetime
import uuid
try:
    import orjson as json
except ImportError:
    import json

from imapclient import IMAPClient
from imapclient.exceptions import IMAPClientError
from imapclient.response_types import Envelope
from jmap import errors, parse

from .base import BaseDB
from email.header import decode_header, make_header


TAG = 1

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
FLAG2KEYWORD = {f.lower(): kw for kw, f in KEYWORD2FLAG.items()}


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

        self.mailboxes = {}
        self.sync_mailboxes()


    def get_messages(self, fields=(), sort={}, inMailbox=None, id__in=(), **filter):
        if inMailbox is None:
            # TODO: implement optional inMailbox
            raise errors.invalidArguments('This JMAP implementation requires inMailbox filter')

        mailbox = self.mailboxes.get(inMailbox, None)
        if not mailbox:
            raise errors.notFound(f'Mailbox {inMailbox} not found')
        fullname = self.mailbox_fullname(mailbox['parentId'], mailbox['name'])
        self.imap.select_folder(fullname, readonly=True)

        if id__in:
            uids = [int(id.split('_')[1]) for id in id__in]
        else:
            search_criteria = as_imap_search(filter) or 'ALL'
            if sort:
                sort_criteria = as_imap_sort(sort) or None
                uids = self.imap.sort(sort_criteria, search_criteria)
            else:
                uids = self.imap.search(search_criteria)

        if fields == 'id':
            return [f"{inMailbox}_{uid}" for uid in uids]

        messages = {}
        res = self.imap.fetch(uids, as_imap_fields(fields.split(',')))
        for uid, msg in res.items():
            # TODO
            messages[uid] = msg
        return messages


    def labels(self):
        self.cursor.execute('SELECT label,ifolderid,jmailboxid,imapname FROM ifolders')
        return {label: (ifolderid, jmailboxid, imapname) for label, ifolderid, jmailboxid, imapname in self.cursor}
    
    def backfill(self):
        rest = 500
        self.cursor.execute('SELECT ifolderid, label FROM ifolders'
                            ' WHERE uidnext > 1 AND uidfirst > 1')
        rows = self.cursor.fetchall()

        if rows:
            for ifolderid, label in rows:
                rest -= self.do_folder(ifolderid, label, rest)
                if rest < 10:
                    break
            self.sync_jmap()
            return 1
    
    def firstsync(self):
        self.sync_folders()
        self.cursor.execute('SELECT ifolderid, label FROM ifolders'
                            ' WHERE UPPER(imapname) = "INBOX" LIMIT 1')
        for ifolderid, label in self.cursor.fetchall():
            self.do_folder(ifolderid, label, 10000)
        self.sync_jmap()
        
    def calcmsgid(self, imapname, uid, msg):
        envelope = msg[b'ENVELOPE']
        # print("msg[b'ENVELOPE']=", msg[b'ENVELOPE'])
        coded = json.dumps([envelope], default=jsonDefault)
        base = hashlib.sha1(coded).hexdigest()[:9]
        msgid = 'm' + base
        in_reply_to = envelope.in_reply_to
        messageid = envelope.message_id
        encsub = envelope.subject
        try:
            encsub = str(make_header(decode_header(encsub)))
        except Exception:
            pass
        sortsub = _normalsubject(encsub)
        self.cursor.execute('SELECT DISTINCT thrid FROM ithread'
               ' WHERE messageid IN (?,?) AND sortsubject=? LIMIT 1',
               (in_reply_to, messageid, sortsub))
        try:
            thrid, = self.cursor.fetchone()
        except Exception:
            thrid = 't' + base
        for id in (in_reply_to, messageid):
            if id:
                self.dbh.execute('INSERT OR IGNORE INTO ithread (messageid, thrid, sortsubject) VALUES (?,?,?)', (id, thrid, sortsub))
        return msgid, thrid
    
    def do_folder(self, ifolderid, forcelabel, batchsize=0):
        self.cursor.execute("SELECT imapname, uidfirst, uidnext, uidvalidity, highestmodseq FROM ifolders WHERE ifolderid=?", [ifolderid])
        data = self.cursor.fetchone()
        if not data:
            return print(f'NO SUCH FOLDER {ifolderid}')

        imapname, uidfirst, uidnext, uidvalidity, highestmodseq = data
        uidfirst = uidfirst or 1
        highestmodseq = 1 # comment in production
        fetch_data = 'UID FLAGS INTERNALDATE ENVELOPE RFC822.SIZE'.split()
        fetch_modifiers = (f'CHANGEDSINCE {highestmodseq}',)

        oldstate = {
            'uidvalidity': uidvalidity,
            'highestmodseq': highestmodseq,
            'uidnext': uidnext,
        }

        res = self.imap.select_folder(imapname, readonly=True)
        exists = int(res[b'EXISTS'])
        uidvalidity = int(res[b'UIDVALIDITY'])
        uidnext = int(res[b'UIDNEXT'])
        highestmodseq = int(res[b'HIGHESTMODSEQ'])
        
        newstate = {
            'uidvalidity': uidvalidity,
            'highestmodseq': highestmodseq,
            'uidnext': uidnext,
            'exists': exists,
        }

        if not batchsize:
            new = self.imap.fetch((oldstate['uidnext'], '*'), fetch_data, fetch_modifiers)
            update = self.imap.fetch((uidfirst, oldstate['uidnext']-1), ('UID', 'FLAGS'))
            backfill = {}
        elif uidfirst > 1:
            end = uidfirst - 1
            uidfirst = max(uidfirst - batchsize, 1)
            new = {}
            update = {}
            self.backfilling = True
            backfill = self.imap.fetch(f'{uidfirst}:{end}', fetch_data, fetch_modifiers)
        else:
            return
        print('fetch_data:', fetch_data)
        print('new:', new)
        print('update:', update)
        print('backfill:', backfill)

        if oldstate['uidvalidity'] != uidvalidity:
            raise Exception(f"UIDVALIDITY CHANGED {imapname}: {oldstate['uidvalidity']} => {uidvalidity}")

        if newstate['uidvalidity'] != uidvalidity:
            # going to want to nuke everything for the existing folder and create this  - but for now, just die
            raise Exception(f"UIDVALIDITY CHANGED {imapname}: {uidvalidity} => {newstate['uidvalidity']}")
        

        self.begin()
        didold = 0
        for uid, msg in backfill.items():
            msgid, thrid = self.calcmsgid(imapname, uid, msg)
            didold += 1
            self.new_record(
                ifolderid,
                uid,
                (f.decode() for f in msg[b'FLAGS'] if f.lower() != b'\\recent'),
                [forcelabel],
                msg[b'ENVELOPE'],
                msg[b'INTERNALDATE'],
                msgid,
                thrid,
                msg[b'RFC822.SIZE'],
            )
        
        for uid, msg in update.items():
            print('msg:', msg)
            self.changed_record(
                ifolderid,
                uid,
                (f.decode() for f in msg[b'FLAGS'] if f.lower() != b'\\recent'),
                [forcelabel],
            )

        for uid, msg in new.items():
            msgid, thrid = self.calcmsgid(imapname, uid, msg)
            self.new_record(
                ifolderid,
                uid,
                (f.decode() for f in msg[b'FLAGS'] if f.lower() != b'\\recent'),
                [forcelabel],
                msg[b'ENVELOPE'],
                msg[b'INTERNALDATE'],
                msgid,
                thrid,
                msg[b'RFC822.SIZE'],
            )

        self.dupdate('ifolders', {
            'highestmodseq': newstate['highestmodseq'],
            'uidfirst': uidfirst,
            'uidnext': newstate['uidnext'],
        }, {'ifolderid': ifolderid})
        self.commit()

        if batchsize:
            return didold
        self.cursor.execute('SELECT COUNT(*) FROM imessages WHERE ifolderid=?', [ifolderid])
        count, = self.cursor.fetchone()

        if uidfirst != 1 or count != newstate['exists']:
            # welcome to the future
            uidnext = newstate['uidnext']
            to = uidnext - 1
            exists = self.imap.search(['UID', f'{uidfirst}:{to}'])
            exists = set(exists)
            self.cursor.execute('SELECT msgid, uid FROM imessages WHERE ifolderid = ? AND uid >= ? AND uid <= ?', [ifolderid, uidfirst, to])
            for msgid, uid in self.cursor:
                if uid not in exists:
                    self.ddelete('imessages', {'ifolderid': ifolderid, 'uid': uid})
                    self.mark_sync(msgid)
            self.commit()
    
    def imap_search(self, *search):
        matches = set()
        for folder in self.dget('ifolders'):
            frm = folder['uidfirst']
            to = folder['uidnext'] - 1
            res = self.imap.search(folder['imapname'], 'uid', f'{frm}:{to}', search)
            if not res[2] == folder['uidvalidity']:
                continue
            uids = (str(uid) for uid in res[3])
            self.cursor.execute("SELECT msgid FROM imessages WHERE ifolderid=? AND uid IN (" + ','.join(uids) + ")", [folder['ifolderid']])
            matches.update([msgid for msgid, in self.cursor])
        return matches

    def mark_sync(self, msgid):
        self.dbh.execute('INSERT OR IGNORE INTO imsgidtodo (msgid) VALUES (?)', [msgid])
    
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
                    self.imap.select_folder(imapname)
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
    
    def new_record(self, ifolderid, uid, flags, labels, envelope, internaldate, msgid, thrid, size):
        self.dinsert('imessages', {
            'ifolderid': ifolderid,
            'uid': uid,
            'flags': json.dumps(sorted(flags)),
            'labels': json.dumps(sorted(labels)),
            'internaldate': internaldate.isoformat(),
            'msgid': msgid,
            'thrid': thrid,
            'envelope': json.dumps(envelope, default=jsonDefault),
            'size': size,
        })
        self.mark_sync(msgid)
    
    def sync_jmap_msgid(self, msgid):
        labels = set()
        flags = set()
        self.cursor.execute('SELECT flags,labels FROM imessages WHERE msgid=?', [msgid])
        for f, l in self.cursor:
            print('flags:', f, 'labels:', l)
            flags.update(json.loads(f))
            labels.update(json.loads(l))

        keywords = {}
        for flag in flags:
            flag = flag.lower()
            keywords[FLAG2KEYWORD.get(flag, flag)] = True
        
        slabels = self.labels()
        print('labels', labels)
        jmailboxids = [slabels[l][1] for l in labels]

        if not jmailboxids:
            return self.delete_message(msgid)
        self.cursor.execute("SELECT msgid FROM jmessages WHERE msgid=? AND deleted=NULL", [msgid])
        if self.cursor.fetchone():
            return self.change_message(msgid, {'keywords': keywords}, jmailboxids)
        else:
            self.cursor.execute("SELECT thrid,internaldate,size,envelope FROM imessages WHERE msgid=?", [msgid])
            msg = self.cursor.fetchone()
            return self.add_message({
                'msgid': msgid,
                'receivedAt': msg['internaldate'],
                'thrid': msg['thrid'],
                'size': msg['size'],
                'keywords': keywords,
                'isDraft': '$draft' in keywords,
                'isUnread': '$seen' not in keywords,
                **_envelopedata(msg['envelope']),
            }, jmailboxids)

    def sync_jmap(self):
        self.cursor.execute('SELECT msgid FROM imsgidtodo')
        msgids = [i for i, in self.cursor]
        for msgid in msgids:
            self.sync_jmap_msgid(msgid)
            self.cursor.execute('DELETE FROM imsgidtodo WHERE msgid=?', [msgid])
        if msgids:
            self.commit()


    def fill_messages(self, ids):
        if not ids:
            return
        self.cursor.execute('SELECT msgid, parsed FROM jrawmessage'
            ' WHERE msgid IN (' + ('?,' * len(ids))[:-1] + ')', ids)
        
        result = {msgid: json.loads(parsed) for msgid, parsed in self.cursor}
        need = [i for i in ids if i not in result]
        udata = defaultdict(dict)
        if need:
            self.cursor.execute('SELECT ifolderid, uid, msgid FROM imessages WHERE msgid IN (' + ('?,' * len(need))[:-1] + ')',
                need)
            for ifolderid, uid, msgid in self.cursor:
                udata[ifolderid][uid] = msgid
        
        foldermap = {}
        for ifolderid, uhash in udata.items():
            for msgid in uhash.values():
                if msgid not in result:
                    foldermap[ifolderid] = self.dgetone('ifolders', {'ifolderid': ifolderid}, 'imapname,uidvalidity')
                    break

        if not udata:
            return result
        
        for ifolderid, uhash in udata.items():
            if ifolderid not in foldermap: continue
            uids = ','.join(str(u) for u,i in uhash.items() if i not in result)
            if not uids: continue
            imapname, uidvalidity = foldermap[ifolderid]
            res = self.imap.select_folder(imapname, readonly=True)
            if res[b'UIDVALIDITY'] != uidvalidity:
                raise Warning('UIDVALIDITY dont matches for ' + imapname)
            res = self.imap.fetch(uids, ['RFC822'])
            for uid, data in res.items():
                msgid = uhash[uid]
                result[msgid] = parse.parse(data[b'RFC822'])
                self.cursor.execute("INSERT OR REPLACE INTO jrawmessage (msgid,parsed,hasAttachment) VALUES (?,?,?)", [
                    msgid,
                    json.dumps(result[msgid]),
                    result[msgid].get('hasAttachment', 0),
                    ])
        self.commit()

        # XXX - handle not getting data that we need?
        # stillneed = ids.difference(result.keys())
        return result


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
    
    def get_mailboxes(self, fields=None, **filter):
        byfullname = {}
        # TODO: LIST "" % RETURN (STATUS (UNSEEN MESSAGES HIGHESTMODSEQ MAILBOXID))
        for flags, sep, fullname in self.imap.list_folders():
            status = self.imap.folder_status(fullname, (['MESSAGES', 'UIDVALIDITY', 'UIDNEXT', 'HIGHESTMODSEQ']))
            flags = [f.lower() for f in flags]
            roles = [f for f in flags if f not in KNOWN_SPECIALS]
            label = roles[0].decode() if roles else fullname
            role = ROLE_MAP.get(label, None)
            can_select = b'\\noselect' not in flags
            byfullname[fullname] = {
                # expecting uidvalidity is generated as unique timestamp by imap
                'id': f"f{status[b'UIDVALIDITY']}",
                'parentId': None,
                'name': fullname,
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
                'sep': sep.decode(),
                # Data sync properties
                'createdModSeq': status[b'UIDVALIDITY'],  # TODO: persist
                'updatedModSeq': status[b'UIDVALIDITY'],  # TODO: from persistent storage
                'updatedNotCountsModSeq': status[b'UIDVALIDITY'],  # TODO: from persistent storage
                'highestUID': status[b'UIDNEXT'] - 1,
                'emailHighestModSeq': status[b'HIGHESTMODSEQ'],
                'deleted': 0,
            }

        # set name and parentId for child folders
        for fullname, mailbox in byfullname.items():
            names = fullname.rsplit(mailbox['sep'], maxsplit=1)
            if len(names) == 2:
                mailbox['parentId'] = byfullname[names[0]]['id']
                mailbox['name'] = names[1]

        # update cache
        self.mailboxes = {mbox['id']: mbox for mbox in byfullname.values()}
        return byfullname.values()


    def sync_mailboxes(self):
        self.get_mailboxes()
    

    def mailbox_fullname(self, parentId, name):
        while parentId:
            parent = self.mailboxes.get(parentId, None)
            if not parent:
                raise errors.notFound('parent folder not found')
            name = parent['name'] + parent['sep'] + name
            parentId = parent.get('parentId', None)
        return name


    def create_mailbox(self, name=None, parentId=None, isSubscribed=True, **kwargs):
        if not name:
            raise errors.invalidProperties('name is required')
        fullname = self.mailbox_fullname(parentId, name)
        # TODO: parse returned MAILBOXID
        try:
            res = self.imap.create_folder(fullname)
        except IMAPClientError as e:
            desc = str(e)
            if '[ALREADYEXISTS]' in desc:
                raise errors.invalidArguments(desc)
        except Exception:
            raise errors.serverFail(res.decode())

        if not isSubscribed:
            self.imap.unsubscribe_folder(fullname)

        status = self.imap.folder_status(fullname, ['UIDVALIDITY'])
        self.sync_mailboxes()
        return f"f{status[b'UIDVALIDITY']}"


    def update_mailbox(self, id, name=None, parentId=None, isSubscribed=None, sortOrder=None, **update):
        mailbox = self.mailboxes.get(id, None)
        if not mailbox:
            raise errors.notFound('mailbox not found')
        fullname = self.mailbox_fullname(mailbox['parentId'], mailbox['name'])

        if (name is not None and name != mailbox['name']) or \
           (parentId is not None and parentId != mailbox['parentId']):
            if not name:
                raise errors.invalidProperties('name is required')
            newfullname = self.mailbox_fullname(parentId, name)
            res = self.imap.rename_folder(fullname, newfullname)
            if b'NO' in res or b'BAD' in res:
                raise errors.serverFail(res.encode())

        if isSubscribed is not None and isSubscribed != mailbox['isSubscribed']:
            if isSubscribed:
                res = self.imap.subscribe_folder(fullname)
            else:
                res = self.imap.unsubscribe_folder(fullname)
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
        fullname = self.mailbox_fullname(mailbox['parentId'], mailbox['name'])
        res = self.imap.delete_folder(fullname)
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

        self.dbh.execute("CREATE TABLE IF NOT EXISTS imsgidtodo (msgid TEXT PRIMARY KEY NOT NULL)");


def as_imap_sort(sort):
    criteria = []
    for crit in sort:
        for prop, field in [
                ('sentAt', 'DATE'),
                ('size', 'SIZE'),
                ('subject', 'SUBJECT'),
                ('from', 'FROM'),
                ('to', 'TO'),
                ('cc', 'CC'),
            ]:
            if crit['property'] == prop:
                if crit['isAscending']:
                    criteria.append('REVERSE')
                criteria.append(field)
    return criteria


def as_imap_search(filter):
    operator = filter.get('operator', None)
    if operator:
        conds = filter['conds']
        if operator == 'NOT':  # NOR
            return ['NOT', [as_imap_search(c) for c in conds]]
        elif operator == 'OR':
            if len(conds) == 1:
                return as_imap_search(conds[0])
            elif len(conds) == 2:
                return ['OR', as_imap_search(conds[0]), as_imap_search(conds[1])]
            elif len(conds) > 2:
                return [
                    'OR',
                    as_imap_search(conds[0]),
                    as_imap_search({'operator': 'OR', 'conditions': conds[1:]})
                ]
            raise errors.unsupportedFilter(f"Empty conditions")
        elif operator == 'AND':
            return [as_imap_search(c) for c in conds]
        raise errors.unsupportedFilter(f"Invalid operator {operator}")

    criteria = []

    if 'header' in filter:
        criteria.extend(filter['header'])

    for field in ('before','after','text','from','to','cc','bcc','subject','body'):
        if field in filter:
            criteria.append(field)
            criteria.append(filter[field])
    for cond, crit in [
            ('minSize', 'NOT SMALLER'),
            ('maxSize', 'NOT LARGER'),
            ('hasKeyword', 'KEYWORD'),
            ('notKeyword', 'UNKEYWORD'),
        ]:
        if cond in filter:
            criteria.append(crit)
            criteria.append(filter[cond])
    return criteria


def as_imap_fields(fields):
    # TODO: map to imap names
    return fields

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


def _envelopedata(data):
    envelope = json.loads(data)
    print('envelope:', envelope)
    encsub = envelope.get('Subject', '')
    try:
        encsub = str(make_header(decode_header(encsub)))
    except Exception:
        pass
    sortsub = _normalsubject(encsub)
    return {
        'sentAt': datetime.fromisoformat(envelope['Date']).isoformat(),
        'subject': encsub,
        'sortsubject': sortsub,
        'sender': json.dumps(envelope.get('Sender', [])),
        'from': json.dumps(envelope.get('From', [])),
        'to': json.dumps(envelope.get('To', [])),
        'cc': json.dumps(envelope.get('Cc', [])),
        'bcc': json.dumps(envelope.get('Bcc', [])),
        'replyTo': json.dumps(envelope.get('Reply-To', [])),
        'inReplyto': envelope.get('In-Reply-To', None),
        'messageId': envelope.get('Message-ID', None),
    }


def jsonDefault(obj):
    if isinstance(obj, Envelope):
        envelope = {'Date': obj.date}
        if obj.subject:
            envelope['Subject'] = obj.subject.decode()
        if obj.in_reply_to:
            envelope['In-Reply-To'] = obj.in_reply_to.decode()
        if obj.message_id:
            envelope['Message-ID'] = obj.message_id.decode()
        for field, attr in [
                ('Sender', 'sender'),
                ('Reply-To', 'reply_to'),
                ('From', 'from_'),
                ('To', 'to'),
                ('Cc', 'cc'),
                ('Bcc', 'bcc')]:
            if getattr(obj, attr):
                envelope[field] = [{
                    'name': a.name and str(make_header(decode_header(a.name.decode()))),
                    'email': (b'%s@%s' % (a.mailbox, a.host)).decode(),
                    } for a in getattr(obj, attr)]
        return envelope

    raise TypeError
