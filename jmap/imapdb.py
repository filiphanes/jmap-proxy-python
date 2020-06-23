from jmap.db import DB
from time import time
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
from jmap import email

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


KEYWORD2FLAG = (
    ('$answered', '\\Answered'),
    ('$flagged', '\\Flagged'),
    ('$draft', '\\Draft'),
    ('$seen', '\\Seen'),
)

class ImapDB(DB):
    def __init__(self, accountid, *args, **kwargs):
        super().__init__(accountid, *args, **kwargs)
        config = self.dgetone('iserver')
        if config:
            username, password, host, port, *_ = config
        else:
            username = accountid
            password = 'h'
            host = 'localhost'
            port = 143
            # raise Exception('User has no configured IMAP connection')
        self.imap = IMAPClient(host, port, use_uid=True, ssl=False)
        self.imap.login(username, password)
        print('Login', username, password)

    def setuser(self, args):
        # TODO: picture, ...
        self.begin()
        data = self.dgetone('iserver')
        if data:
            self.dmaybeupdate('iserver', args)
        else:
            self.dinsert('iserver', args)
        
        user = self.dgetone('account')
        if user:
            self.dmaybeupdate('account', {'email': args['username']})
        else:
            self.dinsert('account', {
                'email': args['username'],
                'jdeletedmodseq': 0,
                'jhighestmodseq': 1,
            })
        self.commit()

    def access_token(self):
        return self.dgetone('iserver', {}, 'imapHost,username,password')

    def access_data(self):
        return self.dgetone('iserver')
    
    def sync_folders(self):
        "Synchronise list from IMAP to local folder cache"
        self.cursor.execute("SELECT * FROM ifolders")
        ifolders = self.cursor.fetchall()
        ibylabel = {f['label']: f for f in ifolders}
        seen = set()
        getstatus = {}
        getuniqueid = {}

        prefix = ''
        for flags, sep, name in self.imap.list_folders():
            sep = sep.decode()
            roles = [f for f in flags if f.lower() not in KNOWN_SPECIALS]
            if roles:
                label = roles[0].decode()
            else:
                label = name
            if label in ibylabel:
                folder = ibylabel[label]
                id = folder['ifolderid']
                self.dmaybeupdate('ifolders',
                    {'sep': sep, 'imapname': name},
                    {'ifolderid': id})
                if not folder['uidvalidity']:
                    getstatus[name] = id
                if not folder['uniqueid']:
                    getuniqueid[name] = id
            else:
                id = self.dinsert('ifolders', {
                    'sep': sep,
                    'imapname': name,
                    'label': label,
                    })
                getstatus[name] = id
                getuniqueid[name] = id
            seen.add(id)
        
        # delete not existing folders
        for f in ifolders:
            if f['ifolderid'] not in seen:
                self.ddelete('ifolders', {'ifolderid': f['ifolderid']})
        
        self.dmaybeupdate('iserver', {
            'imapPrefix': prefix,
            'lastfoldersync': datetime.now(),
        })
        # self.commit()

        for name, id in getstatus.items():
            status = self.imap.folder_status(name, ['UIDVALIDITY', 'UIDNEXT', 'HIGHESTMODSEQ'])
            self.dmaybeupdate('ifolders', {
                'uidvalidity': status[b'UIDVALIDITY'],
                'highestmodseq': status[b'HIGHESTMODSEQ'],
                'uidnext': status[b'UIDNEXT'],
                'uidfirst': status[b'UIDNEXT'],
            }, {'ifolderid': id})
        self.commit()
        self.sync_jmailboxes()

    def sync_jmailboxes(self):
        "synchronise from the imap folder cache to the jmap mailbox listing"
        jmailboxes = self.dget('jmailboxes')

        jbyid = {}
        roletoid = {}
        byname = defaultdict(dict)
        for mbox in jmailboxes:
            jbyid[mbox['jmailboxid']] = mbox
            if mbox['role']:
                roletoid[mbox['role']] = mbox['jmailboxid']
            byname[mbox['parentId'] or ''][mbox['name']] = mbox['jmailboxid']
        
        seen = set()
        self.cursor.execute("SELECT * FROM ifolders")
        ifolders = self.cursor.fetchall()
        for folder in ifolders:
            print('folder:', list(folder))
            fname = folder['imapname']
            # check for roles first
            #TODO: n.decode('IMAP-UTF-7')
            bits = [n for n in fname.split(folder['sep'])]
            if bits[0] == 'INBOX' and len(bits) > 1:
                bits = bits[1:]
            if not bits:
                continue
            role = ROLE_MAP.get((folder['label'] or '').lower(), None)
            id = ''
            parentId = ''
            name = None
            if role:
                sortOrder = 2
            elif role == 'inbox':
                sortOrder = 1
            else:
                sortOrder = 3
            while bits:
                name, *bits = bits
                if id:
                    seen.add(id)
                parentId = id
                id = byname[parentId].get(name, None)
                if id is None and bits:
                    # need to create intermediate folder ...
                    # XXX  - label noselect?
                    id = str(uuid.uuid4())
                    self.dmake('jmailboxes', {
                            'name': name,
                            'jmailboxid': id,
                            'sortOrder': 4,
                            'parentId': parentId,
                        },
                        'jnoncountsmodseq')
                    byname[parentId][name] = id
            if not name:
                continue
            # TODO: get MYRIGHTS and SUBSCRIBED from server?
            details = {
                'name': name,
                'parentId': parentId,
                'sortOrder': sortOrder,
                'isSubscribed': 1,
                'mayReadItems': 1,
                'mayAddItems': 1,
                'mayRemoveItems': 1,
                'maySetSeen': 1,
                'maySetKeywords': 1,
                'mayCreateChild': 1,
                'mayRename': 0 if role else 1,
                'mayDelete': 0 if role else 1,
                'maySubmit': 1,
                'active': 1,
            }
            if id:
                if role and role in roletoid and roletoid[role] != id:
                    # still gotta move it
                    id = roletoid[role]
                    self.dmaybedirty('jmailboxes', details, {'jmailboxid': id}, ['jnoncountsmodseq'])
                elif not jbyid[id]['active']:
                    # reactivate!
                    self.dmaybedirty('jmailboxes', {'active': 1}, {'jmailboxid': id}, ['jnoncountsmodseq'])
            else:
                # case: role - we need to see if there's a case for moving this thing
                id = roletoid.get(role, None)
                if id:
                    self.dmaybedirty('jmailboxes', details, {'jmailboxid': id}, ['jnoncountsmodseq'])
                else:
                    id = folder['uniqueid'] or str(uuid.uuid4())
                    del details['active']
                    details['role'] = role
                    details['jmailboxid'] = id
                    self.dmake('jmailboxes', details, ['jnoncountsmodseq'])
                    byname[parentId][name] = id
                    if role:
                        roletoid[role] = id
            seen.add(id)
            self.dmaybeupdate('ifolders', {'jmailboxid': id},
                {'ifolderid': folder['ifolderid']})
            
        for mailbox in jmailboxes:
            id = mailbox['jmailboxid']
            if mailbox['active'] and id not in seen:
                self.dmaybeupdate('jmailboxes', {'active': 0}, {'jmailboxid': id})

        self.commit()

    def labels(self):
        self.cursor.execute('SELECT label,ifolderid,jmailboxid,imapname FROM ifolders')
        return {label: (ifolderid, jmailboxid, imapname) for label, ifolderid, jmailboxid, imapname in self.cursor}
    
    def sync_imap(self):
        self.cursor.execute('SELECT ifolderid,label,imapname,uidvalidity,highestmodseq FROM ifolders')
        rows = self.cursor.fetchall()
        for ifolderid, label, imapname, uidvalidity, highestmodseq in rows:
            status = self.imap.folder_status(imapname, ['UIDVALIDITY', 'HIGHESTMODSEQ'])
            # TODO: better handling of uidvalidity change?
            if status[b'UIDVALIDITY'] == uidvalidity and \
               status[b'HIGHESTMODSEQ'] == highestmodseq:
                continue
            self.do_folder(ifolderid, label)
        self.sync_jmap()

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
            self.do_folder(ifolderid, label, 50)
        self.sync_jmap()
        
    def calcmsgid(self, imapname, uid, msg):
        envelope = msg[b'ENVELOPE']
        print("msg[b'ENVELOPE']=", msg[b'ENVELOPE'])
        coded = json.dumps([envelope])
        base = hashlib.sha1(coded).hexdigest()[:9]
        msgid = 'm' + base
        in_reply_to = (envelope.get('In-Reply-To', '') or '').strip()
        messageid = (envelope.get('Message-ID', '') or '').strip()
        encsub = (envelope.get('Subject') or '')
        try:
            encsub = encsub.decode('MIME-Header')
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
        highestmodseq = 0 # comment in production
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
            new = self.imap.fetch((oldstate['uidnext'],'*'), fetch_data,fetch_modifiers)
            update = self.imap.fetch((uidfirst, oldstate['uidnext']-1), ('UID', 'FLAGS'))
            backfill = {}
        elif uidfirst > 1:
            end = uidfirst - 1
            uidfirst = max(uidfirst - batchsize, 1)
            new = {}
            update = {}
            self.backfilling = True
            backfill = self.imap.fetch((uidfirst, end), fetch_data, fetch_modifiers)
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
            e = msg[b'ENVELOPE']
            print(e.reply_to)
            envelope = {'Date': e.date}
            if e.subject:
                envelope['Subject'] = e.subject.decode()
            if e.in_reply_to:
                envelope['In-Reply-To'] = e.in_reply_to.decode()
            if e.message_id:
                envelope['Message-ID'] = e.message_id.decode()

            for field, attr in [
                    ('Sender', 'sender'),
                    ('Reply-To', 'reply_to'),
                    ('From', 'from_'),
                    ('To', 'to'),
                    ('Cc', 'cc'),
                    ('Bcc', 'bcc')]:
                if getattr(e, attr):
                    envelope[field] = [{
                        'name': a.name and a.name.decode(),
                        'email': (b'%s@%s' % (a.mailbox, a.host)).decode(),
                        } for a in getattr(e, attr)]
            msg[b'ENVELOPE'] = envelope

            msgid, thrid = self.calcmsgid(imapname, uid, msg)
            didold += 1
            self.new_record(ifolderid, uid, msg[b'FLAGS'], [forcelabel], msg[b'ENVELOPE'], msg[b'INTERNALDATE'], msgid, thrid, msg[b'RFC822.SIZE'])
        
        for uid, msg in update.items():
            print('msg:', msg)
            self.changed_record(ifolderid, uid, msg[b'FLAGS'], [forcelabel])

        for uid, msg in new.items():
            msgid, thrid = self.calcmsgid(imapname, uid, msg)
            self.new_record(ifolderid, uid, msg[b'FLAGS'], [forcelabel], msg[b'ENVELOPE'], msg[b'INTERNALDATE'], msgid, thrid, msg[b'RFC822.SIZE'])

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
            'flags': json.dumps([f for f in flags if f.lower() != '\\recent']),
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
        for kw, flag in KEYWORD2FLAG:
            if flags.pop(kw):
                flags.add(flag)
        internaldate = time()
        date = datetime.fromisoformat()(internaldate)
        appendres = self.imap.append('imapname', '(' + ' '.join(flags) + ')', date, rfc822)
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
        message = email.parse(rfc822, msgdata['msgid'])
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
        rows = self.dget('imessages', {
            'msgid': ('IN', msgids)},
            'msgid,ifolderid,uid')
        for msgid, ifolderid, uid in rows:
            if not msgid in map:
                map[msgid] = {}
            if not ifolderid in map[msgid]:
                map[msgid][ifolderid] = set()
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
        jidmap = {d['jmailboxid']: d.get('role', '') for d in jmapdata}
        jrolemap = {d['role']: d['jmailboxid'] for d in jmapdata if 'role' in d}

        for msgid in map.keys():
            action = changes[msgid]
            try:
                for ifolderid, uids in map[msgid].items():
                    # TODO: merge similar actions?
                    imapname = foldermap[ifolderid].get('imapname')
                    uidvalidity = foldermap[ifolderid].get('uidvalidity')
                    if imapname and uidvalidity and 'keywords' in action:
                        flags = set(action['keywords'])
                        for kw, flag in KEYWORD2FLAG:
                            if flags.pop(kw):
                                flags.add(flag)
                        self.imap.update(imapname, uidvalidity, uids, flags)

                if 'mailboxIds' in action:
                    mboxes = [idmap[k] for k in action['mailboxIds'].keys()]
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
            'flags': json.dumps([f.decode() for f in flags if f.lower() != b'\\recent']),
            'labels': json.dumps(sorted(labels)),
            'internaldate': internaldate,
            'msgid': msgid,
            'thrid': thrid,
            'envelope': json.dumps(envelope),
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
            for kw, f in KEYWORD2FLAG:
                if flag == f:
                    keywords[kw] = True
                    break
                else:
                    keywords[flag] = True
        
        slabels = self.labels()
        print('labels', labels)
        jmailboxids = [slabels[l][1] for l in labels]

        if not jmailboxids:
            return self.delete_message(msgid)
        self.cursor.execute("SELECT msgid FROM jmessages WHERE msgid=? AND active=1", [msgid])
        if self.cursor.fetchone():
            return self.change_message(msgid, {'keywords': keywords}, jmailboxids)
        else:
            self.cursor.execute("SELECT thrid,internaldate,size,envelope FROM imessages WHERE msgid=?", [msgid])
            msg = self.cursor.fetchone()
            return self.add_message({
                'msgid': msgid,
                'internaldate': msg['internaldate'],
                'thrid': msg['thrid'],
                'msgsize': msg['size'],
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
        
        ids = set(ids)
        result = {msgid: json.loads(parsed) for msgid, parsed in self.cursor}
        need = ids.difference(result.keys())
        udata = defaultdict(dict)
        if need:
            self.cursor.execute('SELECT ifolderid, uid, msgid FROM imessages WHERE msgid IN (' + ('?,' * len(need))[:-1] + ')',
                list(need))
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
                result[msgid] = email.parse(data[b'RFC822'])
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
    
    def create_mailboxes(self, new):
        if not new:
            return {}, {}
        todo = set()
        notcreated = {}
        for cid, mailbox in new.items():
            if not mailbox.get('name', ''):
                notcreated[cid] = {'type': 'invalidProperties', 'description': 'name is required'}
                continue
            try:
                encname = mailbox['name'].encode('IMAP-UTF-7')
            except Exception:
                notcreated[cid] = {'type': 'invalidProperties', 'description': 'name, can\'t be used with IMAP proxy'}
            if mailbox.get('parentId', None):
                row = self.dgetone('ifolders', {'jmailboxid': mailbox['parentId']}, 'imapname,sep')
                if not row:
                    notcreated[cid] = {'type': 'notFound', 'description': 'parent folder not found'}
                    continue
                todo[cid] = [row['imapname'] + row['sep'] + encname, row['sep']]
            else:
                for sep, in self.dbh.execute('SELECT sep FROM ifolders ORDER BY ifolderid'):
                    prefix = self.dgetfield('iserver', {}, 'imapPrefix')
                if not prefix:
                    prefix = ''
                todo[cid] = [prefix + encname, sep]
        createmap = {}
    
    def update_mailboxes(self, update, idmap):
        if not update:
            return {}, {}
        changed = {}
        notchanged = {}
        namemap = {}
        # XXX - reorder the crap out of this if renaming multiple mailboxes due to deep rename
        for jid, mailbox in update.items():
            if not mailbox:
                notchanged[jid] = {'type': 'invalidProperties', 'description': "nothing to change"}
            continue

            data = self.dgetone('jmailboxes', {'jmailboxid': jid})
            for key in mailbox.keys():  # TODO: check if valid
                data[key] = update[key]
            parentId = data.get('parentId', None)
            if parentId:
                parentId = idmap[parentId]
            
            try:
                encname = data['name'].encode('IMAP-UTF-7')
            except ValueError:
                notchanged[jid] = {'type': 'invalidProperties', 'description': "name can\'t be used with IMAP proxy"}
                continue

            old = self.dgeone('ifolders', {'jmailboxid': jid}, 'imapname,ifolderid')
            if parentId:
                parent = self.dgetone('ifolders', {'jmailboxid': parentId}, 'imapname,sep')
                if not parent:
                    notchanged[jid] = {'type': 'invalidProperties', 'description': "parent folder not found"}
                namemap[old['imapname']] = (parent['imapname'] + parent['sep'] + encname, jid, old['ifolderid'])
            else:
                prefix = self.dgetfield('iserver', {}, 'imapPrefix') or ''
                namemap = [old['imapname']] = (prefix + encname, jid, old['ifolderid'])
        
        toupdate = {}
        for oldname in namemap.keys():
            imapname, jid, ifolderid = namemap[oldname]
            if imapname == oldname:
                changed[jid] = None
                continue
            res = self.imap.rename_mailbox(oldname, imapname)
            if res[1] == 'ok':
                changed[jid] = None
                toupdate[jid] = (imapname, ifolderid)
            else:
                notchanged[jid] = {'type': 'serverError', 'description': res[2]}

        for jid in toupdate.keys():
            impaname, ifolderid = toupdate[jid]
            change = update[jid]
            self.dmaybeupdate('ifolders', {'imapname': imapname}, {'ifolderid': ifolderid})
            changes = {}
            if 'name' in change:
                changes['name'] = change['name']
            if 'parentId' in change:
                changes['parentId'] = change['parentId']
            if 'sortOrder' in change:
                changes['sortOrder'] = change['sortOrder']
            self.dmaybedirty('jmailboxes', changes, {'jmailboxid': jid})
        self.commit()
        return changed, notchanged

    def destroy_mailboxes(self, destroy, destroyMessages):
        if not destroy:
            return [], {}
        
        destroyed = []
        notdestroyed = {}
        namemap = {}
        for jid in destroy:
            old = self.dgetone('ifolders', {'jmailboxid': jid}, 'imapname,ifolderid')
            if old:
                if not destroyMessages:
                    # check if empty
                    if self.dcount('imessages', {'ifolderid', old['ifolderid']}):
                        notdestroyed[jid] = {'type': 'mailboxHasEmail'}
                        continue
                namemap[old['imapname']] = (jid, old['ifolderid'])
            else:
                notdestroyed[jid] = {'type': 'invalidProperties', 'description': 'parent folder not found'}
        
        # we reverse so we delete children before parents
        toremove = {}
        for oldname in reversed(sorted(namemap.keys())):
            jid, ifolderid = namemap[oldname]
            res = self.imap.delete_mailbox(oldname)
            if res[1] == 'ok':
                destroyed.append(jid)
                toremove[jid] = ifolderid
            else:
                notdestroyed[jid] = {'type': 'serverError', 'description': res[2]}
        
        if toremove:
            for jid in sorted(toremove.keys()):
                ifolderid = toremove[jid]
                self.ddelete('ifolders', {'ifolderid', ifolderid})
                self.ddupdate('jmailboxes', {'active': 0}, {'jmailboxid': jid})
            self.commit()

        return destroyed, notdestroyed

    def create_submission(self, new, idmap):
        if not new:
            return {}, {}
        
        todo = {}
        createmap = {}
        notcreated = {}
        for cid, sub in new.items():
            msgid = idmap[sub['emailId']]
            if not msgid:
                notcreated[cid] = {'error': 'nos msgid provided'}
                continue
            thrid = self.dgetfield('jmessages', {'msgid': msgid, 'active': 1}, 'thrid')
            if not thrid:
                notcreated[cid] = {'error': 'message does not exist'}
                continue
            id = self.dmake('jsubmission', {
                'sendat': datetime.fromisoformat()(sub['sendAt']) if sub['sendAt'] else time(),
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
            active = self.dgetfield('jsubmission', {'jsubid': subid}, 'active')
            if active:
                destroy.append(subid)
                self.ddelete('jsubmission', {'jsubid': subid})
            else:
                notdestroyed[subid] = {'type': 'notFound', 'description': 'submission not found'}
        self.commit()
        return destroyed, notdestroyed
    
    def _initdb(self):
        super()._initdb()
        # XXX - password encryption?
        self.dbh.execute("""
            CREATE TABLE IF NOT EXISTS iserver (
            username TEXT PRIMARY KEY,
            password TEXT,
            imapHost TEXT,
            imapPort INTEGER,
            imapSSL INTEGER,
            imapPrefix TEXT,
            smtpHost TEXT,
            smtpPort INTEGER,
            smtpSSL INTEGER,
            caldavURL TEXT,
            carddavURL TEXT,
            lastfoldersync DATE,
            mtime DATE NOT NULL
            )""" )

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

        # not used for Gmail, but it doesn't hurt to have it
        self.dbh.execute("""
            CREATE TABLE IF NOT EXISTS ithread (
            messageid TEXT PRIMARY KEY,
            sortsubject TEXT,
            thrid TEXT
            )""")

        self.dbh.execute("CREATE INDEX IF NOT EXISTS ithrid ON ithread (thrid)");

        self.dbh.execute("""
            CREATE TABLE IF NOT EXISTS icalendars (
            icalendarid INTEGER PRIMARY KEY NOT NULL,
            href TEXT,
            name TEXT,
            isReadOnly INTEGER,
            sortOrder INTEGER,
            color TEXT,
            syncToken TEXT,
            jcalendarid INTEGER,
            mtime DATE NOT NULL
            )""")

        self.dbh.execute("""
            CREATE TABLE IF NOT EXISTS ievents (
            ieventid INTEGER PRIMARY KEY NOT NULL,
            icalendarid INTEGER,
            href TEXT,
            etag TEXT,
            uid TEXT,
            content TEXT,
            mtime DATE NOT NULL
            )""")

        self.dbh.execute("CREATE INDEX IF NOT EXISTS ieventcal ON ievents (icalendarid)");
        self.dbh.execute("CREATE INDEX IF NOT EXISTS ieventuid ON ievents (uid)");

        self.dbh.execute("""
            CREATE TABLE IF NOT EXISTS iaddressbooks (
            iaddressbookid INTEGER PRIMARY KEY NOT NULL,
            href TEXT,
            name TEXT,
            isReadOnly INTEGER,
            sortOrder INTEGER,
            syncToken TEXT,
            jaddressbookid INTEGER,
            mtime DATE NOT NULL
            )""")

        # XXX - should we store 'kind' in this?  Means we know which j table to update
        # if someone reuses a UID from a contact to a group or vice versa...
        self.dbh.execute("""
            CREATE TABLE IF NOT EXISTS icards (
            icardid INTEGER PRIMARY KEY NOT NULL,
            iaddressbookid INTEGER,
            href TEXT,
            etag TEXT,
            uid TEXT,
            kind TEXT,
            content TEXT,
            mtime DATE NOT NULL
            )""")

        self.dbh.execute("CREATE INDEX IF NOT EXISTS icardbook ON icards (iaddressbookid)");
        self.dbh.execute("CREATE INDEX IF NOT EXISTS icarduid ON icards (uid)");

        self.dbh.execute("CREATE TABLE IF NOT EXISTS imsgidtodo (msgid TEXT PRIMARY KEY NOT NULL)");

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
        # TODO: implement correct decoder
        encsub = encsub.decode('MIME-HEADER')
    except Exception:
        pass
    sortsub = _normalsubject(encsub)
    return {
        'msgdate': envelope['Date'],
        'msgsubject': encsub,
        'sortsubject': sortsub,
        'msgfrom': json.dumps(envelope.get('From', [])),
        'msgto': json.dumps(envelope.get('To', [])),
        'msgcc': json.dumps(envelope.get('Cc', [])),
        'msgbcc': json.dumps(envelope.get('Bcc', [])),
        'msginreplyto': envelope.get('In-Reply-To', None),
        'msgmessageid': envelope.get('Message-ID', None),
    }


def _trimh(val):
    "DEPRECATED: Use directly val.strip()"
    return val.strip()