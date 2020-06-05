from jmap.db import DB
from time import time
import hashlib
from collections import defaultdict
import re
import imaplib
import datetime
try:
    import orjson as json
except ImportError:
    import json

TAG = 1

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
        self.imap = imaplib.IMAP4(
            host=host,
            port=port,
            )
        self.imap.login(username, password)

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
        prefix, folders = self.imap.folders()
        ifolders = self.dget('ifolders')
        ibylabel = {f['label']: f for f in ifolders}
        seen = set()
        getstatus = {}
        getuniqueid = {}

        for name in folders.keys():
            sep = folders[name][0]
            label = folders[name][1]
            id = ibylabel[label]['ifolderid']
            if id:
                self.dmaybeupdate('ifolders',
                    {'sep': sep, 'imapname': name},
                    {'ifolderid': id})
            else:
                id = self.dinsert('ifolders', {
                    'sep': sep,
                    'imapname': name,
                    'label': label,
                    })
            seen.add(id)
            if not ibylabel[label]['uidvalidity']:
                getstatus[name] = id
            if not ibylabel[label]['uniqueid']:
                getuniqueid[name] = id
        
        # delete not existing folders
        for f in ifolders:
            if f['ifolderid'] not in seen:
                self.ddelete('ifolders', {'ifolderid': f['ifolderid']})
        
        self.maybeupdate('iserver', {
            'imapPrefix': prefix,
            'lastfoldersync': time(),
        })
        self.commit()

        if getstatus:
            for name, status in self.imap.status(*getstatus.keys()).items():
                if isinstance(status, 'dict'):
                    self.dmaybeupdate('ifolders', {
                        'uidvalidity': status['uidvalidity'],
                        'uidnext': status['uidnext'],
                        'uidfirst': status['uidnext'],
                        'highestmodseq': status['highestmodseq'],
                    }, {'ifolderid': getstatus[name]})
            self.commit()
        
        if getuniqueid:
            for name, status in self.imap_getuniqueid(*getuniqueid.keys()):
                if isinstance(status, 'dict'):
                    self.dmaybeupdate('ifolders', {
                        'uniqueid': status['/vendor/cmu/cyrus-imapd/uniqueid'],
                    }, {'ifolderid': getstatus[name]})
            self.commit()
        
        self.sync_jmailboxes()

    def sync_jmailboxes(self):
        "synchronise from the imap folder cache to the jmap mailbox listing"
        ifolders = self.dget('ifolders')
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
        for folder in ifolders:
            if folder['label'].lower() == '\\allmail':
                # we dont show this folder
                continue
            fname = folder['imapname']
            # check for roles first
            bits = [n.decode('IMAP-UTF-7') for n in fname.split(folder['sep'])]
            if bits[0] == 'INBOX' and len(bits) > 1:
                bits = bits[1:]
            if bits[0] == '[Gmail]':
                bits = bits[1:]
            if not bits:
                continue
            role = ROLE_MAP[folder['label'].lower()]
            id = ''
            parentId = ''
            name = None
            if role:
                sortOrder = 3
            elif role == 'inbox':
                sortOrder = 1
            else:
                sortOrder = 3
            while bits:
                item = bits[0]
                bits = bits[1:]
                if id:
                    seen.add(id)
                name = item
                parentId = id
                id = byname[parentId][name]
                if not id and bits:
                    id = new_uuid_string()
                    self.dmake('jmailboxes', {
                        'name': name,
                        'jmailboxid': id,
                        'sortOrder': 4,
                        'parentId': parentId,
                    }, 'jnoncountsmodseq')
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
                if role and roletoid[role] and roletoid[role] != id:
                    # still gotta move it
                    id = roletoid[role]
                    self.dmaybedirty('jmailboxes', details, {'jmailboxid': id}, 'jnoncountsmodseq')
                elif not folder['active']:
                    # reactivate!
                    self.dmaybedirty('jmailboxes', {'active': 1}, {'jmailboxid': id}, 'jnoncountsmodseq')
            else:
                # case: role - we need to see if there's a case for moving this thing
                id = roletoid.get(role, None)
                if id:
                    self.dmaybedirty('jmailboxes', details, {'jmailboxid': id}, 'jnoncountsmodseq')
                else:
                    id = folder['uniqueid'] or new_uuid_string()
                    del details['active']
                    details['role'] = role
                    details['jmailboxid'] = id
                    self.dmake('jmailboxes', details, 'jnoncountsmodseq')
                    byname[parentId][name] = id
                    if role:
                        roletoid[role] = id
                seen.add(id)
                self.dmaybeupdate('ifolders', {'jmailboxid': id},
                    {'ifolderid': folder['ifolderid']})
            
        for mailbox in jmailboxes:
            id = mailbox['jmailboxid']
            if mailbox['active'] and id not in seen:
                self.dmaybeupdate('jmailboxes', {'active': 0}, {'jmailboxid': id}, 'jnoncountsmodseq')

        self.commit()

    def labels(self):
        rows = self.dget('ifolders', {}, 'ifolderid,jmailboxid,imapname')
        return {row['label']: row for row in rows}
    
    def sync_imap(self):
        rows = self.dget('ifolders').fetchall()
        imapnames = [row['imapname'] for row in rows]
        status = self.imap.status(imapnames)
        for row in data:
            # TODO: better handling of uidvalidity change?
            if status[row['imapname']['uidvalidity']] == row['uidvalidity'] and status[row['imapname']].get('highestmodseq', None) == row['highestmodseq']:
                continue
            label = row['label']
            if label.lower() == '\\allmail':
                label = None 
            self.do_folder(row['ifolderid'], label)
        self.sync_jmap()

    def backfill(self):
        rest = 500
        rows = self.dget('ifolders', {
            'uidnext': ('>', 1),
            'uidfirst': ('>', 1),
        }, 'ifolderid,label')

        if rows:
            for id, label in rows:
                if label.lower() == '\\allmail':
                    label = None
                rest -= self.do_folder(id, label, rest)
                if rest < 10:
                    break
            self.sync_jmap()
            return 1
    
    def firstsync(self):
        self.sync_folders()
        rows = self.dget('ifolders')
        for row in rows:
            if row['imapname'].lower() == 'inbox':
              self.do_folder(row['ifolderid'], row['label'], 50)
              break
        self.sync_jmap()
    
    def calclabels(self, forcelabel, row):
        if forcelabel:
            return forcelabel
        try:
            return row['x-gm-labels']
        except KeyError:
            print(f'No way to calculate labels for {row}')
    
    def calcmsgid(self, imapname, uid, msg):
        if 'digest.sha1' in msg and 'cid' in msg:
            return msg['digest.sha1'], msg['cid']
        
        envelope = msg['envelope']
        coded = json.dumps([envelope])
        base = hashlib.sha1(coded).hexdigest()[:9]
        msgid = 'm' + base
        replyto = envelope.get('In-Reply-To', '').trim()
        messageid = envelope.get('Message-ID', '').trim()
        encsub = envelope.get('Subject', '')
        try:
            encsub = encsub.decode('MIME-Header')
        except Exception:
            pass
        sortsub = _normalsubject(encsub)
        rows = self.dbh.execute('SELECT DISTIMCT thrid FROM ithread'
               ' WHERE messageid IN (?,?) AND sortsubject=? LIMIT 1',
               (replyto, messageid, sortsub))
        try:
            thrid = rows.fetchone()[0]
        except Exception:
            thrid = 't' + base
        for id in (replyto, messageid):
            if id:
                self.dbh.execute('INSERT OR IGNORE INTO ithread (messageid, thrid, sortsubject) VALUES (?,?,?)', (id, thrid, sortsub))
        return msgid, thrid
    
    def do_folder(self, ifolderid, forcelabel, batchsize=0):
        data = self.dgetone('ifolders', {'ifolderid': ifolderid})
        if not data:
            return print(f'NO SUCH FOLDER {ifolderid}')
        imapname = data['imapname']
        uidfirst = data['uidfirst']
        uidnext = data['uidnext']
        uidvalidity = data['uidvalidity']
        highestmodseq = data['highestmodseq']
        fetches = {}
        if batchsize:
            if uidfirst > 1:
                end = uidfirst - 1
                uidfirst -= batchsize
                if uidfirst < 1:
                    uidfirst = 1
                fetches['backfill'] = (uidfirst, end, 1)
        else:
            fetches['new'] = (uidnext, '*', 1)
            fetches['update'] = (uidfirst, uidnext - 1, 0, highestmodseq)
        if not fetches:
            return
        
        res = self.imap.fetch('imapname', {
            'uidvalidity': uidvalidity,
            'highestmodseq': highestmodseq,
            'uidnext': uidnext,
        }, fetches)

        if res['newstate']['uidvalidity'] != uidvalidity:
            # going to want to nuke everything for the existing folder and create this  - but for now, just die
            raise Exception(f"UIDVALIDITY CHANGED {imapname}: {uidvalidity} => res['newstate']['uidvalidity'] {data}")
            
        self.begin()
        if batchsize:
            self.t.backfilling = 1
        didold = 0
        for uid, msg in res['backfill'][1].items():
            msgid, thrid = self.calcmsgid(imapname, uid, msg)
            labels = self.calclabels(forcelabel, msg)
            didold += 1
            self.new_record(ifolderid, uid, msg['flags'], labels, msg['envelope'], strptime(msg['internaldate']), msgid, thrid, msg['rfc822.size'])
        
        for uid, msg in res['new'][1]:
            msgid, thrid = self.calcmsgid(imapname, uid, msg)
            labels = self.calclabels(forcelabel, msg)
            self.new_record(ifolderid, uid, msg['flags'], labels, msg['envelope'], strptime(msg['internaldate']), msgid, thrid, msg['rfc822.size'])

        self.dupdate('ifolders', {
            'highestmodseq': res['newstate']['highestmodseq'],
            'uidfirst': uidfirst,
            'uidnext': res['newstate']['uidnext'],
        }, {'ifolderid': ifolderid})
        self.commit()

        if batchsize:
            return didold

        count, = self.dbh.executre('SELECT COUNT(*) FROM imessages WHERE ifolderid=?', [ifolderid]).fetchone()

        if uidfirst != 1 or count != res['newstate']['exists']:
            # welcome to the future
            uidnext = res['newstate']['uidnext']
            to = uidnext - 1
            res = self.imap.count(imapname, uidvalidity, f'{uidfirst}:{to}')
            rows = self.dbh.execute('SELECT uid FROM imessages WHERE ifolderid = ? AND uid >= ? AND uid <= ?', [ifolderid, uidfirst, to])
            exists = set(res['data'])
            for uid, in rows:
                if uid not in exists:
                    self.deleted_record(ifolderid, uid)
            self.commit()
    
    def imap_search(self, *search):
        matches = set()
        for item in self.dget('ifolders'):
            frm = item['uidfirst']
            to = item['uidnext'] - 1
            res = self.imap.search(item['imapname'], 'uid', f'{frm}:{to}', search)
            if not res[2] == item['uidvalidity']:
                continue
            for uid in res[3]:
                msgid = self.dgetfield('imessages', {
                    'ifolderid': item['ifolderid'],
                    'uid': uid,
                }, 'msgid')
                matches.add(msgid)
        return matches

    def mark_sync(self, msgid):
        self.dbh.execute('INSERT OR IGNORE INTO imsgidtodo (msgid) VALUES (?)', [msgid])
    
    def changed_record(self, ifolderid, uid, flags=(), labels=()):
        res = self.dmaybeupdate('imessages', {
            'flags': json.dumps([f for f in sorted(flags) if f.lower() != '\\recent']),
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
        date = strftime(internaldate)
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
        message = jmap.EmailObject.parse(rfc822, msgdata['msgid'])
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
            'flags': json.dumps([f for f in flags if f.lower() != '\\recent']),
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
        for row in self.dget('imessages', {'msgid': msgid}, 'flags,labels'):
            flags.update(json.loads(row['flags']))
            labels.update(json.loads(row['labels']))

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
        jmailboxids = [slabels[l][1] for l in labels]

        if not jmailboxids:
            return self.delete_message(msgid)
        
        if self.dgetfield('jmessages', {'msgid': msgid, 'active': 1}, 'msgid'):
            return self.change_message(msgid, {'keywords': keywords}, jmailboxids)
        else:
            data = self.dgetone('imessages', {'msgid': msgid}, 'thrid,internaldate,size,envelope')
            return self.add_message({
                'msgid': msgid,
                'internaldate': data['internaldate'],
                'thrid': data['thrid'],
                'msgsize': data['size'],
                'keywords': keywords,
                'isDraft': '$draft' in keywords,
                'isUnread': '$seen' not in keywords,
                **_envelopedata(data['envelope']),
            }, jmailboxids)

    def sync_jmap(self):
        for msgid in self.dgetcol('imsgidtodo', {}, 'msgid'):
            self.sync_jmap_msgid()
            self.ddelete('imsgidtodo', {'msgid': msgid})

    def fill_messages(self, ids):
        if not ids:
            return
        ids = set(ids)
        rows = self.dbh.execute('SELECT msgid, parsed FROM jrawmessage WHERE msgid IN (' + ('?,' * len(ids))[:-1] + ')')
        result = {}
        for msgid, parsed in rows:
            result[msgid] = json.loads(parsed)

        need = ids.difference(result.keys())
        udata = defaultdict(dict)
        if need:
            uids = self.dbh.execute('SELECT ifolderid, uid, msgid FROM imessages WHERE msgid IN (' + ('?,' * len(need))[:-1] + ')')
            for ifolderid, uid, msgid in uids:
                udata[ifolderid][uid] = msgid
        
        foldermap = {}
        for ifolderid, uhash in udata.items():
            uids = ','.join(u for u in uhash.keys() if u not in result[u])
            if uids:
                foldermap[ifolderid] = self.dgetone('ifolders', {'ifolderid': ifolderid}, 'imapname,uidvalidity')
        
        if not udata:
            return result
        
        parsed = {}
        for ifolderid, uhash in udata.items():
            uids = ','.join(u for u in uhash.keys() if u not in result[u])
            if not uids or not foldermap[ifolderid]:
                continue
            imapname, uidvalidity = foldermap[ifolderid]
            res = self.imap.fill(imapname, uidvalidity, uids)
            for uid in res['data'].keys():
                rfc822 = res['data'][uid]
                if rfc822:
                    msgid = uhash[uid]
                    if msgid not in result:
                        result[msgid] = parsed[msgid] = jmap.EmailObject.parse(rfc822, msgid)
        
        self.begin()
        for msgid, message in parsed.items():
            self.dinsert('jrawmessage', {
                'msgid': msgid,
                'parsed': json.dumps(message),
                'hasAttachment': message.get('hasAttachment', False),
            })
        self.commit()

        # XXX - handle not getting data that we need?
        # stillneed = ids.difference(result.keys())
        return result

    def get_raw_message(self, msgid, part):
        imapname, uidvalidity, uid = self.dhh.execute('SELECT imapname,uidvalidity,uid FROM ifolders JOIN imessages USING (ifolderid) WHERE msgid=?', [msgid])
        if not imapname:
            return None
        typ = 'message/rfc822'
        if part:
            parsed = self.fill_messages(msgid)
            typ = find_type(parsed[msgid], part)

        res = self.imap.getpart(imapname, uidvalidity, uid, part)
        return type, res['data']
    
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
                'sendat': str2time(sub['sendAt']) if sub['sendAt'] else time(),
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
    
def _envelopedata(data='{}'):
    envelope = json.loads(data)
    encsub = envelope.get('subject', '').decode('MIME-HEADER')
    if not encsub:
        encsub = envelope['subject']
    sortsub = _normalsubject(encsub)
    return {
        'msgsubject': encsub,
        'sortsubject': sortsub,
        'msgfrom': envelope.get('From', ''),
        'msgto': envelope.get('To', ''),
        'msgcc': envelope.get('To', ''),
        'msgbcc': envelope.get('Cc', ''),
        'msgdate': str2time(envelope.get('Date', '')),
        'msginreplyto': envelope.get('In-Reply-To', '').strip(),
        'msgmessageid': envelope.get('Message-ID', '').strip(),
    }


def _trimh(val):
    "DEPRECATED: Use directly val.trim()"
    return val.trim()