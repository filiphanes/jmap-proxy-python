from db import DB
from time import time
import json
import hashlib

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

class ImapDB(DB):
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

    def backend_cmd(self, cmd, *args, **kwargs):
        if self.in_transaction():
            return print('in transaction')
        if not hasattr(self, 'backend'):
            config = self.access_data()
            self.backend = jmap.sync.Standard(config)
        method = getattr(self.backend, cmd, None)
        if not method:
            raise NotImplementedError(f'No such command {cmd}')
        return method(*args, **kwargs)
    
    def sync_folders(self):
        "Synchronise list from IMAP to local folder cache"
        prefix, folders = self.backend.folders()
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
            for name, status in self.backend.imap_status(*getstatus.keys()).items():
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
        status = self.backend.imap_status(imapnames)
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
        
        res = self.backend.imap_fetch('imapname', {
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
            res = self.backend.imap_count(imapname, uidvalidity, f'{uidfirst}:{to}')
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
            res = self.backend.imap_search(item['imapname'], 'uid', f'{frm}:{to}', search)
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
        }, {'ifolderid': folderid, 'uid', uid})
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
        flags = set()
        for kw, flag in (
            ('$answered', '\\Answered'),
            ('$flagged', '\\Flagged'),
            ('$draft', '\\Draft'),
            ('$seen', '\\Seen'),
            ):
            flags.add(flag if kw in keywords else kw)
        internaldate = time()
        date = strftime(internaldate)
        appendres = self.backend.imap_append('imapname', '(' + ' '.join(flags) + ')', date, rfc822)
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
    
        



def _trimh(val):
    "DEPRECATED: Use directly val.trim()"
    return val.trim()