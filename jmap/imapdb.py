from db import DB
from time import time

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
