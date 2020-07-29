from jmap.account.imap.imap_utf7 import imap_utf7_decode, imap_utf7_encode

KNOWN_SPECIALS = set('\\HasChildren \\HasNoChildren \\NoSelect \\NoInferiors \\UnMarked \\Subscribed'.lower().split())

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


class ImapMailbox(dict):
    __slots__ = ('db',)

    def __missing__(self, key):
        return getattr(self, key)()

    def name(self):
        try:
            parentname, name = self['imapname'].rsplit(self['sep'], maxsplit=1)
        except ValueError:
            name = self['imapname']
        self['name'] = imap_utf7_decode(name.encode())
        return self['name']

    def parentId(self):
        try:
            parentname, name = self['imapname'].rsplit(self['sep'], maxsplit=1)
            self['parentId'] = self.db.byimapname[parentname]['id']
        except ValueError:
            self['parentId'] = None
        return self['parentId']
        
    def role(self):
        for f in self['flags']:
            if f not in KNOWN_SPECIALS:
                self['role'] = ROLE_MAP.get(f, None)
                break
        else:
            self['role'] = ROLE_MAP.get(self['imapname'].lower(), None)
        return self['role']

    def sortOrder(self):
        return 2 if self['role'] else (1 if self['role'] == 'inbox' else 3)

    def isSubscribed(self):
        return '\\subscribed' in self['flags']

    def totalEmails(self):
        return 0

    def totalThreads(self):
        return self['totalEmails']

    def unreadEmails(self):
        return 0

    def unreadThreads(self):
        return self['unreadEmails']

    def myRights(self):
        can_select = '\\noselect' not in self['flags']
        self['myRights'] = {
            'mayReadItems': can_select,
            'mayAddItems': can_select,
            'mayRemoveItems': can_select,
            'maySetSeen': can_select,
            'maySetKeywords': can_select,
            'mayCreateChild': True,
            'mayRename': False if self['role'] else True,
            'mayDelete': False if self['role'] else True,
            'maySubmit': can_select,
        }
        return self['myRights']

    def imapname(self):
        encname = imap_utf7_encode(self['name']).decode()
        if self['parentId']:
            parent = self.db.mailboxes[self['parentId']]
            self['imapname'] = parent['imapname'] + parent['sep'] + encname
        else:
            self['imapname'] = encname
        return self['imapname']

    def created(self):
        return self['uidvalidity']

    def updated(self):
        return self['uidvalidity'] * self['uidnext']

    def deleted(self):
        return None

