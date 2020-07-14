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


class ImapMailbox(dict):
    def __missing__(self, key):
        self[key] = getattr(self, key)()
        return self[key]

    def name(self):
        try:
            parentname, name = self['imapname'].rsplit(self['sep'], maxsplit=1)
            self['parentId'] = self['byimapname'][parentname]['id']
            return name
        except ValueError:
            return self['imapname']

    def parentId(self):
        try:
            parentname, name = self['imapname'].rsplit(self['sep'], maxsplit=1)
            self['name'] = name
            return self['byimapname'][parentname]['id']
        except ValueError:
            return None
        
    def role(self):
        for f in (F.lower() for F in self['flags']):
            if f not in KNOWN_SPECIALS:
                return ROLE_MAP.get(f.decode(), None)
        return ROLE_MAP.get(self['imapname'].lower(), None)

    def sortOrder(self):
        return 2 if self['role'] else (1 if self['role'] == 'inbox' else 3)

    def isSubscribed(self):
        return True  # TODO: use LSUB

    def totalThreads(self):
        return self['totalEmails']

    def unreadEmails(self):
        return 0

    def unreadThreads(self):
        return self['unreadEmails']

    def myRights(self):
        can_select = b'\\noselect' not in self['flags']
        return {
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

    def imapname(self):
        if self['parentId']:
            parent = self['byid'][self['parentId']]
            return parent['imapname'] + parent['sep'] + self['name']
        return self['name']
