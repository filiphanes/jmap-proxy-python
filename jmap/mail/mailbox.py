from jmap import errors

class Mailbox:
    def api_Mailbox_get(self, accountId=None, ids=None, properties=None, **kwargs):
        user = self.db.get_user()
        if accountId and accountId != self.db.accountid:
            self.db.rollback()
            raise errors.errors.accountNotFound()
        new_state = user.get('jstateMailbox', None)
        rows = self.db.dget('jmailboxes', {'active': 1})

        if ids:
            want = set(self.idmap(i) for i in ids)
        else:
            want = set(d['jmailboxid'] for d in rows)

        lst = []
        for item in rows:
            if item['jmailboxid'] not in want:
                continue
            want.remove(item['jmailboxid'])
            rec = {
                'name': item['name'],
                'parentId': item['parentId'] or None,
                'role': item['role'],
                'sortOrder': item['sortOrder'] or 0,
                'totalEmails': item['totalEmails'] or 0,
                'unreadEmails': item['unreadEmails'] or 0,
                'totalThreads': item['totalThreads'] or 0,
                'unreadThreads': item['unreadThreads'] or 0,
                'myRights': {k: bool(item[k]) for k in (
                    'mayReadItems',
                    'mayAddItems',
                    'mayRemoveItems',
                    'maySetSeen',
                    'maySetKeywords',
                    'mayCreateChild',
                    'mayRename',
                    'mayDelete',
                    'maySubmit',
                    )},
                'isSubscribed': bool(item['isSubscribed']),
            }
            if properties:
                rec = {k: rec[k] for k in properties if k in rec}
            rec['id'] = item['jmailboxid']
            lst.append(rec)

        return {
            'list': lst,
            'accountId': self.db.accountid,
            'state': new_state,
            'notFound': list(want)
        }

    def api_Mailbox_query(self, accountId=None, sort=None, filter=None, position=0, anchor=None, anchorOffset=0, limit=None, **kwargs):
        user = self.db.get_user()
        if accountId and accountId != self.db.accountid:
            raise errors.accountNotFound()
        rows = self.db.dget('jmailboxes', {'active': 1})
        if filter:
            rows = [d for d in rows if self._mailbox_match(d, filter)]

        storage = {'data': rows}
        data = _mailbox_sort(rows, sort, storage)

        start = position
        if anchor:
            # need to calculate the position
            i = [x['jmailboxid'] for x in data].index(anchor)
            if i < 0:
                raise errors.anchorNotFound()
            start = i + anchorOffset
        
        if limit:
            end = start + limit - 1
        else:
            end = len(data)
        
        return {
            'accountId': self.db.accountid,
            'filter': filter,
            'sort': sort,
            'queryState': user.get('jstateMailbox', None),
            'canCalculateChanges': False,
            'position': start,
            'total': len(data),
            'ids': [x['jmailboxid'] for x in data[start:end]],
        }

    def api_Mailbox_changes(self, sinceState, accountId=None, maxChanges=None, **kwargs):
        if accountId and accountId != self.db.accountid:
            raise errors.accountNotFound()
        user = self.db.get_user()
        new_state = user['jstateMailbox']
        if user['jdeletemodseq'] and sinceState <= str(user['jdeletedmodseq']):
            raise errors.cannotCalculateChanges(f'new_state: {new_state}')
        rows = self.db.dget('jmailboxes', {'jmodseq': ['>', sinceState]})

        if maxChanges and len(rows) > maxChanges:
            raise errors.cannotCalculateChanges(f'new_state: {new_state}')

        created = []
        updated = []
        removed = []
        only_counts = 0
        for item in rows:
            if item['active']:
                if item['jcreated'] <= sinceState:
                    updated.append(item['jmailboxid'])
                    if item['jnoncountsmodseq'] > sinceState:
                        only_counts = 0
                else:
                    created.append(item['jmailboxid'])
            else:
                if item['jcreated'] <= sinceState:
                    removed.append(item['jmailboxid'])
                # otherwise never seen

        return {
            'accountId': self.db.accountid,
            'oldState': sinceState,
            'newState': new_state,
            'created': created,
            'updated': updated,
            'removed': removed,
            'changedProperties': ["totalEmails", "unreadEmails", "totalThreads", "unreadThreads"] if only_counts else None,
        }
    
    def _load_mailbox(self, id):
        return self.db.dgetby('jmessagemap', 'msgid', {'jmailboxid': id}, 'msgid,jmodseq,active')


def _makefullnames(mailboxes):
    idmap = {d['jmailboxid']: d for d in mailboxes}
    idmap.pop('', None)  # just in case
    fullnames = {}
    for id, mbox in idmap.items():
        names = []
        while mbox:
            names.append(mbox['name'])
            mbox = idmap.get(mbox['parentId'], None)
        fullnames[id] = '\x1e'.join(reversed(names))
    return fullnames

def _mailbox_sort(data, sortargs, storage):
    return data
    #TODO: make correct sorting
    def key(item):
        k = []
        for arg in sortargs:
            field = arg['property']
            if field == 'name':
                k.append(item['name'])
            elif field == 'sortOrder':
                k.append(item['sortOrder'])
            elif field == 'parent/name':
                if 'fullnames' not in storage:
                    storage['fullnames'] = _makefullnames(storage['data'])
                    k.append(storage['fullnames'][item['jmailboxid']])
                k.append(item['sortOrder'])
            else:
                raise Exception('Unknown field ' + field)

    return sorted(data, key=key)
