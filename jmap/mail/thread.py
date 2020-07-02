from jmap import errors

class Thread:
    def api_Thread_get(self, accountId, ids: list):
        if accountId and accountId != self.db.accountid:
            raise errors.errors.accountNotFound()
        user = self.db.get_user()
        newState = user['jstateThread']
        lst = []
        seenids = set()
        notFound = []
        for id in ids:
            thrid = self.idmap(id)
            if thrid in seenids:
                continue
            seenids.add(thrid)
            msgids = self.db.dgetcol('jmessages', {'thrid': thrid, 'active': 1}, 'msgid')
            if msgids:
                lst.append({
                    'id': thrid,
                    'emailIds': msgids,
                })
            else:
                notFound.append(thrid)

        return {
            'accountId': accountId,
            'list': lst,
            'state': newState,
            'notFound': notFound,
        }


    def api_Thread_changes(self, accountId, sinceState, maxChanges=None, properties=()):
        if accountId and accountId != self.db.accountid:
            raise errors.errors.accountNotFound()
        user = self.db.get_user()
        newState = user['jstateThread']
        if user['jdeletedmodseq'] and sinceState <= str(user['jdeletedmodseq']):
            raise errors.cannotCalculateChanges(f'new_state: {newState}')
        
        rows = self.db.dget('jthreads', {'jmodseq': ('>', sinceState)},
                            'thrid,active,jcreated')
        if maxChanges and len(rows) > maxChanges:
            raise errors.cannotCalculateChanges(f'new_state: {newState}')
        
        created = []
        updated = []
        removed = []
        for thrid, active, jcreated in rows:
            if active:
                if jcreated <= sinceState:
                    updated.append(thrid)
                else:
                    created.append(thrid)
            elif jcreated <= sinceState:
                removed.append(thrid)
            # else never seen
        
        return {
            'accountId': accountId,
            'oldState': sinceState,
            'newState': newState,
            'created': created,
            'updated': updated,
            'removed': removed,
        }
