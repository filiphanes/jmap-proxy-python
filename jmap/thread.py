from jmap import errors


def register_methods(api):
    api.methods['Thread/get'] = api_Thread_get
    api.methods['Thread/changes'] = api_Thread_changes
    #TODO: api.methods['Thread/queryChanges'] = api_Thread_queryChanges


def api_Thread_get(request, accountId, ids: list):
    account = request.get_account(accountId)
    newState = account.db.highModSeqThread
    lst = []
    seenids = set()
    notFound = []
    for id in ids:
        thrid = request.idmap(id)
        if thrid in seenids:
            continue
        seenids.add(thrid)
        rows = account.db.get_messages('msgid', thrid=thrid, deleted=0)
        if rows:
            lst.append({
                'id': thrid,
                'emailIds': [row['msgid'] for row in rows],
            })
        else:
            notFound.append(thrid)

    return {
        'accountId': accountId,
        'list': lst,
        'state': newState,
        'notFound': notFound,
    }


def api_Thread_changes(request, accountId, sinceState, maxChanges=None, properties=()):
    account = request.get_account(accountId)
    newState = account.db.highModSeqThread
    if sinceState <= str(account.db.lowModSeq):
        raise errors.cannotCalculateChanges({'new_state': newState})
    
    rows = account.db.dget('jthreads', {'jmodseq': ('>', sinceState)},
                        'thrid,deleted,jcreated')
    if maxChanges and len(rows) > maxChanges:
        raise errors.cannotCalculateChanges({'new_state': newState})
    
    created = []
    updated = []
    removed = []
    for thrid, deleted, jcreated in rows:
        if not deleted:
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
