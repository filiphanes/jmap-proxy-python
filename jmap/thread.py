from jmap import errors


def register_methods(api):
    api.methods['Thread/get'] = api_Thread_get
    api.methods['Thread/changes'] = api_Thread_changes
    #TODO: api.methods['Thread/queryChanges'] = api_Thread_queryChanges


def api_Thread_get(request, accountId, ids: list):
    if accountId and accountId != request.db.accountid:
        raise errors.errors.accountNotFound()
    user = request.db.get_user()
    newState = user['jstateThread']
    lst = []
    seenids = set()
    notFound = []
    for id in ids:
        thrid = request.idmap(id)
        if thrid in seenids:
            continue
        seenids.add(thrid)
        msgids = request.db.dgetcol('jmessages', {'thrid': thrid, 'active': 1}, 'msgid')
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


def api_Thread_changes(request, accountId, sinceState, maxChanges=None, properties=()):
    try:
        account = request.user.accounts[accountId]
    except KeyError:
        raise errors.accountNotFound()
    user = account.db.get_user()
    newState = user['jstateThread']
    if user['jdeletedmodseq'] and sinceState <= str(user['jdeletedmodseq']):
        raise errors.cannotCalculateChanges({'new_state': newState})
    
    rows = request.db.dget('jthreads', {'jmodseq': ('>', sinceState)},
                        'thrid,active,jcreated')
    if maxChanges and len(rows) > maxChanges:
        raise errors.cannotCalculateChanges({'new_state': newState})
    
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
