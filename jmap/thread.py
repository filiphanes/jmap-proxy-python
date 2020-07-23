from collections import defaultdict

from jmap import errors


def register_methods(api):
    api.methods['Thread/get'] = api_Thread_get
    api.methods['Thread/changes'] = api_Thread_changes
    #TODO: api.methods['Thread/queryChanges'] = api_Thread_queryChanges


async def api_Thread_get(request, accountId, ids: list=None):
    account = request.get_account(accountId)
    threads = defaultdict(list)
    if ids is None:
        # get all
        messages = await account.db.get_messages('id', deleted=0)
    else:
        notFound = set(request.idmap(id) for id in ids)
        messages = await account.db.get_messages(['id'], threadId__in=notFound, deleted=0)
    for msg in messages:
        threads[msg['threadId']].append(msg['id'])
        if ids is not None:
            notFound.remove(msg['threadId'])

    return {
        'accountId': accountId,
        'list': [{'id': key, 'emailIds': val} for key, val in threads.items()],
        'state': await account.db.thread_state(),
        'notFound': list(notFound),
    }


async def api_Thread_changes(request, accountId, sinceState, maxChanges=None, properties=()):
    account = request.get_account(accountId)
    newState = await account.db.thread_state()
    if sinceState <= account.db.thread_state_low():
        raise errors.cannotCalculateChanges({'new_state': newState})
    
    rows = account.db.threads_get(['id','create','deleted'], updated__gt=sinceState)
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
