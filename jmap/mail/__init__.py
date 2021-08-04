"""
https://jmap.io/spec-mail.html
"""

capability = {}

def register_methods(methods):
    methods.update({
        'Email/get':            api_Email_get,
        'Email/set':            api_Email_set,
        'Email/query':          api_Email_query,
        'Email/changes':        api_Email_changes,
        'Email/queryChanges':   api_Email_queryChanges,
        'Email/copy':           api_Email_copy,
        'Email/import':         api_Email_import,
        'Email/parse':          api_Email_parse,
        'Mailbox/get':          api_Mailbox_get,
        'Mailbox/set':          api_Mailbox_set,
        'Mailbox/query':        api_Mailbox_query,
        'Mailbox/changes':      api_Mailbox_changes,
        'Mailbox/queryChanges': api_Mailbox_queryChanges,
        'Thread/get':           api_Thread_get,
        'Thread/changes':       api_Thread_changes,
        'SearchSnippet/get':    api_SearchSnippet_get,
    })

def api_Email_get(request, accountId, **kwargs):
    return request['user'].get_account(accountId).email_get(request['idmap'], **kwargs)

def api_Email_set(request, accountId, **kwargs):
    return request['user'].get_account(accountId).email_set(request['idmap'], **kwargs)

def api_Email_query(request, accountId, **kwargs):
    return request['user'].get_account(accountId).email_query(**kwargs)

def api_Email_changes(request, accountId, **kwargs):
    return request['user'].get_account(accountId).email_changes(**kwargs)

def api_Email_queryChanges(request, accountId, **kwargs):
    return request['user'].get_account(accountId).email_query_changes(**kwargs)

def api_Email_import(request, accountId, **kwargs):
    return request['user'].get_account(accountId).email_import(**kwargs)

def api_Email_parse(request, accountId, **kwargs):
    return request['user'].get_account(accountId).email_parse(**kwargs)

def api_Email_copy(request, accountId, fromAccountId, ifFromInState=None, ifInState=None,
                   create=None, onSuccessDestroyOriginal=False, destroyFromIfInState=None):
    raise NotImplementedError()
    try:
        fromAccount = request['user'].get_account(fromAccountId)
    except errors.accountNotFound:
        raise errors.fromAccountNotFound()
    toAccount = request['user'].get_account(accountId)
    if create is None:
        create = {}
    res_get = await fromAccountId.email_get(ids=[data['id'] for data in create.items()])
    if res_get['state'] != ifFromInState:
        raise errors.stateMismatch('ifFromInState mismatch')
    # TODO: merge res_get['list'] with create
    res_set = toAccount.email_set(ifInState=ifInState, create={cid: data})
    out = {
        'fromAccountId': fromAccountId,
        'accountId': accountId,
        'oldState': res_get['state'],
        'newState': res_set['newState'],
        'created': res_set['created'],
        'notCreated': res_set['notCreated'],
    }
    if onSuccessDestroyOriginal and res_create['created']:
        destroy = [data['id'] for data in res_create['created'].items()]
        res_destroy = await fromAccount.email_set(ifInState=destroyFromIfInState, destroy=destroy)
        res_destroy['method'] = 'Email/set'
        return out, res_destroy
    return out


def api_Mailbox_get(request, accountId, **kwargs):
    return request['user'].get_account(accountId).mailbox_get(request['idmap'], **kwargs)

def api_Mailbox_set(request, accountId, **kwargs):
    return request['user'].get_account(accountId).mailbox_set(request['idmap'], **kwargs)

def api_Mailbox_query(request, accountId, **kwargs):
    return request['user'].get_account(accountId).mailbox_query(**kwargs)

def api_Mailbox_changes(request, accountId, **kwargs):
    return request['user'].get_account(accountId).mailbox_changes(**kwargs)

def api_Mailbox_queryChanges(request, accountId, **kwargs):
    return request['user'].get_account(accountId).mailbox_query_changes(**kwargs)

def api_Thread_get(request, accountId, **kwargs):
    return request['user'].get_account(accountId).thread_get(request['idmap'], **kwargs)

def api_Thread_changes(request, accountId, **kwargs):
    return request['user'].get_account(accountId).thread_changes(**kwargs)

def api_SearchSnippet_get(request, accountId, **kwargs):
    return request['user'].get_account(accountId).searchsnippet_get(**kwargs)
