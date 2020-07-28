"""
https://jmap.io/spec-mail.html
"""

capabilityValue = {}

def register_methods(methods):
    methods.update({
        'Email/get': api_Email_get,
        'Email/set': api_Email_set,
        'Email/query': api_Email_query,
        'Email/changes': api_Email_changes,
        'Email/queryChanges': api_Email_queryChanges,
        'Mailbox/get': api_Mailbox_get,
        'Mailbox/set': api_Mailbox_set,
        'Mailbox/query': api_Mailbox_query,
        'Mailbox/changes': api_Mailbox_changes,
        'Mailbox/queryChanges': api_Mailbox_queryChanges,
        'Thread/get': api_Thread_get,
        'Thread/changes': api_Thread_changes,
        'SearchSnippet/get': api_SearchSnippet_get,
    })

def api_Email_get(request, accountId, **kwargs):
    return request.get_account(accountId).email_get(request.idmap, **kwargs)

def api_Email_set(request, accountId, **kwargs):
    return request.get_account(accountId).email_set(request.idmap, **kwargs)

def api_Email_query(request, accountId, **kwargs):
    return request.get_account(accountId).email_query(**kwargs)

def api_Email_changes(request, accountId, **kwargs):
    return request.get_account(accountId).email_changes(**kwargs)

def api_Email_queryChanges(request, accountId, **kwargs):
    return request.get_account(accountId).email_queryChanges(**kwargs)

def api_Mailbox_get(request, accountId, **kwargs):
    return request.get_account(accountId).mailbox_get(request.idmap, **kwargs)

def api_Mailbox_set(request, accountId, **kwargs):
    return request.get_account(accountId).mailbox_set(request.idmap, **kwargs)

def api_Mailbox_query(request, accountId, **kwargs):
    return request.get_account(accountId).mailbox_query(**kwargs)

def api_Mailbox_changes(request, accountId, **kwargs):
    return request.get_account(accountId).mailbox_changes(**kwargs)

def api_Mailbox_queryChanges(request, accountId, **kwargs):
    return request.get_account(accountId).mailbox_queryChanges(**kwargs)

def api_Thread_get(request, accountId, **kwargs):
    return request.get_account(accountId).thread_get(request.idmap, **kwargs)

def api_Thread_changes(request, accountId, **kwargs):
    return request.get_account(accountId).thread_changes(**kwargs)

def api_SearchSnippet_get(request, accountId, **kwargs):
    return request.get_account(accountId).searchsnippet_get(**kwargs)
