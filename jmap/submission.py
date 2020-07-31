from jmap import errors

capability = {}


def register_methods(methods):
    methods.update({
        'EmailSubmission/get': api_EmailSubmission_get,
        'EmailSubmission/set': api_EmailSubmission_set,
        'EmailSubmission/query': api_EmailSubmission_query,
        'EmailSubmission/changes': api_EmailSubmission_changes,
        'EmailSubmission/queryChanges': api_EmailSubmission_queryChanges,
        'Identity/get': api_Identity_get,
        'Identity/set': api_Identity_set,
        'Identity/changes': api_Identity_changes,
    })


def api_EmailSubmission_get(request, accountId, ids=None):
    return request['user'].get_account(accountId).emailsubmission_get(ids)

def api_EmailSubmission_set(request, accountId, ids=None):
    return request['user'].get_account(accountId).emailsubmission_set(ids)

def api_EmailSubmission_query(request, accountId, ids=None):
    return request['user'].get_account(accountId).emailsubmission_query(ids)

def api_EmailSubmission_changes(request, accountId, ids=None):
    return request['user'].get_account(accountId).emailsubmission_changes(ids)

def api_EmailSubmission_queryChanges(request, accountId, ids=None):
    return request['user'].get_account(accountId).emailsubmission_changes(ids)


def api_Identity_get(request, accountId, ids=None):
    return request['user'].get_account(accountId).identity_get(ids)

def api_Identity_set(request, accountId, ids=None):
    return request['user'].get_account(accountId).identity_set(ids)

def api_Identity_changes(request, accountId, ids=None):
    return request['user'].get_account(accountId).identity_changes(ids)
