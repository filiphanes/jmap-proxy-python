capability = {}


def register_methods(methods):
    methods['VacationResponse/get'] = api_VacationResponse_get
    methods['VacationResponse/set'] = api_VacationResponse_set


def api_VacationResponse_get(request, accountId, **kwargs):
    return request['user'].get_account(accountId).vacationresponse_get(request['idmap'], **kwargs)

def api_VacationResponse_set(request, accountId, **kwargs):
    return request['user'].get_account(accountId).vacationresponse_set(request['idmap'], **kwargs)
