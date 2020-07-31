capability = {}


def register_methods(methods):
    methods['VacationResponse/get'] = api_VacationResponse_get
    methods['VacationResponse/set'] = api_VacationResponse_set


def api_VacationResponse_get(request, accountId, **kwargs):
    raise NotImplementedError()
    return {
        'accountId': accountId,
        'state': 'dummy',
        'list': [{
            'id': 'singleton',
            'isEnabled': False,
            'fromDate': None,
            'toDate': None,
            'subject': None,
            'textBody': None,
            'htmlBody': None,
        }],
        'notFound': [],
    }


def api_VacationResponse_set(request, accountId, **kwargs):
    #TODO
    raise NotImplementedError()
