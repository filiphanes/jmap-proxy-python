def register_methods(api):
    api.methods['SearchSnippet/get'] = api_SearchSnippet_get


def api_SearchSnippet_get(request, accountId, filter, emailIds):
    raise NotImplementedError
    return {
        'accountId': accountId,
        'list': [],
        'notFound': [],
    }