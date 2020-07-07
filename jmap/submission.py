from jmap import errors

capabilityValue = {}


def register_methods(api):
    api.methods.update({
        #TODO: 'EmailSubmission/get': api_EmailSubmission_get,
        #TODO: 'EmailSubmission/changes': api_EmailSubmission_changes,
        #TODO: 'EmailSubmission/queryChanges': api_EmailSubmission_queryChanges,
        'Identity/get': api_Identity_get,
        #TODO: 'Identity/set': api_Identity_get,
        #TODO: 'Identity/changes': api_Identity_changes,
    })


def api_Identity_get(request, accountId, ids=None):
    account = request.get_account(accountId)

    # TODO:
    return {
        'accountId': accountId,
        'state': 'dummy',
        'list': {
            'id': "id1",
            'displayName': account.displayname or request.user.email,
            'mayDelete': False,
            'email': request.user.email,
            'name': request.user.displayname or request.user.email,
            'textSignature': "-- \ntext signature",
            'htmlSignature': "-- <br><b>html signature</b>",
            'replyTo': request.user.email,
            'autoBcc': "",
            'addBccOnSMTP': False,
            'saveSentTo': None,
            'saveAttachments': False,
            'saveOnSMTP': False,
            'useForAutoReply': False,
            'isAutoConfigured': True,
            'enableExternalSMTP': False,
            'smtpServer': "",
            'smtpPort': 465,
            'smtpSSL': "ssl",
            'smtpUser': "",
            'smtpPassword': "",
            'smtpRemoteService': None,
            'popLinkId': None,
        },
        'notFound': [],
    }
