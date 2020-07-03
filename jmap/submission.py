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
    try:
        account = request.user.accounts[accountId]
    except KeyError:
        raise errors.accountNotFound()
    user = account.db.get_user()

    # TODO:
    return {
        'accountId': accountId,
        'state': 'dummy',
        'list': {
            'id': "id1",
            'displayName': account.displayname or user.email,
            'mayDelete': False,
            'email': user.email,
            'name': user.displayname or user.email,
            'textSignature': "-- \ntext signature",
            'htmlSignature': "-- <br><b>html signature</b>",
            'replyTo': user.email,
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
