from jmap import errors


capabilityValue = {}


def register_methods(api):
    api.methods.update({
        'Calendar/refreshSynced': api_Calendar_refreshSynced,
    })


def api_Calendar_refreshSynced(request, accountId, **kwargs):
    account = request.get_account(accountId)
    account.sync_calendars()
    return {
        'accountId': accountId,
    }
