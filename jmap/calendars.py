from jmap import errors


capabilityValue = {}


def register_methods(api):
    api.methods.update({
        'Calendar/refreshSynced': api_Calendar_refreshSynced,
    })


def api_Calendar_refreshSynced(request, **kwargs):
    request.db.sync_calendars()
    return {}
