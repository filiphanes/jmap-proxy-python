class Calendars:
    capabilityValue = {}

    def api_Calendar_refreshSynced(self, **kwargs):
        self.db.sync_calendars()
        return {}
