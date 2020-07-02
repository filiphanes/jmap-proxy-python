class VacationResponse:
    capabilityValue = {}

    def api_VacationResponse_get(self, accountId, **kwargs):
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
    
