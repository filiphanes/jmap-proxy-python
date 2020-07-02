class Submission:
    capabilityValue = {}

    def api_Identity_get(self, **kwargs):
        self.db.begin()
        user = self.db.get_user()
        self.db.commit()

        # TODO: fix Identity
        return {
            'accountId': self.db.accountid,
            'state': 'dummy',
            'list': {
                'id': "id1",
                'displayName': user.displayname or user.email,
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
