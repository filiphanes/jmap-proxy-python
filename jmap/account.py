from jmap.db.imap import ImapDB

class Account:
    is_personal = True


class ImapAccount(Account):
    @classmethod
    async def init(cls, accountId, password, loop=None):
        self = cls()
        self.id = accountId
        self.name = accountId
        self.db = await ImapDB.init(accountId, password, loop=loop)
        self.capabilities = {
            "urn:ietf:params:jmap:vacationresponse": {},
            "urn:ietf:params:jmap:submission": {
                "submissionExtensions": [],
                "maxDelayedSend": 44236800  # 512 days
            },
            "urn:ietf:params:jmap:mail": {
                "maxSizeMailboxName": 490,
                "maxSizeAttachmentsPerEmail": 50000000,
                "mayCreateTopLevelMailbox": True,
                "maxMailboxesPerEmail": 1,  # IMAP implementation
                "maxMailboxDepth": None,
                "emailQuerySortOptions": [
                    "receivedAt",
                    # "from",
                    # "to",
                    "subject",
                    "size",
                    # "header.x-spam-score"
                ]
            }
        }
        return self
