from jmap.core.http import HttpBlobMixin
from jmap.mail.imap import ImapAccountMixin
from jmap.submission.smtp_scheduled import SmtpScheduledAccountMixin
from jmap.vacationresponse.db import DbVacationResponseMixin


class UserAccount(ImapAccountMixin, HttpBlobMixin, SmtpScheduledAccountMixin, DbVacationResponseMixin):
    """
    This class is responsible for mixing implementations of account methods and capabilities
    """

    is_personal = True

    def __init__(self, db,
                 username=None, password=None, auth=None,
                 imap_host='localhost', imap_port=143,
                 storage_path='http://localhost:8888/',
                 smtp_host='localhost', smtp_port=25,
                 loop=None,
                 ):
        self.id = username
        self.capabilities = {}
        ImapAccountMixin.__init__(self, username, password, auth, imap_host, imap_port, loop)
        # FileBlobMixin.__init__(self, storage_path)
        HttpBlobMixin.__init__(self, storage_path)
        SmtpScheduledAccountMixin.__init__(self, db, username, password, smtp_host, smtp_port, email=username)
        DbVacationResponseMixin.__init__(self, db)

    async def ainit(self):
        await ImapAccountMixin.ainit(self)

    async def upload(self, stream, type=None):
        # Overrides ImapAccount.upload
        return await HttpBlobMixin.upload(self, stream, type)

    async def download(self, blobId: str):
        try:
            return await HttpBlobMixin.download(self, blobId)
        except Exception:
            return await ImapAccountMixin.download(self, blobId)
