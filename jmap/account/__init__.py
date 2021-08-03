from .personal import PersonalAccount
from .imap import ImapAccountMixin
from .smtp.simple import SmtpAccountMixin
from .smtp_scheduled import SmtpScheduledAccountMixin
from .storage import ProxyBlobMixin


class UserAccount(ImapAccountMixin, ProxyBlobMixin, SmtpAccountMixin, PersonalAccount):
    """
    This class is responsible for mixing implementations of account methods and capabilities
    """

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
        ProxyBlobMixin.__init__(self, storage_path)
        SmtpScheduledAccountMixin.__init__(self, db, username, password, smtp_host, smtp_port, email=username)

    async def ainit(self):
        await ImapAccountMixin.ainit(self)

    async def upload(self, stream, type=None):
        # Overrides ImapAccount.upload
        return await ProxyBlobMixin.upload(self, stream, type)

    async def download(self, blobId: str):
        try:
            return await ProxyBlobMixin.download(self, blobId)
        except Exception:
            return await ImapAccountMixin.download(self, blobId)
