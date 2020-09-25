from .personal import PersonalAccount
from .imap import ImapAccount
from .smtp import SmtpAccountMixin
from .storage import ProxyBlobMixin


class UserAccount(ImapAccount, ProxyBlobMixin, SmtpAccountMixin, PersonalAccount):
    def __init__(self,
                 username, password,
                 imap_host='localhost', imap_port=143,
                 storage_path='http://localhost:8888/',
                 smtp_host='localhost', smtp_port=143,
                 loop=None,
                 ):
        ImapAccount.__init__(self, username, password, imap_host, imap_port, loop)
        # FileBlobMixin.__init__(self, storage_path)
        ProxyBlobMixin.__init__(self, storage_path)
        SmtpAccountMixin.__init__(self, username, password, smtp_host, smtp_port, email=username)

    async def ainit(self):
        await ImapAccount.ainit(self)

    async def upload(self, stream, type=None):
        # Overrides ImapAccount.upload
        return await ProxyBlobMixin.upload(self, stream, type)

    async def download(self, blobId: str):
        try:
            return await ProxyBlobMixin.download(self, blobId)
        except Exception:
            return await ImapAccount.download(self, blobId)
