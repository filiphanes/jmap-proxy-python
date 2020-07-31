from .personal import PersonalAccount
from .imap import ImapAccount
from .smtp import SmtpAccountMixin
from .storage import FileBlobMixin


class UserAccount(ImapAccount, FileBlobMixin, SmtpAccountMixin, PersonalAccount):
    def __init__(self,
                 username, password,
                 imap_host='localhost', imap_port=143,
                 storage_path=None,
                 smtp_host='localhost', smtp_port=143,
                 loop=None,
                 ):
        ImapAccount.__init__(self, username, password, imap_host, imap_port, loop)
        FileBlobMixin.__init__(self, storage_path)
        # S3BlobMixin.__init__(self, storage_path)
        SmtpAccountMixin.__init__(self, username, password, smtp_host, smtp_port, email=username)

    async def ainit(self):
        await ImapAccount.ainit(self)

    async def upload(self, stream, type):
        out = await FileBlobMixin.upload(self, stream, type)
        out['blobId'] = 'file:' + out['blobId']
        return out

    async def download(self, blobId: str):
        if blobId.startswith('file:'):
            return await FileBlobMixin.download(self, blobId[5:])
        else:
            return await ImapAccount.download(self, blobId)
