from uuid import uuid4

from aiofiles import open

from jmap import errors


class FileBlobMixin:
    """Provides methods upload and download.
    Stores files in local filesystem directory"""

    chunk_size = 4096

    def __init__(self, dir=None):
        self.dir = dir or './data/'

    async def upload(self, stream, type):
        blobId = uuid4().hex
        size = 0
        async with open(f'data/{blobId}', 'w') as f:
            async for chunk in stream:
                await f.write(chunk)
                size += len(chunk)

        return {
            'accountId': self.id,
            'blobId': blobId,
            'type': type,
            'size': size,
        }

    async def download(self, blobId):
        try:
            async with open(f'data/{blobId}', 'r') as file:
                body = bytearray()
                chunk = range(self.chunk_size)
                while len(chunk) == self.chunk_size:
                    chunk = await file.read(self.chunk_size)
                    body += chunk
                return body
        except FileNotFoundError:
            raise errors.notFound()
