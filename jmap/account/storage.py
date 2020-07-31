import random

from aiofiles import open
from aiohttp.client import ClientSession

from jmap import errors


class FileBlobMixin:
    chunk_size = 4096

    def __init__(self, dir=None):
        self.dir = dir or './data/'

    async def upload(self, stream, type):
        blobId = random.randbytes(16).hex()
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

class S3BlobMixin:
    http = ClientSession()

    def __init__(self, base, http_session=None):
        """base: 'https://s3host.com/bucket/{blobId}'
        """
        self.base = base
        if http_session:
            self.http = http_session or ClientSession()

    async def upload(self, stream, type):
        blobId = random.randbytes(16).hex()
        size = 0
        async with self.http.post(self.base.format(blobId=blobId)) as r:
            async for chunk in stream:
                await r.write(chunk)
                size += len(chunk)

        return {
            'accountId': self.id,
            'blobId': blobId,
            'type': type,
            'size': size,
        }

    async def download(self, blobId):
        async with self.http.get(self.base.format(blobId=blobId)) as res:
            if res.status == 404:
                raise errors.notFound(f'Blob {blobId} not found')
            return await res.body()
