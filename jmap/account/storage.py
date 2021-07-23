import random

from aiofiles import open
from aiohttp.client import ClientSession

from jmap import errors

class FileBlobMixin:
    """Provides methods upload and download.
    Stores files in local filesystem directory"""

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


class ProxyBlobMixin:
    """Provides methods upload and download.
    Proxies request to other HTTP service"""

    http = ClientSession()

    def __init__(self, base, http_session=None):
        self.base = base
        if http_session:
            self.http = http_session or ClientSession()

    async def upload(self, stream, content_type=None):
        headers = {}
        if content_type is not None:
            headers['content-type'] = content_type
        async with self.http.post(f"{self.base}{self.id}", data=stream, headers=headers) as r:
            return await r.json()

    async def download(self, blobId):
        async with self.http.get(f"{self.base}{self.id}/{blobId}") as res:
            if res.status == 200:
                return await res.read()
            elif res.status // 100 == 5:
                raise errors.serverFail()
        raise errors.notFound(f'Blob {blobId} not found')
