from aiohttp.client import ClientSession

from jmap import errors


class HttpBlobMixin:
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
