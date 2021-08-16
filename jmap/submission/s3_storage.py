from binascii import b2a_base64
from hashlib import sha1
import hmac
import os
from time import time
from urllib.parse import urlparse
from wsgiref.handlers import format_date_time

from aiohttp import ClientSession

from .dict_storage import Response


#TODO: reuse one ClientSession for requests
# We create new ClientSession on each request,
# because our tested S3 caused ServerDisconectedError when reusing 1 ClientSession
class EmailSubmissionS3Storage:
    """Requests like API for storing body"""

    def __init__(self, url=None, access_key=None, secret_key=None, http_session=None):
        if url is None:
            url = os.getenv('S3_URL', 'https://s3.us-east-1.amazonaws.com/bucket/optional-prefix')
        self.url = url.rstrip('/')
        self.bucket, _, _ = urlparse(url, 'https').path[1:].partition('/')
        if not self.bucket:
            raise ValueError('Bucket in S3_URL is required')

        if access_key is None:
            access_key = os.getenv('S3_ACCESS_KEY', 'access_key')
        self.access_key = access_key

        if secret_key is None:
            secret_key = os.getenv('S3_SECRET_KEY', 'secret_key')
        self.secret_key = secret_key

        if http_session is None:
            http_session = ClientSession()
        self.http = http_session

    def _auth(self, verb:str, path:str, date:str, typ:str='', md5:str='') -> str:
        toSign = f"{verb}\n{md5}\n{typ}\n{date}\n/{self.bucket}{path}"
        digest = hmac.new(self.secret_key.encode("utf8"), toSign.encode('utf8'), sha1).digest()
        signature = b2a_base64(digest).strip().decode()
        return f"AWS {self.access_key}:{signature}"

    async def get(self, path: str, headers=None):
        if headers is None:
            headers = {}
        headers['date'] = headers.get('date', format_date_time(time()))
        headers['authorization'] = self._auth('GET', path, headers['date'])
        async with ClientSession() as http:
            async with http.get(f"{self.url}{path}", headers=headers) as res:
                return Response(res.status, await res.read())

    async def put(self, path:str, data:bytes, headers=None):
        if headers is None:
            headers = {}
        headers['date'] = headers.get('date', format_date_time(time()))
        headers['content-type'] = headers.get('content-type', 'message/rfc822')
        headers['authorization'] = self._auth('PUT', path, headers['date'], headers['content-type'])
        async with ClientSession() as http:
            async with http.put(f"{self.url}{path}", data=data, headers=headers) as res:
                return Response(res.status, await res.read())

    async def delete(self, path:str, headers=None):
        if headers is None:
            headers = {}
        headers['date'] = headers.get('date', format_date_time(time()))
        headers['authorization'] = self._auth('DELETE', path, headers['date'])
        async with ClientSession() as http:
            async with http.delete(f"{self.url}{path}", headers=headers) as res:
                return Response(res.status, await res.read())
