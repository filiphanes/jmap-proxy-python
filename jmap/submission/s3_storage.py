import base64
from hashlib import sha1
import hmac
import os
from time import time
from wsgiref.handlers import format_date_time


class EmailSubmissionS3Storage:
    """Requests like API for storing body"""

    def __init__(self, url, bucket=None, access_key=None, secret_key=None, http_session=None):
        if url is None:
            url = os.getenv('S3_URL', 'http://127.0.0.1/emailsubmission')
        if bucket is None:
            bucket = os.getenv('S3_BUCKET', 'emailsubmission')
        if access_key is None:
            access_key = os.getenv('S3_ACCESS_KEY', 'access_key')
        if secret_key is None:
            secret_key = os.getenv('S3_SECRET_KEY', 'secret_key')
        if http_session is None:
            import aiohttp
            http_session = aiohttp.ClientSession()
        self.url = url
        self.bucket = bucket
        self.access_key = access_key
        self.secret_key = secret_key
        self.http = http_session

    def _auth(self, verb:str, path:str, date:str, typ:str='', md5:str=''):
        toSign = f"{verb}\n{md5}\n{typ}\n{date}\n/{self.bucket}{path}"
        digest = hmac.new(self.secret_key.encode("utf8"), toSign.encode('utf8'), sha1).digest()
        signature = base64.encodestring(digest).strip().decode()
        return f"AWS {self.access_key}:{signature}"

    async def get(self, path: str, headers=None):
        if headers is None:
            headers = {}
        headers['date'] = headers.get('date') or format_date_time(time())
        headers['authorization'] = self._auth('GET', path, headers['date'])
        async with self.http.get(self.url + path, headers=headers) as res:
            await res.read()
            return res

    async def put(self, path:str, body:bytes, headers=None):
        if headers is None:
            headers = {}
        headers['date'] = headers.get('date') or format_date_time(time())
        headers['content-type'] = headers.get('content-type') or 'message/rfc822'
        headers['authorization'] = self._auth('PUT', path, headers['date'], headers['content-type'])
        async with self.http.put(self.url + path, body=body, headers=headers) as res:
            await res.read()
            return res

    async def delete(self, path:str, headers=None):
        if headers is None:
            headers = {}
        headers['date'] = headers.get('date') or format_date_time(time())
        headers['authorization'] = self._auth('DELETE', path, headers['date'])
        async with self.http.delete(self.url + path, headers=headers) as res:
            await res.read()
            return res
