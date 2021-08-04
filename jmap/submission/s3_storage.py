import base64
from hashlib import sha1
import hmac
import os
from time import time
from wsgiref.handlers import format_date_time

import aiohttp

from jmap import errors


class EmailSubmissionS3Storage:
    """Requests like API for storing body"""

    def __init__(self, url, access_key=None, secret_key=None, bucket=None):
        if url is None:
            url = os.getenv('S3_URL', 'emailsubmission')
        if bucket is None:
            bucket = os.getenv('S3_BUCKET', 'emailsubmission')
        if access_key is None:
            access_key = os.getenv('S3_ACCESS_KEY', 'access_key')
        if secret_key is None:
            secret_key = os.getenv('S3_SECRET_KEY', 'secret_key')
        self.url = url
        self.bucket = bucket
        self.access_key = access_key
        self.secret_key = secret_key
        self.http = aiohttp.ClientSession()

    def auth(self, verb, path, date, typ='', md5=''):
        toSign = f"{verb}\n{md5}\n{typ}\n{date}\n/{self.bucket}{path}"
        digest = hmac.new(self.secret_key.encode("utf8"), toSign.encode('utf8'), sha1).digest()
        signature = base64.encodestring(digest).strip().decode()
        return f"AWS {self.access_key}:{signature}"

    async def get(self, path, headers=None) -> bytes:
        if headers is None:
            headers = {}
        headers['date'] = headers.get('date') or format_date_time(time())
        headers['authorization'] = self.auth('GET', path, headers['date'])

        async with self.http.get(self.url + path, headers=headers) as res:
            if res.status == 200:
                return await res.read()

    async def put(self, path, body, headers=None):
        if headers is None:
            headers = {}
        headers['date'] = headers.get('date') or format_date_time(time())
        headers['content-type'] = headers.get('content-type') or 'message/rfc822'
        headers['authorization'] = self.auth('PUT', path, headers['date'], headers['content-type'])

        async with self.http.put(self.url + path, body=body, headers=headers) as res:
            if 200 <= res.status < 300:
                return
            else:
                raise errors.serverFail(f'PUT status={res.status}')

    async def delete(self, path, headers=None):
        if headers is None:
            headers = {}
        headers['date'] = headers.get('date') or format_date_time(time())
        headers['authorization'] = self.auth('DELETE', path, headers['date'])

        async with self.http.delete(self.url + path, headers=headers) as res:
            if res.status == 404:
                raise errors.notFound()
            elif res.status != 204:
                raise errors.serverFail(f'Failed to delete S3 file, status={res.status}')
