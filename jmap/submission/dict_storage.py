
class Response:
    def __init__(self, status, body=b''):
        self.status = status
        self.body = body
    async def read(self) -> bytes:
        return self.body


class DictStorage(dict):
    """Simple HTTP storage class implementing requests/httpx API"""

    async def get(self, path, **kwargs):
        try:
            return Response(200, self[path])
        except KeyError:
            return Response(404)

    async def put(self, path, body, **kwargs):
        self[path] = body
        return Response(201)

    async def delete(self, path, **kwargs):
        try:
            del self[path]
            return Response(204)
        except KeyError:
            return Response(404)
