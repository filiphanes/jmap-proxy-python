import base64
import binascii

try:
    import orjson as json
    OPT_INDENT_2 = json.OPT_INDENT_2
except ImportError:
    import json
    OPT_INDENT_2 = None

from starlette.applications import Starlette
from starlette.authentication import (
    AuthenticationBackend, AuthenticationError, SimpleUser,
    UnauthenticatedUser, AuthCredentials
)
from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import Response, PlainTextResponse, StreamingResponse
from starlette.routing import Route, Mount
from starlette.staticfiles import StaticFiles

from jmap.api import JmapApi
from account import AccountManager
import asyncio
from time import time

accounts = AccountManager()

class BasicAuthBackend(AuthenticationBackend):
    async def authenticate(self, request):
        if "Authorization" not in request.headers:
            return
        auth = request.headers["Authorization"]
        try:
            scheme, credentials = auth.split()
            if scheme.lower() != 'basic':
                return
            decoded = base64.b64decode(credentials).decode("ascii")
        except (ValueError, UnicodeDecodeError, binascii.Error) as exc:
            raise AuthenticationError('Invalid basic auth credentials')

        username, _, password = decoded.partition(":")
        # TODO: You'd want to verify the username and password here.
        return AuthCredentials(["authenticated"]), SimpleUser(username)


async def api(request):
    if not request.user.is_authenticated:
        raise AuthenticationError('User not authenticated')
    data = json.loads(await request.body())
    db = accounts.get_db(request.user.username)
    api = JmapApi(db)
    res = api.handle_request(data)
    body = json.dumps(res, option=OPT_INDENT_2)
    return Response(body, 200, media_type='application/json')


async def events(request):
    async def stream():
        while True:
            if await request.is_disconnected():
                break
            yield 'event: ping\ndata: %f\n\n' % (time(),)
            await asyncio.sleep(60)
    return StreamingResponse(stream(), 200, media_type='text/event-stream')


async def firstsync(request):
    accountid = request.path_params['accountid']
    db = accounts.get_db(accountid)
    db.firstsync()
    return PlainTextResponse('Synced')


async def syncall(request):
    accountid = request.path_params['accountid']
    db = accounts.get_db(accountid)
    db.sync_folders()
    db.sync_imap()
    # db.sync_addressbooks()
    # db.sync_calendars()
    return PlainTextResponse('Synced')


routes = [
    Route('/api/', api, methods=["GET", "POST"]),
    Route('/events/', events, methods=["GET", "POST"]),
    Route('/firstsync/{accountid}', firstsync),
    Route('/syncall/{accountid}', syncall),
    Mount('/.well-known', StaticFiles(directory=".well-known", html=False)),
    Mount('/', StaticFiles(directory="web", html=True)),
]

middleware = [
    Middleware(CORSMiddleware, allow_origins=['*'], allow_headers=['authorization'], allow_methods=['*']),
    Middleware(AuthenticationMiddleware, backend=BasicAuthBackend()),
]

app = Starlette(debug=True, routes=routes, middleware=middleware)
