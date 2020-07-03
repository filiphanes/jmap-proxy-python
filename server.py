import asyncio
import os

try:
    import orjson as json
    dumps_kw = {'option': json.OPT_INDENT_2}
except ImportError:
    import json
    dumps_kw = {}

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import Response, StreamingResponse
from starlette.routing import Route, Mount
from starlette.staticfiles import StaticFiles

from jmap.api import handle_request, CAPABILITIES
from user import BasicAuthBackend


class JSONResponse(Response):
    media_type = "application/json"

    def render(self, content) -> bytes:
        return json.dumps(content, **dumps_kw)


async def api(request):
    try:
        data = json.loads(await request.body())
    except Exception:
        return JSONResponse({
            "type": "urn:ietf:params:jmap:error:notJson",
            "status": 400,
            "detail": "The content of the request did not parse as JSON."
        }, 400)
    res = handle_request(request.user, data)
    return JSONResponse(res)


async def event_stream(request, types, closeafter, ping):
    while True:
        if await request.is_disconnected():
            break
        if ping:
            yield f'event: ping\ndata: {"interval":{ping}}\n\n'
        await asyncio.sleep(ping or 10)

async def event(request):
    try:
        types = request.query_params['types'].split(',')
    except KeyError:
        types = ['*']
        # return Response('types param required', status_code=400)
    try:
        closeafter = request.query_params['closeafter']
    except KeyError:
        closeafter = ''
        # return Response('closeafter param required', status_code=400)
    try:
        ping = int(request.query_params['ping'])
    except (KeyError, ValueError):
        ping = 0
        # return Response('ping param required', status_code=400)

    return StreamingResponse(
        event_stream(request, types, closeafter, ping),
        200,
        media_type='text/event-stream',
    )



BASEURL = os.getenv('BASEURL', 'http://127.0.0.1:8888')
async def well_known_jmap(request):
    res = {
        "capabilities": {u: c.capabilityValue for u, c in CAPABILITIES.items()},
        "username": request.user.username,
        "accounts": {
            account.id: {
                "name": account.name,
                "isPersonal": account.is_personal,
                "isArchiveUser": False,
                "accountCapabilities": account.capabilities,
                "isReadOnly": False
            } for account in request.user.accounts.values()
        },
        "primaryAccounts": {
            "urn:ietf:params:jmap:submission": request.user.username,
            "urn:ietf:params:jmap:vacationresponse": request.user.username,
            "urn:ietf:params:jmap:mail": request.user.username
        },
        "state": "0",
        "apiUrl": BASEURL + "/api/",
        "downloadUrl": BASEURL + "/download/{accountId}/{blobId}/{name}?type={type}",
        "uploadUrl": BASEURL + "/upload/{accountId}/",
        "eventSourceUrl": BASEURL + "/event/?types={types}&closeafter={closeafter}&ping={ping}",
    }
    return JSONResponse(res)


routes = [
    Route('/api/', api, methods=["GET", "POST"]),
    Route('/event/', event),
    Route('/.well-known/jmap', well_known_jmap),
    Mount('/', StaticFiles(directory="web", html=True)),
]

middleware = [
    Middleware(CORSMiddleware, allow_origins=['*'], allow_headers=['authorization'], allow_methods=['*']),
    Middleware(AuthenticationMiddleware, backend=BasicAuthBackend()),
]

app = Starlette(
    debug=True,
    routes=routes,
    middleware=middleware,
)
