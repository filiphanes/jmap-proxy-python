import asyncio
import os

try:
    import orjson as json
    OPT_INDENT_2 = json.OPT_INDENT_2
except ImportError:
    import json
    OPT_INDENT_2 = None

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import PlainTextResponse, Response, StreamingResponse
from starlette.routing import Route, Mount
from starlette.staticfiles import StaticFiles

from user import BasicAuthBackend

class JSONResponse(Response):
    media_type = "application/json"

    def render(self, content) -> bytes:
        return json.dumps(
            content,
            option=OPT_INDENT_2,
        )


BASEURL = os.getenv('BASEURL', 'http://127.0.0.1:8888')
async def well_known_jmap(request):
    res = {
        "capabilities": {
            "urn:ietf:params:jmap:submission": {},
            "urn:ietf:params:jmap:vacationresponse": {},
            "urn:ietf:params:jmap:mail": {},
            "urn:ietf:params:jmap:core": {
                "collationAlgorithms": [
                    "i;ascii-numeric",
                    "i;ascii-casemap",
                    "i;octet"
                ],
                "maxCallsInRequest": 64,
                "maxObjectsInGet": 1000,
                "maxSizeUpload": 250000000,
                "maxConcurrentRequests": 10,
                "maxObjectsInSet": 1000,
                "maxConcurrentUpload": 10,
                "maxSizeRequest": 10000000
            }
        },
        "username": request.user.username,
        "accounts": {
            request.user.username: {
                "name": request.user.display_name,
                "isPersonal": True,
                "isArchiveUser": False,
                "accountCapabilities": {
                    "urn:ietf:params:jmap:vacationresponse": {},
                    "urn:ietf:params:jmap:submission": {
                        "submissionExtensions": [],
                        "maxDelayedSend": 44236800
                    },
                    "urn:ietf:params:jmap:mail": {
                        "maxSizeMailboxName": 490,
                        "maxSizeAttachmentsPerEmail": 50000000,
                        "mayCreateTopLevelMailbox": True,
                        "maxMailboxesPerEmail": 1000,
                        "maxMailboxDepth": None,
                        "emailQuerySortOptions": [
                            "receivedAt",
                            "from",
                            "to",
                            "subject",
                            "size",
                            "header.x-spam-score"
                        ]
                    }
                },
                "isReadOnly": False
            }
        },
        "primaryAccounts": {
            "urn:ietf:params:jmap:submission": request.user.username,
            "urn:ietf:params:jmap:vacationresponse": request.user.username,
            "urn:ietf:params:jmap:mail": request.user.username
        },
        "state": "azetmail-0",  # TODO: fetch current state
        "apiUrl": BASEURL + "/api/",
        "downloadUrl": BASEURL + "/download/{accountId}/{blobId}/{name}?type={type}",
        "uploadUrl": BASEURL + "/upload/{accountId}/",
        "eventSourceUrl": BASEURL + "/event/?types={types}&closeafter={closeafter}&ping={ping}",
    }
    return JSONResponse(res)


async def api(request):
    data = json.loads(await request.body())
    res = request.user.jmap.handle_request(data)
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
