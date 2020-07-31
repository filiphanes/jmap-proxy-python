import asyncio
import os

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import Response, StreamingResponse
from starlette.routing import Route

from jmap.api import api, CAPABILITIES, JSONResponse
from user import BasicAuthBackend

BASEURL = os.getenv('BASEURL', 'http://127.0.0.1:8888')


async def event_stream(request, types, closeafter, ping):
    while True:
        if await request.is_disconnected():
            break
        if ping:
            yield 'event: ping\ndata: {"interval":%d}\n\n' % ping
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
        ping = 5
        # return Response('ping param required', status_code=400)

    return StreamingResponse(
        event_stream(request, types, closeafter, ping),
        200,
        media_type='text/event-stream',
    )

async def upload(request):
    user = request['user']
    try:
        accountId = request.path_params['accountId']
        account = user.accounts[accountId]
    except KeyError:
        return Response('No access to this accountId', 403)
    res = account.upload(request.stream(), request.headers['content-type'])
    return JSONResponse(res)


async def download(request):
    user = request['user']
    try:
        accountId = request.path_params['accountId']
        account = user.accounts[accountId]
    except KeyError:
        return Response('No access to this accountId', 403)
    blobId = request.path_params['blobId']
    try:
        body = account.download(blobId)
    except Exception as e:
        return Response(str(e), 404)
    name = request.path_params['name']
    headers = {
        'content-disposition': 'attachment; name=' + name
    }
    if 'type' in request.query_params:
        headers['content-type'] = request.query_params['type']
    return Response(body, 200, headers=headers)


async def well_known_jmap(request):
    res = {
        "capabilities": {name: module.capability
                         for name, module in CAPABILITIES.items()},
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
            "urn:ietf:params:jmap:mail": request.user.username,
            "urn:ietf:params:jmap:submission": request.user.username,
            "urn:ietf:params:jmap:vacationresponse": request.user.username,
        },
        "state": "0",
        "apiUrl": BASEURL + "/api/",
        "downloadUrl": BASEURL + "/download/{accountId}/{blobId}/{name}?type={type}",
        "uploadUrl": BASEURL + "/upload/{accountId}/",
        "eventSourceUrl": BASEURL + "/event/"#?types={types}&closeafter={closeafter}&ping={ping}",
    }
    return JSONResponse(res)


routes = [
    Route('/api/', api, methods=["POST", "GET"]),
    Route('/event/', event),
    Route('/upload/{accountId}', upload, methods=["POST"]),
    Route('/download/{accountId}/{blobId}/{name}', download),
    Route('/.well-known/jmap', well_known_jmap),
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
