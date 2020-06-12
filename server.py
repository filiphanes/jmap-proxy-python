import re

try:
    import orjson as json
    OPT_INDENT_2 = json.OPT_INDENT_2
except ImportError:
    import json
    OPT_INDENT_2 = None

from jmap.api import JmapApi
from account import AccountManager

accounts = AccountManager()


async def app(scope, receive, send):
    assert scope['type'] == 'http'

    content_type = None
    for key, val in scope['headers']:
        if key == b'content-type':
            content_type = val
            break

    match = re.match(r'^/(\w+)/([^/]+)(.*)', scope['path'])
    cmd = match.group(1)
    accountid = match.group(2)
    # client = match.group(3)
    if cmd == 'jmap':
        if content_type != b'application/json':
            return await response(send, 400, b'Need json')
        data = json.loads(await read_body(receive))
        db = accounts.get_db(accountid)
        api = JmapApi(db)
        res = api.handle_request(data)
        body = json.dumps(res, option=OPT_INDENT_2)
        await response(send, 200, body, [
            [b'content-type', b'application/json'],
            [b'content-length', b'%d' % len(body)],
        ])
    elif cmd == 'firstsync':
        db = accounts.get_db(accountid)
        db.firstsync()
        await response(send, 200, b'Synced')
    elif cmd == 'syncall':
        db = accounts.get_db(accountid)
        db.sync_folders()
        db.sync_imap()
        # db.sync_addressbooks()
        # db.sync_calendars()
        await response(send, 200, b'Synced')
    else:
        await response(send, 404, b'404 Path Not found')


async def read_body(receive):
    """
    Read and return the entire body from an incoming ASGI message.
    """
    body = bytearray()
    message = await receive()
    body += message.get('body', b'')
    while message.get('more_body', False):
        message = await receive()
        body += message.get('body', b'')
    return body


async def response(send, status, body, headers=None):
    await send({
        'type': 'http.response.start',
        'status': status,
        'headers': headers or [
            [b'content-type', b'text/plain'],
        ],
    })
    await send({
        'type': 'http.response.body',
        'body': body,
    })
