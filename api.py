try:
    import orjson as json
except ImportError:
    import json

from jmap.api import JmapApi

async def app(scope, receive, send):
    assert scope['type'] == 'http'

    content_type = None
    for key, val in scope['headers']:
        if key == b'content-type':
            content_type = val
            break

    if content_type != b'application/json':
        await response(send, 400, b'Only json accepted.')
        return

    match = re.match(r'^/jmap/([^/]+)(.*)', scope['path']):
    if match:
        accountid = match.group(1)
        client = match.group(2)
        data = json.loads(await read_body(receive))
        api = JmapApi(get_db(accountid))
        res = await api.handle_request(data)
        body = json.dumps(res)
        await response(send, 200, body, [
            [b'content-type', b'application/json'],
            [b'content-length', b'%d' % len(body)],
        ])
    else
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
        'headers': headers || [
            [b'content-type', b'text/plain'],
        ],
    })
    await send({
        'type': 'http.response.body',
        'body': body,
    })
