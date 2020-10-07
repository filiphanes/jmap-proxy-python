from inspect import isawaitable
import logging as log
from time import monotonic
import re

from starlette.responses import Response

import jmap.core as core
import jmap.mail as mail
import jmap.submission as submission
import jmap.vacationresponse as vacationresponse
from jmap import errors

try:
    import orjson as json
except ImportError:
    import json

CAPABILITIES = {
    'urn:ietf:params:jmap:core': core,
    'urn:ietf:params:jmap:mail': mail,
    'urn:ietf:params:jmap:submission': submission,
    'urn:ietf:params:jmap:vacationresponse': vacationresponse,
}

METHODS = {}
for module in CAPABILITIES.values():
    module.register_methods(METHODS)


class JSONResponse(Response):
    media_type = "application/json"
    def render(self, content) -> bytes:
        return json.dumps(content)


async def api(request):
    results = []
    results_bytag = {}

    try:
        data = await request.json()
    except Exception:
        return JSONResponse({
            "type": "urn:ietf:params:jmap:error:notJson",
            "status": 400,
            "detail": "The content of the request did not parse as JSON."
        }, 400)

    request.scope['idmap'] = IdMap(data.get('createdIds', {}))

    for method_name, kwargs, tag in data['methodCalls']:
        t0 = monotonic() * 1000
        try:
            method = METHODS[method_name]
        except KeyError:
            results.append(('error', {'error': 'unknownMethod'}, tag))
            continue

        # resolve kwargs
        error = False
        for key in [k for k in kwargs.keys() if k[0] == '#']:
            # we are updating dict over which we iterate
            # please check that your changes don't skip keys
            val = kwargs.pop(key)
            val = _parsepath(val['path'], results_bytag[val['resultOf']])
            if val is None:
                results.append(('error',
                    {'type': 'resultReference', 'message': repr(val)}, tag))
                error = True
                break
            elif not isinstance(val, list):
                val = [val]
            kwargs[key[1:]] = val
        if error:
            continue

        try:
            result = method(request, **kwargs)
            if isawaitable(result):
                result = await result
            if type(result) is tuple:
                # Emailsubmission/set may return 2 responses
                for res in result:
                    results.append((res.pop('method_name', method_name), res, tag))
            else:
                results.append((method_name, result, tag))
            results_bytag[tag] = result
        except errors.JmapError as e:
            results.append(e.to_dict())
        except Exception as e:
            results.append(('error', {
                'type': e.__class__.__name__,
                'message': str(e),
            }, tag))
            raise e
        finally:
            log_method_call(method_name, monotonic() * 1000 - t0, kwargs)

    out = {
        'methodResponses': results,
        'sessionState': request['user'].sessionState,
    }
    if 'createdIds' in data:
        out['createdIds'] = data['createdIds']
    return JSONResponse(out)


class IdMap(dict):
    def __missing__(self, key):
        return key

    def get(self, key):
        return self[key]

    def set(self, key, value):
        self[f"#{key}"] = value


def _parsepath(path, item):
    match = re.match(r'^/([^/]+)', path)
    if not match:
        return item
    selector = match.group(1)
    if isinstance(item, list):
        if selector == '*':
            res = []
            for one in item:
                r = _parsepath(path[match.end():], one)
                if isinstance(r, list):
                    res.extend(r)
                else:
                    res.append(r)
            return res
        if selector.isnumeric():
            return item[int(selector)]

    elif isinstance(item, dict):
        return _parsepath(path[match.end():], item[selector])

    return item


def log_method_call(method_name, elapsed: float, kwargs):
    logbit = ''
    if kwargs.get('ids', None):
        logbit += " [" + (",".join(kwargs['ids'][:10]))
        if len(kwargs['ids']) > 10:
            logbit += ", ..." + str(len(kwargs['ids']))
        logbit += "]"
    if kwargs.get('properties', None):
        logbit += " (" + (",".join(kwargs['properties'][:10]))
        if len(kwargs['properties']) > 10:
            logbit += ", ..." + str(len(kwargs['properties']))
        logbit += ")"
    log.info(f'JMAP CMD {method_name}{logbit} {elapsed:0.3f} ms')
