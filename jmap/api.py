from inspect import isawaitable
import logging as log
from time import monotonic
import re

from jmap.account import ImapAccount
import jmap.core as core
import jmap.mail as mail
import jmap.submission as submission
import jmap.vacationresponse as vacationresponse
from jmap import errors


CAPABILITIES = {
    'urn:ietf:params:jmap:core': core,
    'urn:ietf:params:jmap:mail': mail,
    'urn:ietf:params:jmap:submission': submission,
    'urn:ietf:params:jmap:vacationresponse': vacationresponse,
}

METHODS = {}
for module in CAPABILITIES.values():
    module.register_methods(METHODS)


async def handle_request(user, data):
    results = []
    results_bytag = {}

    api = Api(user, data.get('createdIds', None))

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
            result = method(api, **kwargs)
            if isawaitable(result):
                result = await result
            results.append((method_name, result, tag))
            results_bytag[tag] = result
        except errors.JmapError as e:
            results.append(e.as_dict())
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
        'sessionState': user.sessionState,
    }
    if 'createdIds' in data:
        out['createdIds'] = data['createdIds']
    return out


class Api:
    def __init__(self, user, idmap=None):
        self.user = user
        self.idmap = IdMap(idmap or {})
        self.methods = {}
    
    def get_account(self, accountId) -> ImapAccount:
        try:
            return self.user.accounts[accountId]
        except KeyError:
            raise errors.accountNotFound()


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
