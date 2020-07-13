import logging as log
from time import monotonic
import re

from jmap.account import ImapAccount
import jmap.core as core
import jmap.mail as mail
import jmap.submission as submission
import jmap.vacationresponse as vacationresponse
import jmap.contacts as contacts
import jmap.calendars as calendars
from jmap import errors


CAPABILITIES = {
    'urn:ietf:params:jmap:core': core,
    'urn:ietf:params:jmap:mail': mail,
    # 'urn:ietf:params:jmap:submission': jmap.submission,
    # 'urn:ietf:params:jmap:vacationresponse': jmap.vacationresponse,
    # 'urn:ietf:params:jmap:contacts': jmap.contacts,
    # 'urn:ietf:params:jmap:calendars': jmap.calendars,
}

def handle_request(user, data):
    results = []
    resultsByTag = {}

    api = Api(user, data.get('createdIds', None))
    for capability in data['using']:
        CAPABILITIES[capability].register_methods(api)

    for cmd, kwargs, tag in data['methodCalls']:
        t0 = monotonic()
        logbit = ''
        try:
            func = api.methods[cmd]
        except KeyError:
            results.append(('error', {'error': 'unknownMethod'}, tag))
            continue

        # resolve kwargs
        error = False
        for key in [k for k in kwargs.keys() if k[0] == '#']:
            # we are updating dict over which we iterate
            # please check that your changes don't skip keys
            val = kwargs.pop(key)
            val = _parsepath(val['path'], resultsByTag[val['resultOf']])
            if val is None:
                results.append(('error',
                    {'type': 'resultReference', 'message': repr(val)}, tag))
                error = True
                break
            elif not isinstance(val, list):
                val = [val]
            kwargs[key[1:]] = val
        if error: continue

        try:
            result = func(api, **kwargs)
            results.append((cmd, result, tag))
            resultsByTag[tag] = result
        except Exception as e:
            results.append(('error', {
                'type': e.__class__.__name__,
                'message': str(e),
            }, tag))
            raise e
            api.rollback()

        elapsed = monotonic() - t0

        # log method call
        if kwargs.get('ids', None):
            logbit += " [" + (",".join(kwargs['ids'][:4]))
            if len(kwargs['ids']) > 4:
                logbit += ", ..." + str(len(kwargs['ids']))
            logbit += "]"
        if kwargs.get('properties', None):
            logbit += " (" + (",".join(kwargs['properties'][:4]))
            if len(kwargs['properties']) > 4:
                logbit += ", ..." + str(len(kwargs['properties']))
            logbit += ")"
        log.info(f'JMAP CMD {cmd}{logbit} took {elapsed}')

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
        self._idmap = idmap or {}
        self.methods = {}
    
    def get_account(self, accountId) -> ImapAccount:
        try:
            return self.user.accounts[accountId]
        except KeyError:
            raise errors.accountNotFound()
    
    def setid(self, key, val):
        self._idmap[f'#{key}'] = val

    def idmap(self, key):
        return self._idmap.get(key, key)


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
