import logging as log
from time import monotonic
import re

import jmap


USING_MIXINS = {
    'urn:ietf:params:jmap:calendars': jmap.Calendars,
    'urn:ietf:params:jmap:contacts': jmap.Contacts,
    'urn:ietf:params:jmap:vacationresponse': jmap.VacationResponse,
    'urn:ietf:params:jmap:submission': jmap.Submission,
    'urn:ietf:params:jmap:mail': jmap.Mail,
    'urn:ietf:params:jmap:core': jmap.Core,
}

def handle_request(data, db):
    results = []
    resultsByTag = {}

    # dynamic class creation
    bases = tuple(b for u, b in USING_MIXINS.items() if u in data['using'])
    idmap = data.get('createdIds', None)
    api = type('API', bases, dict(Api.__dict__))(db, idmap)
    # api = Api(db)

    for cmd, kwargs, tag in data['methodCalls']:
        t0 = monotonic()
        logbit = ''
        func = getattr(api, "api_" + cmd.replace('/', '_'), None)
        if not func:
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
            result = func(**kwargs)
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
        'sessionState': '0',
    }
    if 'createdIds' in data:
        out['createdIds'] = data['createdIds']
    return out


class Api:
    def __init__(self, db, idmap=None):
        self.db = db
        self._idmap = idmap or {}

    def _patchitem(self, item, path: str, val=None):
        try:
            prop, path = path.split('/', maxsplit=1)
            return self._patchitem(item[prop], path, val)
        except ValueError:
            if val is not None:
                item[path] = val
            elif path in item:
                del item[path]

    def _resolve_patch(self, accountId, update, get_data):
        for id, item in update.items():
            properties = {}
            for path in item.keys():
                try:
                    prop, _ = path.split('/', maxsplit=1)
                except ValueError:
                    continue
                if prop in properties:
                    properties[prop].append(path)
                else:
                    properties[prop] = [path]
            if not properties:
                continue  # nothing patched in this one

            data = get_data(accountId, ids=[id], properties=properties.keys())
            try:
                data = data['list'][0]
            except (KeyError, IndexError):
                # XXX - if nothing in the list we SHOULD abort
                continue
            for prop, paths in properties.items():
                item[prop] = data[prop]
                for path in paths:
                    self._patchitem(item, path, item.pop(path))

    def getRawBlob(self, selector):
        blobId, filename = selector.split('/', maxsplit=1)
        typ, data = self.db.get_blob(blobId)
        return typ, data, filename

    def uploadFile(self, accountid, typ, content):
        return self.db.put_file(accountid, typ, content)

    def downloadFile(self, jfileid):
        return self.db.get_file(jfileid)

    def setid(self, key, val):
        self._idmap[f'#{key}'] = val

    def idmap(self, key):
        return self._idmap.get(key, key)

    def begin(self):
        self.db.begin()

    def commit(self):
        self.db.commit()

    def rollback(self):
        self.db.rollback()


def _parsepath(path, item):
    match = re.match(r'^/([^/]+)', path)
    if not match:
        return item
    selector = match.group(1).replace('~1', '/').replace('~0', '~')
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
