from jmap import errors


def register_methods(methods):
    methods['Core/echo'] = api_Core_echo
    methods['Blob/copy'] = api_Blob_copy

MAX_OBJECTS_IN_GET = 1000

capability = {
    "collationAlgorithms": [],
    "maxCallsInRequest": 64,
    "maxObjectsInGet": MAX_OBJECTS_IN_GET,
    "maxSizeUpload": 250000000,
    "maxConcurrentRequests": 10,
    "maxObjectsInSet": 1000,
    "maxConcurrentUpload": 10,
    "maxSizeRequest": 10000000
}


def api_Core_echo(request, **kwargs):
    return kwargs


async def api_Blob_copy(request, fromAccountId, accountId, blobIds):
    fromAccount = request['user'].get_account(fromAccountId)
    account = request['user'].get_account(accountId)
    copied = {}
    for blobId in blobIds:
        body = await fromAccount.download(blobId)
        res = await account.upload(body)
        copied[blobId] = res['blobId']

    return {
        'fromAccountId': fromAccountId,
        'accountId': accountId,
        'copied': copied or None,
        'notCopied': None,
    }


def patch_item(item, path: str, val=None):
    try:
        prop, path = path.split('/', maxsplit=1)
        return patch_item(item[prop], path, val)
    except ValueError:
        if val is not None:
            item[path] = val
        elif path in item:
            del item[path]


def resolve_patch(request, accountId, update, get_data):
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

        data = get_data(request, accountId, ids=[id], properties=properties.keys())
        try:
            data = data['list'][0]
        except (KeyError, IndexError):
            # XXX - if nothing in the list we SHOULD abort
            continue
        for prop, paths in properties.items():
            item[prop] = data[prop]
            for path in paths:
                patch_item(item, path, item.pop(path))
