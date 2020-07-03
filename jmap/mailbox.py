from jmap import errors


def register_methods(api):
    api.methods.update({
        'Mailbox/get': api_Mailbox_get,
        #TODO: 'Mailbox/set': api_Mailbox_set,
        'Mailbox/changes': api_Mailbox_changes,
        'Mailbox/query': api_Mailbox_query,
        #TODO: 'Mailbox/queryChanges': api_Mailbox_queryChanges,
    })


def api_Mailbox_get(request, accountId=None, ids=None, properties=None, **kwargs):
    try:
        account = request.user.accounts[accountId]
    except KeyError:
        raise errors.accountNotFound()
    user = account.db.get_user()
    new_state = user.get('jstateMailbox', None)
    rows = request.db.dget('jmailboxes', {'active': 1})

    if ids:
        want = set(request.idmap(i) for i in ids)
    else:
        want = set(d['jmailboxid'] for d in rows)

    lst = []
    for item in rows:
        if item['jmailboxid'] not in want:
            continue
        want.remove(item['jmailboxid'])
        rec = {
            'name': item['name'],
            'parentId': item['parentId'] or None,
            'role': item['role'],
            'sortOrder': item['sortOrder'] or 0,
            'totalEmails': item['totalEmails'] or 0,
            'unreadEmails': item['unreadEmails'] or 0,
            'totalThreads': item['totalThreads'] or 0,
            'unreadThreads': item['unreadThreads'] or 0,
            'myRights': {k: bool(item[k]) for k in (
                'mayReadItems',
                'mayAddItems',
                'mayRemoveItems',
                'maySetSeen',
                'maySetKeywords',
                'mayCreateChild',
                'mayRename',
                'mayDelete',
                'maySubmit',
                )},
            'isSubscribed': bool(item['isSubscribed']),
        }
        if properties:
            rec = {k: rec[k] for k in properties if k in rec}
        rec['id'] = item['jmailboxid']
        lst.append(rec)

    return {
        'list': lst,
        'accountId': request.db.accountid,
        'state': new_state,
        'notFound': list(want)
    }

def api_Mailbox_query(request, accountId=None, sort=None, filter=None, position=0, anchor=None, anchorOffset=0, limit=None, **kwargs):
    try:
        account = request.user.accounts[accountId]
    except KeyError:
        raise errors.accountNotFound()
    user = account.db.get_user()
    rows = request.db.dget('jmailboxes', {'active': 1})
    if filter:
        rows = [d for d in rows if _mailbox_match(d, filter)]

    storage = {'data': rows}
    data = _mailbox_sort(rows, sort, storage)

    start = position
    if anchor:
        # need to calculate the position
        i = [x['jmailboxid'] for x in data].index(anchor)
        if i < 0:
            raise errors.anchorNotFound()
        start = i + anchorOffset
    
    if limit:
        end = start + limit - 1
    else:
        end = len(data)
    
    return {
        'accountId': accountId,
        'filter': filter,
        'sort': sort,
        'queryState': user.get('jstateMailbox', None),
        'canCalculateChanges': False,
        'position': start,
        'total': len(data),
        'ids': [x['jmailboxid'] for x in data[start:end]],
    }

def api_Mailbox_changes(request, accountId, sinceState, maxChanges=None, **kwargs):
    try:
        account = request.user.accounts[accountId]
    except KeyError:
        raise errors.accountNotFound()
    user = account.db.get_user()
    new_state = user['jstateMailbox']
    if user['jdeletedmodseq'] and sinceState <= str(user['jdeletedmodseq']):
        raise errors.cannotCalculateChanges({'new_state': new_state})
    rows = request.db.dget('jmailboxes', {'jmodseq': ['>', sinceState]})

    if maxChanges and len(rows) > maxChanges:
        raise errors.cannotCalculateChanges({'new_state': new_state})

    created = []
    updated = []
    removed = []
    only_counts = 0
    for item in rows:
        if item['active']:
            if item['jcreated'] <= sinceState:
                updated.append(item['jmailboxid'])
                if item['jnoncountsmodseq'] > sinceState:
                    only_counts = 0
            else:
                created.append(item['jmailboxid'])
        else:
            if item['jcreated'] <= sinceState:
                removed.append(item['jmailboxid'])
            # otherwise never seen

    return {
        'accountId': request.db.accountid,
        'oldState': sinceState,
        'newState': new_state,
        'created': created,
        'updated': updated,
        'removed': removed,
        'changedProperties': ["totalEmails", "unreadEmails", "totalThreads", "unreadThreads"] if only_counts else None,
    }


def _mailbox_match(item, filter):
    if 'hasRole' in filter and \
        bool(filter['hasRole']) != bool(item.get('role', False)):
        return False

    if 'isSubscribed' in filter and \
        bool(filter['isSubscribed']) != bool(item.get('isSubscribed', False)):
        return False

    if 'parentId' in filter and \
        filter['parentId'] != item.get('parentId', None):
        return False        

    return True


def _makefullnames(mailboxes):
    idmap = {d['jmailboxid']: d for d in mailboxes}
    idmap.pop('', None)  # just in case
    fullnames = {}
    for id, mbox in idmap.items():
        names = []
        while mbox:
            names.append(mbox['name'])
            mbox = idmap.get(mbox['parentId'], None)
        fullnames[id] = '\x1e'.join(reversed(names))
    return fullnames


def _mailbox_sort(data, sortargs, storage):
    return data
    #TODO: make correct sorting
    def key(item):
        k = []
        for arg in sortargs:
            field = arg['property']
            if field == 'name':
                k.append(item['name'])
            elif field == 'sortOrder':
                k.append(item['sortOrder'])
            elif field == 'parent/name':
                if 'fullnames' not in storage:
                    storage['fullnames'] = _makefullnames(storage['data'])
                    k.append(storage['fullnames'][item['jmailboxid']])
                k.append(item['sortOrder'])
            else:
                raise Exception('Unknown field ' + field)

    return sorted(data, key=key)
