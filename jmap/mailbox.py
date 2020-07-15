from jmap import errors
from jmap.core import resolve_patch


def register_methods(api):
    api.methods.update({
        'Mailbox/get': api_Mailbox_get,
        'Mailbox/set': api_Mailbox_set,
        'Mailbox/changes': api_Mailbox_changes,
        'Mailbox/query': api_Mailbox_query,
        #TODO: 'Mailbox/queryChanges': api_Mailbox_queryChanges,
    })


def api_Mailbox_get(request, accountId=None, ids=None, properties=None):
    """
    https://jmap.io/spec-mail.html#mailboxget
    https://jmap.io/spec-core.html#get
    """
    account = request.get_account(accountId)
    mailboxes = account.db.get_mailboxes(deleted=0)

    if ids:
        want = set(request.idmap(i) for i in ids)
    else:
        want = set(d['id'] for d in mailboxes)

    if not properties:
        properties = ('id', 'name', 'parentId', 'role', 'sortOrder', 'totalEmails',
            'unreadEmails', 'totalThreads', 'unreadThreads', 'myRights', 'isSubscribed')
    lst = []
    for mbox in mailboxes:
        id = mbox['id']
        if id in want:
            want.remove(id)
            try:
                rec = {k: mbox[k] for k in properties}
            except KeyError as e:
                raise errors.invalidProperties(str(e))
            # rec['id'] = id  # always
            lst.append(rec)

    return {
        'accountId': accountId,
        'state': account.db.highModSeqMailbox,
        'list': lst,
        'notFound': list(want),
    }


def api_Mailbox_set(request, accountId=None, ifInState=None, create=None, update=None, destroy=None, onDestroyRemoveEmails=False):
    """
    https://jmap.io/spec-mail.html#mailboxset
    https://jmap.io/spec-core.html#set
    """
    account = request.get_account(accountId)
    account.db.sync_mailboxes()
    oldState = account.db.highModSeqMailbox
    if ifInState is not None and ifInState != oldState:
        raise errors.stateMismatch()

    # CREATE
    created = {}
    notCreated = {}
    if create:
        for cid, mailbox in create.items():
            try:
                id = account.db.create_mailbox(**mailbox)
                created[cid] = {'id': id}
                request.setid(cid, id)
            except errors.JmapError as e:
                notCreated[cid] = e.to_dict()

    # UPDATE
    updated = {}
    notUpdated = {}
    if update:
        for id, mailbox in update.items():
            try:
                account.db.update_mailbox(id, **mailbox)
                updated[id] = mailbox
            except errors.JmapError as e:
                notUpdated[id] = e.to_dict()

    # DESTROY
    destroyed = []
    notDestroyed = {}
    if destroy:
        for id in destroy:
            try:
                account.db.destroy_mailbox(id)
                destroyed.append(id)
            except errors.JmapError as e:
                notDestroyed[id] = e.to_dict()

    return {
        'accountId': accountId,
        'oldState': oldState,
        'newState': account.db.highModSeqMailbox,
        'created': created,
        'notCreated': notCreated,
        'updated': updated,
        'notUpdated': notUpdated,
        'destroyed': destroyed,
        'notDestroyed': notDestroyed,
    }


def api_Mailbox_query(request, accountId=None, sort=None, filter=None, position=0, anchor=None, anchorOffset=0, limit=None):
    """
    https://jmap.io/spec-mail.html#mailboxquery
    https://jmap.io/spec-core.html#get
    """
    account = request.get_account(accountId)
    mailboxes = account.db.get_mailboxes()
    if filter:
        mailboxes = [d for d in mailboxes if _mailbox_match(d, filter)]

    data = _mailbox_sort(mailboxes, sort, {'data': mailboxes})

    start = position
    if anchor:
        # need to calculate the position
        for i, x in enumerate(data):
            if x['id'] == anchor:
                start = i + anchorOffset
                break
        else:
            raise errors.anchorNotFound()
    
    if limit:
        end = start + limit - 1
    else:
        end = len(data)
    
    return {
        'accountId': accountId,
        'filter': filter,
        'sort': sort,
        'queryState': account.db.highModSeqMailbox,
        'canCalculateChanges': False,
        'position': start,
        'total': len(data),
        'ids': [x['id'] for x in data[start:end]],
    }


def api_Mailbox_changes(request, accountId, sinceState, maxChanges=None, **kwargs):
    """
    https://jmap.io/spec-mail.html#mailboxquerychanges
    https://jmap.io/spec-core.html#querychanges
    """
    account = request.get_account(accountId)
    new_state = account.db.highModSeqMailbox
    if sinceState <= str(account.db.lowModSeq):
        raise errors.cannotCalculateChanges({'new_state': new_state})
    mailboxes = account.db.get_mailboxes(modseq__gt=sinceState)

    if maxChanges and len(mailboxes) > maxChanges:
        raise errors.cannotCalculateChanges({'new_state': new_state})

    created = []
    updated = []
    removed = []
    only_counts = 0
    for mbox in mailboxes:
        if not mbox['deleted']:
            if mbox['created'] <= sinceState:
                updated.append(mbox['id'])
                if mbox['jnoncountsmodseq'] > sinceState:
                    only_counts = 0
            else:
                created.append(mbox['id'])
        else:
            if mbox['created'] <= sinceState:
                removed.append(mbox['id'])
            # otherwise never seen

    return {
        'accountId': accountId,
        'oldState': sinceState,
        'newState': new_state,
        'created': created,
        'updated': updated,
        'removed': removed,
        'changedProperties': ["totalEmails", "unreadEmails", "totalThreads", "unreadThreads"] if only_counts else None,
    }


def _mailbox_match(mbox, filter):
    if 'hasRole' in filter and \
        bool(filter['hasRole']) != bool(mbox.get('role', False)):
        return False

    if 'isSubscribed' in filter and \
        bool(filter['isSubscribed']) != bool(mbox.get('isSubscribed', False)):
        return False

    if 'parentId' in filter and \
        filter['parentId'] != mbox.get('parentId', None):
        return False        

    return True


def _makefullnames(mailboxes):
    idmap = {d['id']: d for d in mailboxes}
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
    def key(mbox):
        k = []
        for arg in sortargs:
            field = arg['property']
            if field == 'name':
                k.append(mbox['name'])
            elif field == 'sortOrder':
                k.append(mbox['sortOrder'])
            elif field == 'parent/name':
                if 'fullnames' not in storage:
                    storage['fullnames'] = _makefullnames(storage['data'])
                    k.append(storage['fullnames'][mbox['id']])
                k.append(mbox['sortOrder'])
            else:
                raise errors.unsupportedSort('Unknown field ' + field)

    return sorted(data, key=key)
