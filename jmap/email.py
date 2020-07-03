from collections import defaultdict
try:
    import orjson as json
except ImportError:
    import json

from jmap import errors
from jmap.parse import htmltotext, asAddresses, asDate, asMessageIds, asText, asURLs
from jmap.core import resolve_patch


def register_methods(api):
    api.methods.update({
        'Email/get': api_Email_get,
        'Email/set': api_Email_set,
        'Email/query': api_Email_query,
        'Email/changes': api_Email_changes,
        #TODO: 'Email/queryChanges': api_Email_queryChanges,
    })


def api_Email_query(request, accountId, position=None, anchor=None,
                    anchorOffset=None, sort={}, filter={}, limit=10,
                    collapseThreads=False, calculateTotal=False):
    try:
        account = request.user.accounts[accountId]
    except KeyError:
        raise errors.accountNotFound()
    user = account.db.get_user()

    newQueryState = user['jstateEmail']
    if position is not None and anchor is not None:
        raise ValueError('invalid arguments')
    # anchor and anchorOffset must go together
    if (anchor is None) != (anchorOffset is None):
        raise ValueError('invalid arguments')
    
    start = position or 0
    if start < 0:
        raise ValueError('invalid arguments')
    rows = request.db.dget('jmessages', {'active': 1})
    rows = [dict(row) for row in rows]
    for row in rows:
        row['keywords'] = json.loads(row['keywords'] or '{}')
    storage = {'data': rows}
    rows = _post_sort(rows, sort, storage)
    if filter:
        rows = _messages_filter(request, rows, filter, storage)
    if collapseThreads:
        rows = _collapse_messages(rows)
    
    if anchor:
        # need to calculate position
        for i, row in enumerate(rows):
            if row['msgid'] == anchor:
                start = max(i + anchorOffset, 0)
                break
        else:
            raise Exception('anchor not found')

    end = start + limit if limit else len(rows)
    if end > len(rows):
        end = len(rows)
    
    return {
        'accountId': request.db.accountid,
        'filter': filter,
        'sort': sort,
        'collapseThreads': collapseThreads,
        'queryState': newQueryState,
        'canCalculateChanges': True,
        'position': start,
        'total': len(rows),
        'ids': [rows[i]['msgid'] for i in range(start, end)],
    }


def api_Email_get(request,
        accountId,
        ids: list,
        properties=None,
        bodyProperties=None,
        fetchTextBodyValues=False,
        fetchHTMLBodyValues=False,
        fetchAllBodyValues=False,
        maxBodyValueBytes=0,
    ):
    """https://jmap.io/spec-mail.html#emailget"""
    try:
        account = request.user.accounts[accountId]
    except KeyError:
        raise errors.accountNotFound()
    user = account.db.get_user()
    newState = user['jstateEmail']
    seenids = set()
    notFound = []
    lst = []
    headers_wanted = set()
    content_props = {'attachments', 'hasAttachment', 'headers', 'preview',
                        'body', 'textBody', 'htmlBody', 'bodyValues', 'references'}
    if properties:
        need_content = False
        for prop in properties:
            if prop.startswith('header:'):
                headers_wanted.add(prop)
                need_content = True
            elif prop in content_props:
                need_content = True
    else:
        properties = content_props + {
            'threadId', 'mailboxIds',
            'hasAttachemnt', 'keywords', 'subject', 'sentAt',
            'receivedAt', 'size', 'blobId',
            'from', 'to', 'cc', 'bcc', 'replyTo',
            'messageId', 'inReplyTo', 'references', 'sender',
        }
        need_content = True
    if not bodyProperties:
        bodyProperties = {"partId", "blobId", "size", "name", "type",
            "charset", "disposition", "cid", "language", "location",
        }

    msgids = [request.idmap(i) for i in ids]
    if need_content:
        contents = request.db.fill_messages(msgids)
    else:
        contents = {}

    for msgid in msgids:
        if msgid not in seenids:
            seenids.add(msgid)
            data = request.db.dgetone('jmessages', {'msgid': msgid})
            if not data:
                notFound.append(msgid)
                continue

        msg = {'id': msgid}
        if 'blobId' in properties:
            msg['blobId'] = msgid
        if 'threadId' in properties:
            msg['threadId'] = data['thrid']
        if 'mailboxIds' in properties:
            ids = request.db.dgetcol('jmessagemap', {'msgid': msgid, 'active': 1}, 'jmailboxid')
            msg['mailboxIds'] = {i: True for i in ids}
        if 'keywords' in properties:
            msg['keywords'] = json.loads(data['keywords'])
        if 'messageId' in properties:
            msg['messageId'] = data['messageid'] and [data['messageid']]

        for prop in ('from', 'to', 'cc', 'bcc', 'replyTo', 'sender'):
            if prop in properties:
                msg[prop] = json.loads(data[prop])
        for prop in ('subject', 'size', 'inReplyTo', 'sentAt', 'receivedAt'):
            if prop in properties:
                msg[prop] = data[prop]
        
        if msgid in contents:
            data = contents[msgid]
            for prop in ('preview', 'textBody', 'htmlBody', 'attachments'):
                if prop in properties:
                    msg[prop] = data[prop]
            if 'body' in properties:
                if data['htmlBody']:
                    msg['htmlBody'] = data['htmlBody']
                else:
                    msg['textBody'] = data['textBody']
            if 'textBody' in msg and not msg['textBody']:
                msg['textBody'] = htmltotext(data['htmlBody'])
            if 'hasAttachment' in properties:
                msg['hasAttachment'] = bool(data['hasAttachment'])
            if 'headers' in properties:
                msg['headers'] = data['headers']
            if 'bodyStructure' in properties:
                msg['bodyStructure'] = data['bodyStructure']
            if 'references' in properties:
                for hdr in data['headers']:
                    if hdr['name'].lower() == 'references':
                        msg['references'] = asMessageIds(hdr['value'])
                        break
                else:
                    msg['references'] = []
            if 'bodyValues' in properties:
                if fetchAllBodyValues:
                    msg['bodyValues'] = data['bodyValues']
                elif fetchHTMLBodyValues:
                    msg['bodyValues'] = {k: v for k, v in data['bodyValues'].items() if v['type'] == 'text/html'}
                elif fetchTextBodyValues:
                    msg['bodyValues'] = {k: v for k, v in data['bodyValues'].items() if v['type'] == 'text/plain'}
                if maxBodyValueBytes:
                    for val in msg['bodyValues'].values():
                        val['value'] = val['value'][:maxBodyValueBytes]
                        val['isTruncated'] = True

            for prop in headers_wanted:
                try:
                    _, field, form = prop.split(':')
                except ValueError:
                    field, form = prop[8:], 'raw'
                field = field.lower()
                if form == 'all':
                    msg[prop] = [v for k, v in data['headers'] if k.lower() == field]
                    continue
                elif form == 'asDate':
                    func = asDate
                elif form == 'asText':
                    func = asText
                elif form == 'asURLs':
                    func = asURLs
                elif form == 'asAddresses':
                    func = asAddresses

                for hdr in data['headers']:
                    if hdr['name'].lower() == field:
                        msg[prop] = func(hdr['value'])
                        break
                else:
                    msg[prop] = None

        lst.append(msg)

    return {
        'accountId': accountId,
        'list': lst,
        'state': newState,
        'notFound': notFound
    }


def api_Email_changes(request, accountId, sinceState, maxChanges=None):
    try:
        account = request.user.accounts[accountId]
    except KeyError:
        raise errors.accountNotFound()
    user = account.db.get_user()
    newState = user['jstateEmail']

    if user['jdeletedmodseq'] and sinceState <= str(user['jdeletedmodseq']):
        raise errors.cannotCalculateChanges({'new_state': newState})
    
    rows = request.db.dget('jmessages', {'jmodseq': ('>', sinceState)},
                        'msgid,active,jcreated,jmodseq')
    if maxChanges and len(rows) > maxChanges:
        raise errors.cannotCalculateChanges({'new_state': newState})

    created = []
    updated = []
    removed = []
    for msgid, active, jcreated in rows:
        if active:
            if jcreated <= sinceState:
                updated.append(msgid)
            else:
                created.append(msgid)
        elif jcreated <= sinceState:
            removed.append(msgid)
        # else never seen
    
    return {
        'accountId': accountId,
        'oldState': sinceState,
        'newState': newState,
        'created': created,
        'updated': updated,
        'removed': removed,
    }


def api_Email_set(request, accountId, create={}, update={}, destroy=()):
    try:
        account = request.user.accounts[accountId]
    except KeyError:
        raise errors.accountNotFound()
    user = account.db.get_user()

    # get state up-to-date first
    request.db.sync_imap()
    user = request.db.get_user()
    oldState = user['jstateEmail']
    created, notCreated = request.db.create_messages(create, request.idmap)
    for id, msg in created.items():
        request.setid(id, msg['id'])

    resolve_patch(request, accountId, update, api_Email_get)
    updated, notUpdated = request.db.update_messages(update, request._idmap)
    destroyed, notDestroyed = request.db.destroy_messages(destroy)

    # XXX - cheap dumb racy version
    request.db.sync_imap()
    user = request.db.get_user()
    newState = user['jstateEmail']

    for cid, msg in created.items():
        created[cid]['blobId'] = msg['id']
    
    return {
        'accountId': accountId,
        'oldState': oldState,
        'newState': newState,
        'created': created,
        'notCreated': notCreated,
        'updated': updated,
        'notUpdated': notUpdated,
        'destroyed': destroyed,
        'notDestroyed': notDestroyed,
    }


def _post_sort(data, sortargs, storage):
    return data
    # TODO: sort key function
    fieldmap = {
        'id': ('msgid', 0),
        'receivedAt': ('receivedAt', 1),
        'sentAt': ('sentAt', 1),
        'size': ('size', 1),
        'isUnread': ('isUnread', 1),
        'subject': ('sortsubject', 0),
        'from': ('from', 0),
        'to': ('to', 0),
    }


def _load_msgmap(request, id):
    rows = request.db.dget('jmessagemap', {}, 'msgid,jmailbox,jmodseq,active')
    msgmap = defaultdict(dict)
    for row in rows:
        msgmap[row['msgid']][row['jmailbox']] = row
    return msgmap


def _load_hasatt(request):
    return set(request.db.dgetcol('jrawmessage', {'hasAttachment':1}, 'msgid'))


def _hasthreadkeyword(messages):
    res = {}
    for msg in messages:
        # we get called by getEmailListUpdates, which includes inactive messages
        if not msg['active']:
            continue
        # have already seen a message for this thread
        if msg['thrid'] in res:
            for keyword in msg['keywords'].keys():
                # if not already known about, it wasn't present on previous messages, so it's a "some"
                if not res[msg['thrid']][keyword]:
                    res[msg['thrid']][keyword] = 1
            for keyword in res[msg['thrid']].keys():
                # if it was known already, but isn't on this one, it's a some
                if not msg['keywords'][keyword]:
                    res[msg['thrid']][keyword] = 1
        else:
            # first message, it's "all" for every keyword
            res[msg['thrid']] = {kw: 2 for kw in msg['keywords'].keys()}
    return res


def _match(request, item, condition, storage):
    if 'operator' in condition:
        return _match_operator(request, item, condition, storage)
    
    cond = condition.get('inMailbox', None)
    if cond:
        id = request.idmap(cond)
        if 'mailbox' not in storage:
            storage['mailbox'] = {}
        if id not in storage['mailbox']:
            storage['mailbox'][id] = request.db.dgetby('jmessagemap', 'msgid', {'jmailboxid': id}, 'msgid,jmodseq,active')
        if item['msgid'] not in storage['mailbox'][id]\
            or not storage['mailbox'][id][item['msgid']]['active']:
            return False
    
    cond = condition.get('inMailboxOtherThan', None)
    if cond:
        if 'msgmap' not in storage:
            storage['msgmap'] = _load_msgmap(request)
        if not isinstance(cond, list):
            cond = [cond]
        match = set(request.idmap(id) for id in cond)
        data = storage['msgmap'].get(item['msgid'], {})
        for id, msg in data.items():
            if id not in match and msg['active']:
                break
        else:
            return False
    
    cond = condition.get('hasAttachment', None)
    if cond is not None:
        if 'hasatt' not in storage:
            storage['hasatt'] = _load_hasatt(request)
        if item['msgid'] not in storage['hasatt']:
            return False
    
    if 'search' not in storage:
        search = []
        for field in ('before','after','text','from','to','cc','bcc','subject','body','header'):
            if field in condition:
                search.append(field)
                search.append(condition[field])
        for cond, field in [
                ('minSize', 'LARGER'),   # or NOT SMALLER?
                ('maxSize', 'SMALLER'),  # or NOT LARGER?
                ('hasKeyword', 'KEYWORD'),
                ('notKeyword', 'UNKEYWORD'),
            ]:
            if cond in condition:
                search.append(field)
                search.append(condition[cond])

        if search:
            storage['search'] = set(request.db.imap.search(search))
        else:
            storage['search'] = None

    if storage['search'] is not None and item['msgid'] not in storage['search']:
        return False
    
    #TODO: allInThreadHaveKeyword
    #TODO: someInThreadHaveKeyword
    #TODO: noneInThreadHaveKeyword

    return True


def _match_operator(request, item, filter, storage):
    if filter['operator'] == 'NOT':
        return not _match_operator(request, item, {
            'operator': 'OR',
            'conditions': filter['conditions']},
            storage)
    elif filter['operator'] == 'OR':
        for condition in filter['conditions']:
            if _match(request, item, condition, storage):
                return True
            return False
    elif filter['operator'] == 'AND':
        for condition in filter['conditions']:
            if not _match(request, item, condition, storage):
                return False
            return True
    raise ValueError(f"Invalid operator {filter['operator']}")


def _messages_filter(request, data, filter, storage):
    return [d for d in data if _match(request, d, filter, storage)]


def _collapse_messages(messages):
    out = []
    seen = set()
    for msg in messages:
        if msg['thrid'] not in seen:
            out.append(msg)
            seen.add(msg['thrid'])
    return out
