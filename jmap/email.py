from collections import defaultdict
try:
    import orjson as json
except ImportError:
    import json

from jmap import errors
from jmap.parse import asAddresses, asDate, asGroupedAddresses, asMessageIds, asRaw, asText, asURLs, htmltotext
from jmap.core import resolve_patch
import re


def register_methods(api):
    api.methods.update({
        'Email/get': api_Email_get,
        'Email/set': api_Email_set,
        'Email/query': api_Email_query,
        'Email/changes': api_Email_changes,
        #TODO: 'Email/queryChanges': api_Email_queryChanges,
    })


def api_Email_query(request, accountId, sort={}, filter={},
                    position=None, anchor=None, anchorOffset=None, limit:int=10000,
                    collapseThreads=False, calculateTotal=False):
    account = request.get_account(accountId)
    start = position or 0
    if anchor:
        if position is not None:
            raise errors.invalidArguments("anchor and position can't ")
    elif anchorOffset is not None:
        raise errors.invalidArguments("anchorOffset need anchor")

    if collapseThreads:
        messages = account.db.get_messages(['id','threadId'], sort=sort, **filter)
        # messages = [r['id'] for r in _collapse_messages(messages)]
    else:
        messages = account.db.get_messages('id', sort=sort, **filter)

    if anchor:
        # need to calculate position
        for i, row in enumerate(messages):
            if row['id'] == anchor:
                start = i + (anchorOffset or 0)
                if start < 0: start = 0
                break
        else:
            raise errors.anchorNotFound()
    
    end = start + limit
    if start < 0 and end >= 0:
        end = len(messages)
    
    out = {
        'accountId': accountId,
        'filter': filter,
        'sort': sort,
        'collapseThreads': collapseThreads,
        'queryState': account.db.highModSeqEmail,
        'canCalculateChanges': True,
        'position': start,
        'ids': [m['id'] for m in messages[start:end]],
    }

    if calculateTotal:
        out['total'] = len(messages)
        # raise errors.invalidArguments('calculateTotal not supported')

    return out


ALL_PROPERTIES = {
    'id', 'blobId', 'threadId', 'mailboxIds',
    'hasAttachemnt', 'keywords', 'subject',
    'sentAt', 'receivedAt', 'size',
    'from', 'to', 'cc', 'bcc', 'replyTo',
    'messageId', 'inReplyTo', 'references', 'sender',
    'attachments', 'hasAttachment', 'headers', 'preview',
    'textBody', 'htmlBody', 'bodyValues', 'references',
    # 'body'
}
ALL_BODY_PROPERTIES = {
    "partId", "blobId", "size", "name", "type",
    "charset", "disposition", "cid", "language", "location",
}

header_prop_re = re.compile(r'^header:([^:]+)(?::as(\w+))?(:all)?')

HEADER_FORMS = {
    None: asRaw,
    'Raw': asRaw,
    'Date': asDate,
    'Text': asText,
    'URLs': asURLs,
    'Addresses': asAddresses,
    'GroupedAddresses': asGroupedAddresses,
    'MessageIds': asMessageIds,
}


def api_Email_get(request,
        accountId,
        ids: list=None,
        properties=None,
        bodyProperties=None,
        fetchTextBodyValues=False,
        fetchHTMLBodyValues=False,
        fetchAllBodyValues=False,
        maxBodyValueBytes=0,
    ):
    """
    https://jmap.io/spec-mail.html#emailget
    https://jmap.io/spec-core.html#get
    """
    account = request.get_account(accountId)
    lst = []
    simple_props = set()
    header_props = set()
    if properties:
        for prop in properties:
            m = header_prop_re.match(prop)
            if m:
                header_props.add(m.group(0, 1, 2, 3))
                simple_props.add('headers')
            else:
                simple_props.add(prop)
        if 'body' in simple_props:
            simple_props.remove('body')
            simple_props.add('textBody')
            simple_props.add('htmlBody')
    else:
        properties = ALL_PROPERTIES

    if bodyProperties is None:
        bodyProperties = ALL_BODY_PROPERTIES

    if header_props and 'headers' not in properties:
        simple_props.remove('headers')
    if ids is None:
        # get all
        messages = account.db.get_messages(simple_props)
    else:
        notFound = set(request.idmap(i) for i in ids)
        messages = account.db.get_messages(simple_props, id__in=notFound)

    for msg in messages:
        if ids is not None:
            notFound.remove(msg['id'])
        # Fill most of msg properties except header:*
        data = {prop: msg[prop] for prop in simple_props}
        data['id'] = msg['id']
        if 'textBody' in msg and 'htmlBody' not in msg and not msg['textBody']:
            data['textBody'] = htmltotext(msg['htmlBody'])
        if 'bodyValues' in properties:
            if fetchHTMLBodyValues:
                data['bodyValues'] = {k: v for k, v in msg['bodyValues'].items() if v['type'] == 'text/html'}
            elif fetchTextBodyValues:
                data['bodyValues'] = {k: v for k, v in msg['bodyValues'].items() if v['type'] == 'text/plain'}
            elif fetchAllBodyValues:
                data['bodyValues'] = msg['bodyValues']
            if maxBodyValueBytes:
                for k, bodyValue in data['bodyValues'].items():
                    if len(bodyValue['value']) > maxBodyValueBytes:
                        bodyValue = {k: v for k, v in bodyValue.items()}
                        bodyValue['value'] = bodyValue['value'][:maxBodyValueBytes]
                        bodyValue['isTruncated'] = True,
                        data['bodyValues'][k] = bodyValue

        for prop, name, form, getall in header_props:
            try:
                func = HEADER_FORMS[form]
            except KeyError:
                raise errors.invalidProperties(f'Unknown header-form {form} in {prop}')

            name = name.lower()
            if getall:
                data[prop] = [func(h['value'])
                    for h in msg['headers'] if h['name'].lower() == name]
            else:
                data[prop] = func(msg.get_header(name))

        lst.append(data)

    return {
        'accountId': accountId,
        'list': lst,
        'state': account.db.highModSeqEmail,
        'notFound': list(notFound),
    }


def api_Email_changes(request, accountId, sinceState, maxChanges=None):
    account = request.get_account(accountId)
    newState = account.db.highModSeqEmail

    if sinceState <= str(account.db.lowModSeq):
        raise errors.cannotCalculateChanges({'new_state': newState})
    
    rows = account.db.dget('jmessages', {'jmodseq': ('>', sinceState)},
                        'msgid,deleted,jcreated,jmodseq')
    if maxChanges and len(rows) > maxChanges:
        raise errors.cannotCalculateChanges({'new_state': newState})

    created = []
    updated = []
    removed = []
    for msgid, deleted, jcreated in rows:
        if not deleted:
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
    account = request.get_account(accountId)

    # get state up-to-date first
    account.db.sync_imap()
    oldState = account.db.highModSeqEmail
    created, notCreated = account.db.create_messages(create, request.idmap)
    for id, msg in created.items():
        request.setid(id, msg['id'])

    resolve_patch(request, accountId, update, api_Email_get)
    updated, notUpdated = account.db.update_messages(update, request.idmap)
    destroyed, notDestroyed = account.db.destroy_messages(destroy)

    # XXX - cheap dumb racy version
    account.db.sync_imap()
    newState = account.db.highModSeqEmail

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


def _load_msgmap(account, id):
    rows = account.db.dget('jmessagemap', {}, 'msgid,jmailbox,jmodseq,deleted')
    msgmap = defaultdict(dict)
    for row in rows:
        msgmap[row['msgid']][row['jmailbox']] = row
    return msgmap


def _hasthreadkeyword(messages):
    res = {}
    for msg in messages:
        # we get called by getEmailListUpdates, which includes deleted messages
        if msg['deleted']:
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


def _match(account, item, condition, storage, idmap):
    if 'operator' in condition:
        if condition['operator'] == 'NOT':  # NOR
            for cond in condition['conditions']:
                if _match(account, item, cond, storage, idmap):
                    return False
            return True
        elif condition['operator'] == 'OR':
            for cond in condition['conditions']:
                if _match(account, item, cond, storage, idmap):
                    return True
            return False
        elif condition['operator'] == 'AND':
            for cond in condition['conditions']:
                if not _match(account, item, cond, storage, idmap):
                    return False
            return True
        raise ValueError(f"Invalid operator {condition['operator']}")
    
    cond = condition.get('inMailbox', None)
    if cond:
        id = idmap(cond)
        if 'mailbox' not in storage:
            storage['mailbox'] = {}
        if id not in storage['mailbox']:
            storage['mailbox'][id] = account.db.dgetby('jmessagemap', 'msgid', {'jmailboxid': id}, 'msgid,jmodseq,deleted')
        if item['msgid'] not in storage['mailbox'][id]\
            or storage['mailbox'][id][item['msgid']]['deleted']:
            return False
    
    cond = condition.get('inMailboxOtherThan', None)
    if cond:
        if 'msgmap' not in storage:
            storage['msgmap'] = _load_msgmap(account)
        if not isinstance(cond, list):
            cond = [cond]
        match = set(idmap(id) for id in cond)
        data = storage['msgmap'].get(item['msgid'], {})
        for id, msg in data.items():
            if id not in match and not msg['deleted']:
                break
        else:
            return False
    
    cond = condition.get('hasAttachment', None)
    if cond is not None:
        if 'hasatt' not in storage:
            storage['hasatt'] = set(account.db.dgetcol('jrawmessage', {'hasAttachment':1}, 'msgid'))
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

        # TODO: this is not correct when there are searches in multiple filter branches
        if search:
            storage['search'] = set(account.db.imap.search(search))
        else:
            storage['search'] = None

    if storage['search'] is not None and item['msgid'] not in storage['search']:
        return False
    
    #TODO: allInThreadHaveKeyword
    #TODO: someInThreadHaveKeyword
    #TODO: noneInThreadHaveKeyword

    return True


def _collapse_messages(messages):
    out = []
    seen = set()
    for msg in messages:
        if msg['thrid'] not in seen:
            out.append(msg)
            seen.add(msg['thrid'])
    return out
