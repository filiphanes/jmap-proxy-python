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


async def api_Email_query(request, accountId, sort={}, filter={},
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
        messages = await account.db.get_messages(['id','threadId'], sort=sort, deleted=0, **filter)
        # messages = [r['id'] for r in _collapse_messages(messages)]
    else:
        messages = await account.db.get_messages('id', sort=sort, deleted=0, **filter)

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
        'queryState': await account.db.email_state(),
        'canCalculateChanges': False,
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


async def api_Email_get(request,
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
        messages = await account.db.get_messages(simple_props, deleted=0)
    else:
        notFound = set(request.idmap(i) for i in ids)
        messages = await account.db.get_messages(simple_props, id__in=notFound, deleted=0)

    for msg in messages:
        if ids is not None:
            try:
                notFound.remove(msg['id'])
            except KeyError:
                print(f"Got msg {msg['id']}, but not requested")
                raise
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
        'state': await account.db.email_state(),
        'notFound': list(notFound),
    }


async def api_Email_changes(request, accountId, sinceState, maxChanges=None):
    account = request.get_account(accountId)
    newState = await account.db.email_state()

    if sinceState <= await account.db.email_state_low():
        raise errors.cannotCalculateChanges({'new_state': newState})

    messages = await account.db.get_messages(['id','created','updated','deleted'], updated__gt=sinceState)
    if maxChanges and len(messages) > maxChanges:
        raise errors.cannotCalculateChanges({'new_state': newState})

    removed = []
    created = []
    updated = []
    for msg in messages:
        if msg['deleted']:
            # dont append if it was created and deleted
            if msg['created'] <= sinceState:
                removed.append(msg['id'])
        elif msg['created'] > sinceState:
            created.append(msg['id'])
        else:
            updated.append(msg['id'])

    return {
        'accountId': accountId,
        'oldState': sinceState,
        'newState': newState,
        'hasMoreChanges': False,
        'created': created,
        'updated': updated,
        'removed': removed,
    }


async def api_Email_set(request, accountId, ifInState=None, create={}, update={}, destroy=()):
    account = request.get_account(accountId)
    oldState = await account.db.email_state()
    if ifInState is not None and ifInState != oldState:
        raise errors.stateMismatch()

    # CREATE
    created = {}
    notCreated = {}
    if create:
        for cid, message in create.items():
            try:
                id = await account.db.create_message(**message)
                created[cid] = {'id': id}
                request.setid(cid, id)
            except errors.JmapError as e:
                notCreated[cid] = e.to_dict()

    # UPDATE
    updated = {}
    notUpdated = {}
    messages = await account.db.get_messages(('keywords', 'mailboxIds'), id__in=update.keys(), deleted=0)
    byid = {msg['id']: msg for msg in messages}
    if update:
        for id, message in update.items():
            if id not in byid:
                notUpdated[id] = errors.notFound().to_dict()
                continue
            try:
                await account.db.update_message(id, ifInState, **message)
                updated[id] = message
            except errors.JmapError as e:
                notUpdated[id] = e.to_dict()

    # DESTROY
    destroyed = []
    notDestroyed = {}
    if destroy:
        for id in destroy:
            try:
                await account.db.destroy_message(id)
                destroyed.append(id)
            except errors.JmapError as e:
                notDestroyed[id] = errors.notFound().to_dict()

    for cid, msg in created.items():
        created[cid]['blobId'] = msg['id']
    
    return {
        'accountId': accountId,
        'oldState': oldState,
        'newState': await account.db.email_state(),
        'created': created,
        'notCreated': notCreated,
        'updated': updated,
        'notUpdated': notUpdated,
        'destroyed': destroyed,
        'notDestroyed': notDestroyed,
    }


def _collapse_messages(messages):
    out = []
    seen = set()
    for msg in messages:
        if msg['thrid'] not in seen:
            out.append(msg)
            seen.add(msg['thrid'])
    return out
