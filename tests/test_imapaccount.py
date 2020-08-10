import pytest
from random import random

import jmap
from jmap import errors


@pytest.mark.asyncio
async def test_mailbox_get_all(account, idmap):
    response = await account.mailbox_get(idmap)
    assert response['accountId'] == account.id
    assert int(response['state']) > 0
    assert isinstance(response['notFound'], list)
    assert len(response['notFound']) == 0
    assert isinstance(response['list'], list)
    assert len(response['list']) > 0
    for mailbox in response['list']:
        assert mailbox['id']
        assert mailbox['name']
        assert mailbox['myRights']
        assert 'role' in mailbox
        assert 'sortOrder' in mailbox
        assert 'totalEmails' in mailbox
        assert 'totalThreads' in mailbox
        assert 'unreadThreads' in mailbox
        assert 'isSubscribed' in mailbox
        assert 'parentId' in mailbox


@pytest.mark.asyncio
async def test_mailbox_get_notFound(account, idmap):
    wrong_ids = ['notexisting', 123]
    properties = ['name', 'myRights']
    response = await account.mailbox_get(
        idmap,
        ids=wrong_ids,
        properties=properties,
    )
    assert response['accountId'] == account.id
    assert int(response['state']) > 0
    assert isinstance(response['notFound'], list)
    assert set(response['notFound']) == set(wrong_ids)
    assert isinstance(response['list'], list)
    assert len(response['list']) == 0


@pytest.mark.asyncio
async def test_mailbox_set_fail(account, idmap):
    with pytest.raises(errors.stateMismatch):
        await account.mailbox_set(idmap, ifInState='wrongstate')

@pytest.mark.asyncio
async def test_mailbox_create_duplicate(account, idmap):
    response = await account.mailbox_set(
            idmap,
            create={
                "test": {
                    "parentId": None,
                    "name": 'INBOX',
                }
            }
        )
    assert response['notCreated']['test']['type'] == 'invalidArguments'


@pytest.mark.asyncio
async def test_mailbox_create_rename_destroy(account, idmap, inbox_id):
    # Create
    response = await account.mailbox_set(
        idmap,
        create={
            "test": {
                "parentId": inbox_id,
                "name": str(random())[2:10],
                "isSubscribed": False,
            }
        }
    )
    newId = response['created']['test']['id']
    assert not response['notCreated']
    assert not response['updated']
    assert not response['notUpdated']
    assert not response['destroyed']
    assert not response['notDestroyed']

    # Rename
    update = {newId: {"name": " ÁÝŽ-\\"}}
    response = await account.mailbox_set(idmap, update=update)
    assert not response['created']
    assert not response['notCreated']
    assert response['updated'] == update
    assert not response['notUpdated']
    assert not response['notUpdated']
    assert not response['destroyed']

    # Destroy
    response = await account.mailbox_set(idmap, destroy=[newId])
    assert not response['created']
    assert not response['notCreated']
    assert not response['updated']
    assert not response['notUpdated']
    assert response['destroyed'] == [newId]
    assert not response['notDestroyed']


@pytest.mark.asyncio
async def test_mailbox_query(account, inbox_id):
    response = await account.mailbox_query(
        filter={"parentId": inbox_id},
        sort=[{"property": "sortOrder"},{"property": "name"}],
        position=0,
        limit=10,
        calculateTotal=True,
    )
    assert response['accountId'] == account.id
    assert isinstance(response['ids'], list)
    assert 0 < len(response['ids']) <= 10


@pytest.mark.asyncio
async def test_email_query_inMailbox(account, inbox_id, email_id):
    response = await account.email_query(**{
        "filter": {"inMailbox": inbox_id},
        "anchor": email_id,
        "collapseThreads": False,
        "limit": 10,
        "calculateTotal": True
    })
    assert response['accountId'] == account.id
    assert response['position'] > 0
    assert response['total'] > 0
    assert response['collapseThreads'] == False
    assert response['queryState']
    assert isinstance(response['ids'], list)
    assert 0 < len(response['ids']) <= 10
    assert response['canCalculateChanges'] in (True, False)


@pytest.mark.asyncio
async def test_email_get_all(account, idmap, uidvalidity):
    response = await account.email_get(idmap)
    assert response['accountId'] == account.id
    assert isinstance(response['list'], list)
    assert 0 < len(response['list']) <= 1000
    assert response['notFound'] == []
    for msg in response['list']:
        assert msg['id']
        assert msg['threadId']


@pytest.mark.asyncio
async def test_email_get(account, idmap, uidvalidity, email_id, email_id2):
    properties = {
        'threadId', 'mailboxIds', 'inReplyTo', 'keywords', 'subject',
        'sentAt', 'receivedAt', 'size', 'blobId',
        'from', 'to', 'cc', 'bcc', 'replyTo',
        'attachments', 'hasAttachment',
        'headers', 'preview', 'body',
    }
    good_ids = [email_id, email_id2]
    wrong_ids = [
        "notsplit",
        "not-int",
        f"{uidvalidity}-{1 << 33}",
        f"{uidvalidity}-{1 << 32}",
        f"{uidvalidity}-{(1<<32)-1}",
        f"{uidvalidity}-0",
        f"{uidvalidity}--10",
        f"{uidvalidity}-1e2",
        f"{uidvalidity}-str",
        1234,
    ]
    response = await account.email_get(
        idmap,
        ids=good_ids + wrong_ids,
        properties=list(properties),
        maxBodyValueBytes=1024,
    )
    assert response['accountId'] == account.id
    assert isinstance(response['list'], list)
    assert len(response['list']) == 2
    assert isinstance(response['notFound'], list)
    assert set(response['notFound']) == set(wrong_ids)
    for msg in response['list']:
        assert msg['id'] in good_ids
        for prop in properties - {'body'}:
            assert prop in msg
        assert 'textBody' in msg or 'htmlBody' in msg


@pytest.mark.asyncio
async def test_email_query_get_threads(account, idmap, inbox_id):
    response = await account.email_query(**{
        "filter": {"inMailbox": inbox_id},
        "sort": [{"property": "receivedAt", "isAscending": False}],
        "collapseThreads": True,
        "position": 0,
        "limit": 30,
        "calculateTotal": True,
    })

    response = await account.email_get(idmap, ids=response['ids'], properties=["threadId"])
    assert isinstance(response['notFound'], list)
    assert len(response['notFound']) == 0
    assert isinstance(response['list'], list)
    assert len(response['list']) == 30
    for msg in response['list']:
        assert msg['id']
        assert msg['threadId']
    thread_ids = [msg['threadId'] for msg in response['list']]

    response = await account.thread_get(idmap, ids=thread_ids)
    assert len(response['notFound']) == 0
    assert len(response['list']) >= 30
    email_ids = []
    for thread in response['list']:
        assert thread['id']
        assert thread['emailIds']
        email_ids.extend(thread['emailIds'])

    properties = ["threadId","mailboxIds","keywords",
                  "hasAttachment","from","to","subject",
                  "receivedAt","size","preview"]
    response = await account.email_get(idmap, ids=email_ids, properties=properties)
    assert len(response['notFound']) == 0
    assert len(response['list']) >= 30
    for msg in response['list']:
        for prop in properties:
            assert prop in msg


@pytest.mark.asyncio
async def test_email_get_detail(account, idmap, email_id):
    properties = {
        "blobId", "messageId", "inReplyTo", "references",
        "header:list-id:asText", "header:list-post:asURLs",
        "sender", "cc", "bcc", "replyTo", "sentAt",
        "bodyStructure", "bodyValues",
    }
    bodyProperties = [
        "partId", "blobId", "size", "name", "type",
        "charset", "disposition", "cid", "location",
    ]
    response = await account.email_get(idmap, **{
        "ids": [email_id],
        "properties": list(properties),
        "fetchHTMLBodyValues": True,
        "bodyProperties": bodyProperties,
    })
    assert response['accountId'] == account.id
    assert isinstance(response['notFound'], list)
    assert len(response['notFound']) == 0
    assert isinstance(response['list'], list)
    assert len(response['list']) == 1
    for msg in response['list']:
        for prop in properties - {'body'}:
            assert prop in msg


@pytest.mark.asyncio
async def test_email_setget_seen(account, idmap, email_id):
    for state in (True, False):
        response = await account.email_set(
            idmap,
            update={
                email_id: {"keywords/$seen": state}
            }
        )
        assert response['accountId'] == account.id
        assert isinstance(response['updated'], dict)
        assert isinstance(response['notUpdated'], dict)
        assert isinstance(response['created'], dict)
        assert isinstance(response['notCreated'], dict)
        assert isinstance(response['destroyed'], list)
        assert isinstance(response['notDestroyed'], dict)
        assert len(response['updated']) > 0
        assert len(response['notUpdated']) == 0
        assert len(response['created']) == 0
        assert len(response['notCreated']) == 0
        assert len(response['destroyed']) == 0
        assert len(response['notDestroyed']) == 0

        response = await account.email_get(
            idmap,
            ids=[email_id],
            properties=['keywords']
        )
        assert response['list'][0]['id'] == email_id
        assert response['list'][0]['keywords'].get('$seen', False) == state


@pytest.mark.asyncio
async def test_email_changes(account, uidvalidity):
    response = await account.email_changes(sinceState=f"{uidvalidity},1,1", maxChanges=3000)
    changes = response['created'] + response['updated'] + response['removed']
    assert 0 < len(changes) < 3000


@pytest.mark.asyncio
async def test_thread_changes(account, uidvalidity):
    response = await account.thread_changes(sinceState=f"{uidvalidity},1,10", maxChanges=30)
    changes = response['created'] + response['updated'] + response['removed']
    assert 0 < len(changes) < 30


@pytest.mark.asyncio
async def test_mailbox_changes(account):
    with pytest.raises(jmap.errors.cannotCalculateChanges):
        await account.mailbox_changes(sinceState="1", maxChanges=300)
