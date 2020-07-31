import pytest
from random import random

INBOX_ID = "988f1121e9afae5e81cb000039771c66"
EMAIL_ID = "100"


@pytest.mark.asyncio
async def test_Mailbox_get_all(account, idmap):
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
async def test_Mailbox_create_destroy(account, idmap):
    # Create
    response = await account.mailbox_set(
        idmap,
        create={
            "test": {
                "parentId": INBOX_ID,
                "name": str(random())[2:10],
            }
        }
    )
    newId = response['created']['test']['id']
    assert not response['notCreated']
    assert not response['updated']
    assert not response['notUpdated']
    assert not response['destroyed']
    assert not response['notDestroyed']

    # Destroy
    response = await account.mailbox_set(idmap, destroy=[newId])
    assert not response['created']
    assert not response['notCreated']
    assert not response['updated']
    assert not response['notUpdated']
    assert response['destroyed'] == [newId]
    assert not response['notDestroyed']


@pytest.mark.asyncio
async def test_Email_query_inMailbox(account):
    response = await account.email_query(**{
        "filter": {"inMailbox": INBOX_ID},
        "position": 0,
        "collapseThreads": False,
        "limit": 10,
        "calculateTotal": False
    })
    assert response['accountId'] == account.id
    assert response['position'] == 0
    # assert response['total']
    assert response['collapseThreads'] == False
    assert response['queryState']
    assert isinstance(response['ids'], list)
    assert len(response['ids']) > 0
    assert 'filter' in response
    assert 'sort' in response
    assert 'canCalculateChanges' in response


@pytest.mark.asyncio
async def test_Email_get(account, idmap):
    properties = {
        'threadId', 'mailboxIds', 'inReplyTo', 'keywords', 'subject',
        'sentAt', 'receivedAt', 'size', 'blobId',
        'from', 'to', 'cc', 'bcc', 'replyTo',
        'attachments', 'hasAttachment',
        'headers', 'preview', 'body',
    }
    response = await account.email_get(
        idmap,
        ids=[EMAIL_ID, "notexisting"],
        properties=list(properties),
    )
    assert response['accountId'] == account.id
    assert isinstance(response['notFound'], list)
    assert len(response['notFound']) == 1
    assert isinstance(response['list'], list)
    assert len(response['list']) == 1
    for msg in response['list']:
        for prop in properties - {'body'}:
            assert prop in msg
        assert 'textBody' in msg or 'htmlBody' in msg


@pytest.mark.asyncio
async def test_Email_query_get_threads(account, idmap):
    response = await account.email_query(**{
        "filter": {"inMailbox": INBOX_ID},
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
    assert len(response['list']) >= 30
    for msg in response['list']:
        assert msg['id']
        assert msg['threadId']
    thread_ids = [msg['threadId'] for msg in response['list']]

    response = await account.thread_get(idmap, ids=thread_ids)
    assert len(response['notFound']) == 0
    assert len(response['list']) == 30
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
async def test_Email_get_detail(account, idmap):
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
        "ids": [EMAIL_ID],
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
async def test_Email_seen_unseen(account, idmap):
    for state in (True, False):
        response = await account.email_set(
            idmap,
            update={
                EMAIL_ID: {
                    "keywords/$seen": state
                }
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


@pytest.mark.asyncio
async def test_Email_changes(account):
    response = await account.email_changes(sinceState="1,1", maxChanges=3000)
    changes = response['created'] + response['updated'] + response['removed']
    assert 0 < len(changes) < 3000


@pytest.mark.asyncio
async def test_Thread_changes(account):
    response = await account.thread_changes(sinceState="1,39", maxChanges=30)
    changes = response['created'] + response['updated'] + response['removed']
    assert 0 < len(changes) < 30


@pytest.mark.asyncio
async def test_Mailbox_changes(account):
    response = await account.mailbox_changes(sinceState="1", maxChanges=300)
    assert response['accountId'] == account.id
    assert response['list']
    assert response['oldState']
    assert response['newState']
