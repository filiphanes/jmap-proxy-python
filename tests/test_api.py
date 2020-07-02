import pytest
from jmap.api import handle_request

def test_Mailbox_get_all(db, accountId):
    res = handle_request({
        "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
        "methodCalls": [
            ["Mailbox/get", {"accountId": accountId, "ids": None}, "0"]
        ]
    }, db)
    assert len(res['methodResponses']) == 1
    for method, response, tag in res['methodResponses']:
        assert method == "Mailbox/get"
        assert tag == "0"
        assert response['accountId'] == accountId
        assert int(response['state']) > 0
        assert isinstance(response['notFound'], list)
        assert len(response['notFound']) == 0
        assert isinstance(response['list'], list)
        assert len(response['list']) > 0
        for mailbox in response['list']:
            assert mailbox['name']
            assert mailbox['id']
            assert mailbox['myRights']
            assert 'role' in mailbox
            assert 'sortOrder' in mailbox
            assert 'totalEmails' in mailbox
            assert 'totalThreads' in mailbox
            assert 'unreadThreads' in mailbox
            assert 'isSubscribed' in mailbox
            assert 'parentId' in mailbox


def test_Email_query_inMailbox(db, accountId):
    res = handle_request({
        "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
        "methodCalls": [
            ["Email/query", {
                "accountId": accountId,
                "filter": {
                    "inMailbox": "d9ff53eb-0f98-4e17-8205-f7f418d9bc5a" # inbox
                },
                "position": 0,
                "collapseThreads": True,
                "limit": 10,
                "calculateTotal": True
            }, "0"]
        ]
    }, db)
    assert len(res['methodResponses']) == 1
    for method, response, tag in res['methodResponses']:
        assert method == "Email/query"
        assert tag == "0"
        assert response['accountId'] == accountId
        assert response['position'] == 0
        assert response['total']
        assert response['collapseThreads'] == True
        assert int(response['queryState']) > 0
        assert isinstance(response['ids'], list)
        assert len(response['ids']) > 0
        assert 'filter' in response
        assert 'sort' in response
        assert 'canCalculateChanges' in response


def test_Email_get(db, accountId):
    properties = {
        'threadId', 'mailboxIds', 'inReplyTo', 'keywords', 'subject',
        'sentAt', 'receivedAt', 'size', 'blobId',
        'from', 'to', 'cc', 'bcc', 'replyTo',
        'attachments', 'hasAttachment',
        'headers', 'preview', 'body', 'textBody', 'htmlBody',
    }
    res = handle_request({
        "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
        "methodCalls": [
            ["Email/get", {
                "accountId": "u1",
                "ids": ["mdfe661a66", "notexisting"],
                "properties": list(properties),
            }, "1"]
        ]
    }, db)
    assert len(res['methodResponses']) == 1
    for method, response, tag in res['methodResponses']:
        assert method == "Email/get"
        assert tag == "1"
        assert response['accountId'] == "u1"
        assert isinstance(response['notFound'], list)
        assert len(response['notFound']) == 1
        assert isinstance(response['list'], list)
        assert len(response['list']) == 1
        for msg in response['list']:
            for prop in properties - {'body'}:
                assert prop in msg

def test_Email_get_detail(db, accountId):
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
    res = handle_request({
        "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
        "methodCalls": [
            ["Email/get", {
                "accountId": "u1",
                "ids": ["m9dda32a70"],
                "properties": list(properties),
                "fetchHTMLBodyValues": True,
                "bodyProperties": bodyProperties,
            }, "0"]
        ],
    }, db)
    assert len(res['methodResponses']) == 1
    for method, response, tag in res['methodResponses']:
        assert method == "Email/get"
        assert tag == "0"
        assert response['accountId'] == "u1"
        assert isinstance(response['notFound'], list)
        assert len(response['notFound']) == 0
        assert isinstance(response['list'], list)
        assert len(response['list']) == 1
        for msg in response['list']:
            for prop in properties - {'body'}:
                assert prop in msg


def test_Email_set(db, accountId):
    res = handle_request({
        "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
        "methodCalls": [
            ["Email/set", {
                "accountId": accountId,
                "update": {
                    "mdfe661a66": {
                        "keywords/$seen": None
                    }
                }
            }, "0"]
        ]
    }, db)
    assert len(res['methodResponses']) == 1
    for method, response, tag in res['methodResponses']:
        assert method == "Email/set"
        assert tag == "0"
        assert response['accountId'] == accountId
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


def test_Email_query_first_page(db, accountId):
    properties = [
        "threadId", "mailboxIds", "subject", "receivedAt",
        "keywords", "hasAttachment", "from", "to", "preview",
    ]
    res = handle_request({
        "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
        "methodCalls": [
        # First we do a query for the id of first 10 messages in the mailbox
        ["Email/query", {
            "accountId": accountId,
            "filter": {
                "inMailbox": "d9ff53eb-0f98-4e17-8205-f7f418d9bc5a"   # inbox
            },
            "sort": [
                {"property": "receivedAt", "isAscending": False}
            ],
            "position": 0,
            "collapseThreads": True,
            "limit": 10,
            "calculateTotal": True
        }, "0"],

        # Then we fetch the threadId of each of those messages
        ["Email/get", {
            "accountId": accountId,
            "#ids": {
                "name": "Email/query",
                "path": "/ids",
                "resultOf": "0"
            },
            "properties": ["threadId"]
        }, "1"],

        # Next we get the emailIds of the messages in those threads
        ["Thread/get", {
            "accountId": accountId,
            "#ids": {
                "name": "Email/get",
                "path": "/list/*/threadId",
                "resultOf": "1"
            }
        }, "2"],

        # Finally we get the data for all those emails
        ["Email/get", {
            "accountId": accountId,
            "#ids": {
                "name": "Thread/get",
                "path": "/list/*/emailIds",
                "resultOf": "2"
            },
            "properties": properties
        }, "3"]
    ]}, db)
    assert len(res['methodResponses']) == 4
    for method, response, tag in res['methodResponses']:
        if tag == '0':
            assert len(response['ids']) > 0
        elif tag == '1':
            assert len(response['list']) > 0
        elif tag == '2':
            assert len(response['list']) > 0
            assert response['notFound'] == []
        elif tag == '3':
            assert len(response['list']) > 0
            for msg in response['list']:
                for prop in properties:
                    assert prop in msg


def test_Email_query_second_page(db, accountId):
    properties = [
        "threadId", "mailboxIds", "subject", "receivedAt",
        "keywords", "hasAttachment", "from", "to", "preview",
    ]
    res = handle_request({
        "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
        "methodCalls": [
        [ "Email/query", {
            "accountId": accountId,
            "filter": {
                "inMailbox": "d9ff53eb-0f98-4e17-8205-f7f418d9bc5a"   # inbox
            },
            "sort": [
                { "property": "receivedAt", "isAscending": False }
            ],
            "collapseThreads": True,
            "position": 4,
            "limit": 10
        }, "0" ],
        [ "Email/get", {
            "accountId": accountId,
            "#ids": {
                "name": "Email/query",
                "path": "/ids",
                "resultOf": "0"
            },
            "properties": properties
        }, "1" ]
    ]}, db)
    assert len(res['methodResponses']) == 2
    for method, response, tag in res['methodResponses']:
        if tag == '0':
            assert len(response['ids']) > 0
        elif tag == '1':
            assert len(response['list']) > 0
            for msg in response['list']:
                for prop in properties:
                    assert prop in msg


def test_Email_changes(db, accountId):
    res = handle_request({
        "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
        "methodCalls": [
        # Fetch a list of created/updated/deleted Emails
        [ "Email/changes", {
            "accountId": accountId,
            "sinceState": "1",
            "maxChanges": 30
        }, "0"],
    ]}, db)
    assert len(res['methodResponses']) == 2


def test_Thread_changes(db, accountId):
    res = handle_request({
        "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
        "methodCalls": [
        # Fetch a list of created/udpated/deleted Threads
        [ "Thread/changes", {
            "accountId": accountId,
            "sinceState": "1",
            "maxChanges": 30
        }, "0"],
    ]}, db)
    assert len(res['methodResponses']) == 2


def test_Mailbox_changes(db, accountId):
    res = handle_request({
        "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
        "methodCalls": [
        # Fetch a list of mailbox ids that have changed
        [ "Mailbox/changes", {
            "accountId": accountId,
            "sinceState": "1"
        }, "0"],
        # Fetch any mailboxes that have been created
        [ "Mailbox/get", {
            "accountId": accountId,
            "#ids": {
                "name": "Mailbox/changes",
                "path": "/created",
                "resultOf": "0",
            }
        }, "1" ],
        # Fetch any mailboxes that have been updated
        [ "Mailbox/get", {
            "accountId": accountId,
            "#ids": {
                "name": "Mailbox/changes",
                "path": "/updated",
                "resultOf": "0"
            },
            "#properties": {
                "name": "Mailbox/changes",
                "path": "/updatedProperties",
                "resultOf": "0"
            }
        }, "2" ]
    ]}, db)
    assert len(res['methodResponses']) == 1
    for method, response, tag in res['methodResponses']:
        if tag == '0':
            assert len(response['list']) > 0
