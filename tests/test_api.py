import pytest
from jmap.api import api

try:
    import orjson as json
except ImportError:
    import json

INBOX_ID = "988f1121e9afae5e81cb000039771c66"
EMAIL_ID = "100"

@pytest.mark.asyncio
async def test_Email_query_first_page(req):
    properties = [
        "threadId", "mailboxIds", "subject", "receivedAt",
        "keywords", "hasAttachment", "from", "to", "preview",
    ]
    user = req['user']
    req._json = {
        "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
        "methodCalls": [
        # First we do a query for the id of first 10 messages in the mailbox
        ["Email/query", {
            "accountId": user.username,
            "filter": {
                "inMailbox": INBOX_ID,  # Junk
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
            "accountId": user.username,
            "#ids": {
                "name": "Email/query",
                "path": "/ids",
                "resultOf": "0"
            },
            "properties": ["threadId"]
        }, "1"],

        # Next we get the emailIds of the messages in those threads
        ["Thread/get", {
            "accountId": user.username,
            "#ids": {
                "name": "Email/get",
                "path": "/list/*/threadId",
                "resultOf": "1"
            }
        }, "2"],

        # Finally we get the data for all those emails
        ["Email/get", {
            "accountId": user.username,
            "#ids": {
                "name": "Thread/get",
                "path": "/list/*/emailIds",
                "resultOf": "2"
            },
            "properties": properties
        }, "3"]
    ]}
    res = json.loads((await api(req)).body)
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


@pytest.mark.asyncio
async def test_Email_query_second_page(req):
    properties = [
        "threadId", "mailboxIds", "subject", "receivedAt",
        "keywords", "hasAttachment", "from", "to", "preview",
    ]
    user = req['user']
    req._json = {
        "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
        "methodCalls": [
        [ "Email/query", {
            "accountId": user.username,
            "filter": {
                "inMailbox": INBOX_ID
            },
            "sort": [
                { "property": "receivedAt", "isAscending": False }
            ],
            "collapseThreads": True,
            "position": 4,
            "limit": 10
        }, "0" ],
        [ "Email/get", {
            "accountId": user.username,
            "#ids": {
                "name": "Email/query",
                "path": "/ids",
                "resultOf": "0"
            },
            "properties": properties
        }, "1" ]
    ]}
    res = json.loads((await api(req)).body)
    assert len(res['methodResponses']) == 2
    for method, response, tag in res['methodResponses']:
        if tag == '0':
            assert len(response['ids']) > 0
        elif tag == '1':
            assert len(response['list']) > 0
            for msg in response['list']:
                for prop in properties:
                    assert prop in msg
