# test requests inspired by from https://jmap.io/client.html

import pytest


def test_Mailbox_get_all(api):
    res = api.handle_request({"methodCalls": [
        ["Mailbox/get", {"accountId": "u1", "ids": None}, "0"]
    ]})
    assert len(res['methodResponses']) == 1
    for method, response, tag in res['methodResponses']:
        assert method == "Mailbox/get"
        assert tag == "0"
        assert response['accountId'] == "u1"
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


def test_Email_query_inMailbox(api):
    res = api.handle_request({"methodCalls": [
        ["Email/query", {
            "accountId": "u1",
            "filter": {
                "inMailbox": "b7c21828-32b1-475d-b8bd-998c01c92b71"   # inbox
            },
            "position": 0,
            "collapseThreads": True,
            "limit": 10,
            "calculateTotal": True
        }, "0"]
    ]})
    assert len(res['methodResponses']) == 1
    for method, response, tag in res['methodResponses']:
        assert method == "Email/query"
        assert tag == "0"
        assert response['accountId'] == "u1"
        assert response['position'] == 0
        assert response['total']
        assert response['collapseThreads'] == True
        assert int(response['queryState']) > 0
        assert isinstance(response['ids'], list)
        assert len(response['ids']) > 0
        assert 'filter' in response
        assert 'sort' in response
        assert 'canCalculateChanges' in response


def test_Email_get(api):
    properties = {
        'threadId', 'mailboxIds', 'inReplyToEmailId', 'keywords', 'subject',
        'sentAt', 'receivedAt', 'size', 'blobId', #'replyTo',
        'from', 'to', 'cc', 'bcc',
        'attachments', 'hasAttachment',
        'headers', 'preview', 'body', 'textBody', 'htmlBody',
    }
    res = api.handle_request({"methodCalls": [
        ["Email/get", {
            "accountId": "u1",
            "ids": ["ma854e1c42", "me400ec47d"],
            "properties": list(properties),
        }, "1"]
    ]})
    assert len(res['methodResponses']) == 1
    for method, response, tag in res['methodResponses']:
        assert method == "Email/get"
        assert tag == "1"
        assert response['accountId'] == "u1"
        assert isinstance(response['notFound'], list)
        assert len(response['notFound']) == 0
        assert isinstance(response['list'], list)
        assert len(response['list']) == 2
        for msg in response['list']:
            for prop in properties - {'body'}:
                assert prop in msg


def test_Email_query_first_page(api):
    res = api.handle_request({"methodCalls": [
        # First we do a query for the id of first 10 messages in the mailbox
        ["Email/query", {
            "accountId": "u1",
            "filter": {
                "inMailbox": "b7c21828-32b1-475d-b8bd-998c01c92b71"   # inbox
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
            "accountId": "u1",
            "#ids": {
                "name": "Email/query",
                "path": "/ids",
                "resultOf": "0"
            },
            "properties": ["threadId"]
        }, "1"],

        # Next we get the emailIds of the messages in those threads
        ["Thread/get", {
            "accountId": "u1",
            "#ids": {
                "name": "Email/get",
                "path": "/list/*/threadId",
                "resultOf": "1"
            }
        }, "2"],

        # Finally we get the data for all those emails
        ["Email/get", {
            "accountId": "u1",
            "#ids": {
                "name": "Thread/get",
                "path": "/list/*/emailIds",
                "resultOf": "2"
            },
            "properties": ["subject", "receivedAt"]
        }, "3"]
    ]})
    assert len(res['methodResponses']) == 4
    for method, response, tag in res['methodResponses']:
        assert len(response['ids']) > 0 \
            or len(response['list']) > 0
