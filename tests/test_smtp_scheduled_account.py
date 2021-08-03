from datetime import datetime
from uuid import uuid4
import pytest

from jmap import errors
from jmap.account.smtp_scheduled import to_sql_sort, to_sql_where, SmtpScheduledAccountMixin


@pytest.mark.asyncio
async def test_identity_get(smtp_scheduled_account, idmap):
    account = smtp_scheduled_account
    response = await account.identity_get(idmap)
    assert response['accountId'] == account.id
    assert isinstance(response['notFound'], list)
    assert response['notFound'] == []
    assert isinstance(response['list'], list)
    assert len(response['list']) > 0
    for identity in response['list']:
        assert identity['id']
        assert identity['name']
        # assert identity['replyTo']
        # assert identity['bcc']
        # assert identity['textSignature']
        # assert identity['htmlSignature']
        assert identity['mayDelete'] in (True, False)


@pytest.mark.asyncio
async def test_emailsubmission_set(smtp_scheduled_account, idmap, email_id):
    account = smtp_scheduled_account
    response = await account.emailsubmission_set(
        idmap,
        create={
            "test": {
                "identityId": account.id,
                "emailId": email_id,
                "envelope": {
                    "mailFrom": {
                        "email": account.id,
                        "parameters": None
                    },
                    "rcptTo": [{
                        "email": account.id,
                        "parameters": None
                    }]
                }
            }
        }
    )
    assert response['accountId'] == account.id
    assert isinstance(response['notCreated'], dict)
    assert not response['notCreated']
    assert isinstance(response['created'], dict)
    assert response['created']['test']['id']
    assert isinstance(response['oldState'], str)
    assert response['oldState']
    assert isinstance(response['newState'], str)
    assert response['newState']


@pytest.mark.asyncio
async def test_emailsubmission_set_with_update(smtp_scheduled_account, idmap, email_id, inbox_id, drafts_id):
    account = smtp_scheduled_account
    response1, response2 = await account.emailsubmission_set(
        idmap,
        create={
            "test": {
                "identityId": account.id,
                "emailId": email_id,
                "envelope": {
                    "mailFrom": {
                        "email": account.id,
                        "parameters": None
                    },
                    "rcptTo": [{
                        "email": account.id,
                        "parameters": None
                    }]
                }
            }
        },
        onSuccessUpdateEmail={
            "#test": {
                "mailboxIds/"+drafts_id: None,
                "mailboxIds/"+inbox_id: True,
                "keywords/$draft": None
            }
        }
    )
    assert response1['accountId'] == account.id
    assert isinstance(response1['notCreated'], dict)
    assert not response1['notCreated']
    assert isinstance(response1['created'], dict)
    assert response1['created']
    for cid, id in response1['created'].items():
        assert id
        assert cid
    assert response2['method_name'] == 'Email/set'
    assert len(response2['updated']) == 1


@pytest.mark.asyncio
async def test_emailsubmission_get(smtp_scheduled_account, idmap):
    account = smtp_scheduled_account
    properties = {
        'id', 'identityId', 'accountId', 'emailId', 'threadId',
        'envelope', 'sendAt', 'undoStatus', 'deliveryStatus',
        'dsnBlobIds', 'mdnBlobIds',
    }
    good_ids = ['3e5d0b61ff41487c93144336fa482645']
    wrong_ids = ["notexists", 1234]
    response = await account.emailsubmission_get(
        idmap,
        ids=good_ids + wrong_ids,
        properties=list(properties),
    )
    assert response['accountId'] == account.id
    assert isinstance(response['list'], list)
    assert len(response['list']) == 1
    assert isinstance(response['notFound'], list)
    assert set(response['notFound']) == set(wrong_ids)
    for submission in response['list']:
        assert submission['id'] in good_ids
        for prop in properties:
            assert prop in submission


@pytest.mark.asyncio
async def test_emailsubmission_changes(smtp_scheduled_account):
    response = await smtp_scheduled_account.emailsubmission_changes(sinceState="1", maxChanges=2)
    assert response['accountId'] == smtp_scheduled_account.id
    changes = len(response['created']) \
            + len(response['updated']) \
            + len(response['destroyed'])
    assert 1 <= changes <= 2
    assert response['hasMoreChanges'] is True
    assert response['oldState'] == "1"
    assert response['newState'] and response['newState'] != "1"


@pytest.mark.asyncio
async def test_emailsubmission_query(smtp_scheduled_account):
    account = smtp_scheduled_account
    response = await account.emailsubmission_query(
        filter={"identityIds": [account.id], 'after':'2021-08-03 12:03:02'},
        sort=[
            {"property": 'sendAt'},
            {"property": 'emailId'},
            {"property": 'threadId', 'isAscending': False},
        ],
        anchor="3e5d0b61ff41487c93144336fa482645",
        limit=10,
        calculateTotal=True
    )
    assert response['accountId'] == account.id
    assert response['position'] >= 0
    assert response['total'] > 0
    assert isinstance(response['queryState'], str)
    assert response['queryState']
    assert isinstance(response['ids'], list)
    assert 0 < len(response['ids']) <= 10
    assert response['canCalculateChanges'] in (True, False)


@pytest.mark.asyncio
async def test_to_sql_sort():
    # Valid
    sql = bytearray()
    to_sql_sort([
        {'property': 'emailId', 'isAscending': True},
        {'property': 'threadId', 'isAscending': False},
        {'property': 'sendAt'},
    ], sql)
    assert sql.decode() == 'emailId ASC,threadId DESC,sendAt ASC'

    # Empty
    sql = bytearray()
    to_sql_sort([], sql)
    assert sql.decode() == ''

    # Invalid field
    sql = bytearray()
    with pytest.raises(errors.unsupportedSort):
        to_sql_sort([{'property': 'typo'}], sql)


@pytest.mark.asyncio
async def test_to_sql_where():
    # Valid
    sql, args = bytearray(), list()
    to_sql_where({
        'identityIds': ['a','b','c'],
        'emailIds': ['x','y','z'],
        'threadIds': ['k'],
        'undoStatus': 'pending',
        'before': '2021-08-30T14:12:00+01:00',
        'after': '2021-08-01T00:00:00+01:00',
    }, sql, args)
    assert sql.decode() == 'identityId IN(%s,%s,%s) AND emailId IN(%s,%s,%s) AND threadId IN(%s) AND undoStatus=%s AND sendAt<%s AND sendAt>=%s'
    assert len(args) == 10
    assert args[0] == 'a'
    assert args[3] == 'x'
    assert args[6] == 'k'
    assert args[-3] == 0  # 'pending'
    assert isinstance(args[-2], datetime)
    assert args[-2].day == 30
    assert isinstance(args[-1], datetime)
    assert args[-1].day == 1

    # Operators
    sql, args = bytearray(), list()
    to_sql_where({
        'operator': 'OR',
        'conditions': [
            {'identityIds': ['a','b','c']},
            {
                'operator': 'NOT',
                'conditions': [{'emailIds': ['x','y','z']}],
            }, {
                'operator': 'AND',
                'conditions': [
                    {'threadIds': ['k','l','m'], 'undoStatus': 'pending',},
                    {'before': '2021-08-30T14:12:00+01:00'},
                    {'after': '2021-08-01T00:00:00+01:00'},
                ]
            }
        ]
    }, sql, args)
    assert sql.decode() =='(identityId IN(%s,%s,%s))OR(NOT((emailId IN(%s,%s,%s))))OR((threadId IN(%s,%s,%s) AND undoStatus=%s)AND(sendAt<%s)AND(sendAt>=%s))'
    assert len(args) == 12

    # Unknown Operator
    sql, args = bytearray(), list()
    with pytest.raises(errors.unsupportedFilter):
        to_sql_where({'operator': 'NOR', 'conditions': [{'identityIds': ['a']}]}, sql, args)

    # Operator without conditions
    sql, args = bytearray(), list()
    with pytest.raises(errors.unsupportedFilter):
        to_sql_where({'operator': 'OR'}, sql, args)

    # Unknown field
    sql, args = bytearray(), list()
    with pytest.raises(errors.unsupportedFilter):
        to_sql_where({'abc': 'def'}, sql, args)
