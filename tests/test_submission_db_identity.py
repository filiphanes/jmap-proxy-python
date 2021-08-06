import pytest


@pytest.mark.asyncio
async def test_identity_set_get(db_identity_account, idmap):
    account = db_identity_account
    # SET
    response = await account.identity_set(
        idmap,
        create={
            "1": {
                "name": 'John Tester',
                "email": 'tester@example.com',
                "replyTo": [{'name':'John Tester','email':'tester@example.com'}],
                "bcc": None,
                "textSignature": '<p>Best regards <b>Tester</b></p>',
                "htmlSignature": 'Best regards Tester',
            }
        }
    )
    assert response['accountId'] == account.id
    assert isinstance(response['notCreated'], dict)
    assert not response['notCreated']
    assert isinstance(response['created'], dict)
    assert response['created']['1']['id']
    assert isinstance(response['oldState'], str)
    assert response['oldState']
    assert isinstance(response['newState'], str)
    assert response['newState']

    # GET
    properties = {'id', 'name', 'email', 'replyTo', 'bcc',
                  'textSignature', 'htmlSignature', 'mayDelete'}
    good_ids = [response['created']['1']['id']]
    wrong_ids = ["notexists", 1234]
    response = await account.identity_get(idmap, ids=good_ids + wrong_ids, properties=list(properties))
    assert response['accountId'] == account.id
    assert isinstance(response['list'], list)
    assert len(response['list']) == 1
    assert isinstance(response['notFound'], list)
    assert set(response['notFound']) == set(wrong_ids)
    for submission in response['list']:
        assert submission['id'] in good_ids
        for prop in properties:
            assert prop in submission

    # GET ALL
    response = await account.identity_get(idmap)
    assert response['accountId'] == account.id
    assert isinstance(response['notFound'], list)
    assert response['notFound'] == []
    assert isinstance(response['list'], list)
    assert len(response['list']) > 0
    for identity in response['list']:
        assert identity['id']
        assert identity['email']
        assert 'replyTo' in identity
        assert 'bcc' in identity
        assert 'textSignature' in identity
        assert 'htmlSignature' in identity
        assert identity['mayDelete'] in (True, False)

    # SET 2
    response = await account.identity_set(
        idmap,
        create={
            "2": {
                "name": 'Jane Tester',
                "email": 'mrs.tester@example.com',
                "replyTo": None,
                "bcc": [{'name':'John Tester','email':'tester@example.com'}],
                "textSignature": '<p>Best regards <b>J. Tester</b></p>',
                "htmlSignature": 'Best regards J. Tester',
            }
        }
    )

    # SET 3
    response = await account.identity_set(
        idmap,
        create={
            "3": {
                "name": 'Joe Tester',
                "email": 'ms.tester@example.com',
                "replyTo": None,
                "bcc": [{'name':'Joe Tester','email':'tester@example.com'}],
                "textSignature": '<p>Best regards <b>Joe Tester</b></p>',
                "htmlSignature": 'Best regards Joe Tester',
            }
        }
    )

    # CHANGES
    response = await account.identity_changes(sinceState="1", maxChanges=1)
    assert response['accountId'] == account.id
    changes = len(response['created']) \
            + len(response['updated']) \
            + len(response['destroyed'])
    assert 1 <= changes <= 1
    assert response['hasMoreChanges'] is True
    assert response['oldState'] == "1"
    assert response['newState'] and response['newState'] != "1"
