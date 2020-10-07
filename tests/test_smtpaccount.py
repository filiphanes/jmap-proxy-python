import pytest


@pytest.mark.asyncio
async def test_identity_get(account, idmap):
    response = await account.identity_get(idmap)
    assert response['accountId'] == account.id
    assert isinstance(response['notFound'], list)
    assert response['notFound'] == []
    assert isinstance(response['list'], list)
    assert len(response['list']) > 0
    for identity in response['list']:
        assert identity['id']
        assert identity['name']
        assert identity['replyTo']
        # assert identity['bcc']
        # assert identity['textSignature']
        # assert identity['htmlSignature']
        assert identity['mayDelete'] in (True, False)


@pytest.mark.asyncio
async def test_emailsubmission_set(account, idmap, email_id, inbox_id):
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
                "mailboxIds/Drafts-GUID": True,
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
