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
async def test_emailsubmission_set(account, idmap):
    response = await account.identity_set(
        idmap,
        create={
            "test": {
                "identityId": account.id,
                "emailId": EMAIL_ID,
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
                "mailboxIds/"+INBOX_ID: None,
                "mailboxIds/"+INBOX_ID: True,
                "keywords/$draft": None
            }
        }
    )
    assert response['accountId'] == account.id
    assert isinstance(response['notFound'], list)
    assert response['notFound'] == []
    assert isinstance(response['list'], list)
    assert response['list']
    for identity in response['list']:
        assert identity['id']
        assert identity['name']
        assert identity['replyTo']
        # assert identity['bcc']
        # assert identity['textSignature']
        # assert identity['htmlSignature']
        assert identity['mayDelete'] in (True, False)
