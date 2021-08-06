import pytest


@pytest.mark.asyncio
async def test_vacationresponse_set_get(db_vacationresponse_account, idmap):
    account = db_vacationresponse_account
    # GET
    properties = {'id','isEnabled','fromDate','toDate','subject','textBody','htmlBody'}
    good_ids = ['singleton']
    wrong_ids = ["notexists", 1234]
    response = await account.vacationresponse_get(idmap, ids=good_ids + wrong_ids, properties=list(properties))
    assert response['accountId'] == account.id
    assert isinstance(response['list'], list)
    assert len(response['list']) == 1
    assert isinstance(response['notFound'], list)
    assert set(response['notFound']) == set(wrong_ids)
    for submission in response['list']:
        assert submission['id'] in good_ids
        for prop in properties:
            assert prop in submission

    # SET create
    response = await account.vacationresponse_set(
        idmap,
        create={
            "1": {
                "isEnabled": True,
                "fromDate": None,
                "toDate": None,
            }
        }
    )
    assert response['accountId'] == account.id
    assert isinstance(response['notCreated'], dict)
    assert response['notCreated']['1']['type'] == 'singleton'
    assert not response['created']
    assert not response['updated']
    assert not response['notUpdated']
    assert not response['destroyed']
    assert not response['notDestroyed']

    # SET update
    response = await account.vacationresponse_set(
        idmap,
        update={
            "singleton": {
                "isEnabled": True,
                "textBody": None,
                "htmlBody": '',
                "fromDate": "2000-10-30T06:12:00Z",
                "toDate": "2014-10-30T14:12:00+08:00",
            },
            "id1": {
                "isEnabled": True,
            },
        },
        destroy=['singleton', 'id1'],
    )
    assert response['accountId'] == account.id
    assert response['oldState'] != response['newState']
    assert isinstance(response['notCreated'], dict)
    assert not response['notCreated']
    assert not response['created']
    assert response['updated'] == ['singleton']
    assert response['notUpdated']['id1']['type'] == 'singleton'
    assert not response['destroyed']
    assert response['notDestroyed']['id1']['type'] == 'singleton'
    assert response['notDestroyed']['singleton']['type'] == 'singleton'

    # GET ALL
    response = await account.vacationresponse_get(idmap)
    assert response['accountId'] == account.id
    assert isinstance(response['notFound'], list)
    assert response['notFound'] == []
    assert isinstance(response['list'], list)
    assert len(response['list']) == 1
    vacationresponse, = response['list']
    assert vacationresponse['id'] == 'singleton'
    assert vacationresponse['isEnabled']

