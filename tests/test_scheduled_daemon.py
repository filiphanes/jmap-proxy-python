import pytest


@pytest.mark.asyncio
async def test_scheduled_daemon(scheduled_account, email_id, idmap):
    account = scheduled_account

    # SET 1
    response = await account.emailsubmission_set(
        idmap,
        create={
            "1": {
                "identityId": 'identity1',
                "emailId": email_id,
                "envelope": {
                    "mailFrom": {"email": account.id, "parameters": None},
                    "rcptTo": [{"email": account.id, "parameters": None}]
                }
            }
        }
    )

    # TODO: run daemon and check aiosmtp.send is called
