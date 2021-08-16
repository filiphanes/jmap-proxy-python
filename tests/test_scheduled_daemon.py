import asyncio
from unittest.mock import AsyncMock

import aiosmtplib
import pytest


@pytest.mark.asyncio
async def test_scheduled_daemon(scheduled_account, scheduled_daemon, email_id, idmap, monkeypatch):
    account = scheduled_account

    smtp_send = AsyncMock()
    monkeypatch.setattr(aiosmtplib, "send", smtp_send)

    # Create submission
    response = await account.emailsubmission_set(
        idmap,
        create={
            "1": {
                "identityId": 'identity1',
                "emailId": email_id,
                "envelope": {
                    "mailFrom": {"email": account.id},
                    "rcptTo": [{"email": account.id}]
                }
            }
        }
    )
    oldState = response['newState']
    submissionId = response['created']['1']['id']

    # Start daemon
    task = asyncio.create_task(scheduled_daemon.start())
    # Wait for daemon processing
    await asyncio.sleep(scheduled_daemon.poll_secs * 2)
    scheduled_daemon.stop()
    await task

    body = account.blobs[account.emails[email_id]['blobId']]
    smtp_send.assert_called_once_with(body, account.id, ['u1'], hostname='127.0.0.1', port=1025, username='user1', password='pass1')

    # Check if submission finalized and change state
    response = await account.emailsubmission_get(
        idmap,
        ids=[submissionId],
        properties=['undoStatus'],
    )
    for submission in response['list']:
        assert submission['undoStatus'] == 'final'
    assert oldState != response['state']
