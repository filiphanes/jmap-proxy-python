import asyncio

import pytest


@pytest.fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope='module')
def accountId():
    return 'u1'


@pytest.fixture(scope='module')
@pytest.mark.asyncio
async def db(accountId):
    from jmap.db import ImapDB
    return await ImapDB.init(accountId)


@pytest.fixture(scope='module')
@pytest.mark.asyncio
async def user(accountId, event_loop):
    from user import User
    return await User.init(accountId, 'h', loop=event_loop)


@pytest.fixture(scope='module')
def api(user):
    from jmap.api import Api
    return Api(user)
