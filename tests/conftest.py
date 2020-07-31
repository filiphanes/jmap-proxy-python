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
async def user(accountId, event_loop):
    from user import User
    user = User(accountId, 'h', loop=event_loop)
    await user.ainit()
    return user


@pytest.fixture(scope='module')
def account(user, accountId):
    return user.get_account(accountId)


@pytest.fixture()
def idmap():
    from jmap.api import IdMap
    return IdMap({})


@pytest.fixture(scope='module')
def req(user):
    from starlette.requests import Request
    scope = {'type': 'http', 'user': user}
    return Request(scope)
