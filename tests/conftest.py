import asyncio

import pytest


@pytest.fixture()
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture()
def accountId():
    return 'u1'


@pytest.fixture()
@pytest.mark.asyncio
async def user(accountId, event_loop):
    from user import User
    user = User(accountId, 'h', loop=event_loop)
    await user.ainit()
    return user


@pytest.fixture()
def account(user, accountId):
    return user.get_account(accountId)


@pytest.fixture()
def idmap():
    from jmap.api import IdMap
    return IdMap({})


@pytest.fixture()
def req(user):
    from starlette.requests import Request
    scope = {'type': 'http', 'user': user}
    return Request(scope)


@pytest.fixture()
def inbox_id():
    return "988f1121e9afae5e81cb000039771c66"


@pytest.fixture()
def drafts_id():
    return "b0e0b8292940de5ecddd000039771c66"


@pytest.fixture()
def uidvalidity():
    return 1596626536


@pytest.fixture()
def email_id(uidvalidity):
    return f"{uidvalidity}-201"


@pytest.fixture()
def email_id2(uidvalidity):
    return f"{uidvalidity}-202"

