import pytest


@pytest.fixture
def accountId():
    return 'u1'


@pytest.fixture
def db(accountId):
    from jmap.db import ImapDB
    return ImapDB(accountId)


@pytest.fixture
def user(accountId):
    from user import User
    return User(accountId, 'h')


@pytest.fixture
def api(user):
    from jmap.api import Api
    return Api(user)
