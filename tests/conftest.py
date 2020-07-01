import pytest


@pytest.fixture
def accountId():
    return 'u1'


@pytest.fixture
def db(accountId):
    from jmap.imapdb import ImapDB
    return ImapDB()


@pytest.fixture
def api(db):
    from jmap.api import JmapApi
    return JmapApi(db)
