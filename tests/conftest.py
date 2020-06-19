import pytest


@pytest.fixture
def db():
    from jmap.imapdb import ImapDB
    return ImapDB("u1")


@pytest.fixture
def api(db):
    from jmap.api import JmapApi
    return JmapApi(db)
