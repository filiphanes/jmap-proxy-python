import pytest

@pytest.fixture
def accountId():
    return 'u1'


@pytest.fixture
def db(accountId):
    from jmap.db import ImapDB
    return ImapDB(accountId)


@pytest.fixture
def api(db):
    from jmap.api import Api, USING_MIXINS
    bases = tuple(USING_MIXINS.values())
    return type('API', bases, Api.__dict__)(db)
