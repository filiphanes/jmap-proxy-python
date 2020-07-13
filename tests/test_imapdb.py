from jmap.db import ImapDB


def test_init():
    username = 'u1'
    db = ImapDB(username)
    assert db.accountid == username
