from jmap import ImapDB

def test_init():
    db = ImapDB('foo_buddy@azet.sk')
    assert db.accountid() == 'foo_buddy@azet.sk'
    