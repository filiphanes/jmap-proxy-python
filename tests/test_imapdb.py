from jmap.db import ImapDB


def test_encode_seqset():
    from jmap.db.imap.db import encode_seqset
    assert encode_seqset([1]) == b'1'
    assert encode_seqset([1,2,3]) == b'1:3'
    assert encode_seqset([1,5,3]) == b'1,3,5'
    assert encode_seqset([1,5,3,2]) == b'1:3,5'
    assert encode_seqset([5,6,7,8,3,2,11,12,13]) == b'2:3,5:8,11:13'


def test_init():
    username = 'u1'
    db = ImapDB(username)
    assert db.accountid == username

