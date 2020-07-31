from jmap.account.imap.aioimaplib import encode_messageset


def test_encode_messageset():
    assert encode_messageset([1]) == b'1'
    assert encode_messageset([1,2,3]) == b'1:3'
    assert encode_messageset([1,5,3]) == b'1,3,5'
    assert encode_messageset([1,5,3,2]) == b'1:3,5'
    assert encode_messageset([5,6,7,8,3,2,11,12,13]) == b'2:3,5:8,11:13'
