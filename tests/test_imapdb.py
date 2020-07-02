from jmap.db import ImapDB


def test_init():
    username = 'u1'
    db = ImapDB(username)
    assert db.accountid == username


def test_firstsync(db):
    db.firstsync()


def test_sync_folders(db):
    db.sync_folders()


def test_sync_imap(db):
    db.sync_imap()


def test_sync_jmap(db):
    db.sync_jmap()
