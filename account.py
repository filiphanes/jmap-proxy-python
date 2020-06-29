import sqlite3
from jmap.imapdb import ImapDB
import datetime

class AccountManager:
    def __init__(self, dbpath="./data/accounts.sqlite3"):
        self.dbh = sqlite3.connect(dbpath);
        self.dbh.execute("""CREATE TABLE IF NOT EXISTS accounts (
            email TEXT PRIMARY KEY,
            accountid TEXT,
            type TEXT
        )""")
        self.byid = {}

    def get_db(self, accountid):
        if accountid not in self.byid:
            rows = self.dbh.execute('SELECT email, type FROM accounts WHERE accountid=?', [accountid])
            for email, type in rows:
                if type == 'imap':
                    db = ImapDB(accountid)
                    self.byid[accountid] = db
                    db.firstsync()
                    db.sync_imap()
                else:
                    raise Exception(f'Account type {type} unknown.')
        return self.byid[accountid]
