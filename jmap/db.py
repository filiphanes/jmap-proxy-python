import os
import sqlite3
import time

TABLE2GROUPS = {
  'jmessages': ['Email'],
  'jthreads': ['Thread'],
  'jmailboxes': ['Mailbox'],
  'jmessagemap': ['Mailbox'],
  'jrawmessage': [],
  'jfiles': [], # for now
  'jcalendars': ['Calendar'],
  'jevents': ['CalendarEvent'],
  'jaddressbooks': [], # not directly
  'jcontactgroups': ['ContactGroup'],
  'jcontactgroupmap': ['ContactGroup'],
  'jcontacts': ['Contact'],
  'jclientprefs': ['ClientPreferences'],
  'jcalendarprefs': ['CalendarPreferences'],
}

class JmapDatabase:
    def __init__(self, accountid):
        self.accountid = accountid
        self.dbpath = f"/home/jmap/data/{accountid}.db"
        self.dbh = sqlite3.connect(self.dbpath)
        self.dbh.row_factory = sqlite3.Row
        self._initdb()

    def delete(self):
        self.dbh.close()
        os.unlink(self.dbpath)
    
    def in_transaction(self):
        return bool(self.t)

    def begin(self):
        self.t = self.dbh.cursor()
        self.t.begin()
    
    def commit(self):
        self.t.commit()

    def get_user(self):    
        if not hasattr(self, 'user'):
            cursor = self.dbh.cursor()
            cursor.row_factory = sqlite3.Row
            self.user = None
            for row in cursor.execute("SELECT * FROM account LIMIT 1"):
                self.user = row
        if not self.user:
            self.user = {
                'jhighestmodseq': 1,
            }
            self.dbh.execute("INSERT INTO account (jhighestmodseq) VALUES (?)", [self.user['jhighestmodseq']])
        return self.user

    def _initdb(self):
        self.dbh.execute("""
        CREATE TABLE IF NOT EXISTS jmessages (
            msgid TEXT PRIMARY KEY,
            thrid TEXT,
            internaldate INTEGER,
            sha1 TEXT,
            isDraft BOOL,
            isUnread BOOL,
            keywords TEXT,
            msgfrom TEXT,
            msgto TEXT,
            msgcc TEXT,
            msgbcc TEXT,
            msgsubject TEXT,
            msginreplyto TEXT,
            msgmessageid TEXT,
            msgdate INTEGER,
            msgsize INTEGER,
            sortsubject TEXT,
            jcreated INTEGER,
            jmodseq INTEGER,
            mtime DATE,
            active BOOLEAN
        );""")

        self.dbh.execute("CREATE INDEX IF NOT EXISTS jthrid ON jmessages (thrid)")
        self.dbh.execute("CREATE INDEX IF NOT EXISTS jmsgmessageid ON jmessages (msgmessageid)")

        self.dbh.execute("""
        CREATE TABLE IF NOT EXISTS jthreads (
            thrid TEXT PRIMARY KEY,
            data TEXT,
            jcreated INTEGER,
            jmodseq INTEGER,
            mtime DATE,
            active BOOLEAN
        );""")

        self.dbh.execute("""
        CREATE TABLE IF NOT EXISTS jmailboxes (
            jmailboxid TEXT NOT NULL PRIMARY KEY,
            parentId INTEGER,
            role TEXT,
            name TEXT,
            sortOrder INTEGER,
            isSubscribed INTEGER,
            mayReadItems BOOLEAN,
            mayAddItems BOOLEAN,
            mayRemoveItems BOOLEAN,
            maySetSeen BOOLEAN,
            maySetKeywords BOOLEAN,
            mayCreateChild BOOLEAN,
            mayRename BOOLEAN,
            mayDelete BOOLEAN,
            maySubmit BOOLEAN,
            totalEmails INTEGER,
            unreadEmails INTEGER,
            totalThreads INTEGER,
            unreadThreads INTEGER,
            jcreated INTEGER,
            jmodseq INTEGER,
            jnoncountsmodseq INTEGER,
            mtime DATE,
            active BOOLEAN
        );""")

        self.dbh.execute("""
        CREATE TABLE IF NOT EXISTS jmessagemap (
            jmailboxid TEXT,
            msgid TEXT,
            jcreated INTEGER,
            jmodseq INTEGER,
            mtime DATE,
            active BOOLEAN,
            PRIMARY KEY (jmailboxid, msgid)
        );""")

        self.dbh.execute("CREATE INDEX IF NOT EXISTS msgidmap ON jmessagemap (msgid)")

        self.dbh.execute("""
        CREATE TABLE IF NOT EXISTS account (
            email TEXT,
            displayname TEXT,
            picture TEXT,
            jdeletedmodseq INTEGER NOT NULL DEFAULT 1,
            jhighestmodseq INTEGER NOT NULL DEFAULT 1,
            jstateMailbox TEXT NOT NULL DEFAULT 1,
            jstateThread TEXT NOT NULL DEFAULT 1,
            jstateEmail TEXT NOT NULL DEFAULT 1,
            jstateContact TEXT NOT NULL DEFAULT 1,
            jstateContactGroup TEXT NOT NULL DEFAULT 1,
            jstateCalendar TEXT NOT NULL DEFAULT 1,
            jstateCalendarEvent TEXT NOT NULL DEFAULT 1,
            jstateUserPreferences TEXT NOT NULL DEFAULT 1,
            jstateClientPreferences TEXT NOT NULL DEFAULT 1,
            jstateCalendarPreferences TEXT NOT NULL DEFAULT 1,
            mtime DATE
        );""")

        self.dbh.execute("""
        CREATE TABLE IF NOT EXISTS jrawmessage (
            msgid TEXT PRIMARY KEY,
            parsed TEXT,
            hasAttachment INTEGER,
            mtime DATE
        );""")

        self.dbh.execute("""
        CREATE TABLE IF NOT EXISTS jfiles (
            jfileid INTEGER PRIMARY KEY,
            type TEXT,
            size INTEGER,
            content BLOB,
            expires DATE,
            mtime DATE,
            active BOOLEAN
        );""")

        self.dbh.execute("""
        CREATE TABLE IF NOT EXISTS jcalendars (
            jcalendarid INTEGER PRIMARY KEY,
            name TEXT,
            color TEXT,
            isVisible BOOLEAN,
            mayReadFreeBusy BOOLEAN,
            mayReadItems BOOLEAN,
            mayAddItems BOOLEAN,
            mayModifyItems BOOLEAN,
            mayRemoveItems BOOLEAN,
            mayDelete BOOLEAN,
            mayRename BOOLEAN,
            jcreated INTEGER,
            jmodseq INTEGER,
            mtime DATE,
            active BOOLEAN
        );""")

        self.dbh.execute("""
        CREATE TABLE IF NOT EXISTS jevents (
            eventuid TEXT PRIMARY KEY,
            jcalendarid INTEGER,
            firststart DATE,
            lastend DATE,
            payload TEXT,
            jcreated INTEGER,
            jmodseq INTEGER,
            mtime DATE,
            active BOOLEAN
        );""")

        self.dbh.execute("CREATE INDEX IF NOT EXISTS jeventcal ON jevents (jcalendarid)")

        self.dbh.execute("""
        CREATE TABLE IF NOT EXISTS jaddressbooks (
            jaddressbookid INTEGER PRIMARY KEY,
            name TEXT,
            isVisible BOOLEAN,
            mayReadItems BOOLEAN,
            mayAddItems BOOLEAN,
            mayModifyItems BOOLEAN,
            mayRemoveItems BOOLEAN,
            mayDelete BOOLEAN,
            mayRename BOOLEAN,
            jcreated INTEGER,
            jmodseq INTEGER,
            mtime DATE,
            active BOOLEAN
        ); """)

        self.dbh.execute("""
        CREATE TABLE IF NOT EXISTS jcontactgroups (
            groupuid TEXT PRIMARY KEY,
            jaddressbookid INTEGER,
            name TEXT,
            jcreated INTEGER,
            jmodseq INTEGER,
            mtime DATE,
            active BOOLEAN
        );""")

        self.dbh.execute("CREATE INDEX IF NOT EXISTS jgroupbook ON jcontactgroups (jaddressbookid)")

        self.dbh.execute("""
        CREATE TABLE IF NOT EXISTS jcontactgroupmap (
            groupuid TEXT,
            contactuid TEXT,
            mtime DATE,
            PRIMARY KEY (groupuid, contactuid)
        );""")

        self.dbh.execute("CREATE INDEX IF NOT EXISTS jcontactmap ON jcontactgroupmap (contactuid)")

        self.dbh.execute("""
        CREATE TABLE IF NOT EXISTS jcontacts (
            contactuid TEXT PRIMARY KEY,
            jaddressbookid INTEGER,
            isFlagged BOOLEAN,
            payload TEXT,
            jcreated INTEGER,
            jmodseq INTEGER,
            mtime DATE,
            active BOOLEAN
        );""")

        self.dbh.execute("CREATE INDEX IF NOT EXISTS jcontactbook ON jcontacts (jaddressbookid)")

        self.dbh.execute("""
        CREATE TABLE IF NOT EXISTS jsubmission (
            jsubid INTEGER PRIMARY KEY,
            msgid TEXT,
            thrid TEXT,
            envelope TEXT,
            sendAt INTEGER,
            jcreated INTEGER,
            jmodseq INTEGER,
            mtime DATE,
            active BOOLEAN
        );""")

        self.dbh.execute("""
        CREATE TABLE IF NOT EXISTS juserprefs (
            jprefid TEXT PRIMARY KEY,
            payload TEXT,
            jcreated INTEGER,
            jmodseq INTEGER,
            mtime DATE,
            active BOOLEAN
        );""")

        self.dbh.execute("""
        CREATE TABLE IF NOT EXISTS jclientprefs (
            jprefid TEXT PRIMARY KEY,
            payload TEXT,
            jcreated INTEGER,
            jmodseq INTEGER,
            mtime DATE,
            active BOOLEAN
        );""")

        self.dbh.execute("""
        CREATE TABLE IF NOT EXISTS jcalendarprefs (
            jprefid TEXT PRIMARY KEY,
            payload TEXT,
            jcreated INTEGER,
            jmodseq INTEGER,
            mtime DATE,
            active BOOLEAN
        );""")