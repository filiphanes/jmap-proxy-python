import os
import sqlite3
import time
from collections import defaultdict
import re
from datetime import datetime
try:
    import orjson as json
except ImportError:
    import json

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

class DB:
    def __init__(self, accountid, path='./data/'):
        self.accountid = accountid
        self.dbpath = os.path.join(path, accountid + '.db')
        print('Opening dbh', self.dbpath)
        self.dbh = sqlite3.connect(self.dbpath)
        self.dbh.row_factory = sqlite3.Row
        self._initdb()
        self.cursor = self.dbh.cursor()
        self.cursor.row_factory = sqlite3.Row
        self.cursor.execute('BEGIN DEFERRED')
        self.modseq = 0
        self.tables = {}
        self.backfilling = False
        self.updated_mailbox_counts = {}
        self.change_cb = None

    def delete(self):
        self.dbh.close()
        os.unlink(self.dbpath)
    
    def begin(self):
        if not self.dbh.in_transaction:
            self.cursor.execute("BEGIN DEFERRED")
            self.in_transaction = True
        else:
            print('Already in transaction')
    
    def commit(self):
        if not self.dbh.in_transaction:
            print('Not in transaction')
            return

        for jmailboxid, val in self.updated_mailbox_counts.items():
            update = {}
            self.cursor.execute("""SELECT
                    COUNT(DISTINCT msgid)
                FROM jmessages JOIN jmessagemap USING (msgid)
                WHERE jmailboxid = ?
                  AND jmessages.active = 1
                  AND jmessagemap.active = 1
                """, [jmailboxid])
            update['totalEmails'] = self.cursor.fetchone()[0]

            self.cursor.execute("""SELECT
                    COUNT(DISTINCT msgid)
                FROM jmessages JOIN jmessagemap USING (msgid)
                WHERE jmailboxid = ?
                  AND jmessages.isUnread = 1
                  AND jmessages.active = 1
                  AND jmessagemap.active = 1
                """, [jmailboxid])
            update['unreadEmails'] = self.cursor.fetchone()[0]

            self.cursor.execute("""SELECT
                COUNT(DISTINCT thrid)
                FROM jmessages JOIN jmessagemap USING (msgid)
                WHERE jmailboxid = ?
                  AND jmessages.active = 1
                  AND jmessagemap.active = 1
                  AND thrid IN
                        (SELECT thrid
                        FROM jmessages JOIN jmessagemap USING (msgid)
                        WHERE isUnread = 1
                            AND jmessages.active = 1
                            AND jmessagemap.active = 1)
                """ , [jmailboxid])
            update['totalThreads'] = self.cursor.fetchone()[0]

            self.dmaybedirty('jmailboxes', update, {'jmailboxid': jmailboxid})
        self.updated_mailbox_counts = {}

        if self.modseq and self.change_cb:
            map = {}
            dbdata = {'jhighestmodseq': self.modseq}
            state = self.modseq
            for table in self.tables.keys():
                for group in TABLE2GROUPS[table]:
                    map[group] = state
                    dbdata['jstate' + group] = state
            self.dupdate('account', dbdata)
            if not self.backfilling:
                self.change_cb(self, map, state)
        self.cursor.execute('COMMIT')
    
    def rollback(self):
        if not self.dbh.in_transaction:
            print('Not in transaction')
            return
        self.cursor.execute('ROLLBACK')
    
    # handy for error cases
    def reset(self):
        if not self.dbh.in_transaction:
            print('Not in transaction')
            return
        self.cursor.execute('ROLLBACK')

    def dirty(self, table):
        if not self.modseq:
            user = self.get_user()
            user['jhighestmodseq'] = user['jhighestmodseq'] + 1
            self.modseq = user['jhighestmodseq']
        self.tables[table] = self.modseq
        return self.modseq

    def get_user(self):
        if not hasattr(self, 'user'):
            self.cursor.execute("SELECT * FROM account LIMIT 1")
            self.user = dict(self.cursor.fetchone())
        # bootstrap
        if not self.user:
            self.user = {'jhighestmodseq': 1}
            self.cursor.execute("INSERT INTO account (jhighestmodseq) VALUES (?)", [self.user['jhighestmodseq']])
        return self.user

    def touch_thread_by_msgid(self, msgid):
        thrid = self.dgetfield('jmessages', {'msgid': msgid}, 'thrid')
        if not thrid: return
        messages = self.dget('jmessages', {'thrid': thrid, 'active': 1})
        if not messages:
            self.dmaybedirty('jthreads', {'active': 0, 'data': '[]'}, {'thrid': thrid})
            return
        
        drafts = defaultdict(list)
        msgs = []
        seenmsgs = set()
        for msg in messages:
            if msg['isDraft'] and msg['msginreplyto']:
                # push the rest of the drafts to the end
                drafts[msg['msginreplyto']].append(msg['msgid'])

        for msg in messages:
            if msg['isDraft']: continue
            msgs.append(msg['msgid'])
            seenmsgs.add(msg['msgid'])
            if msg['msgmessageid']:
                for draft in drafts.get(msg['msgmessageid'], ()):
                    msgs.append(draft)
                    seenmsgs.add(draft)
        # make sure unlinked drafts aren't forgotten!
        for msg in messages:
            if msg['msgid'] in seenmsgs: continue
            msgs.append(msg['msgid'])
            seenmsgs.add(msg['msgid'])
        # have to handle doesn't exist case dammit, dmaybdirty isn't good for that
        exists = self.dgetfield('jtreads', {'thrid': thrid}, 'jcreated')
        if exists:
            self.dmaybedirty('jthreads',
                             {'active': 1, 'data': json.dumps(msgs)},
                             {'thrid': thrid})
        else:
            self.dmake('jthreads',
                       {'active': 1, 'data': json.dumps(msgs)},
                       {'thrid': thrid})
    
    def add_message(self, data, mailboxes):
        if mailboxes:
            self.dmake('jmessages', {
                **data,
                'keywords': json.dumps(data['keywords']),
                })
            for mailbox in mailboxes:
                self.add_message_to_mailbox(data['msgid'], mailbox)
            self.touch_thread_by_msgid(data['msgid'])

    def update_prefs(self, type, data):
        if type == 'UserPreferences':
            table = 'juserprefs'
        elif type == 'ClientPreferences':
            table = 'jclientprefs',
        elif type == 'CalendarPreferences':
            table = 'jcalendarprefs'
        
        modseq = self.dirty(table)
        self.cursor.execute(f"""INSERT INTO {table}
            (jprefid, payload, jcreated, jmodseq, active) VALUES
            (?,?,?,?,?)""",
            [data['id'], json.dumps(data)], modseq, modseq, 1)

    def update_mailbox_counts(self, jmailboxid, jmodseq):
        self.updated_mailbox_counts[jmailboxid] = jmodseq
    
    def add_message_to_mailbox(self, msgid, jmailboxid):
        data = {
            'msgid': msgid,
            'jmailboxid': jmailboxid,
        }
        self.dmake('jmessagemap', data)
        self.update_mailbox_counts(jmailboxid, data['jmodseq'])
        self.ddirty('jmessages', {}, {'msgid': msgid})
    
    def delete_message_from_mailbox(self, msgid, jmailboxid):
        data = {'active': 0}
        self.dmaybedirty('jmessagemap', data, {
            'msgid': msgid,
            'jmailboxid': jmailboxid,
        })
        self.update_mailbox_counts(jmailboxid, data['jmodseq'])
        self.ddirty('jmessages', {}, {'msgid': msgid})
    
    def change_message(self, msgid, data, newids):
        keywords = data.get('keywords', {})
        bump = self.dmaybedirty('jmessages', {
            'keywords': json.dumps(keywords),
            'isDraft': bool(keywords.get('draft', False)),
            'isUnread': not bool(keywords.get('seen', False)),
        }, {'msgid': msgid})

        oldids = self.dgetcol('jmessagemap', {
            'msgid': msgid,
            'active': 1,
        }, 'jmailboxid')
        old = set(oldids)

        for jmailboxid in newids:
            if old.pop(jmailboxid):
                # just bump the modseq
                if bump:
                    self.update_mailbox_counts(jmailboxid, data['jmodseq'])
            else:
                self.add_message_to_mailbox(msgid, jmailboxid)
        for jmailboxid in old:
            self.delete_message_from_mailbox(msgid, jmailboxid)
        self.touch_thread_by_msgid(msgid)
    
    def get_blob(self, blobId):
        match = re.match(r'^([mf])-([^-]+)(?:-(.*))?', blobId)
        if not match: return
        source = match.group(1)
        id = match.group(2)
        if source == 'f':
            return self.get_file(id)
        if source == 'm':
            part = match.group(3)
            return self.get_raw_message(id, part)

    # NOTE: this can ONLY be used to create draft messages
    def create_messages(self, args, idmap):
        if not args:
            return {}, {}
        self.begin()
        # XXX - get draft mailbox ID
        draftid = self.dgetfield('jmailboxes', {'role': 'drafts'}, 'jmailboxid')
        self.commit()

        todo = {}
        for cid, item in args.items():
            mailboxIds = item.pop('mailboxIds', ())
            keywords = item.pop('keywords', ())
            item['msgdate'] = datetime.now()
            item['headers']['Message-ID'] += '<' + new_uuid_string() + '.' + item['msgdate'] + os.getenv('jmaphost')
            message = jmap.EmailObject.make(item, self.get_blob())
            todo[cid] = (message, mailboxIds, keywords)
        
        created = {}
        notCreated = {}
        for cid in todo.keys():
            message, mailboxIds, keywords = todo[cid]
            mailboxes = [idmap[k] for k in mailboxIds.keys()]
            msgid, thrid = self.import_message(message, mailboxes, keywords)
            created[cid] = {
                'id': msgid,
                'threadId': thrid,
                'size': len(message)
                # TODO: other fields to reply
            }
        return created, notCreated
    
    def update_messages(self):
        return NotImplementedError()

    def destroy_messages(self):
        return NotImplementedError()
    
    def delete_message(self, msgid):
        self.dmaybedirty('jmessages', {'active': 0}, {'msgid': msgid})
        oldids = self.dgetcol('jmessagemap', {'msgid': msgid, 'active': 1}, 'jmailboxid')
        for oldid in oldids:
            self.delete_message_from_mailbox(msgid, oldid)
        self.touch_thread_by_msgid(msgid)
    
    def report_messages(self, msgids, asSpam):
        # TODO: actually report the messages (or at least check that they exist)
        return msgids, ()

    def put_file(self, accountid, type, content, expires):
        size = len(content)
        c = self.cursor.execute('INSERT OR REPLACE INTO jfiles (type, size, content, expires) VALUES (?, ?, ?, ?)',
            (type, size, content, expires))
        id = c.last_insert_id()
        jmaphost = os.getenv('jmaphost')

        return {
            'accountId': accountid,
            'blobId': f'f-{id}',
            'expires': expires,
            'size': size,
            'url': f'https://{jmaphost}/raw/{accountid}/f-{id}'
        }
    
    def get_file(self, id):
        data = self.dgetone('jfiles', {'jfileid': id}, 'type,content')
        if data:
            return data['type'], data['content']

    def _dbl(*args):
        return '(' + ', '.join(args) + ')'
    
    def dinsert(self, table, values):
        values['mtime'] = datetime.now()
        sql = f"INSERT OR REPLACE INTO {table} (" \
            + ','.join(values.keys()) \
            + ") VALUES (" \
            + ('?,' * len(values))[:-1] + ")"
        print(sql, values.values())
        cursor = self.cursor.execute(sql, list(values.values()))
        return cursor.lastrowid
    
    def dmake(self, table, values, modseqfields=()):
        modseq = self.dirty(table)
        values['jcreated'] = modseq
        values['jmodseq'] = modseq
        for field in modseqfields:
            values[field] = modseq
        values['active'] = 1
        return self.dinsert(table, values)

    def dupdate(self, table, values, filter={}):
        values['mtime'] = datetime.now()
        sql = f'UPDATE {table} SET ' \
            + ', '.join([k + '=?' for k in values.keys()])
        if filter:
            sql += ' WHERE ' + ' AND '.join([k + '=?' for k in filter.keys()])
        self.cursor.execute(sql, list(values.values()) + list(filter.values()))
    
    def filter_values(self, table, values, filter={}):
        sql = 'SELECT ' + ','.join(values.keys()) + ' FROM ' + table
        if filter:
            sql += ' WHERE ' + ' AND '.join([k + '=?' for k in filter.keys()])
        for row in self.cursor.execute(sql, list(filter.values())):
            data = row
        else:
            data = {}
        for key in values.keys():
            if filter.get(key, None) or (data.get(key, None) == values[key]):
                del values[key]
        return values

    def dmaybeupdate(self, table, values, filter={}):
        filtered = self.filter_values(table, values, filter)
        if filtered:
            return self.dupdate(table, filtered, filter)
    
    def ddirty(self, table, values, filter={}):
        values['jmodseq'] = self.dirty(table)
        return self.dupdate(table, values, filter)

    def dmaybedirty(self, table, values=None, filter={}, modseqfields=()):
        filtered = self.filter_values(table, values, filter)
        if not filtered:
            return
        modseq = self.dirty(table)
        for field in ('jmodseq', *modseqfields):
            filtered[field] = values[field] = modseq
        return self.dupdate(table, filtered, filter)

    def dnuke(self, table, filter={}):
        modseq = self.dirty(table)
        sql = f'UPDATE {table} SET active=0, jmodseq=? WHERE active=1'
        if filter:
            sql += ' AND ' + ' AND '.join([k + '=?' for k in filter.keys()])
        return self.cursor.execute(sql, [modseq] + filter.values())
    
    def ddelete(self, table, filter={}):
        sql = f'DELETE FROM {table}'
        if filter:
            sql += ' WHERE ' + ' AND '.join([k + '=?' for k in filter.keys()])
        return self.cursor.execute(sql, list(filter.values()))

    def dget(self, table, filter={}, fields='*'):
        sql = f'SELECT {fields} FROM {table}'
        conditions = []
        values = []
        for key, val in filter.items():
            if type(val) in (tuple, list):
                conditions.append(f'{key} {val[0]} ?')
                values.append(val[1])
            else:
                conditions.append(key + '=?')
                values.append(val)
        if conditions:
            sql += ' WHERE ' + ' AND '.join(conditions)
        self.cursor.execute(sql, values)
        return self.cursor.fetchall()

    def dcount(self, table, filter={}):
        sql = f'SELECT COUNT(*) FROM {table}'
        conditions = []
        values = []
        for key, val in filter:
            if type(val) in (tuple, list):
                conditions.append(f'{key} {val[0]} ?')
                values.append(val[1])
            else:
                conditions.append(key + '=?')
                values.append(val)
        if conditions:
            sql += ' WHERE ' + ' AND '.join(conditions)
        self.cursor.execute(sql, values)
        return self.cursor.fetchone()[0]

    def dgetby(self, table, hashkey, filter={}, fields='*'):
        data = self.dget(table, filter, fields)
        return {d[hashkey]: d for d in data}

    def dgetone(self, table, filter={}, fields='*'):
        sql = f'SELECT {fields} FROM {table}'
        conditions = []
        values = []
        for key, val in filter.items():
            if type(val) in (tuple, list):
                conditions.append(f'{key} {val[0]} ?')
                values.append(val[1])
            else:
                conditions.append(key + '=?')
                values.append(val)
        if conditions:
            sql += ' WHERE ' + ' AND '.join(conditions)
        sql += ' LIMIT 1'
        self.cursor.execute(sql, values)
        return self.cursor.fetchone()

    def dgetfield(self, table, filter, field):
        res = self.dgetone(table, filter, field)
        return res.get(field, None)
    
    def dgetcol(self, table, filter={}, field=0):
        return [row[field] for row in self.dget(table, filter, field)]

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