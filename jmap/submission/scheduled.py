from datetime import datetime
from email.utils import parsedate_to_datetime, getaddresses
from enum import Enum
import itertools
from uuid import uuid4

import aiomysql

from jmap import errors
from jmap.core import MAX_OBJECTS_IN_GET
from jmap.parse import BytesHeaderParser, asAddresses

CREATE_TABLE_SQL = '''
CREATE TABLE emailSubmissions (
  `id` VARCHAR(64) NOT NULL,
  `accountId` VARCHAR(64) NOT NULL,
  `identityId` VARCHAR(64) NOT NULL,
  `emailId` VARCHAR(64) NOT NULL,
  `threadId` VARCHAR(64) NULL,
  `sender` VARCHAR(64) NOT NULL,
  `recipients` TEXT(64000) NULL,
  `sendAt` DATETIME NULL,
  `undoStatus` TINYINT NOT NULL DEFAULT 0,
  `created` INT UNSIGNED NOT NULL DEFAULT 0,
  `updated` INT UNSIGNED NULL,
  `destroyed` INT UNSIGNED NULL,
  `lockedBy` VARCHAR(64) NULL,
  `retry` TINYINT NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  INDEX `accountId` (`accountId` ASC)
);
'''

EMAIL_SUBMISSION_PROPERTIES = set('id identityId accountId emailId threadId envelope sendAt undoStatus deliveryStatus dsnBlobIds mdnBlobIds'.split())

class UndoStatus(Enum):
    pending = 0
    final = 1
    canceled = 2


class ScheduledSubmissionMixin:
    """
    Implements email submission and identities
    """
    def __init__(self, db, storage, smtp=None):
        self.db = db
        self.storage = storage
        self.smtp = smtp
        self.capabilities["urn:ietf:params:jmap:submission"] = {
            "submissionExtensions": [],
            "maxDelayedSend": 44236800  # 512 days
        }

    async def emailsubmission_set(self, idmap, ifInState=None,
                                  create=None, update=None, destroy=None,
                                  onSuccessUpdateEmail=None,
                                  onSuccessDestroyEmail=None):
        async with self.db.acquire() as conn:
            await conn.begin()
            async with conn.cursor() as cursor:
                oldState = await self.emailsubmission_state(cursor)
                try:
                    if ifInState and int(ifInState) != oldState:
                        raise errors.stateMismatch('ifInState mismatch', newState=oldState)
                except ValueError:
                    raise errors.stateMismatch('ifInState mismatch', newState=oldState)
                newState = oldState + 1

                # CREATE
                created = {}
                notCreated = {}
                if create:
                    await self.fill_emails(['blobId', 'threadId'], [e['emailId'] for e in create.values()])
                    await self.fill_identities()
                    for cid, submission in create.items():
                        try:
                            created[cid] = await self.create_emailsubmission(submission, newState, cursor)
                            idmap.set(cid, created[cid]['id'])
                        except errors.JmapError as e:
                            notCreated[cid] = e.to_dict()
                        except Exception as e:
                            notCreated[cid] = errors.serverFail().to_dict()

                # UPDATE
                updated = []
                notUpdated = {}
                for id, data in (update or {}).items():
                    try:
                        undoStatus = UndoStatus[data['undoStatus']]
                        if undoStatus != UndoStatus.canceled:
                            notUpdated[id] = errors.invalidArguments('undoStatus can be only canceled').to_dict()
                            continue
                        sql = f"""UPDATE emailSubmissions
                            SET updated=%s,
                                undoStatus=%s
                            WHERE id=%s
                              AND undoStatus={UndoStatus.pending.value}
                              AND lockedBy=NULL"""
                        await cursor.execute(sql, [newState, undoStatus.value, id])
                        if cursor.rowcount == 0:
                            # Check where is problem
                            await cursor.execute("SELECT undoStatus FROM emailSubmission WHERE id=%s", [id])
                            for us in await cursor.fetchall():
                                if UndoStatus(us) != UndoStatus.pending:
                                    notUpdated[id] = errors.invalidPatch('undoStatus is not pending').to_dict()
                                else:
                                    notUpdated[id] = errors.cannotUnsend('emailSubmission locked').to_dict()
                            if id not in notUpdated:
                                notUpdated[id] = errors.notFound().to_dict()
                    except Exception as e:
                        notUpdated[id] = errors.notFound().to_dict()
                    except KeyError:
                        notUpdated[id] = errors.invalidProperties(f"Unknown undoStatus {data['undoStatus']}",
                                                                  properties=['undoStatus']).to_dict()

                # DESTROY
                destroyed = []
                notDestroyed = {}
                for id in (destroy or ()):
                    try:
                        await cursor.execute('UPDATE emailSubmissions SET destroyed=%s WHERE id=%s',
                                             [newState, id])
                        if cursor.rowcount == 0:
                            notDestroyed[id] = errors.notFound().to_dict()
                    except Exception as e:
                        notDestroyed[id] = errors.serverFail().to_dict()
            # TODO: rollback
            await conn.commit()

        result = {
            "accountId": self.id,
            "oldState": str(oldState),
            "newState": str(newState),
            "created": created,
            "notCreated": notCreated,
            "updated": updated,
            "notUpdated": notUpdated,
            "destroyed": destroyed,
            "notDestroyed": notDestroyed,
        }

        if onSuccessUpdateEmail or onSuccessDestroyEmail:
            successfull = set(created.keys())
            successfull.update(updated, destroyed)

            updateEmail = {}
            for id in successfull:
                patch = onSuccessUpdateEmail.get(f"#{id}", None)
                if patch:
                    updateEmail[create[id]['emailId']] = patch
            if onSuccessDestroyEmail is None:
                onSuccessDestroyEmail = []
            destroyEmail = [id for id in successfull if f"#{id}" in onSuccessDestroyEmail]

            if updateEmail or destroyEmail:
                update_result = await self.email_set(
                    idmap,
                    update=updateEmail,
                    destroy=destroyEmail,
                )
                update_result['method_name'] = 'Email/set'
                return result, update_result
        return result

    async def create_emailsubmission(self, submission, newState, cursor):
        missingProps = [p for p in ['identityId', 'emailId'] if p not in submission]
        if missingProps:
            raise errors.invalidProperties("missing", properties=missingProps)
        identity = self.identities.get(submission['identityId'])
        if identity is None:
            raise errors.notFound(f"Identity {submission['identityId']} not found")
        email = self.emails.get(submission['emailId'])
        if not email:
            raise errors.notFound(f"EmailId {submission['emailId']} not found")

        body = await self.download(email['blobId'])
        msg = BytesHeaderParser.parse_from_bytes(body)
        try:
            sendAt = parsedate_to_datetime(msg.get('Date').encode())
        except AttributeError:
            sendAt = datetime.now()
        except Exception:
            raise errors.invalidEmail('Date header parse error')

        envelope = submission.get('envelope')
        if envelope:
            sender = envelope['mailFrom']['email']
            recipients = {a['email'] for a in envelope['rcptTo']}
        else:
            try:
                addresses, = msg.get_all('sender') or msg.get_all('from')
                sender, = asAddresses(addresses)
            except ValueError:
                raise errors.invalidEmail('multiple sender/from addresses', properties=['emailId'])
            sender_user, _, sender_domain = sender['email'].partition('@')
            identity_user, _, identity_domain = identity['email'].partition('@')
            # If the address found from this is not allowed by the Identity associated
            # with this submission, the email property from the Identity MUST be used instead.
            if sender['email'] != identity['email']:
                if identity_user == '*' and sender_domain == identity_domain:
                    # If the mailbox part of the address is * (e.g., *@example.com)
                    # then the client may use any valid address ending in that domain
                    pass
                else:
                    sender['email'] = identity['email']
            addresslist = getaddresses(itertools.chain(
                msg.get_all('to'),
                msg.get_all('cc'),
                msg.get_all('bcc'),
            ))
            recipients = {email for name, email in addresslist}  # dedupliction
            envelope = {
                'mailFrom': sender,
                # The deduplicated set of email addresses from the To, Cc, and Bcc
                # header fields, if present, with no parameters for any of them.
                'rcptTo': [{'email': e} for e in recipients],
            }

        id = uuid4().hex
        res = await self.storage.put(f'/{id}', body)
        if res.status >= 400:
            raise errors.serverFail(f'PUT status={res.status}')

        try:
            await cursor.execute('''INSERT INTO emailSubmissions
                (id, accountId, identityId, emailId, threadId, sendAt, sender, recipients, undoStatus, created)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);''', [
                    id,
                    self.id,
                    submission['identityId'],
                    submission['emailId'],
                    email['threadId'],
                    sendAt,
                    sender,
                    ','.join(recipients),
                    UndoStatus.pending.value,
                    newState,
                ])
        except Exception as e:
            raise errors.serverFail(repr(e))

        return {'id': id}

    async def emailsubmission_state(self, cursor=None):
        """Return state as integer, needs to be stringified for JMAP"""
        # destroyed > updated > created or NULL if not set and created NOT NULL
        sql = 'SELECT MAX(COALESCE(destroyed, updated, created)) FROM emailSubmissions WHERE accountId=%s'
        if cursor is None:
            async with self.db.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(sql, [self.id])
                    status, = await cursor.fetchone()
        else:
                    await cursor.execute(sql, [self.id])
                    status, = await cursor.fetchone()
        return status or 0

    async def emailsubmission_state_low(self, cursor=None):
        # created state is first so there will be lowest state
        sql = 'SELECT MIN(created) FROM emailSubmissions WHERE accountId=%s'
        if cursor is None:
            async with self.db.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(sql, [self.id])
                    status, = await cursor.fetchone()
        else:
                    await cursor.execute(sql, [self.id])
                    status, = await cursor.fetchone()
        return status or 0

    async def emailsubmission_get(self, idmap, ids=None, properties=None):
        if properties:
            properties = set(properties)
            if not properties.issubset(EMAIL_SUBMISSION_PROPERTIES):
                raise errors.invalidProperties(properties=list(properties - EMAIL_SUBMISSION_PROPERTIES))
        else:
            properties = EMAIL_SUBMISSION_PROPERTIES

        columns = properties - {'dsnBlobIds', 'mdnBlobIds', 'deliveryStatus', 'envelope'}
        columns.add('id')  # always present
        if 'envelope' in properties:  # break to db columns
            columns.update(['sender', 'recipients'])

        # Build SQL
        # don't afraid of injection, columns are checked against EMAIL_SUBMISSION_PROPERTIES
        sql = f"SELECT {','.join(columns)} FROM emailSubmissions WHERE accountId=%s"
        sql_args = [self.id]
        if ids:
            if len(ids) > MAX_OBJECTS_IN_GET:
                raise errors.tooLarge('Requested more than {MAX_OBJECTS_IN_GET} ids')
            notFound = set([idmap.get(id) for id in ids])
            sql += ' AND id IN (' + ('%s,'*len(notFound))[:-1] + ')'
            sql_args.extend(notFound)
        else:
            notFound = set()

        # TODO: raise proper errors.tooLarge, when count > MAX_OBJECTS_IN_GET
        sql += f' LIMIT {MAX_OBJECTS_IN_GET}'

        lst = []
        async with self.db.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as c:
                await c.execute(sql, sql_args)
                for submission in await c.fetchall():
                    if 'envelope' in properties:
                        submission['envelope'] = {
                            'mailFrom': {'email': submission.pop('sender')},
                            'rcptTo': [{'email': e} for e in submission.pop('recipients').split()],
                        }
                    if 'undoStatus' in submission:
                        submission['undoStatus'] = UndoStatus(submission['undoStatus']).name
                    if 'deliveryStatus' in properties:
                        submission['deliveryStatus'] = None
                    if 'dsnBlobIds' in properties:
                        submission['dsnBlobIds'] = []
                    if 'mdnBlobIds' in properties:
                        submission['mdnBlobIds'] = []
                    notFound.discard(submission['id'])
                    lst.append(submission)
                state = await self.emailsubmission_state(c)

        return {
            'accountId': self.id,
            'list': lst,
            'state': str(state),
            'notFound': list(notFound),
        }

    async def emailsubmission_changes(self, sinceState, maxChanges=None):
        try:
            sinceState = int(sinceState)
        except ValueError:
            raise errors.invalidArguments('sinceState is not integer')
        if maxChanges is None:
            maxChanges = 10000
        if maxChanges <= 0:
            raise errors.invalidArguments('maxChanges is not positive integer')

        async with self.db.acquire() as conn:
            async with conn.cursor() as cursor:
                lowestState = await self.emailsubmission_state_low(cursor)
                if sinceState < lowestState:
                    raise errors.cannotCalculateChanges()

                created_ids = []
                updated_ids = []
                destroyed_ids = []
                newState = 0
                changes = 0
                hasMoreChanges = False
                created, updated, destroyed = 0, 0, 0
                # COALESCE(destroyed, updated, created) returns maximum value
                # because destroyed > updated > created and NULL if not set and created is NOT NULL
                # GREATEST(created, updated, destroyed) can be used on MariaDB,
                # but COALESCE is supported on more databases
                sql = '''SELECT id, COALESCE(created, 0), COALESCE(updated, 0), COALESCE(destroyed, 0)
                    FROM jmap.emailSubmissions
                    WHERE accountId=%s
                      AND COALESCE(destroyed, updated, created) > %s
                    ORDER BY COALESCE(destroyed, updated, created) ASC
                    LIMIT %s'''
                await cursor.execute(sql, [self.id, sinceState, maxChanges+1])
                for id, created, updated, destroyed in await cursor.fetchmany(maxChanges):
                    if created > sinceState:
                        if destroyed:
                            continue
                        created_ids.append(id)
                    elif destroyed > sinceState:
                        destroyed_ids.append(id)
                    else:
                        updated_ids.append(id)
                    changes += 1
                    if changes == maxChanges:
                        newState = max(created, updated, destroyed)
                        for id, created, updated, destroyed in await cursor.fetchmany(1):
                            hasMoreChanges = True
                            if newState == max(created, updated, destroyed):
                                # don't miss changes made in this state
                                # WARNING: if maxChanges is less than count of changes
                                # made in one state then client could not get all changes
                                # and could loop infinitely if not checking state progress
                                newState -= 1
                        break
                else:  # no break, no more changes, use last state
                    newState = max(created, updated, destroyed)

        return {
            'accountId': self.id,
            'oldState': str(sinceState),
            'newState': str(newState),
            'hasMoreChanges': hasMoreChanges,
            'created': created_ids,
            'updated': updated_ids,
            'destroyed': destroyed_ids,
        }

    async def emailsubmission_query(self, sort=None, filter=None, position=None, limit=None,
                                    anchor=None, anchorOffset=None, calculateTotal=False):
        out = {
            'accountId': self.id,
            'canCalculateChanges': False,
        }

        if limit is not None and (not isinstance(limit, int) or limit < 0):
            raise errors.invalidArguments('limit has to be positive integer')
        elif limit > 1000:
            limit = 1000
            out['limit'] = limit

        sql = bytearray(b'SELECT id FROM emailSubmissions')
        where = bytearray(b' WHERE accountId=%s')
        args = [self.id]
        if filter:
            where += b' AND '
            to_sql_where(filter, where, args)
            sql += where
        if sort:
            sql += b' ORDER BY '
            to_sql_sort(sort, sql)

        async with self.db.acquire() as conn:
            async with conn.cursor() as cursor:
                if calculateTotal:
                    await cursor.execute('SELECT COUNT(*) FROM emailSubmissions' + where.decode(), args)
                    out['total'], = await cursor.fetchone()

                if position and not anchor:
                    if not isinstance(position, int) or position < 0:
                        raise errors.invalidArguments('position has to be positive integer')
                    sql += b' OFFSET %s'
                    args.append(position)
                    sql += b' LIMIT %s'
                    args.append(limit)

                # MAYBE: calc more exact state of query
                out['queryState'] = str(await self.emailsubmission_state(cursor))
                await cursor.execute(sql.decode(), args)
                out['ids'] = [id for id, in await cursor.fetchall()]

        if anchor:
            try:
                position = out['ids'].index(anchor)
            except ValueError:
                raise errors.anchorNotFound()
            position += max(0, anchorOffset or 0)
            out['ids'] = out['ids'][position:position+limit]
        out['position'] = position

        return out

    async def emailsubmission_query_changes(self, sort=None, filter=None,
                                            sinceQueryState=None, maxChanges=None,
                                            upToId=None, calculateTotal=False):
        raise errors.cannotCalculateChanges()


def to_sql_where(criteria, sql: bytearray, args: list):
    if 'operator' in criteria:
        operator = criteria['operator']
        try:
            conds = criteria['conditions']
        except KeyError:
            raise errors.unsupportedFilter(f"missing conditions in FilterOperator")
        if not conds:
            raise errors.unsupportedFilter(f"Empty filter conditions")
        if 'NOT' == operator:
            sql += b'NOT(('
            for c in conds:
                to_sql_where(c, sql, args)
                sql += b')OR('
            sql[-3:] = b')'
        elif 'OR' == operator:
            sql += b'('
            for c in conds:
                to_sql_where(c, sql, args)
                sql += b')OR('
            del sql[-3:]
        elif 'AND' == operator:
            sql += b'('
            for c in conds:
                to_sql_where(c, sql, args)
                sql += b')AND('
            del sql[-4:]
        else:
            raise errors.unsupportedFilter(f"Invalid operator {operator}")
        return

    for crit, value in criteria.items():
        if not value:
            raise errors.unsupportedFilter(f"Empty value in criteria")
        #TODO: check value types
        if crit in {'identityIds', 'emailIds', 'threadIds'}:
            sql += crit.encode()[:-1]
            sql += b' IN('
            for _ in value:
                sql += b'%s,'
            sql[-1] = ord(b')')
            args.extend(value)
        elif 'undoStatus' == crit:
            sql += b'undoStatus=%s'
            args.append(UndoStatus[value].value)
        elif 'before' == crit:
            sql += b'sendAt<%s'
            args.append(datetime.fromisoformat(value.rstrip('Z')))
        elif 'after' == crit:
            sql += b'sendAt>=%s'
            args.append(datetime.fromisoformat(value.rstrip('Z')))
        else:
            raise errors.unsupportedFilter(f'Filter {crit} not supported')
        sql += b' AND '
    if criteria:  # remove ' AND '
        del sql[-5:]


def to_sql_sort(sort, sql: bytearray):
    for crit in sort:
        if crit['property'] in {'emailId', 'threadId', 'sendAt'}:
            sql += crit['property'].encode()
        else:
            raise errors.unsupportedSort(f"Property {crit['property']} is not sortable")
        if crit.get('isAscending', True):
            sql += b' ASC,'
        else:
            sql += b' DESC,'
    if sort:
        sql.pop()
