from uuid import uuid4

from aiomysql import DictCursor
import aiomysql
from pymysql.err import IntegrityError
try:
    import orjson as json
except ImportError:
    import json

from jmap import errors
from jmap.core import MAX_OBJECTS_IN_GET

CREATE_TABLE_SQL = '''
CREATE TABLE identities (
  `id` VARCHAR(64) NOT NULL,
  `accountId` VARCHAR(64) NOT NULL,
  `name` VARCHAR(120) NULL,
  `email` VARCHAR(120) NOT NULL,
  `replyTo` TEXT NULL,
  `bcc` TEXT NULL,
  `textSignature` TEXT NOT NULL DEFAULT '',
  `htmlSignature` TEXT NOT NULL DEFAULT '',
  `mayDelete` TINYINT NOT NULL DEFAULT 0,
  `created` INT NOT NULL DEFAULT 0,
  `updated` INT NULL,
  `destroyed` INT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `accountIdEmail` (`accountId` ASC, `email` ASC));
'''

IDENTITY_PROPERTIES = {'id', 'name', 'email', 'replyTo', 'bcc', 'textSignature', 'htmlSignature', 'mayDelete'}


class DbIdentityMixin:
    """
    Implements identities
    """
    def __init__(self, db):
        self.db = db
        self.identities = {}

    async def fill_identities(self):
        """Read ids identities from db to self.identities"""
        sql = f'SELECT id, name, email FROM identities WHERE accountId=%s'
        async with self.db.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(sql, [self.id])
                for row in await cursor.fetchall():
                    self.identities[row['id']] = row

    async def identity_state(self, cursor=None):
        """Return state as integer, needs to be stringified for JMAP"""
        # destroyed > updated > created or NULL if not set and created NOT NULL
        sql = 'SELECT MAX(COALESCE(destroyed, updated, created)) FROM identities WHERE accountId=%s'
        if cursor is None:
            async with self.db.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(sql, [self.id])
                    status, = await cursor.fetchone()
        else:
                    await cursor.execute(sql, [self.id])
                    status, = await cursor.fetchone()
        return status or 0

    async def identity_state_low(self, cursor=None):
        # created state is first so there will be lowest state
        sql = 'SELECT MIN(created) FROM identities WHERE accountId=%s'
        if cursor is None:
            async with self.db.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(sql, [self.id])
                    status, = await cursor.fetchone()
        else:
                    await cursor.execute(sql, [self.id])
                    status, = await cursor.fetchone()
        return status or 0
        
    async def identity_set(self, idmap, ifInState=None, create=None, update=None, destroy=None):
        async with self.db.acquire() as conn:
            await conn.begin()
            async with conn.cursor() as cursor:
                oldState = await self.identity_state(cursor)
                try:
                    if ifInState and int(ifInState) != oldState:
                        raise errors.stateMismatch('ifInState mismatch', newState=oldState)
                except ValueError:
                    raise errors.stateMismatch('ifInState mismatch', newState=oldState)
                newState = oldState + 1

                # CREATE
                created = {}
                notCreated = {}
                for cid, identity in (create or {}).items():
                    try:
                        created[cid] = await self.create_identity(identity, newState, cursor)
                        idmap.set(cid, created[cid]['id'])
                    except errors.JmapError as e:
                        notCreated[cid] = e.to_dict()
                    except Exception as e:
                        notCreated[cid] = errors.serverFail().to_dict()

                # UPDATE
                updated = []
                notUpdated = {}
                for id, data in (update or {}).items():
                    if 'email' in data:
                        notUpdated[id] = errors.invalidPatch('name is immutable').to_dict()
                        continue
                    try:
                        sql = 'UPDATE identities SET '
                        args = []
                        for property, value in data.items():
                            if property in {'replyTo', 'rss'}:
                                args.append(json.dumps(value) if value else None)
                            elif property in {'textSignature', 'htmlSignature'}:
                                args.append(value)
                            else:
                                notUpdated[id] = errors.invalidPatch(f'{property} cannot be set').to_dict()
                                continue
                            # fear sql injection? only known properties get to sql
                            sql += f'{property}=%s,'
                        sql += 'updated=%s WHERE accountId=%s AND id=%s'
                        args.extend([newState, self.id, id])
                        await cursor.execute(sql, args)
                        if cursor.rowcount == 0:
                            notUpdated[id] = errors.notFound().to_dict()
                    except Exception as e:
                        notUpdated[id] = errors.serverFail().to_dict()

                # DESTROY
                destroyed = []
                notDestroyed = {}
                for id in (destroy or ()):
                    try:
                        await cursor.execute('UPDATE identities SET destroyed=%s WHERE accountId=%s AND id=%s',
                                             [newState, self.id, id])
                        if cursor.rowcount == 0:
                            notDestroyed[id] = errors.notFound().to_dict()
                    except Exception as e:
                        notDestroyed[id] = errors.serverFail().to_dict()
            # TODO: rollback
            await conn.commit()

        return {
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

    async def create_identity(self, identity, newState, cursor):
        identityId = uuid4().hex
        try:
            await cursor.execute('''INSERT INTO identities
                (id, accountId, name, email, replyTo, bcc, textSignature, htmlSignature, mayDelete, created)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);''', [
                    identityId,
                    self.id,
                    identity.get('name', ''),
                    identity['email'],
                    json.dumps(identity['replyTo']) if identity['replyTo'] else None,
                    json.dumps(identity['bcc']) if identity['bcc'] else None,
                    identity.get('textSignature', ''),
                    identity.get('htmlSignature', ''),
                    1,
                    newState,
                ])
        except IntegrityError:
            await cursor.execute('SELECT id FROM identities WHERE accountId=%s AND email=%s',
                                 [self.id, identity['email']])
            existingId, = await cursor.fetchone()
            raise errors.alreadyExists('Identity with this email already exists.', existingId=existingId)
        except Exception as e:
            raise errors.serverFail(repr(e))

        return {'id': identityId}


    async def identity_get(self, idmap, ids=None, properties=None):
        if properties:
            properties = set(properties)
            if not properties.issubset(IDENTITY_PROPERTIES):
                raise errors.invalidProperties(properties=list(properties - IDENTITY_PROPERTIES))
            properties.add('id')  # always present
        else:
            properties = IDENTITY_PROPERTIES

        # Build SQL
        # don't afraid of injection, properties are checked against IDENTITY_PROPERTIES
        sql = f"SELECT {','.join(properties)} FROM identities WHERE accountId=%s"
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
            async with conn.cursor(DictCursor) as c:
                await c.execute(sql, sql_args)
                lst = await c.fetchall()
                notFound.difference_update([data['id'] for data in lst])
                state = await self.identity_state(c)

        return {
            'accountId': self.id,
            'list': lst,
            'state': str(state),
            'notFound': list(notFound),
        }

    async def identity_changes(self, sinceState, maxChanges=None):
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
                lowestState = await self.identity_state_low(cursor)
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
                    FROM identities
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
