from uuid import uuid4

try:
    import orjson as json
except ImportError:
    import json

from jmap import errors


'''
CREATE TABLE identities (
  `id` VARCHAR(64) NOT NULL,
  `accountId` VARCHAR(64) NOT NULL,
  `name` VARCHAR(120) NULL,
  `email` VARCHAR(120) NULL,
  `replyTo` TEXT(64000) NULL,
  `bcc` TEXT(64000) NULL,
  `textSignature` TEXT(64000) NOT NULL DEFAULT '',
  `htmlSignature` TEXT(64000) NOT NULL DEFAULT '',
  `mayDelete` TINYINT NOT NULL DEFAULT 0,
  `created` INT UNSIGNED NOT NULL DEFAULT 0,
  `updated` INT UNSIGNED NULL,
  `destroyed` INT UNSIGNED NULL,
  PRIMARY KEY (`id`),
  INDEX `accountId` (`accountId` ASC)
);'''


class DbIdentityMixin:
    """
    Implements identities
    """
    def __init__(self, db):
        self.db = db

        self.identities = {
            self.smtp_user: {
                'id': self.smtp_user,
                'name': self.name or self.smtp_user,
                'email': self.email,
                'replyTo': None,
                'bcc': None,
                'textSignature': "",
                'htmlSignature': "",
                'mayDelete': False,
            }
        }

    async def identity_get(self, idmap, ids=None, properties=None):
        lst = []
        notFound = []
        if ids is None:
            ids = self.identities.keys()

        for id in ids:
            try:
                lst.append(self.identities[idmap.get(id)])
            except KeyError:
                notFound.append(id)

        return {
            'accountId': self.id,
            'state': '1',
            'list': lst,
            'notFound': notFound,
        }

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
        
    async def indentity_set(self, idmap, ifInState=None, create=None, update=None, destroy=None):
        async with self.db.acquire() as conn:
            await conn.begin()
            async with conn.cursor() as cursor:
                oldState = await self.identity_state(cursor)
                try:
                    if ifInState and int(ifInState) != oldState:
                        raise errors.stateMismatch({"newState": str(oldState)})
                except ValueError:
                    raise errors.stateMismatch({"newState": str(oldState)})
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
        except Exception as e:
            raise errors.serverFail(str(e))

        return {'id': identityId}

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
                lowestState = await self.emailsubmission_state_low(cursor)
                if sinceState < lowestState:
                    raise errors.cannotCalculateChanges()

                created_ids = []
                updated_ids = []
                destroyed_ids = []
                newState = 0
                changes = 0
                hasMoreChanges = False
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


ALL_IDENTITY_PROPERTIES = set('id name email replyTo bcc textSignature htmlSignature mayDelete'.split())