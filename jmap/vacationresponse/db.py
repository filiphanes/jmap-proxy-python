from datetime import datetime
from jmap.submission.db_identity import CREATE_TABLE_SQL

import aiomysql
from jmap import errors

CREATE_TABLE_SQL = '''
CREATE TABLE vacationResponses (
  `accountId` VARCHAR(64) NOT NULL,
  `isEnabled` TINYINT NOT NULL DEFAULT 0,
  `fromDate` DATETIME NULL,
  `toDate` DATETIME NULL,
  `subject` TEXT(6400) NULL,
  `textBody` TEXT(64000) NULL,
  `htmlBody` TEXT(64000) NULL,
  `updated` INT UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY (`accountId`)
);
'''

VACATION_RESPONSE_PROPERTIES = {'id','isEnabled','fromDate','toDate','subject','textBody','htmlBody'}


class DbVacationResponseMixin:
    """
    Implements vacationResponses in sql table
    """
    def __init__(self, db):
        self.db = db
        self.capabilities["urn:ietf:params:jmap:vacationresponse"] = {}

    async def vacationresponse_get(self, idmap, ids=None, properties=None):
        if properties is None:
            properties = VACATION_RESPONSE_PROPERTIES
        else:
            properties = set(properties)
            invalidProperties = [p for p in properties if p not in VACATION_RESPONSE_PROPERTIES]
            if invalidProperties:
                raise errors.invalidProperties(properties=invalidProperties)
        properties.discard('id')  # singleton

        if ids is None:
            ids = ['singleton']

        out = {
            'accountId': self.id,
            'state': '0',
            'list': None,
            'notFound': [idmap.get(id) for id in ids if id != 'singleton'],
        }

        async with self.db.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                sql = f"SELECT {','.join(properties)},updated FROM vacationResponses WHERE accountId=%s"
                await cursor.execute(sql, [self.id])
                vacation = await cursor.fetchone()
                if vacation:
                    out['state'] = str(vacation.pop('updated'))
                if 'singleton' in ids:
                    if not vacation:
                        vacation = {p:None for p in properties}
                        vacation['isEnabled'] = False
                    vacation['id'] = 'singleton'
                    out['list'] = [vacation]
        return out

    async def vacationresponse_set(self, idmap, ifInState=None, create=None, update=None, destroy=None):
        async with self.db.acquire() as conn:
            async with conn.cursor() as cursor:
                oldState = await self.vacationresponse_state(cursor)
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
                    for cid in create.keys():
                        notCreated[cid] = errors.singleton().to_dict()

                # UPDATE
                updated = []
                notUpdated = {}
                for id, data in (update or {}).items():
                    if id != 'singleton':
                        notUpdated[id] = errors.singleton().to_dict()
                        continue
                    invalidProperties = [p for p in data.keys() if p not in VACATION_RESPONSE_PROPERTIES]
                    if data.get('id', id) != id:
                        invalidProperties.append('id')
                    if invalidProperties:
                        notUpdated[id] = errors.invalidProperties(properties=invalidProperties).to_dict()
                        continue
                    try:
                        sql = 'INSERT vacationResponses (accountId,updated'
                        args = [self.id, newState]
                        for property, value in data.items():
                            sql += f',{property}'
                            if property in {'fromDate','toDate'} and value:
                                value = datetime.fromisoformat(value.rstrip('Z'))
                            args.append(value)
                        sql += ')VALUES(%s,%s' \
                            + ',%s'*len(data) \
                            + ') ON DUPLICATE KEY UPDATE updated'
                        for property in data.keys():
                            sql += f'=%s,{property}'
                        sql += '=%s'
                        args.extend(args[1:])  # without accountId
                        await cursor.execute(sql, args)
                        await conn.commit()
                        updated.append(id)
                    except Exception as e:
                        notUpdated[id] = errors.serverFail(repr(e)).to_dict()

                # DESTROY
                destroyed = []
                notDestroyed = {}
                for id in (destroy or ()):
                    notDestroyed[id] = errors.singleton().to_dict()
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

    async def vacationresponse_state(self, cursor):
        """Return state as integer, have to be str for JMAP"""
        await cursor.execute('SELECT MAX(updated) FROM vacationResponses WHERE accountId=%s', [self.id])
        status, = await cursor.fetchone()
        return status or 0
