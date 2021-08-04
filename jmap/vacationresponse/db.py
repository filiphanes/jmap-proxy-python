from datetime import datetime

import aiomysql
from jmap import errors

'''
CREATE TABLE vacationResponses (
  `accountId` VARCHAR(64) NOT NULL,
  `isEnabled` TINYINT NOT NULL DEFAULT 0,
  `fromDate` DATETIME NULL,
  `toDate` DATETIME NULL,
  `subject` TEXT(64000) NULL,
  `textBody` TEXT(64000) NOT NULL DEFAULT '',
  `htmlBody` TEXT(64000) NOT NULL DEFAULT '',
  `updated` INT UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY (`accountId`)
);'''

VACATION_RESPONSE_PROPERTIES = set('id isEnabled fromDate toDate subject textBody htmlBody'.split())


class DbVacationResponseMixin:
    """
    Implements vacationResponses in sql table
    """
    def __init__(self, db):
        self.db = db
        self.capabilities["urn:ietf:params:jmap:vacationresponse"] = {}

    async def vacationresponse_get(self, idmap, ids=None, properties=None):
        if properties:
            properties = set(properties)
            if not properties.issubset(VACATION_RESPONSE_PROPERTIES):
                raise errors.invalidProperties(f'Invalid {properties - VACATION_RESPONSE_PROPERTIES}')

        if ids is None:
            ids = []

        out = {
            'accountId': self.id,
            'state': '0',
            'list': None,
            'notFound': [idmap.get(id) for id in ids if id != 'singleton'],
        }

        async with self.db.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as c:
                await c.execute("SELECT * FROM vacationResponses WHERE accountId=%s", [self.id])
                vacation = await c.fetchone()
                if vacation:
                    out['state'] = str(vacation.pop('updated'))
                if 'singleton' in ids:
                    if not vacation:
                        vacation = {p:None for p in VACATION_RESPONSE_PROPERTIES}
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
                        raise errors.stateMismatch({"newState": str(oldState)})
                except ValueError:
                    raise errors.stateMismatch({"newState": str(oldState)})
                newState = oldState + 1

                # CREATE
                created = {}
                notCreated = {}
                for cid in (create or {}).keys():
                    e = errors.invalidArguments('Cannot create more VacationResponses')
                    notCreated[cid] = e.to_dict()

                # UPDATE
                updated = []
                notUpdated = {}
                for id, data in (update or {}).items():
                    if id != 'singleton':
                        e = errors.invalidPatch(f'Exactly one object with id "singleton" exists.')
                        notUpdated[id] = e.to_dict()
                        continue
                    try:
                        sql = 'UPDATE vacationResponses SET '
                        args = []
                        for property, value in data.items():
                            if property in {'fromDate','toDate'}:
                                args.append(datetime.fromisoformat(value))
                            if property in {'isEnabled','subject','textBody', 'htmlBody'}:
                                args.append(value)
                            else:
                                e = errors.invalidPatch(f'{property} cannot be set')
                                notUpdated[id] = e.to_dict()
                                continue
                            # sql injection? NO, only known properties are in sql
                            sql += f'{property}=%s,'
                        sql += 'updated=%s WHERE accountId=%s'
                        args.extend([newState, self.id])
                        await cursor.execute(sql, args)
                    except Exception:
                        notUpdated[id] = errors.serverFail().to_dict()

                # DESTROY
                destroyed = []
                notDestroyed = {}
                for id in (destroy or ()):
                    e = errors.invalidArguments('VacationResponse object cannot be destroyed.')
                    notDestroyed[id] = e.to_dict()
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
