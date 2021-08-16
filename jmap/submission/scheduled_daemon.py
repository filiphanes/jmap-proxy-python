import asyncio
from enum import Enum
from jmap.vacationresponse import db
import logging
import os
import signal
import socket
from urllib.parse import urlparse

import aiomysql
import aiosmtplib

from jmap.submission.s3_storage import EmailSubmissionS3Storage
from .scheduled import UndoStatus

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
log = logging.getLogger('Scheduled')
log.setLevel(LOG_LEVEL)

class STATUS(Enum):
    success = 1
    failed = 2
    retry = 3
    nosuchkey = 4


class ScheduledDaemon:
    def __init__(self, name=None, storage=None, db_pool=None, smtp_url=None) -> None:
        self.poll_secs = int(os.getenv('POLL_SECS', 5)) # seconds
        self.name = name or f'{socket.gethostname()}_{os.getpid()}'
        self.batch_count = 10
        self.max_retry = 7

        self.db_pool = db_pool

        if smtp_url is None:
            smtp_url = os.getenv('SMTP_URL', 'smtp://127.0.0.1:25')
        url = urlparse(smtp_url, 'smtp')
        self.smtp_credentials = dict(
            hostname = url.hostname,
            port = url.port or 25,
            username = url.username,
            password = url.password,
        )

        if storage is None:
            storage = EmailSubmissionS3Storage()
        self.storage = storage

        self._stop = False
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

    async def start(self):
        if self.db_pool is None:
            mysql_url = os.getenv('MYSQL_URL', 'mysql://root@127.0.0.1:3306/jmap')
            url = urlparse(mysql_url)
            self.db_pool = await aiomysql.create_pool(
                minsize=0,
                host=url.hostname,
                port=url.port or 3306,
                user=url.username,
                password=url.password,
                db=url.path[1:],
                charset=os.getenv('MYSQL_CHARSET', 'utf8mb4'),
                use_unicode=True,
                autocommit=True
            )

        while not self._stop:
            try:
                log.debug('Daemon loop')
                async with self.db_pool.acquire() as self.db:
                    await self.lock_items()
                    items = await self.get_locked_items()
                    await self.process_items(items)

            except aiomysql.OperationalError as e:
                log.exception('Reconnect?', exc_info=True)
                await asyncio.sleep(self.poll_secs*9)

            except Exception as e:
                log.exception(f'Unknown exception', exc_info=True)

            await asyncio.sleep(self.poll_secs)

        log.debug('Stopped')

    def stop(self, signum=0, stack=None):
        log.info(f"Recieved {signum}, shutting down")
        self._stop = True

    async def smtp_send(self, id, sender, recipients, sendAt, retry):
        status = STATUS.success
        try:
            try:
                recipients = recipients.split(',')
            except Exception:
                log.exception(f'Cannot split recipients {recipients}')
            res = await self.storage.get(f"/{id}")
            if res.status < 300:
                body = await res.read()
                smtp_response = await aiosmtplib.send(body, sender, recipients, **self.smtp_credentials)
                log.info(f'Sent id={id} sendAt={sendAt} rcpt={recipients} response={smtp_response}')
            elif res.status == 404:
                log.debug(f'Id {id} not found in storage')
                status = STATUS.nosuchkey
            else:
                status = STATUS.retry
        except (aiosmtplib.SMTPResponseException, UnicodeEncodeError) as e:
            # TODO: podpora smtputf8
            log.exception('SMTP Response Error')
            status = STATUS.failed
        except Exception as e:
            log.exception('Error while sending', exc_info=True)
            status = STATUS.retry

        try:
            if status == STATUS.success:
                try:
                    res = await self.storage.delete(f'/{id}')
                    if res.status == 404:
                        log.info(f'DELETE {id} returned 404')
                    elif res.status != 204:
                        log.error(f'DELETE {id} returned {res.status}')
                except Exception as e:
                    log.error(f"DELETE {id} exception: {e}")
            elif status == STATUS.retry and retry >= self.max_retry:
                # If task at self.max_retry limit, fail
                status = STATUS.failed
        except Exception:
            log.exception('Failed to create report status task')

        return status, id

    async def get_locked_items(self):
        sql = """SELECT id, sender, recipients, sendAt, retry
                FROM emailSubmissions
                WHERE lockedBy = %s;"""
        async with self.db.cursor() as cursor:
            await cursor.execute(sql, [self.name])
            log.debug(f'selected {cursor.rowcount}')
            return await cursor.fetchall()

    async def lock_items(self):
        sql = f"""UPDATE emailSubmissions
                    SET lockedBy = %s,
                        retry = retry + 1
                WHERE sendAt < NOW() - INTERVAL retry * retry MINUTE
                    AND lockedBy IS NULL
                    AND retry < %s
                    AND undoStatus = {UndoStatus.pending.value}
                LIMIT {self.batch_count};"""
        async with self.db.cursor() as cursor:
            await cursor.execute(sql, [self.name, self.max_retry])
            await self.db.commit()
            log.debug(f'locked {cursor.rowcount}')
            return cursor.rowcount

    async def finish_ids(self, ids):
        if not ids:
            return
        sql = f"""
            UPDATE emailSubmissions es
            SET destroyed=(SELECT MAX(COALESCE(destroyed, updated, created))+1 FROM emailSubmissions WHERE accountId=es.accountId),
                updated=  (SELECT MAX(COALESCE(destroyed, updated, created))+1 FROM emailSubmissions WHERE accountId=es.accountId),
                lockedBy=NULL,
                undoStatus={UndoStatus.final.value}
            WHERE id IN ({('%s,'*len(ids))[:-1]});
        """
        async with self.db.cursor() as cursor:
            await cursor.execute(sql, ids)
            await self.db.commit()
            log.debug(f'finished {cursor.rowcount} rows')
            return cursor.rowcount

    async def unlock_ids(self, ids):
        if not ids:
            return
        sql = f"""UPDATE emailSubmissions
                SET lockedBy = NULL
                WHERE id IN ({('%s,'*len(ids))[:-1]});"""
        async with self.db.cursor() as cursor:
            await cursor.execute(sql, ids)
            await self.db.commit()
            log.debug(f'unlocked {cursor.rowcount} rows')
            return cursor.rowcount

    async def unlock_old(self):
        sql = """UPDATE emailSubmissions
                    SET lockedBy = NULL
                WHERE sendAt < NOW() - INTERVAL 1 DAY
                    AND lockedBy IS NOT NULL;"""
        async with self.db.cursor() as cursor:
            await cursor.execute(sql)
            await self.db.commit()
            log.debug(f'old unlocked {cursor.rowcount}')
            return cursor.rowcount

    async def update_failed_ids(self, ids):
        if not ids:
            return
        sql = f"""
            UPDATE emailSubmissions
            SET lockedBy = NULL,
                retry = {self.max_retry}
            WHERE id IN ({('%s,'*len(ids))[:-1]});
        """
        async with self.db.cursor() as cursor:
            await cursor.execute(sql, ids)
            await self.db.commit()
            log.debug(f'update failed {cursor.rowcount}')
            return cursor.rowcount

    async def clean_old(self):
        CHUNK_LENGTH = 200
        # first select to avoid safe mode restriction on db
        sql = """SELECT id FROM emailSubmissions
                  WHERE sendAt < NOW() - INTERVAL 1 DAY
                    AND destroyed != NULL
                  LIMIT 5000"""
        async with self.db.cursor() as cursor:
            await cursor.execute(sql)
            ids = [id for id, in await cursor.fetchall()]
            for start in range(0, len(ids), CHUNK_LENGTH):
                chunk = ids[start:start+CHUNK_LENGTH]
                sql = f"DELETE FROM emailSubmissions WHERE id IN ({('%s,'*len(chunk))[:-1]})"
                await cursor.execute(sql, chunk)
            await self.db.commit()
            log.debug(f'deleted {len(ids)}')
            return cursor.rowcount

    async def process_items(self, items):
        if not items:
            return
        tasks = {
            STATUS.success:   [],
            STATUS.retry:     [],
            STATUS.failed:    [],
            STATUS.nosuchkey: [],
        }
        results = await asyncio.gather(*[self.smtp_send(*args) for args in items])
        for status, id in results:
            tasks[status].append(id)

        try:
            ids = tasks[STATUS.nosuchkey]
            if ids:
                async with self.db.cursor() as cursor:
                    sql = f"SELECT id FROM emailSubmissions WHERE id IN ({('%s,'*len(ids))[:-1]})"
                    await cursor.execute(sql, ids)
                    existing = {id for id, in await cursor.fetchall()}
                if existing:
                    log.error(f'NoSuchKey but id exist in db for {",".join(existing)}')
                tasks[STATUS.success].extend([id for id in ids if id not in existing])
        except Exception as e:
            tasks[STATUS.success].append(id)

        await self.finish_ids(tasks[STATUS.success])
        await self.unlock_ids(tasks[STATUS.retry])
        await self.update_failed_ids(tasks[STATUS.failed])
        await self.unlock_old()
        await self.clean_old()


if __name__ == '__main__':
    daemon = ScheduledDaemon()

    from logging.handlers import SysLogHandler
    formatter = logging.Formatter('ScheduledDaemon %(message)s')
    sysloghandler = SysLogHandler(address='/dev/log')
    sysloghandler.setFormatter(formatter)
    log.addHandler(sysloghandler)
    log.addFilter(logging.Filter('Scheduled'))

    asyncio.run(daemon.start())
