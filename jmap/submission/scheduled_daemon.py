import asyncio
from enum import Enum
import logging
import os
import socket
from urllib.parse import urlparse

import aiohttp
import aiomysql
import aiosmtplib

from jmap.submission.s3_storage import EmailSubmissionS3Storage
from .scheduled import UndoStatus

DELAY_TIME = int(os.getenv('DELAY_TIME', 5)) # seconds
WORKER_NAME = f'{socket.gethostname()}_{os.getpid()}'
ITEM_CONCURENT = 10
MAX_RETRY = 7

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
log = logging.getLogger('Scheduled')
log.setLevel(LOG_LEVEL)

MYSQL_URL = os.getenv('MYSQL_URL', 'mysql://root:password@127.0.0.1:3306/database')
MYSQL_CHARSET = os.getenv('MYSQL_CHARSET', 'UTF-8')

SMTP_URL = os.getenv('SMTP_URL', '127.0.0.1:25')
url = urlparse(SMTP_URL)
SMTP_CREDENTIALS = dict(
    hostname = url.hostname,
    port = url.port or 25,
    user = url.username,
    password = url.password,
)

class STATUS(Enum):
    success = 1
    failed = 2
    retry = 3
    nosuchkey = 4

_stop = False


async def smtp_send(storage, id, sender, recipients, sendAt, retry):
    status = STATUS.success
    try:
        try:
            recipients = recipients.split(',')
        except Exception:
            log.exception(f'Cannot split recipients {recipients}')
        res = await storage.get(f"/{id}")
        if res.status < 300:
            body = await res.read()
            smtp_response = await aiosmtplib.send(body, sender, recipients, **SMTP_CREDENTIALS)
            log.info(f'Sent id={id} sendAt={sendAt} rcpt={recipients} response={smtp_response}')
        elif res.status == 404:
            log.debug(f'Id {id} not found in storage')
            status = STATUS.nosuchkey
        else:
            status = STATUS.retry
    except (aiosmtplib.SMTPResponseException, UnicodeEncodeError):
        # TODO: podpora smtputf8
        log.exception('SMTP Response Error')
        status = STATUS.failed
    except Exception:
        log.exception('Error while sending')
        status = STATUS.retry

    try:
        if status == STATUS.success:
            try:
                res = await storage.delete(f'/{id}')
                if res.status == 404:
                    log.info(f'DELETE {id} returned 404')
                elif res.status != 204:
                    log.error(f'DELETE {id} returned {res.status}')
            except Exception as e:
                log.error(f"DELETE {id} exception: {e}")
        elif status == STATUS.retry and retry >= MAX_RETRY:
            # If task at MAX_RETRY limit, fail
            status = STATUS.failed
    except Exception:
        log.exception('Failed to create report status task')

    return status, id


async def get_locked_items(db):
    sql = """SELECT id, sender, recipients, sendAt, retry
               FROM emailSubmissions
              WHERE sendAt < NOW()
                AND lockedBy = %s;"""
    async with db.cursor() as cursor:
        await cursor.execute(sql, [WORKER_NAME])
        log.debug(f'selected {cursor.rowcount}')
        return await cursor.fetchall()


async def lock_items(db):
    sql = f"""UPDATE emailSubmissions
                SET lockedBy = %s,
                    retry = retry + 1
              WHERE sendAt < NOW() - INTERVAL retry * retry MINUTE
                AND lockedBy IS NULL
                AND retry < %s
                AND undoStatus = {UndoStatus.pending.value}
              LIMIT {ITEM_CONCURENT};"""
    async with db.cursor() as cursor:
        await cursor.execute(sql, [WORKER_NAME, MAX_RETRY])
        await db.commit()
        log.debug(f'locked {cursor.rowcount}')
        return cursor.rowcount


async def finish_ids(db, ids):
    if not ids:
        return
    sql = f"""
        UPDATE emailSubmissions es
        SET destroyed=(SELECT MAX(COALESCE(destroyed, updated, created))+1 FROM emailSubmissions WHERE accountId=es.accountId),
            updated=  (SELECT MAX(COALESCE(destroyed, updated, created))+1 FROM emailSubmissions WHERE accountId=es.accountId),
            lockedBy=NULL,
            undoStatus={UndoStatus.final.value}
        WHERE id IN {('%s,'*len(ids))[:-1]};
    """
    async with db.cursor() as cursor:
        await cursor.execute(sql, ids)
        log.debug(f'finished {cursor.rowcount} rows')
        return cursor.rowcount


async def unlock_ids(db, ids):
    if not ids:
        return
    sql = f"""UPDATE emailSubmissions
               SET lockedBy = NULL
             WHERE id IN ({('%s,'*len(ids))[:-1]});"""
    async with db.cursor() as cursor:
        await cursor.execute(sql, ids)
        log.debug(f'unlocked {cursor.rowcount} rows')
        return cursor.rowcount


async def unlock_old(db):
    sql = """UPDATE emailSubmissions
                SET lockedBy = NULL
              WHERE sendAt < NOW() - INTERVAL 1 DAY
                AND lockedBy IS NOT NULL;"""
    async with db.cursor() as cursor:
        await cursor.execute(sql)
        log.debug(f'old unlocked {cursor.rowcount}')
        return cursor.rowcount


async def update_failed_ids(db, ids):
    if not ids:
        return
    sql = f"""
        UPDATE emailSubmissions
        SET lockedBy = NULL,
            retry = {MAX_RETRY}
        WHERE id IN ({('%s,'*len(ids))[:-1]});
    """
    async with db.cursor() as cursor:
        await cursor.execute(sql, ids)
        log.debug(f'update failed {cursor.rowcount}')
        return cursor.rowcount


async def clean_old(db):
    sql = """DELETE emailSubmissions
              WHERE sendAt < NOW() - INTERVAL 1 DAY
                AND destroyed IS NOT NULL"""
    async with db.cursor() as cursor:
        await cursor.execute(sql)
        log.debug(f'deleted {cursor.rowcount}')
        return cursor.rowcount


async def process_items(db, storage, items):
    if not items:
        return
    results = await asyncio.gather(*[smtp_send(storage, *args) for args in items])
    tasks = {
        STATUS.success:   [],
        STATUS.retry:     [],
        STATUS.failed:    [],
        STATUS.nosuchkey: [],
    }
    for status, id in results:
        tasks[status].append(id)

    try:
        ids = tasks[STATUS.nosuchkey]
        cursor = await db.execute(f"SELECT id FROM emailSubmissions WHERE id IN {('%s,'*len(ids))[:-1]}", ids)
        existing = {id for id, in await cursor.fetchall()}
        if existing:
            log.error(f'NoSuchKey but id exist in db for {",".join(existing)}')
        tasks[STATUS.success].extend([id for id in ids if id not in existing])
    except Exception:
        tasks[STATUS.success].append(id)

    await finish_ids(db, tasks[STATUS.success])
    await unlock_ids(db, tasks[STATUS.retry])
    await update_failed_ids(db, tasks[STATUS.failed])
    await unlock_old(db)
    await clean_old(db)


async def daemon_start(db=None):
    global _stop
    async with aiohttp.ClientSession() as http_session:
        storage = EmailSubmissionS3Storage(http_session=http_session)
        url = urlparse(MYSQL_URL)
        while not _stop:
            try:
                log.debug('Daemon loop')
                db = await aiomysql.connect(
                    host=url.hostname,
                    port=url.port or 3306,
                    user=url.username,
                    password=url.password,
                    db=url.path[1:],
                    charset=MYSQL_CHARSET,
                    use_unicode=True,
                    autocommit=True
                )

                await lock_items(db)
                items = await get_locked_items(db)
                await process_items(db, storage, items)

            except aiomysql.OperationalError:
                log.exception('Reconnect?')
                await asyncio.sleep(DELAY_TIME*9)

            except Exception:
                log.exception(f'Unknown exception ')

            db.close()
            await asyncio.sleep(DELAY_TIME)

    log.debug('Stopped')


def stop(signum, stack):
    global _stop
    log.info(f"Recieved {signum}, shutting down")
    _stop = True



if __name__ == '__main__':
    import signal
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    from logging.handlers import SysLogHandler
    formatter = logging.Formatter('ScheduledDaemon %(message)s')
    sysloghandler = SysLogHandler(address='/dev/log')
    sysloghandler.setFormatter(formatter)
    log.addHandler(sysloghandler)
    log.addFilter(logging.Filter('Scheduled'))

    asyncio.run(daemon_start())
