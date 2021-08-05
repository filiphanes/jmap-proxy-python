import asyncio
import os

import pytest

from jmap import errors


@pytest.fixture()
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.fixture()
def accountId():
    return 'u1'


@pytest.fixture()
@pytest.mark.asyncio
async def user(accountId, event_loop):
    from user import User
    user = User(accountId, 'h', loop=event_loop)
    await user.ainit()
    return user


@pytest.fixture()
def account(user, accountId):
    return user.get_account(accountId)


@pytest.fixture()
@pytest.mark.asyncio
async def db_identity_account(db_pool, accountId):
    from jmap.submission.db_identity import DbIdentityMixin, CREATE_TABLE_SQL
    class AccountMock(DbIdentityMixin):
        def __init__(self, db, accountId):
            self.id = accountId
            self.name = accountId
            self.capabilities = {}
            super().__init__(db)

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute('DROP TABLE identities;')
            await cursor.execute(CREATE_TABLE_SQL)

    return AccountMock(db_pool, accountId)


@pytest.fixture()
@pytest.mark.asyncio
async def smtp_scheduled_account(db_pool, accountId, email_id, email_id2):
    from jmap.submission.smtp_scheduled import SmtpScheduledAccountMixin, CREATE_TABLE_SQL
    blobId1 = 'blob1'
    blobId2 = 'blob2'
    class DictStorage(dict):
        async def get(self, path):
            return self[path]
        async def put(self, path, body):
            self[path] = body
        async def delete(self, path):
            del self[path]

    class AccountMock(SmtpScheduledAccountMixin):
        def __init__(self, db, username, password, smtp_host, smtp_port, email):
            self.id = username
            self.name = username
            self.capabilities = {}
            storage = DictStorage()
            super().__init__(db, storage, username, password=password, smtp_host=smtp_host, smtp_port=smtp_port, email=email)
            self.emails = {
                email_id: {
                    'blobId': blobId1,
                    'threadId': 'thread1',
                },
                email_id2: {
                    'blobId': blobId2,
                    'threadId': 'thread2',
                },
            }
            self.blobs = {
                blobId1: b'''body1''',
                blobId2: b'''body2''',
            }

        async def email_set(self, idmap, ifInState=None, create=None, update=None, destroy=None):
            return {
                'accountId': self.id,
                'oldState': '1',
                'newState': '2',
                'created': {cid:{'id':'123','blobId':'123'} for cid,data in (create or {}).items()},
                'updated': list(update.keys()) if update else None,
                'destroyed': destroy,
            }
        
        async def fill_emails(self, properties, ids):
            pass  # tested emails are already filled in self.emails

        async def download(self, blobId):
            try:
                return self.blobs[blobId]
            except KeyError:
                raise errors.notFound()

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute('DROP TABLE emailSubmissions;')
            await cursor.execute(CREATE_TABLE_SQL)
    return AccountMock(db_pool, accountId, 'h', '127.0.0.1', 25, accountId)


@pytest.fixture()
@pytest.mark.asyncio
async def db_pool():
    import aiomysql
    db_pool = await aiomysql.create_pool(
        host='127.0.0.1',
        port=3306,
        user='root',
        password='',
        db='jmap',
        charset=os.getenv('MYSQL_CHARSET', 'utf8'),
        use_unicode=True,
        autocommit=False
    )
    yield db_pool
    db_pool.close()
    await db_pool.wait_closed()


@pytest.fixture()
def idmap():
    from jmap.api import IdMap
    return IdMap({})


@pytest.fixture()
def req(user):
    from starlette.requests import Request
    scope = {'type': 'http', 'user': user}
    return Request(scope)


@pytest.fixture()
def inbox_id():
    return "988f1121e9afae5e81cb000039771c66"


@pytest.fixture()
def drafts_id():
    return "b0e0b8292940de5ecddd000039771c66"


@pytest.fixture()
def uidvalidity():
    return 1596626536


@pytest.fixture()
def email_id(uidvalidity):
    return f"{uidvalidity}-201"


@pytest.fixture()
def email_id2(uidvalidity):
    return f"{uidvalidity}-202"

