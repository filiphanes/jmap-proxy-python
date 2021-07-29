from datetime import datetime
from email.utils import parsedate_to_datetime
import os
from uuid import uuid4
try:
    import orjson as json
except ImportError:
    import json

import aioboto3
import aiomysql
import aiosmtplib

from jmap import errors
from jmap.core import MAX_OBJECTS_IN_GET
from jmap.parse import HeadersBytesParser


S3_CREDENTIALS = os.getenv('S3_CREDENTIALS', '')
S3_BUCKET = os.getenv('S3_BUCKET', 'jmap')

EMAIL_SUBMISSION_PROPERTIES = set('id identityId accountId emailId threadId envelope sendAt undoStatus deliveryStatus dsnBlobIds mdnBlobIds'.split())
'''
CREATE TABLE emailSubmissions IF NOT EXISTS (
    id UUID PRIMARY KEY,
    accountId VARCHAR,
    identityId VARCHAR,
    emailId VARCHAR,
    threadId VARCHAR,
    envelope VARCHAR,
    sendAt DATETIME,
    undoStatus VARCHAR,
    smtpReply VARCHAR,
    delivered VARCHAR,
    displayed TINYINT,
    created INT,
    updated INT,
    destroyed INT
)
'''

class SmtpScheduledAccountMixin:
    """
    Implements email submission and identities
    """
    def __init__(self, db, username, password=None, smtp_host='localhost', smtp_port=25, email=None):
        self.db = db
        self.smtp_user = username
        self.smtp_pass = password
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.email = email or username
        self.capabilities["urn:ietf:params:jmap:submission"] = {
            "submissionExtensions": [],
            "maxDelayedSend": 44236800  # 512 days
        },

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

    async def identity_get(self, idmap, ids=None):
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

    async def indentity_set(self, idmap, ifInState=None, create=None, update=None, destroy=None):
        raise NotImplemented()

    async def identity_changes(self, sinceState, maxChanges=None):
        raise errors.cannotCalculateChanges()

    async def emailsubmission_set(self, idmap, ifInState=None,
                                  create=None, update=None, destroy=None,
                                  onSuccessUpdateEmail=None,
                                  onSuccessDestroyEmail=None):
        async with self.db.acquire() as conn:
            async with conn.cursor() as c:
                oldState = await self.emailsubmission_state(c)
                try:
                    if ifInState and int(ifInState) != oldState:
                        raise errors.stateMismatch({"newState": str(oldState)})
                except ValueError:
                    raise errors.stateMismatch({"newState": str(oldState)})
                newState = oldState + 1

                # CREATE
                created = {}
                notCreated = {}
                if create:
                    emailIds = [e['emailId'] for e in create.values()]
                    await self.fill_emails(['blobId'], emailIds)
                else:
                    create = {}
                for cid, submission in create.items():
                    identity = self.identities.get(submission['identityId'])
                    if identity is None:
                        raise errors.notFound(f"Identity {submission['identityId']} not found")
                    email = self.emails.get(submission['emailId'], None)
                    if not email:
                        raise errors.notFound(f"EmailId {submission['emailId']} not found")

                    body = await self.download(email['blobId'])
                    message = HeadersBytesParser.parse_from_bytes(body)
                    try:
                        sendAt = parsedate_to_datetime(message.get('Date').encode())
                    except AttributeError:
                        sendAt = datetime.now()
                    except Exception:
                        notCreated[cid] = errors.invalidEmail('Date header parse error').to_dict()
                        continue

                    submissionId = uuid4().hex
                    try:
                        self.emailsubmission_body_upload(submissionId, body)
                    except errors.JmapError as e:
                        notCreated[cid] = e.to_dict()
                        continue

                    try:
                        await c.execute('''INSERT INTO emailSubmissions
                            (id, sendAt, identityId, envelope, undoStatus, created)
                            VALUES (%s,%s,%s,%s,%s,%s);''', [
                                submissionId,
                                sendAt,
                                identity['id'],
                                json.dumps(submission.get('envelope')),
                                'pending',
                                newState,
                            ])
                    except Exception as e:
                        notCreated[cid] = errors.serverFail(str(e)).to_dict()
                        continue

                    idmap.set(cid, submissionId)
                    created[cid] = {'id': submissionId}

                updated = []
                notUpdated = {}
                for submissionId, data in (update or {}).items():
                    try:
                        if data['undoStatus'] != 'canceled':
                            notUpdated[submissionId] = errors.invalidArguments('undoStatus can be only canceled').to_dict()
                            continue
                        await c.execute('UPDATE emailSubmissions SET updated=%s, undoStatus=%s WHERE accountId=%s AND id=%s',
                                        [newState, data['undoStatus'], self.id, submissionId])
                        if c.rowcount == 0:
                            notUpdated[submissionId] = errors.notFound().to_dict()
                    except Exception as e:
                        notUpdated[submissionId] = errors.notFound().to_dict()
                        continue

                destroyed = []
                notDestroyed = {}
                for submissionId in (destroy or ()):
                    try:
                        await c.execute('UPDATE emailSubmissions SET destroyed=%s WHERE accountId=%s AND id=%s', [newState, self.id, submissionId])
                    except Exception as e:
                        notDestroyed[submissionId] = errors.notFound().to_dict()
                        continue
                await c.commit()

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

    async def emailsubmission_state(self, cursor=None):
        """Return state as integer, needs to be stringified for JMAP"""
        sql = 'SELECT MAX(MAX(created, updated, destroyed)) FROM emailSubmission WHERE accoundId=%s'
        if cursor is None:
            async with self.db.acquire() as conn:
                async with conn.cursor() as cursor:
                    for state, in await cursor.execute(sql, [self.id]):
                        return state
        else:
                    for state, in await cursor.execute(sql, [self.id]):
                        return state
        return 0

    async def emailsubmission_state_low(self, cursor=None):
        # created state is first so there will be lowest state
        sql = 'SELECT MIN(created) FROM emailSubmission WHERE accoundId=%s'
        if cursor is None:
            async with self.db.acquire() as conn:
                async with conn.cursor() as cursor:
                    for state, in await cursor.execute(sql, [self.id]):
                        return state
        else:
                    for state, in await cursor.execute(sql, [self.id]):
                        return state
        return 0

    async def emailsubmission_body_get(id) -> bytes:
        async with aioboto3.client(**S3_CREDENTIALS) as s3_client:
            res = await s3_client.get_object(Bucket=S3_BUCKET, Key=id)
            res = res['ResponseMetadata']
            if res["HTTPStatusCode"] != 200:
                raise errors.serverFail(f'S3 PUT returned status={res["HTTPStatusCode"]}')
            return await res['Body'].read()

    async def emailsubmission_body_put(id, body):
        async with aioboto3.client(**S3_CREDENTIALS) as s3_client:
            res = await s3_client.put_object(Bucket=S3_BUCKET, Key=id, Body=body)
            res = res['ResponseMetadata']
            if res["HTTPStatusCode"] != 200:
                raise errors.serverFail(f'S3 PUT returned status={res["HTTPStatusCode"]}')

    async def emailsubmission_body_delete(id):
        res = await s3_client.delete_object(Bucket=S3_BUCKET, Key=id)
        res = res['ResponseMetadata']
        if res['HTTPStatusCode'] == 204:
            raise errors.notFound(f'Email submission {id} not found')
        elif res['HTTPStatusCode'] != 204:
            raise errors.serverFail(f'Failed to remove S3 file, status={res["HTTPStatusCode"]}')

    async def emailsubmission_get(self, idmap, ids=None, properties=None):
        if properties:
            properties = set(properties)
            if not properties.issubset(EMAIL_SUBMISSION_PROPERTIES):
                raise errors.invalidProperties(f'Invalid {properties - EMAIL_SUBMISSION_PROPERTIES}')
        else:
            properties = EMAIL_SUBMISSION_PROPERTIES

        columns = properties - {'dsnBlobIds', 'mdnBlobIds', 'deliveryStatus'}
        columns.add('id')  # always present
        if 'deliveryStatus' in properties:  # break to db columns
            columns.update(['smtpReply', 'delivered', 'displayed'])

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
                for submission in await c.execute(sql, sql_args):
                    if 'envelope' in submission:
                        submission['envelope'] = json.loads(submission['envelope'])
                    if 'deliveryStatus' in properties:  # subdict from columns
                        submission['deliveryStatus'] = {
                            'smtpReply': submission.pop('smtpReply'),
                            'delivered': submission.pop('delivered'),
                            'displayed': 'yes' if submission.pop('displayed') else 'unknown',
                        }
                    if 'dsnBlobIds' in properties:
                        submission['dsnBlobIds'] = []
                    if 'mdnblobIds' in properties:
                        submission['mdnblobIds'] = []
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
            async with conn.cursor(aiomysql.DictCursor) as cursor:
                lowestState = await self.emailsubmission_state_low(cursor)
                if sinceState <= lowestState:
                    raise errors.cannotCalculateChanges()

                created_ids = []
                updated_ids = []
                destroyed_ids = []
                newState = 0
                changes = 0
                hasMoreChanges = False
                sql = '''SELECT id, COALESCE(created, 0), COALESCE(updated, 0), COALESCE(destroyed, 0)
                         WHERE accountId=%s
                         AND MAX(created, updated, destroyed) > %s)
                         ORDER BY MAX(created, updated, destroyed) ASC
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