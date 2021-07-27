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


class ScheduledSmtpAccountMixin:
    """
    Implements email submission and identities
    """
    def __init__(self, username, password=None, smtp_host='localhost', smtp_port=25, email=None):
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

    async def emailsubmission_get(self, idmap, ids=None, properties=None):
        if properties:
            columns = set(properties)
            diff = columns.difference(EMAIL_SUBMISSION_PROPERTIES)
            if diff:
                raise errors.invalidProperties('Unknown properties: ' + (', '.join(diff)))
            columns.add('id')  # always present
        else:
            columns = EMAIL_SUBMISSION_PROPERTIES
        
        try:  # break to columns
            columns.remove('deliveryStatus')
            columns.update(['smtpReply', 'delivered', 'displayed'])
        except KeyError:
            pass

        # don't afraid of injection, columns are checked against EMAIL_SUBMISSION_PROPERTIES
        sql = f"SELECT {','.join(columns)} FROM emailSubmissions WHERE accountId=%s"
        params = [self.id]
        if ids:
            if len(ids) > MAX_OBJECTS_IN_GET:
                raise errors.tooLarge('Requested more than {MAX_OBJECTS_IN_GET} ids')
            notFound = set([idmap.get(id) for id in ids])
            sql += ' AND id IN (' + ('%s,'*len(notFound))[:-1] + ')'
            params.extend(notFound)
        else:
            notFound = set()

        # TODO: raise errors.tooLarge, when number of objects is larger
        sql += f' LIMIT {MAX_OBJECTS_IN_GET}'

        lst = []
        async with self.db.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as c:
                for submission in await c.execute(sql, params):
                    if 'smtpReply' in submission:  # pack from columns
                        submission['deliveryStatus'] = {
                            'smtpReply': submission.pop('smtpReply'),
                            'delivered': submission.pop('delivered'),
                            'displayed': 'yes' if submission.pop('displayed') else 'unknown',
                        }
                    if 'envelope' in submission:
                        submission['envelope'] = json.loads(submission['envelope'])
                    notFound.discard(submission['id'])
                    lst.append(submission)

        return {
            'accountId': self.id,
            'list': lst,
            'state': await self.emailsubmission_state(),
            'notFound': list(notFound),
        }

    async def emailsubmission_set(self, idmap, ifInState=None,
                                  create=None, update=None, destroy=None,
                                  onSuccessUpdateEmail=None,
                                  onSuccessDestroyEmail=None):
        oldState = await self.emailsubmission_state()
        if ifInState and ifInState != oldState:
            raise errors.stateMismatch({"newState": oldState})

        # CREATE
        created = {}
        notCreated = {}
        if create:
            emailIds = [e['emailId'] for e in create.values()]
            await self.fill_emails(['blobId'], emailIds)
        else:
            create = {}
        for cid, submission in create.items():
            identity = self.identities.get(submission['identityId'], None)
            if identity is None:
                raise errors.notFound(f"Identity {submission['identityId']} not found")
            email = self.emails.get(submission['emailId'], None)
            if not email:
                raise errors.notFound(f"EmailId {submission['emailId']} not found")
            envelope = submission.get('envelope', None)
            if envelope:
                sender = envelope['mailFrom']['email']
                recipients = [to['email'] for to in envelope['rcptTo']]
            else:
                # TODO: If multiple addresses are present in one of these header fields,
                #       or there is more than one Sender/From header field, the server
                #       SHOULD reject the EmailSubmission as invalid; otherwise,
                #       it MUST take the first address in the last Sender/From header field.
                sender = (email['sender'] or email['from'])[0]['email']
                recipients = set(to['email'] for to in email['to'] or ())
                recipients.update(to['email'] for to in email['cc'] or ())
                recipients.update(to['email'] for to in email['bcc'] or ())

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
                async with self.db.acquire() as conn:
                    async with conn.cursor() as c:
                        await c.execute('''INSERT INTO submissions
                            (submissionId, mailFrom, rcptTo, sendAt, identityId, envelope, undoStatus)
                            VALUES (%s,%s,%s,%s,%s,%s,%s, %);''',
                            [
                                submissionId,
                                sender,
                                ','.join(recipients),
                                sendAt,
                                identity.id,
                                json.dumps(envelope),
                                'pending'
                            ])
            except Exception as e:
                notCreated[cid] = errors.serverFail(str(e)).to_dict()
                continue

            if sendAt is None:
                await aiosmtplib.send(
                    body,
                    sender=sender,
                    recipients=recipients,
                    hostname=self.smtp_host,
                    port=self.smtp_port,
                    # username=self.smtp_user,
                    # password=self.smtp_pass,
                )

            idmap.set(cid, submissionId)
            created[cid] = {'id': submissionId}

        updated = []
        destroyed = []
        notDestroyed = []

        result = {
            "accountId": self.id,
            "oldState": oldState,
            "newState": await self.emailsubmission_state(),
            "created": created,
            "notCreated": notCreated,
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

    async def emailsubmission_state(self):
        return "1"

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
