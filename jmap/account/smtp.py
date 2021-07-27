import aiosmtplib as aiosmtplib

from jmap import errors


class SmtpAccountMixin:
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
            "maxDelayedSend": 10  # immediate
        },

        # static identities, only allows sending from main user email
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

    async def identity_set(self, idmap, ifInState=None, create=None, update=None, destroy=None):
        raise NotImplemented()

    async def identity_changes(self, sinceState, maxChanges=None):
        raise errors.cannotCalculateChanges()

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

            await aiosmtplib.send(
                body,
                sender=sender,
                recipients=recipients,
                hostname=self.smtp_host,
                port=self.smtp_port,
                # username=self.smtp_user,
                # password=self.smtp_pass,
            )
            id = 'fOobAr'
            idmap.set(cid, id)
            created[cid] = {'id': id}

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
