import logging as log
import time
import re
import json

class AccountNotFound(Exception):
    pass

class AnchorNotFound(Exception):
    pass

class CannotCalculateChanges(Exception):
    pass

class JmapApi:
    def __init__(self, db):
        self.db = db
        self.results = []
        self.resultsByTag = {}

    def push_result(self, cmd, result, tag):
        r = (cmd, result, tag)
        self.results.append(r)
        if cmd != 'error':
            self.resultsByTag[tag] = r

    def resolve_backref(self, tag, path):
        try:
            result = self.resultsByTag[tag]
        except KeyError:
            log.error(f'No such result {tag}')

        res = _parsepath(path, result)
        if res is not None and not isinstance(res, list):
            return [res]
        return res
    
    def resolve_args(self, args):
        res = {}
        for key, value in args.items():
            if key.startswith('#'):
                r = self.resolve_backref(value['resultOf'], value['path'])
                if r is None:
                    return None, {'type': 'resultReference', 'message': repr(r)}
                res[key[1:]] = r
            else:
                res[key] = value
        return res, None

    def handle_request(self, request):
        self.results = []
        self.resultsByTag = {}

        for cmd, args, tag in request['methodCalls']:
            t0 = time.monotonic()
            logbit = ''
            func = getattr(self, "api_" + cmd.replace('/', '_'), None)
            if not func:
                self.push_result('error', {'error': 'unknownMethod'}, tag)
                continue

            kwargs, error = self.resolve_args(args)
            if not kwargs:
                self.push_result('error', error, tag)
                continue

            if kwargs['ids']:
                logbit += " [" + (",".join(kwargs['ids'][:4]))
                if len(kwargs['ids']) > 4:
                    logbit += ", ..." + len(kwargs['ids'])
                logbit += "]"
            if kwargs['properties']:
                logbit += " (" + (",".join(kwargs['properties'][:4]))
                if len(kwargs['properties']) > 4:
                    logbit += ", ..." + len(kwargs['properties'])
                logbit += ")"
            try:
                result = func(**kwargs)
                self.push_result(cmd, result, tag)
            except Exception as e:
                self.push_result('error', {
                    'type': e.__name__,
                    'message': str(e),
                }, tag)
                self.rollback()

            elapsed = time.monotonic() - t0
            log.info(f'JMAP CMD {cmd}{logbit} took {elapsed}')
        
        return {
            'methodResponses': self.results,
        }

    def api_Calendar_refreshSynced(self, **kwargs):
        self.db.sync_calendars()
        return {}
    
    def api_UserPreferences_get(self, ids=(), **kwargs):
        self.db.begin()
        user = self.db.get_user()
        accountid = self.db.accountid()
        data = self.db.dgetcol('juserprefs', {}, 'payload')
        self.db.commit()

        lst = [json.loads(j) for j in data]
        if not lst:
            lst = [{
                'id': 'singleton',
                'remoteServices': {},
                'displayName': user.displayname or user.email,
                'language': 'en-us',
                'timeZone': 'Europe/London',
                'use24hClock': 'yes',
                'theme': 'default',
                'enableNewsletter': True,
                'defaultIdentityId': 'id1',
                'useDefaultFromOnSMTP': False,
                'excludeContactsFromBlacklist': False,
            }]

        return {
            'accountId': accountid,
            'state': user.jstateClientPreferences,
            'list': _filter_list(lst, ids),
            'notFound': [],
        }

    def api_UserPreferences_set(self, accountId=None, create=None, update=None, destroy=None, **kwargs):
        self.db.begin()
        user = self.db.get_user()
        accountid = self.db.accountid()
        if accountId and accountId != accountid:
            self.db.rollback()
            raise AccountNotFound()
        self.db.commit()

        old_state = user.jstateClientPreferences
        if create is None: create = {}
        if update is None: update = {}
        if destroy is None: destroy = []
        
        created = {}
        notCreated = {k: "Can't create singleton types" for k in create.keys()}
        updated = {}
        notUpdated = {}
        for key in update.keys():
            if key == 'singleton':
                value = self.update_singleton_value('api_UserPreferences_get', update[key])
                ret = self.db.update_prefs('UserPreferences', value)
                if ret:
                    notUpdated[key] = ret
                else:
                    updated[key] = True
            else:
                notUpdated[key] = "Can't update anything except singleton"
        destroyed = []
        notDestroyed = {k: "Can't delete singleton types" for k in create.keys()}

        self.db.begin()
        user = self.db.get_user()
        self.db.commit()
        new_state = user.jstateClientPreferences

        return {
            'accountId': accountid,
            'oldState': old_state,
            'newState': new_state,
            'created': created,
            'notCreated': notCreated,
            'updated': updated,
            'notUpdated': notUpdated,
            'destroyed': destroyed,
            'notDestroyed': notDestroyed,
        }

    def api_ClientPreferences_get(self, ids=None, **kwargs):
        self.db.begin()
        user = self.db.get_user()
        accountid = self.db.accountid()
        data = self.db.dgetcol('juserprefs', {}, 'payload')
        self.db.commit()

        lst = [json.loads(j) for j in data]
        if not lst:
            lst.append({
                'id': 'singleton',
                'useSystemFont': False,
                'enableKBShortcuts': True,
                'enableConversations': True,
                'deleteEntireConversation': True,
                'showDeleteWarning': True,
                'showSidebar': True,
                'showReadingPane': False,
                'showPreview': True,
                'showAvatar': True,
                'afterActionGoTo': 'mailbox',
                'viewTextOnly': False,
                'allowExternalContent': 'always',
                'extraHeaders': [],
                'autoSaveContacts': True,
                'replyFromDefault': True,
                'defaultReplyAll': True,
                'composeInHTML': True,
                'replyInOrigFormat': True,
                'defaultFont': None,
                'defaultSize': None,
                'defaultColour': None,
                'sigPositionOnReply': 'before',
                'sigPositionOnForward': 'before',
                'replyQuoteAs': 'inline',
                'forwardQuoteAs': 'inline',
                'replyAttribution': '',
                'canWriteSharedContacts': False,
                'contactsSort': 'lastName',
            })

        return {
            'accountId': accountid,
            'state': user.jstateClientPreferences,
            'list': _filter_list(lst, ids),
            'notFound': [],
        }

    def api_VacationResponse_get(self, **kwargs):
        self.db.begin()
        user = self.db.get_user()
        accountid = self.db.accountid()
        self.db.commit()
        return {
            'accountId': accountid,
            'state': 'dummy',
            'list': [{
                'id': 'singleton',
                'isEnabled': False,
                'fromDate': None,
                'toDate': None,
                'subject': None,
                'textBody': None,
                'htmlBody': None,
            }],
            'notFound': [],
        }
    
    def api_Quota_get(self, ids=None, **kwargs):
        self.begin()
        user = self.db.get_user()
        accountid = self.db.accountid()
        self.db.commit()
        lst = (
            {
                'id': 'mail',
                'used': 1,
                'total': 2,
            },
            {
                'id': 'files',
                'used': 1,
                'total': 2,
            },
        )
        return {
            'accountId': accountid,
            'state': 'dummy',
            'list': _filter_list(lst, ids),
            'notFound': [],
        }
    
    def api_Identity_get(self, **kwargs):
        self.db.begin()
        user = self.db.get_user()
        accountid = self.db.accountid()
        self.db.commit()

        # TODO: fix Identity
        return {
            'accountId': accountid,
            'state': 'dummy',
            'list': {
                'id': "id1",
                'displayName': user.displayname or user.email,
                'mayDelete': False,
                'email': user.email,
                'name': user.displayname or user.email,
                'textSignature': "-- \ntext signature",
                'htmlSignature': "-- <br><b>html signature</b>",
                'replyTo': user.email,
                'autoBcc': "",
                'addBccOnSMTP': False,
                'saveSentTo': None,
                'saveAttachments': False,
                'saveOnSMTP': False,
                'useForAutoReply': False,
                'isAutoConfigured': True,
                'enableExternalSMTP': False,
                'smtpServer': "",
                'smtpPort': 465,
                'smtpSSL': "ssl",
                'smtpUser': "",
                'smtpPassword': "",
                'smtpRemoteService': None,
                'popLinkId': None,
            },
            'notFound': [],
        }

    def api_Mailbox_get(self, accountId=None, ids=None, properties=None, **kwargs):
        self.db.begin()
        user = self.db.get_user()
        accountid = self.db.accountid()
        if accountId and accountId != accountid:
            self.db.rollback()
            raise AccountNotFound()
        new_state = user.jstateMailbox
        data = self.db.dget('jmailboxes', {'active': 1})
        self.db.commit()

        if ids:
            want = set(self.idmap(i) for i in ids)
        else:
            want = set(x['jmailboxid'] for x in data)

        lst = []
        for item in data:
            if not want.pop(item['jmailboxid']):
                continue
            rec = {
                'name': item['name'].decode('utf8'),
                'parentId': item.get('parentId', None),
                'role': item['role'],
                'sortOrder': item.get('sortOrder', 0),
                'totalEmails': item.get('totalEmails', 0),
                'unreadEmails': item.get('unreadEmails', 0),
                'totalThreads': item.get('totalThreads', 0),
                'unreadThreads': item.get('unreadThreads', 0),
                'myRights': {k: bool(item.get(k, False)) for k in (
                    'mayReadItems',
                    'mayAddItems',
                    'mayRemoveItems',
                    'maySetSeen',
                    'maySetKeywords',
                    'mayCreateChild',
                    'mayRename',
                    'mayDelete',
                    'maySubmit',
                    )},
                'isSubscribed': bool(item.get('isSubscribed', False)),
            }
            if properties:
                rec = {k: rec[k] for k in properties if k in rec}
            rec['id'] = item['jmailboxid']
            lst.append(rec)

        return {
            'list': lst,
            'accountId': accountid,
            'state': new_state,
            'notFound': list(want)
        }

    def api_Mailbox_query(self, accountId=None, sort=None, filter=None, position=0, anchor=None, anchorOffset=0, limit=None, **kwargs):
        self.db.begin()
        user = self.db.get_user()
        accountid = self.db.accountid()
        if accountId and accountId != accountid:
            self.db.rollback()
            raise AccountNotFound()
        data = self.db.dget('jmailboxes', {'active': 1})
        self.db.commit()

        storage = {'data': data}
        data = self._mailbox_sort(data, sort, storage)
        if filter:
            data = self._mailbox_filter(data, filter, storage)

        start = position
        if anchor:
            # need to calculate the position
            i = [x['jmailboxid'] for x in data].index(anchor)
            if i < 0:
                raise AnchorNotFound()
            start = i + anchorOffset
        
        if limit:
            end = start + limit - 1
        else:
            end = len(data)
        
        return {
            'accountId': accountid,
            'filter': filter,
            'sort': sort,
            'queryState': user.jstateMailbox,
            'canCalculateChanges': False,
            'position': start,
            'total': len(data),
            'ids': [x['jmailboxid'] for x in data[start:end]],
        }

    def api_Mailbox_changes(self, sinceState, accountId=None, maxChanges=None, **kwargs):
        self.db.begin()
        user = self.db.get_user()
        accountid = self.db.accountid()
        if accountId and accountId != accountid:
            self.db.rollback()
            raise AccountNotFound()

        new_state = user.jstateMailbox
        if user.jdeletemodseq and sinceState <= user.jdeletemodseq:
            raise CannotCalculateChanges(f'new_state: {new_state}')
        data = self.db.dget('jmailboxes', {'jmodseq': ['>', sinceState]})

        if maxChanges and len(data) > maxChanges:
            raise CannotCalculateChanges(f'new_state: {new_state}')
        self.db.commit()

        created = []
        updated = []
        removed = []
        only_counts = 0
        for item in data:
            if item['active']:
                if item['jcreated'] <= sinceState:
                    updated.append(item['jmailboxid'])
                    if item['jnoncountsmodseq'] > sinceState:
                        only_counts = 0
                else:
                    created.append(item['jmailboxid'])
            else:
                if item['jcreated'] <= sinceState:
                    removed.append(item['jmailboxid'])
                # otherwise never seen

        return {
            'accountId': accountid,
            'oldState': sinceState,
            'newState': new_state,
            'created': created,
            'updated': updated,
            'removed': removed,
            'changedProperties': ["totalEmails", "unreadEmails", "totalThreads", "unreadThreads"] if only_counts else None,
        }

    def idmap(self, key):
        if not key:
            return
        return self.idmap.get(key, key)

    def begin(self):
        "DEPRECATED: use self.db.begin()"
        self.db.begin()

    def commit(self):
        "DEPRECATED: use self.db.commit()"
        self.db.commit()

    def _transError(self, error):
        "DEPRECATED: use self.db.rollback()"
        if self.db.in_transaction():
            self.db.rollback()
        return error


def _parsepath(path, item):
    match = re.match(r'^/([^/]+)', path)
    if not match:
        return item
    selector = match.group(1).replace('~1','/').replace('~0', '~')
    if isinstance(item, list):
        if selector == '*':
            res = []
            for one in item:
                r = _parsepath(path[match.end():], one)
                if isinstance(r, list):
                    res.extend(r)
                else:
                    res.append(r)
            return res
        if selector.isnumeric():
            return item[int(selector)]

    elif isinstance(item, dict):
        return _parsepath(path[match.end():], item[selector])

    return item

def _filter_list(lst, ids):
    if not ids:
        return lst
    return [x for x in lst if lst.id in ids]

def _prop_wanted(args, prop):
    return prop == 'id' \
        or not args['properties'] \
        or prop in args['properties']
