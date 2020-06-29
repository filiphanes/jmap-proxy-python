import logging as log
import time
import re
from datetime import datetime
from collections import defaultdict
from jmap.email import htmltotext
try:
    import orjson as json
except ImportError:
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
        self._idmap = {}

    def push_result(self, cmd, result, tag):
        r = (cmd, result, tag)
        self.results.append(r)
        if cmd != 'error':
            self.resultsByTag[tag] = result

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

            if kwargs.get('ids', None):
                logbit += " [" + (",".join(kwargs['ids'][:4]))
                if len(kwargs['ids']) > 4:
                    logbit += ", ..." + str(len(kwargs['ids']))
                logbit += "]"
            if kwargs.get('properties', None):
                logbit += " (" + (",".join(kwargs['properties'][:4]))
                if len(kwargs['properties']) > 4:
                    logbit += ", ..." + str(len(kwargs['properties']))
                logbit += ")"

            try:
                result = func(**kwargs)
                self.push_result(cmd, result, tag)
            except Exception as e:
                self.push_result('error', {
                    'type': e.__class__.__name__,
                    'message': str(e),
                }, tag)
                raise e
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
        user = self.db.get_user()
        payloads = self.db.dgetcol('juserprefs', {}, 'payload')

        lst = [json.loads(j) for j in payloads]
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
            'accountId': self.db.accountid,
            'state': user.jstateClientPreferences,
            'list': _filter_list(lst, ids),
            'notFound': [],
        }

    def api_UserPreferences_set(self, accountId=None, create=None, update=None, destroy=None, **kwargs):
        user = self.db.get_user()
        if accountId and accountId != self.db.accountid:
            raise AccountNotFound()

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
            'accountId': self.db.accountid,
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
            'accountId': self.db.accountid,
            'state': user.jstateClientPreferences,
            'list': _filter_list(lst, ids),
            'notFound': [],
        }

    def api_VacationResponse_get(self, **kwargs):
        self.db.begin()
        user = self.db.get_user()
        self.db.commit()
        return {
            'accountId': self.db.accountid,
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
            'accountId': self.db.accountid,
            'state': 'dummy',
            'list': _filter_list(lst, ids),
            'notFound': [],
        }
    
    def api_Identity_get(self, **kwargs):
        self.db.begin()
        user = self.db.get_user()
        self.db.commit()

        # TODO: fix Identity
        return {
            'accountId': self.db.accountid,
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

    def _mailbox_match(self, item, filter):
        if 'hasRole' in filter and \
            bool(filter['hasRole']) != bool(item.get('role', False)):
            return False

        if 'isSubscribed' in filter and \
            bool(filter['isSubscribed']) != bool(item.get('isSubscribed', False)):
            return False

        if 'parentId' in filter and \
            filter['parentId'] != item.get('parentId', None):
            return False        

        return True

    def api_Mailbox_get(self, accountId=None, ids=None, properties=None, **kwargs):
        user = self.db.get_user()
        if accountId and accountId != self.db.accountid:
            self.db.rollback()
            raise AccountNotFound()
        new_state = user.get('jstateMailbox', None)
        rows = self.db.dget('jmailboxes', {'active': 1})

        if ids:
            want = set(self.idmap(i) for i in ids)
        else:
            want = set(d['jmailboxid'] for d in rows)

        lst = []
        for item in rows:
            if item['jmailboxid'] not in want:
                continue
            want.remove(item['jmailboxid'])
            rec = {
                'name': item['name'],
                'parentId': item['parentId'] or None,
                'role': item['role'],
                'sortOrder': item['sortOrder'] or 0,
                'totalEmails': item['totalEmails'] or 0,
                'unreadEmails': item['unreadEmails'] or 0,
                'totalThreads': item['totalThreads'] or 0,
                'unreadThreads': item['unreadThreads'] or 0,
                'myRights': {k: bool(item[k]) for k in (
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
                'isSubscribed': bool(item['isSubscribed']),
            }
            if properties:
                rec = {k: rec[k] for k in properties if k in rec}
            rec['id'] = item['jmailboxid']
            lst.append(rec)

        return {
            'list': lst,
            'accountId': self.db.accountid,
            'state': new_state,
            'notFound': list(want)
        }

    def api_Mailbox_query(self, accountId=None, sort=None, filter=None, position=0, anchor=None, anchorOffset=0, limit=None, **kwargs):
        user = self.db.get_user()
        if accountId and accountId != self.db.accountid:
            raise AccountNotFound()
        rows = self.db.dget('jmailboxes', {'active': 1})
        if filter:
            rows = [d for d in rows if self._mailbox_match(d, filter)]

        storage = {'data': rows}
        data = _mailbox_sort(rows, sort, storage)

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
            'accountId': self.db.accountid,
            'filter': filter,
            'sort': sort,
            'queryState': user.get('jstateMailbox', None),
            'canCalculateChanges': False,
            'position': start,
            'total': len(data),
            'ids': [x['jmailboxid'] for x in data[start:end]],
        }

    def api_Mailbox_changes(self, sinceState, accountId=None, maxChanges=None, **kwargs):
        if accountId and accountId != self.db.accountid:
            raise AccountNotFound()
        user = self.db.get_user()

        new_state = user['jstateMailbox']
        if user['jdeletemodseq'] and sinceState <= user['jdeletemodseq']:
            raise CannotCalculateChanges(f'new_state: {new_state}')
        rows = self.db.dget('jmailboxes', {'jmodseq': ['>', sinceState]})

        if maxChanges and len(rows) > maxChanges:
            raise CannotCalculateChanges(f'new_state: {new_state}')

        created = []
        updated = []
        removed = []
        only_counts = 0
        for item in rows:
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
            'accountId': self.db.accountid,
            'oldState': sinceState,
            'newState': new_state,
            'created': created,
            'updated': updated,
            'removed': removed,
            'changedProperties': ["totalEmails", "unreadEmails", "totalThreads", "unreadThreads"] if only_counts else None,
        }
    
    def _patchitem(self, item, path: str, val=None):
        try:
            prop, path = path.split('/', maxsplit=1)
            return self._patchitem(item[prop], path, val)
        except ValueError:
            if val is not None:
                item[path] = val
            elif path in item:
                del item[path]
    
    def _resolve_patch(self, accountId, update, get_data):
        for id, item in update.items():
            properties = {}
            for path in item.keys():
                try:
                    prop, _ = path.split('/', maxsplit=1)
                except ValueError:
                    continue
                if prop in properties:
                    properties[prop].append(path)
                else:
                    properties[prop] = [path]
            if not properties:
                continue  # nothing patched in this one

            data = get_data(accountId, ids=[id], properties=properties.keys())
            try:
                data = data['list'][0]
            except KeyError:
                # XXX - if nothing in the list we SHOULD abort
                continue
            for prop, paths in properties.items():
                item[prop] = data[prop]
                for path in paths:
                    self._patchitem(item, path, item.pop(path))
        

    def _post_sort(self, data, sortargs, storage):
        return data
        # TODO: sort key function
        fieldmap = {
            'id': ('msgid', 0),
            'receivedAt': ('internaldate', 1),
            'sentAt': ('msgdate', 1),
            'size': ('msgsize', 1),
            'isunread': ('isUnread', 1),
            'subject': ('sortsubject', 0),
            'from': ('msgfrom', 0),
            'to': ('msgto', 0),
        }

    def _load_mailbox(self, id):
        return self.db.dgetby('jmessagemap', 'msgid', {'jmailboxid': id}, 'msgid,jmodseq,active')

    def _load_msgmap(self, id):
        rows = self.db.dget('jmessagemap', {}, 'msgid,jmailbox,jmodseq,active')
        msgmap = defaultdict(dict)
        for row in rows:
            msgmap[row['msgid']][row['jmailbox']] = row
        return msgmap

    def _load_hasatt(self):
        return set(self.db.dgetcol('jrawmessage', {'hasAttachment':1}, 'msgid'))

    def _hasthreadkeyword(self, messages):
        res = {}
        for msg in messages:
            # we get called by getEmailListUpdates, which includes inactive messages
            if not msg['active']:
                continue
            # have already seen a message for this thread
            if msg['thrid'] in res:
                for keyword in msg['keywords'].keys():
                    # if not already known about, it wasn't present on previous messages, so it's a "some"
                    if not res[msg['thrid']][keyword]:
                        res[msg['thrid']][keyword] = 1
                for keyword in res[msg['thrid']].keys():
                    # if it was known already, but isn't on this one, it's a some
                    if not msg['keywords'][keyword]:
                        res[msg['thrid']][keyword] = 1
            else:
                # first message, it's "all" for every keyword
                res[msg['thrid']] = {kw: 2 for kw in msg['keywords'].keys()}
        return res

    def _match(self, item, condition, storage):
        if 'operator' in condition:
            return self._match_operator(item, condition, storage)
        
        cond = condition.get('inMailbox', None)
        if cond:
            id = self.idmap(cond)
            if 'mailbox' not in storage:
                storage['mailbox'] = {}
            if id not in storage['mailbox']:
                storage['mailbox'][id] = self._load_mailbox(id)
            if item['msgid'] not in storage['mailbox'][id]\
                or not storage['mailbox'][id][item['msgid']]['active']:
                return False
        
        cond = condition.get('inMailboxOtherThan', None)
        if cond:
            if 'msgmap' not in storage:
                storage['msgmap'] = self._load_msgmap()
            if not isinstance(cond, list):
                cond = [cond]
            match = set(self.idmap(id) for id in cond)
            data = storage['msgmap'].get(item['msgid'], {})
            for id, msg in data.items():
                if id not in match and msg['active']:
                    break
            else:
                return False
        
        cond = condition.get('hasAttachment', None)
        if cond is not None:
            if 'hasatt' not in storage:
                storage['hasatt'] = self._load_hasatt()
            if item['msgid'] not in storage['hasatt']:
                return False
        
        if 'search' not in storage:
            search = []
            for field in ('before','after','text','from','to','cc','bcc','subject','body','header'):
                if field in condition:
                    search.append(field)
                    search.append(condition[field])
            for cond, field in [
                    ('minSize', 'LARGER'),   # NOT SMALLER?
                    ('maxSize', 'SMALLER'),  # NOT LARGER?
                    ('hasKeyword', 'KEYWORD'),
                    ('notKeyword', 'UNKEYWORD'),
                ]:
                if cond in condition:
                    search.append(field)
                    search.append(condition[cond])

            if search:
                storage['search'] = set(self.db.imap.search(search))
            else:
                storage['search'] = None

        if storage['search'] is not None and item['msgid'] not in storage['search']:
            return False
        
        #TODO: allInThreadHaveKeyword
        #TODO: someInThreadHaveKeyword
        #TODO: noneInThreadHaveKeyword

        return True

    def _match_operator(self, item, filter, storage):
        if filter['operator'] == 'NOT':
            return not self._match_operator(item, {
                'operator': 'OR',
                'conditions': filter['conditions']},
                storage)
        elif filter['operator'] == 'OR':
            for condition in filter['conditions']:
                if self._match(item, condition, storage):
                    return True
                return False
        elif filter['operator'] == 'AND':
            for condition in filter['conditions']:
                if not self._match(item, condition, storage):
                    return False
                return True
        raise ValueError(f"Invalid operator {filter['operator']}")

    def _messages_filter(self, data, filter, storage):
        return [d for d in data if self._match(d, filter, storage)]
    
    def _collapse_messages(self, messages):
        out = []
        seen = set()
        for msg in messages:
            if msg['thrid'] not in seen:
                out.append(msg)
                seen.add(msg['thrid'])
        return out

    def api_Email_query(self, accountId, position=None, anchor=None,
                        anchorOffset=None, sort={}, filter={}, limit=10,
                        collapseThreads=False, calculateTotal=False):
        if accountId and accountId != self.db.accountid:
            raise AccountNotFound()
        user = self.db.get_user()
        newQueryState = user['jstateEmail']
        if position is not None and anchor is not None:
            raise ValueError('invalid arguments')
        # anchor and anchorOffset must go together
        if (anchor is None) != (anchorOffset is None):
            raise ValueError('invalid arguments')
        
        start = position or 0
        if start < 0:
            raise ValueError('invalid arguments')
        rows = self.db.dget('jmessages', {'active': 1})
        rows = [dict(row) for row in rows]
        for row in rows:
            row['keywords'] = json.loads(row['keywords'] or '{}')
        storage = {'data': rows}
        rows = self._post_sort(rows, sort, storage)
        if filter:
            rows = self._messages_filter(rows, filter, storage)
        if collapseThreads:
            rows = self._collapse_messages(rows)
        
        if anchor:
            # need to calculate position
            for i, row in enumerate(rows):
                if row['msgid'] == anchor:
                    start = max(i + anchorOffset, 0)
                    break
            else:
                raise Exception('anchor not found')

        end = start + limit if limit else len(rows)
        if end > len(rows):
            end = len(rows)
        
        return {
            'accountId': self.db.accountid,
            'filter': filter,
            'sort': sort,
            'collapseThreads': collapseThreads,
            'queryState': newQueryState,
            'canCalculateChanges': True,
            'position': start,
            'total': len(rows),
            'ids': [rows[i]['msgid'] for i in range(start, end)],
        }

    def api_Email_get(self,
            accountId,
            ids: list,
            properties=None,
            bodyProperties=None,
            fetchHTMLBodyValues=False):
        if accountId and accountId != self.db.accountid:
            raise AccountNotFound()
        user = self.db.get_user()
        newState = user['jstateEmail']
        seenids = set()
        notFound = []
        lst = []
        headers_wanted = set()
        content_props = {'attachments', 'hasAttachment', 'headers', 'preview',
                         'body', 'textBody', 'htmlBody', 'bodyValues'}
        if properties:
            need_content = False
            for prop in properties:
                if prop.startswith('headers.'):
                    headers_wanted.add(prop[8:])
                    need_content = True
                elif prop in content_props:
                    need_content = True
        else:
            properties = content_props + {
                'threadId', 'mailboxIds',
                'hasAttachemnt', 'keywords', 'subject', 'sentAt',
                'receivedAt', 'size', 'blobId', 'replyTo'
                'from', 'to', 'cc', 'bcc', 'replyTo',
                'messageId', 'inReplyTo', 'references', 'sender',
            }
            need_content = True
        if not bodyProperties:
            bodyProperties = {"partId", "blobId", "size", "name", "type",
                "charset", "disposition", "cid", "language", "location",
            }

        msgids = [self.idmap(i) for i in set(ids)]
        if need_content:
            contents = self.db.fill_messages(msgids)
        else:
            contents = {}

        for msgid in msgids:
            if msgid not in seenids:
                seenids.add(msgid)
                data = self.db.dgetone('jmessages', {'msgid': msgid})
                if not data:
                    notFound.append(msgid)
                    continue

            msg = {'id': msgid}
            if 'threadId' in properties:
                msg['threadId'] = data['thrid']
            if 'mailboxIds' in properties:
                ids = self.db.dgetcol('jmessagemap', {'msgid': msgid, 'active': 1}, 'jmailboxid')
                msg['mailboxIds'] = {i: True for i in ids}
            if 'inReplyToEmailId' in properties:
                msg['inReplyToEmailId'] = data['msginreplyto']
            if 'keywords' in properties:
                msg['keywords'] = json.loads(data['keywords'])
            for prop in ('from', 'to', 'cc', 'bcc'):
                if prop in properties:
                    msg[prop] = json.loads(data['msg' + prop])
            if 'subject' in properties:
                msg['subject'] = data['msgsubject']
            if 'sentAt' in properties:
                msg['sentAt'] = data['msgdate']
            if 'size' in properties:
                msg['size'] = data['msgsize']
            if 'receivedAt' in properties:
                msg['receivedAt'] = data['internaldate']
            if 'blobId' in properties:
                msg['blobId'] = msgid
            
            if msgid in contents:
                data = contents[msgid]
                for prop in ('preview', 'textBody', 'htmlBody', 'attachments'):
                    if prop in properties:
                        msg[prop] = data[prop]
                if 'body' in properties:
                    if data['htmlBody']:
                        msg['htmlBody'] = data['htmlBody']
                    else:
                        msg['textBody'] = data['textBody']
                if 'textBody' in msg and not msg['textBody']:
                    msg['textBody'] = htmltotext(data['htmlBody'])
                if 'hasAttachment' in properties:
                    msg['hasAttachment'] = bool(data['hasAttachment'])
                if 'headers' in properties:
                    msg['headers'] = data['headers']
                elif headers_wanted:
                    msg['headers'] = {}
                    for hdr in headers_wanted:
                        if hdr in data['headers']:
                            msg['headers'][hdr.lower()] = data['headers'][hdr]

            lst.append(msg)

        return {
            'accountId': accountId,
            'list': lst,
            'state': newState,
            'notFound': notFound
        }
    
    def getRawBlob(self, selector):
        blobId, filename = selector.split('/', maxsplit=1)
        typ, data = self.db.get_blob(blobId)
        return typ, data, filename
    
    def uploadFile(self, accountid, typ, content):
        return self.db.put_file(accountid, typ, content)

    def downloadFile(self, jfileid):
        return self.db.get_file(jfileid)
    
    def api_Email_changes(self, accountId, sinceState, maxChanges=None):
        if accountId and accountId != self.db.accountid:
            raise AccountNotFound()

        user = self.db.get_user()
        newState = user['jstateEmail']

        if user['jdeletedmodseq'] and sinceState <= user['deletedmodseq']:
            raise CannotCalculateChanges(f'new_state: {newState}')
        
        rows = self.db.dget('jmessages', {'jmodseq': ('>', sinceState)},
                            'msgid,active,jcreated')
        if maxChanges and len(rows) > maxChanges:
            raise CannotCalculateChanges(f'new_state: {newState}')

        created = []
        updated = []
        removed = []
        for msgid, active, jcreated in rows:
            if active:
                if jcreated <= sinceState:
                    updated.append(msgid)
                else:
                    created.append(msgid)
            elif jcreated <= sinceState:
                removed.append(msgid)
            # else never seen
        
        return {
            'accountId': accountId,
            'oldState': sinceState,
            'newState': newState,
            'created': created,
            'updated': updated,
            'removed': removed,
        }

    def api_Email_set(self, accountId, create={}, update={}, destroy=()):
        if accountId and accountId != self.db.accountid:
            raise AccountNotFound()
        # scoped_lock = self.db.begin_superlock()

        # get state up-to-date first
        self.db.sync_imap()
        user = self.db.get_user()
        oldState = user['jstateEmail']
        created, notCreated = self.db.create_messages(create, self.idmap)
        for id, msg in created.items():
            self.setid(id, msg['id'])

        self._resolve_patch(accountId, update, self.api_Email_get)
        updated, notUpdated = self.db.update_messages(update, self._idmap)
        destroyed, notDestroyed = self.db.destroy_messages(destroy)

        # XXX - cheap dumb racy version
        self.db.sync_imap()
        user = self.db.get_user()
        newState = user['jstateEmail']

        for cid, msg in created.items():
            created[cid]['blobId'] = msg['id']
        
        return {
            'accountId': accountId,
            'oldState': oldState,
            'newState': newState,
            'created': created,
            'notCreated': notCreated,
            'updated': updated,
            'notUpdated': notUpdated,
            'destroyed': destroyed,
            'notDestroyed': notDestroyed,
        }


    def api_Thread_get(self, accountId, ids: list):
        if accountId and accountId != self.db.accountid:
            raise AccountNotFound()
        user = self.db.get_user()
        newState = user['jstateThread']
        lst = []
        seenids = set()
        notFound = []
        for id in ids:
            thrid = self.idmap(id)
            if thrid in seenids:
                continue
            seenids.add(thrid)
            msgids = self.db.dgetcol('jmessages', {'thrid': thrid, 'active': 1}, 'msgid')
            if msgids:
                lst.append({
                    'id': thrid,
                    'emailIds': msgids,
                })
            else:
                notFound.append(thrid)

        return {
            'accountId': accountId,
            'list': lst,
            'state': newState,
            'notFound': notFound,
        }


    def api_Thread_changes(self, accountId, sinceState, maxChanges=None, properties=()):
        if accountId and accountId != self.db.accountid:
            raise AccountNotFound()
        user = self.db.get_user()
        newState = user['jstateThread']
        if user['jdeletedmodseq'] and sinceState <= user['deletedmodseq']:
            raise CannotCalculateChanges(f'new_state: {newState}')
        
        rows = self.db.dget('jthreads', {'jmodseq': ('>', sinceState)},
                            'thrid,active,jcreated')
        if maxChanges and len(rows) > maxChanges:
            raise CannotCalculateChanges(f'new_state: {newState}')
        
        created = []
        updated = []
        removed = []
        for thrid, active, jcreated in rows:
            if active:
                if jcreated <= sinceState:
                    updated.append(thrid)
                else:
                    created.append(thrid)
            elif jcreated <= sinceState:
                removed.append(thrid)
            # else never seen
        
        return {
            'accountId': accountId,
            'oldState': sinceState,
            'newState': newState,
            'created': created,
            'updated': updated,
            'removed': removed,
        }




    def setid(self, key, val):
        self._idmap[f'#{key}'] = val
    
    def idmap(self, key):
        return self._idmap.get(key, key)

    def begin(self):
        self.db.begin()

    def commit(self):
        self.db.commit()

    def rollback(self):
        self.db.rollback()

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

def _makefullnames(mailboxes):
    idmap = {d['jmailboxid']: d for d in mailboxes}
    idmap.pop('', None)  # just in case
    fullnames = {}
    for id, mbox in idmap.items():
        names = []
        while mbox:
            names.append(mbox['name'])
            mbox = idmap.get(mbox['parentId'], None)
        fullnames[id] = '\x1e'.join(reversed(names))
    return fullnames

def _mailbox_sort(data, sortargs, storage):
    return data
    #TODO: make correct sorting
    def key(item):
        k = []
        for arg in sortargs:
            field = arg['property']
            if field == 'name':
                k.append(item['name'])
            elif field == 'sortOrder':
                k.append(item['sortOrder'])
            elif field == 'parent/name':
                if 'fullnames' not in storage:
                    storage['fullnames'] = _makefullnames(storage['data'])
                    k.append(storage['fullnames'][item['jmailboxid']])
                k.append(item['sortOrder'])
            else:
                raise Exception('Unknown field ' + field)

    return sorted(data, key=key)
