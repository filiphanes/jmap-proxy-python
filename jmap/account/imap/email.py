import email
from email.policy import default
import re

from .aioimaplib import unquoted
from jmap.parse import asAddresses, asDate, asMessageIds, asText, bodystructure, htmltotext, make, parseStructure, \
    htmlpreview

KEYWORD2FLAG = {
    '$answered':'\\Answered',
    '$flagged': '\\Flagged',
    '$draft':   '\\Draft',
    '$seen':    '\\Seen',
}
FLAG2KEYWORD = {flag.lower(): kw for kw, flag in KEYWORD2FLAG.items()}

def keyword2flag(kw):
    return KEYWORD2FLAG.get(kw, None) or kw.encode()


header_re = re.compile(r'^([\w-]+)\s*:\s*(.+?)\r\n(?=[\w\r])',
                       re.I | re.M | re.DOTALL | re.ASCII)


class ImapEmail(dict):
    __slots__ = ()

    def __missing__(self, key):
        try:
            return getattr(self, key)()
        except TypeError:
            raise KeyError(key)
        except AttributeError:
            raise KeyError(key)

    def get_header(self, name: str):
        "Return raw value from last header instance, name needs to be lowercase."
        return self['LASTHEADERS'].get(name, None)

    def EML(self):
        self['EML'] = email.message_from_bytes(self['BODY[]'], policy=default)
        return self['EML']

    def LASTHEADERS(self):
        # make headers dict with only last instance of each header
        # as required by JMAP spec for single header get
        self['LASTHEADERS'] = {name.lower(): raw
                for name, raw in header_re.findall(self['DECODEDHEADERS'])}
        return self['LASTHEADERS']

    def DECODEDHEADERS(self):
        try:
            self['DECODEDHEADERS'] = self['BODY[HEADER]'].decode()
            # free memory but keep in dict to avoid fetching it again
            self['BODY[HEADER]'] = None
            return self['DECODEDHEADERS']
        except KeyError:
            match = re.search(rb'\r\n\r\n', self['BODY[]'])
            if match:
                self['DECODEDHEADERS'] = str(memoryview(self['BODY[]'])[:match.end()])
                return self['DECODEDHEADERS']

    def blobId(self):
        return f"G{self['X-GUID']}"
        # TODO: OBJECTID extension: return self['EMAILID'][0]

    def threadId(self):
        return self['X-GUID']
        # TODO: OBJECTID extension: self['THREADID'][0]

    def hasAttachment(self):
        # Dovecot with mail_attachment_detection_options = add-flags-on-save
        return '$HasAttachment' in self['FLAGS']

    def headers(self):
        return [{'name': name, 'value': value}
                for name, value in header_re.findall(self['DECODEDHEADERS'])]

    def inReplyTo(self):
        return asMessageIds(self.get_header('in-reply-to'))

    def keywords(self):
        self['keywords'] = {FLAG2KEYWORD.get(f.lower(), f): True for f in self['FLAGS']}
        return self['keywords']

    def messageId(self):
        return asMessageIds(self.get_header('message-id'))

    def mailboxIds(self):
        # needs to by set by instanciator
        raise KeyError('mailboxIds')
        # return [self.db.byimapname[self['X-MAILBOX']]['id']]

    def preview(self):
        try:
            preview = self['PREVIEW'][1]
            if isinstance(preview, str):
                return unquoted(preview)
            else:
                return preview.decode()
        except KeyError:
            pass
        for part in self['bodyValues'].values():
            if part['type'] == 'text/plain':
                return part['value'].strip()[:256]
        for part in self['bodyValues'].values():
            if part['type'] == 'text/html':
                return htmlpreview(part['value'], 256)
        return None

    def receivedAt(self):
        return asDate(unquoted(self['INTERNALDATE']))

    def references(self):
        return asMessageIds(self.get_header('references'))

    def replyTo(self):
        return asAddresses(self.get_header('reply-to'))

    def sentAt(self):
        return asDate(self.get_header('date'))

    def size(self):
        try:
            return int(self['RFC822.SIZE'])
        except KeyError:
            return len(self['BODY[]'])

    def subject(self):
        return asText(self.get_header('subject')) or ''

    def _bodystructure(self):
        self['bodyValues'], self['bodyStructure'] \
            = bodystructure(self['blobId'], self['EML'])

    def bodyStructure(self):
        self._bodystructure()
        return self['bodyStructure']

    def bodyValues(self):
        self._bodystructure()
        return self['bodyValues']

    def _parseStructure(self):
        self['textBody'], self['htmlBody'], self['attachments'] \
            = parseStructure([self['bodyStructure']], 'mixed', False)

    def textBody(self):
        self._parseStructure()
        return self['textBody']

    def htmlBody(self):
        self._parseStructure()
        return self['htmlBody']

    def attachments(self):
        self._parseStructure()
        return self['attachments']

    def deleted(self):
        # True is set by instantiator
        return False

    def created(self):
        "Get state when this message was created"
        return EmailState(*parse_email_id(self['id']), 1 << 64)

    def updated(self):
        "Get state when this message was udpated"
        return EmailState(*parse_email_id(self['id'][1]), self['MODSEQ'])

    # Used when creating message
    def FLAGS(self):
        return [keyword2flag(kw) for kw in self['keywords']]

    def BODY(self, blobs):
        return make(self, blobs)

# Define address getters
def address_getter(field):
    def get(self):
        self[field] = asAddresses(self.get_header(field))
        return self[field]
    return get

# "from" is python reserved keyword, others are similar
for prop in ('from', 'to', 'cc', 'bcc', 'sender'):
    setattr(ImapEmail, prop, address_getter(prop))


class EmailState:
    __slots__ = ('uidvalidity', 'uid', 'modseq')

    @classmethod
    def from_string(cls, state):
        uidvalidity, uid, modseq = state.split(',')
        return cls(int(uidvalidity), int(uid), int(modseq))

    def __init__(self, uidvalidity, uid, modseq):
        self.uidvalidity = uidvalidity
        self.uid = uid
        self.modseq = modseq

    def __gt__(self, other):
        if isinstance(other, str):
            other = EmailState.from_string(other)
        return (self.uidvalidity, self.uid, self.modseq) > \
               (other.uidvalidity, other.uid, other.modseq)

    def __le__(self, other):
        if isinstance(other, str):
            other = EmailState.from_string(other)
        return (self.uidvalidity, self.uid, self.modseq) <= \
               (other.uidvalidity, other.uid, other.modseq)

    def __str__(self):
        return f"{self.uidvalidity},{self.uid},{self.modseq}"


def parse_email_id(self, id):
    uidvalidity, uid = id.split('-')


# def format_email_id(mailboxid, uidvalidity, uid):
#     "creates message id from components"
#     return f'{mailboxid}_{uidvalidity}_{uid}'
# def parse_email_id(messageid):
#     "parses given messageid to components"
#     mailboxid, uidvalidity, uid = messageid.split('_')
#     return mailboxid, int(uidvalidity), int(uid)

# def format_email_id(mailboxid, uidvalidity, uid):
#     return b2a_base64(
#         bytes.fromhex(mailboxid) +
#         uidvalidity.to_bytes(4, 'big') + 
#         uid.to_bytes(4, 'big'),
#         newline=False
#     ).replace(b'+', b'-').replace(b'/', b'_').decode()
# def parse_email_id(messageid):
#     b = a2b_base64(messageid.encode().replace(b'-', b'+').replace(b'_', b'/'))
#     return b[:16].hex(), \
#            int.from_bytes(b[16:20], 'big'), \
#            int.from_bytes(b[20:24], 'big')


# ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz-_.~"
ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+_.~"
def base_encode(n: int, b: int = 64, a=ALPHABET):
    if not n:
        return a[0]
    s = ''
    dm = divmod  # Access to locals is faster.
    while n:
        n, r = dm(n, b)
        s = a[r] + s
    return s

ALPHABET_DICT = {c: v for v, c in enumerate(ALPHABET)}
def base_decode(s: str, b: int = 64, d=ALPHABET_DICT):
    n = 0
    for c in s:
        n = n * b + d[c]
    return n
