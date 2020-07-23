from binascii import a2b_base64, b2a_base64
import email
from email.policy import default
import re

from jmap.parse import asAddresses, asDate, asMessageIds, asText, bodystructure, htmltotext, make, parseStructure
from jmap import errors

KEYWORD2FLAG = {
    '$answered':'\\Answered',
    '$flagged': '\\Flagged',
    '$draft':   '\\Draft',
    '$seen':    '\\Seen',
}
FLAG2KEYWORD = {flag.lower(): kw for kw, flag in KEYWORD2FLAG.items()}

def keyword2flag(kw):
    return KEYWORD2FLAG.get(kw, None) or kw.encode()


FIELDS_MAP = {
    'blobId': 'X-GUID',  # Dovecot
    # 'blobId':       'EMAILID',  # OBJECTID imap extension
    'hasAttachment': 'FLAGS',
    # 'hasAttachment':'RFC822',  # when IMAP don't set $HasAttachment flag
    'keywords':     'FLAGS',
    'preview':      'PREVIEW',
    'receivedAt':   'INTERNALDATE',
    'size':         'RFC822.SIZE',
    'attachments':  'RFC822',
    'bodyStructure':'RFC822',
    'bodyValues':   'RFC822',
    'textBody':     'RFC822',
    'htmlBody':     'RFC822',
    'headers':      'RFC822.HEADER',
    'subject':      'RFC822.HEADER',
    'from':         'RFC822.HEADER',
    'to':           'RFC822.HEADER',
    'cc':           'RFC822.HEADER',
    'bcc':          'RFC822.HEADER',
    'replyTo':      'RFC822.HEADER',
    'inReplyTo':    'RFC822.HEADER',
    'sentAt':       'RFC822.HEADER',
    'references':   'RFC822.HEADER',
    'created':      'MODSEQ',
    'updated':      'MODSEQ',
}


class EmailState:
    __slots__ = ('uid', 'modseq')

    @classmethod
    def from_str(cls, state):
        uid, modseq = state.split(',')
        return cls(int(uid), int(modseq))

    def __init__(self, uid, modseq):
        self.uid = uid
        self.modseq = modseq

    def __gt__(self, other):
        if isinstance(other, str):
            other = EmailState.from_str(other)
        return (self.uid, self.modseq) > (other.uid, other.modseq)

    def __le__(self, other):
        if isinstance(other, str):
            other = EmailState.from_str(other)
        return (self.uid, self.modseq) <= (other.uid, other.modseq)

    def __str__(self):
        return f"{self.uid},{self.modseq}"


class ImapMessage(dict):
    header_re = re.compile(r'^([\w-]+)\s*:\s*(.+?)\r\n(?=[\w\r])', re.I | re.M | re.DOTALL)

    def __missing__(self, key):
        try:
            self[key] = getattr(self, key)()
            return self[key]
        except TypeError:
            raise KeyError

    def get_header(self, name: str):
        "Return raw value from last header instance, name needs to be lowercase."
        return self['LASTHEADERS'].get(name, None)

    def EML(self):
        return email.message_from_bytes(self['RFC822'], policy=default)

    def LASTHEADERS(self):
        # make headers dict with only last instance of each header
        # as required by JMAP spec for single header get
        return {name.lower(): raw
                for name, raw in self.header_re.findall(self['DECODEDHEADERS'])}

    def DECODEDHEADERS(self):
        try:
            return self.pop('RFC822.HEADER').decode()
        except KeyError:
            match = re.search(rb'\r\n\r\n', self['RFC822'])
            if match:
                return str(memoryview(self['RFC822'])[:match.end()])

    def blobId(self):
        return self['X-GUID']

    def hasAttachment(self):
        # Dovecot with mail_attachment_detection_options = add-flags-on-save
        return '$HasAttachment' in self['FLAGS']

    def headers(self):
        return [{'name': name, 'value': value}
                for name, value in self.header_re.findall(self['DECODEDHEADERS'])]

    def inReplyTo(self):
        return asMessageIds(self.get_header('in-reply-to'))

    def keywords(self):
        return {FLAG2KEYWORD.get(f.lower(), f): True for f in self['FLAGS']}

    def messageId(self):
        return asMessageIds(self.get_header('message-id'))

    def mailboxIds(self):
        return [parse_message_id(self['id'])]

    def preview(self):
        return str(self.pop('PREVIEW')[1])

    def receivedAt(self):
        return self.pop('INTERNALDATE')

    def references(self):
        return asMessageIds(self.get_header('references'))

    def replyTo(self):
        return asAddresses(self.get_header('reply-to'))

    def sentAt(self):
        return asDate(self.get_header('date'))

    def size(self):
        try:
            return self.pop('RFC822.SIZE')
        except KeyError:
            return len(self['RFC822'])

    def subject(self):
        return asText(self.get_header('subject')) or ''

    def threadId(self):
        # TODO: threading
        return f"t{self['id']}"

    def bodyStructure(self):
        self['bodyValues'], bodyStructure \
            = bodystructure(self['id'], self['EML'])
        return bodyStructure

    def bodyValues(self):
        bodyValues, self['bodystructure'] \
            = bodystructure(self['id'], self['EML'])
        return bodyValues

    def textBody(self):
        textBody, self['htmlBody'], self['attachments'] \
            = parseStructure([self['bodyStructure']], 'mixed', False)
        return textBody

    def htmlBody(self):
        self['textBody'], htmlBody, self['attachments'] \
            = parseStructure([self['bodyStructure']], 'mixed', False)
        return htmlBody

    def attachments(self):
        self['textBody'], self['htmlBody'], attachments \
            = parseStructure([self['bodyStructure']], 'mixed', False)
        return attachments

    # Used when creating message
    def getFLAGS(self):
        return [KEYWORD2FLAG.get(kw.lower(), kw.encode()) for kw in self['keywords']]

    def getRFC822(self):
        return make(self, {})

    def deleted(self):
        # True is set by instantiator
        return False

    def uid(self):
        return parse_message_id(self['id'])

    def created(self):
        "Get state when this message was created"
        return EmailState(self['uid'], 1 << 64)

    def updated(self):
        "Get state when this message was udpated"
        return EmailState(self['uid'], self['MODSEQ'])


# Define address getters
# "from" is python reserved keyword, others are similar
def address_getter(field):
    def get(self):
        return asAddresses(self.get_header(field))
    return get

for prop in ('from', 'to', 'cc', 'bcc', 'sender'):
    setattr(ImapMessage, prop, address_getter(prop))


def format_message_id(uid):
    return str(uid)
def parse_message_id(id):
    return int(id)

# def format_message_id(mailboxid, uidvalidity, uid):
#     "creates message id from components"
#     return f'{mailboxid}_{uidvalidity}_{uid}'
# def parse_message_id(messageid):
#     "parses given messageid to components"
#     mailboxid, uidvalidity, uid = messageid.split('_')
#     return mailboxid, int(uidvalidity), int(uid)

# def format_message_id(mailboxid, uidvalidity, uid):
#     return b2a_base64(
#         bytes.fromhex(mailboxid) +
#         uidvalidity.to_bytes(4, 'big') + 
#         uid.to_bytes(4, 'big'),
#         newline=False
#     ).replace(b'+', b'-').replace(b'/', b'_').decode()
# def parse_message_id(messageid):
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
