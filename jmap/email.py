import hashlib
import email
from email.header import decode_header, make_header
from email.message import EmailMessage
from email.utils import format_datetime, getaddresses, parsedate_to_datetime
from datetime import datetime
import re


def parse(rfc822, id=None):
    if id is None:
        id = hashlib.sha1(rfc822).hexdigest()
    eml = email.message_from_bytes(rfc822)
    res = parse_email(eml)
    res['id'] = id
    res['size'] = len(rfc822)
    return res


def parse_email(eml, part=None):
    values = {}
    bodyStructure = bodystructure(values, id, eml)
    textBody, htmlBody, attachments = parseStructure(eml)
    subject = str(make_header(decode_header(eml['Subject'])))
    return {
        'from': asAddresses(eml.get_all('From', [])),
        'to': asAddresses(eml.get_all('To', [])),
        'cc': asAddresses(eml.get_all('Cc', [])),
        'bcc': asAddresses(eml.get_all('Bcc', [])),
        'replyTo': asAddresses(eml.get_all('Reply-To', [])),
        'subject': subject,
        'date': asDate(eml['Date']),
        'preview': (textBody[0] or htmltotext(htmlBody[0])).strip()[:256],
        'hasAttachment': len(attachments),
        'headers': dict(eml),
        'bodyStructure': bodyStructure,
        'bodyValues': values,
        'textBody': textBody,
        'htmlBody': htmlBody,
        'attachments': attachments,
    }


def bodystructure(values, id, eml, partno=None):
    parts = []  # eml.get_content()
    typ = eml.get_content_type()
    # TODO ...


def parseStructure(eml):
    textBody = []
    htmlBody = []
    attachments = []
    for part in eml.walk():
        typ = part.get_content_type()
        if part.get_content_disposition() == 'attachment':
            attachments.append(part)
        elif typ == 'text/plain':
            payload = part.get_payload(decode=True)
            textBody.append(payload.decode())
        elif typ == 'text/html':
            payload = part.get_payload(decode=True)
            htmlBody.append(payload.decode())

    return textBody, htmlBody, attachments


def asDate(val):
    return parsedate_to_datetime(val)


def asAddresses(hdrs):
    return [{
        'name': str(make_header(decode_header(n))),
        'email': e,
    }
        for n,e in getaddresses(hdrs)]


def htmltotext(html):
    # TODO: remove html tags ...
    return html

def _mkone(a):
    if a['name']:
        return f"\"{a['name']}\" <{a['email']}>"
    else:
        return f"{a['email']}"

def _mkemail(aa):
    return ', '.join(_mkone(a) for a in aa)

def _detect_encoding(content, typ):
    if typ.startswith('message'):
        match = re.match(r'[^\x20-\x7f]')
        if match:
            return '8bit'
        return '7bit'
    elif type.startswith('text'):
        #XXX - also line lengths?
        match = re.match(r'[^\x20-\x7f]')
        if match:
            return 'quoted-printable'
        return '7bit'
    return 'base64'

def _makeatt(att, blobs):
    msg = EmailMessage()
    msg.add_header('Content-Type', att['type'], name=att['name'])
    msg.add_header('Content-Disposition',
        'inline' if att['isInline'] else 'attachment',
        filename=att['name'],
        )

    if att['cid']:
        msg.add_header('Content-ID', "<" + att['cid'] + ">")
    typ, content = blobs[att['blobId']]
    msg.add_header('Content-Transfer-Encoding', _detect_encoding(content, att['type']))
    msg.set_payload(content)
    return msg

def make(args, blobs):
    msg = EmailMessage()
    msg['From'] = _mkemail(args['from'])
    msg['To'] = _mkemail(args['to'])
    msg['Cc'] = _mkemail(args['cc'])
    msg['Bcc'] = _mkemail(args['bcc'])
    msg['Subject'] = args['subject']
    msg['Date'] = format_datetime(args['msgdate'])
    for header, val in args['headers'].items():
        msg[header] = val
    if 'replyTo' in args:
        msg['replyTo'] = args['replyTo']

    if 'textBody' in args:
        text = args['textBody']
    else:
        text = htmltotext(args['htmlBody'])
    msg.add_header('Content-Type', 'text/plain')
    msg.set_content(text)

    if 'htmlBody' in args:
        htmlpart = EmailMessage()
        htmlpart.add_header('Content-Type', 'text/html')
        htmlpart.set_content(args['htmlBody'])
        msg.make_alternative()
        msg.add_alternative(htmlpart)

    for att in args.get('attachments', ()):
        msg.add_attachment(_makeatt(att, blobs))
    
    return msg.as_string()
