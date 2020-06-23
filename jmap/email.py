import hashlib
import email
from email.header import decode_header, make_header
from email.utils import getaddresses, parsedate_to_datetime
from datetime import datetime


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
