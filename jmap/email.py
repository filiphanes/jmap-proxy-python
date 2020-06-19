import hashlib
import email
from datetime import datetime


def parse(rfc822, id=None):
    if id is None:
        id = hashlib.sha1(rfc822).digest()
    eml = email.message_from_bytes(rfc822)
    res = parse_email(eml)
    res['id'] = id
    res['size'] = len(rfc822)
    return res


def parse_email(eml, part=None):
    values = {}
    bodyStructure = bodystructure(values, id, eml)
    textBody, htmlBody, attachments = parseStructure(eml)
    return {
        'from': asAddresses(eml['From']),
        'to': asAddresses(eml['To']),
        'cc': asAddresses(eml['Cc']),
        'bcc': asAddresses(eml['Bcc']),
        'replyTo': asAddresses(eml['Reply-To']),
        'subject': eml['Subject'],
        'date': asDate(eml['Date']),
        'preview': (textBody or htmltotext(htmlBody)).strip()[:256],
        'hasAttachment': bool(attachments),
        'headers': dict(eml),
        'bodyStructure': bodyStructure,
        'bodyValues': values,
        'textBody': textBody,
        'htmlBody': eml.get_bod,
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
        if part.is_attachment():
            attachments.append(part)
        elif typ == 'text/plain':
            textBody.append(part.get_content())
        elif typ == 'text/html':
            htmlBody.append(part.get_content())

    return textBody, htmlBody, attachments


def asDate(val):
    return datetime.fromisoformat(val)


def asAddresses(hdr):
    return [{
        'name': a.display_name,
        'email': a.username + '@' + a.domain,
    }
        for a in hdr.addresses]


def htmltotext(html):
    # TODO: remove html tags ...
    return html
