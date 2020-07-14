from datetime import datetime
import email
from email.header import decode_header, make_header
from email.message import EmailMessage
from email.policy import default
from email.utils import format_datetime, getaddresses, parseaddr, parsedate_to_datetime
from email._parseaddr import AddressList
import hashlib
import re


MEDIA_MAIN_TYPES = ('image', 'audio', 'video')


def asAddresses(raw):
    return raw and [{'name': asText(n), 'email': e} for n, e in AddressList(raw).addresslist]

def asGroupedAddresses(raw):
    # TODO
    return raw and [{'name': asText(n), 'email': e} for n, e in AddressList(raw).addresslist]

def asMessageIds(raw):
    return raw and [v.strip('<>') for v in asCommaList(raw)]

def asCommaList(raw):
    return raw and re.split(r'\s*,\s*', raw.strip())

def asDate(raw):
    return raw and parsedate_to_datetime(raw)

def asURLs(raw):
    return raw and re.split(r'>?\s*,?\s*<?', raw.strip())

def asOneURL(raw):
    return raw and raw.strip("<>")

def asRaw(raw):
    return raw

def asText(raw):
    return raw and str(make_header(decode_header(raw))).strip()



def parse(rfc822, id=None):
    if id is None:
        id = hashlib.sha1(rfc822).hexdigest()
    eml = email.message_from_bytes(rfc822, policy=default)
    res = parse_email(id, eml)
    res['id'] = id
    res['size'] = len(rfc822)
    return res


def parse_email(id, eml, part=None):
    bodyStructure, bodyValues = bodystructure(id, eml)
    textBody, htmlBody, attachments = parseStructure([bodyStructure], 'mixed', False)
    subject = str(make_header(decode_header(eml['Subject'])))
    return {
        'from': hdrAsAddresses(eml['From']),
        'to': hdrAsAddresses(eml['To']),
        'cc': hdrAsAddresses(eml['Cc']),
        'bcc': hdrAsAddresses(eml['Bcc']),
        'replyTo': hdrAsAddresses(eml['Reply-To']),
        'subject': subject,
        'date': asDate(eml['Date']),
        'preview': preview(bodyValues),
        'hasAttachment': len(attachments),
        'headers': headers(eml),
        'bodyStructure': bodyStructure,
        'bodyValues': bodyValues,
        'textBody': textBody,
        'htmlBody': htmlBody,
        'attachments': attachments,
    }


def bodystructure(id, eml, partno=None):
    hdrs = headers(eml)
    typ = eml.get_content_type().lower()
    bodyValues = {}

    if eml.is_multipart():
        subParts = []
        for n, part in enumerate(eml.iter_parts()):
            subBodyValues, part = bodystructure(id, part, f"{partno}.{n}" if partno else str(n))
            subParts.append(part)
            if subBodyValues:
                bodyValues.update(subBodyValues)
        return bodyValues, {
            'partId': None,
            'blobId': None,
            'type': typ,
            'size': 0,
            'headers': hdrs,
            'name': None,
            'cid': None,
            'disposition': 'none',
            'subParts': subParts,
        }

    partno = partno or '1'
    body = eml.get_content()
    if typ.startswith('text/'):
        bodyValues[partno] = {'value': body, 'type': typ}
    return bodyValues, {
        'partId': partno,
        'blobId': f"{id}-{partno}",
        'type': typ,
        'size': len(body),
        'headers': hdrs,
        'name': eml.get_filename(),
        'cid': asOneURL(eml['Content-ID']),
        'language': asCommaList(eml['Content-Language']),
        'location': hdrAsText(eml['Content-Location']),
        'disposition': eml.get_content_disposition() or 'none',
    }


def parseStructure(parts, multipartType, inAlternative):
    textBody = []
    htmlBody = []
    attachments = []

    for i, part in enumerate(parts):
        maintype, subtype = part['type'].split('/', maxsplit=1)
        isInline = part['disposition'] != 'attachment' and \
            (part['type'] in ('text/plain', 'text/html') or maintype in MEDIA_MAIN_TYPES) and \
            (i == 0 or (multipartType != 'related' and (maintype in MEDIA_MAIN_TYPES or not part['name'])))
        if maintype == 'multipart':
            textBody2, htmlBody2, attachments2 = \
                parseStructure(part['subParts'], subtype, inAlternative or (subtype == 'alternative'))
            textBody.extend(textBody2)
            htmlBody.extend(htmlBody2)
            attachments.extend(attachments2)
        elif isInline:
            if multipartType == 'alternative':
                if part['type'] == 'text/plain':
                    textBody.append(part)
                elif part['type'] == 'text/html':
                    htmlBody.append(part)
                else:
                    attachments.append(part)
                continue
            elif inAlternative:
                if part['type'] == 'text/plain':
                    textBody = None
                elif part['type'] == 'text/html':
                    htmlBody = None
            if textBody:
                textBody.append(part)
            if htmlBody:
                htmlBody.append(part)
            if (not textBody or not htmlBody) and maintype in MEDIA_MAIN_TYPES:
                attachments.append(part)
        else:
            attachments.append(part)
    
    if multipartType == 'alternative' and textBody and htmlBody:
        if not textBody and htmlBody:
            textBody.extend(htmlBody)
        if textBody and not htmlBody:
            htmlBody.extend(textBody)
    
    return textBody, htmlBody,attachments


def hdrAsAddresses(hdr):
    return hdr and [{
        'name': str(a.display_name),
        'email': f"{a.username}@{a.domain}",
    } for a in hdr.addresses]

def hdrAsText(hdr):
    return hdr and str(make_header(decode_header(hdr))).strip()

def headers(eml):
    return [{'name': k, 'value': v} for k, v in eml.items()]

def preview(bodyValues):
    for part in bodyValues.values():
        if part['type'] == 'text/plain':
            return part['value'][256:]
    for part in bodyValues.values():
        if part['type'] == 'text/html':
            return htmltotext(part['value'])[256:]
    return None
    

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


def normal_subject(subject):
    # Re: and friends
    subject = re.sub(r'^[ \t]*[A-Za-z0-9]+:', subject, '')
    # [LISTNAME] and friends
    sub = re.sub(r'^[ \t]*\\[[^]]+\\]', subject, '')
    # any old whitespace
    sub = re.sub(r'[ \t\r\n]+', subject, '')
