from datetime import datetime
import email
from email.header import decode_header, make_header
from email.message import EmailMessage
from email.policy import default
from email.utils import format_datetime, getaddresses, parsedate_to_datetime
import hashlib
import re


MEDIA_MAIN_TYPES = ('image', 'audio', 'video')


def parse(rfc822, id=None):
    if id is None:
        id = hashlib.sha1(rfc822).hexdigest()
    eml = email.message_from_bytes(rfc822, policy=default)
    res = parse_email(id, eml)
    res['id'] = id
    res['size'] = len(rfc822)
    return res


def parse_email(id, eml, part=None):
    bodyValues = {}
    bodyStructure = bodystructure(bodyValues, id, eml)
    textBody = []
    htmlBody = []
    attachments = []
    parseStructure([bodyStructure], 'mixed', False, textBody, htmlBody, attachments)
    subject = str(make_header(decode_header(eml['Subject'])))
    return {
        'from': asAddresses(eml['From']),
        'to': asAddresses(eml['To']),
        'cc': asAddresses(eml['Cc']),
        'bcc': asAddresses(eml['Bcc']),
        'replyTo': asAddresses(eml['Reply-To']),
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


def bodystructure(bodyValues, id, eml, partno=None):
    hdrs = headers(eml)
    typ = eml.get_content_type().lower()

    if eml.is_multipart():
        subParts = []
        for n, part in enumerate(eml.iter_parts()):
            subParts.append(bodystructure(bodyValues, id, part, f"{partno}.{n}" if partno else str(n)))
        return {
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
    return {
        'partId': partno,
        'blobId': f"m-{id}-{partno}",
        'type': typ,
        'size': len(body),
        'headers': hdrs,
        'name': eml.get_filename(),
        'cid': asOneURL(eml['Content-ID']),
        'language': asCommaList(eml['Content-Language']),
        'location': asText(eml['Content-Location']),
        'disposition': eml.get_content_disposition() or 'none',
    }


def parseStructure(parts, multipartType, inAlternative, textBody, htmlBody, attachments):
    textLen = len(textBody) or -1
    htmlLen = len(htmlBody) or -1

    for i, part in enumerate(parts):
        maintype, subtype = part['type'].split('/', maxsplit=1)
        isInline = part['disposition'] != 'attachment' and \
            (part['type'] in ('text/plain', 'text/html') or maintype in MEDIA_MAIN_TYPES) and \
            (i == 0 or (multipartType != 'related' and (maintype in MEDIA_MAIN_TYPES or not part['name'])))
        if maintype == 'multipart':
            parseStructure(part['subParts'], subtype, inAlternative or (subtype == 'alternative'), htmlBody, textBody, attachments)
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
        if textLen == len(textBody) and htmlLen != len(htmlBody):
            textBody.extend(htmlBody)
        if textLen != len(textBody) and htmlLen == len(htmlBody):
            htmlBody.extend(textBody)

def asMessageIds(val):
    return val and [v.strip('<>') for v in asCommaList(val)]

def asCommaList(val):
    return val and re.split(r'\s*,\s*', val.strip())

def asDate(val):
    return parsedate_to_datetime(val)

def asURLs(val):
    return val and re.split(r'>?\s*,?\s*<?', val.strip())

def asOneURL(val):
    return val and val.strip("<>")

def asText(val):
    return val and str(make_header(decode_header(val))).strip()    

def asAddresses(hdr):
    return hdr and [{
        'name': str(a.display_name),
        'email': f"{a.username}@{a.domain}",
    } for a in hdr.addresses]

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
