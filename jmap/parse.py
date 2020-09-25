from datetime import datetime
from email.header import decode_header, make_header
from email.message import EmailMessage
from email.utils import format_datetime, parsedate_to_datetime
from io import BytesIO
from random import randrange

import lxml
from email._parseaddr import AddressList
import re

MEDIA_MAIN_TYPES = {'image', 'audio', 'video'}


def asAddresses(raw):
    if raw:
        return [{'name': asText(n) or None, 'email': e} for n, e in AddressList(raw.strip()).addresslist]
    return None

def asGroupedAddresses(raw):
    # TODO
    if raw:
        return [{'name': asText(n), 'email': e} for n, e in AddressList(raw).addresslist]
    return None

messageid_re = re.compile(r'<"?([^>]+?|[^>"]+?)"?>')
def asMessageIds(raw):
    if raw:
        return messageid_re.findall(raw)
    return None

def asCommaList(raw):
    if raw:
        return re.split(r'\s*,\s*', raw.strip())
    return None

def asDate(raw):
    if raw:
        try:
            return parsedate_to_datetime(raw)
        except Exception:
            return None
    return None

def asURLs(raw):
    if raw:
        return messageid_re.findall(raw)
    return None

def asOneURL(raw):
    return raw and raw.strip(" <>")

def asRaw(raw):
    return raw

def asText(raw):
    return raw and str(make_header(decode_header(raw))).strip()


def bodystructure(blobId, part, partno=None):
    hdrs = [{'name': k, 'value': v} for k, v in part.items()]
    typ = part.get_content_type().lower()
    bodyValues = {}

    if typ.startswith('multipart/'):
        subparts = []
        for n, subpart in enumerate(part.iter_parts(), 1):
            subBodyValues, subpart = bodystructure(id, subpart, f"{partno}-{n}" if partno else f"{n}")
            bodyValues.update(subBodyValues)
            subparts.append(subpart)
        return bodyValues, {
            'partId': None,
            'blobId': None,
            'type': typ,
            'size': 0,
            'headers': hdrs,
            'name': None,
            'cid': None,
            'disposition': None,
            'subParts': subparts,
        }

    partno = partno or '1'
    body = part.get_content()
    if typ in ('text/plain', 'text/html'):
        bodyValues[partno] = {'value': body, 'type': typ}
    return bodyValues, {
        'partId': partno,
        'blobId': f"{blobId}-{partno}",
        'type': typ,
        'size': len(body),
        'headers': hdrs,
        'name': part.get_filename(),
        'cid': asOneURL(part['Content-ID']),
        'language': asCommaList(part['Content-Language']),
        'location': asText(part['Content-Location']),
        'disposition': part.get_content_disposition() or 'none',
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
    
    return textBody, htmlBody, attachments


def htmltotext(html):
    doc = lxml.etree.html.fromstring(html)
    doc = lxml.etree.html.clean_html(doc)
    return doc.text_content()

def htmlpreview(html, maxlen=256, tags=('title','p','div','span','a','td','li','b','strong','code')):
    preview = ''
    if hasattr(html, 'encode'):
        html = html.encode()
    for e, elem in lxml.etree.iterparse(BytesIO(html), events=("end",), tag=tags, no_network=True, remove_blank_text=True, remove_comments=True, remove_pis=True, html=True):
        if elem.tag in tags:
            text = elem.text.strip()
            # TODO: elem.tail?
            if text:
                preview += text + ' '
                if len(preview) > maxlen:
                    break
    return preview[:maxlen]


def format_email_header(a):
    if a['name']:
        return f"\"{a['name']}\" <{a['email']}>"
    else:
        return f"{a['email']}"


def detect_encoding(content, typ):
    if typ.startswith('message'):
        match = re.match(rb'[^\x20-\x7f]', content)
        if match:
            return '8bit'
        return '7bit'
    elif typ.startswith('text'):
        #XXX - also line lengths?
        match = re.match(rb'[^\x20-\x7f]', content)
        if match:
            return 'quoted-printable'
        return '7bit'
    return 'base64'


def make_attachment(att, blobs):
    msg = EmailMessage()
    msg.add_header('Content-Type', att['type'], name=att['name'])
    msg.add_header('Content-Disposition',
        'inline' if att['isInline'] else 'attachment',
        filename=att['name'],
        )
    if att.get('cid', False):
        msg.add_header('Content-ID', "<" + att['cid'] + ">")
    typ, content = blobs[att['blobId']]
    msg.add_header('Content-Transfer-Encoding', detect_encoding(content, att['type']))
    msg.set_payload(content)
    return msg


def make(data, blobs):
    msg = EmailMessage()
    msg.add_header('Content-Type', 'text/plain')
    rand_id = f"{randrange(2**64)}"
    if 'textBody' in data:
        partId = data['textBody'][0]['partId']
        msg.set_content(data['bodyValues'][partId]['value'])

    if 'htmlBody' in data:
        partId = data['htmlBody'][0]['partId']
        html = data['bodyValues'][partId]['value']
        if 'textBody' not in data:
            msg.set_content(htmltotext(html))
        msg.make_alternative(boundary=rand_id)
        msg.add_alternative(html, subtype='html')

    for att in data.get('attachments', ()):
        msg.add_attachment(make_attachment(att, blobs))

    msg.add_header('Date', format_datetime(data.get('msgdate', datetime.now())))
    if 'subject' in data:
        msg.add_header('Subject', data['subject'])
    if 'messageId' in data:
        msg.add_header('Message-ID', f"<{data.get['messageId']}>")
    else:
        msg.add_header('Message-ID', f"<{rand_id}@example.com>")
    for addr in ('from', 'to', 'cc', 'bcc'):
        if addr in data:
            msg.add_header(addr.capitalize(), ', '.join(format_email_header(a) for a in data[addr]))
    if 'replyTo' in data:
        msg.add_header('Reply-To', ', '.join(format_email_header(a) for a in data[addr]))
    for header, val in data.items():
        if header.startswith('header:'):
            msg[header[7:]] = val

    return msg.as_bytes()
