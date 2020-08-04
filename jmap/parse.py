from email.header import decode_header, make_header
from email.message import EmailMessage
from email.utils import format_datetime, parsedate_to_datetime
from io import BytesIO
import lxml
from email._parseaddr import AddressList
import re

MEDIA_MAIN_TYPES = {'image', 'audio', 'video'}


def asAddresses(raw):
    return raw and [{'name': asText(n), 'email': e} for n, e in AddressList(raw).addresslist]

def asGroupedAddresses(raw):
    # TODO
    return raw and [{'name': asText(n), 'email': e} for n, e in AddressList(raw).addresslist]

messageid_re = re.compile(r'<("[^>]+?"|[^>]+?)>')
def asMessageIds(raw):
    return raw and messageid_re.findall(raw)

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


def bodystructure(id, eml, partno=None):
    hdrs = [{'name': k, 'value': v} for k, v in eml.items()]
    typ = eml.get_content_type().lower()
    bodyValues = {}

    if eml.is_multipart():
        subParts = []
        for n, part in enumerate(eml.iter_parts()):
            subBodyValues, part = bodystructure(id, part, f"{partno}.{n}" if partno else str(n))
            bodyValues.update(subBodyValues)
            subParts.append(part)
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
    if typ in ('text/plain', 'text/html'):
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
        'location': asText(eml['Content-Location']),
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


def htmltotext(html):
    doc = lxml.etree.html.fromstring(html)
    doc = lxml.etree.html.clean_html(doc)
    return doc.text_content()

def htmlpreview(html, maxlen=256, tags=('title','p','div','span','a','li','b','strong','code')):
    preview = ''
    if hasattr(html, 'encode'):
        html = html.encode()
    for e, elem in lxml.etree.iterparse(BytesIO(html), events=("end",), tag=tags, no_network=True, remove_blank_text=True, remove_comments=True, remove_pis=True, html=True):
        if elem.tag in tags:
            text = elem.text.strip()
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
    if att['cid']:
        msg.add_header('Content-ID', "<" + att['cid'] + ">")
    typ, content = blobs[att['blobId']]
    msg.add_header('Content-Transfer-Encoding', detect_encoding(content, att['type']))
    msg.set_payload(content)
    return msg


def make(data, blobs):
    msg = EmailMessage()
    msg['From'] = ', '.join(format_email_header(a) for a in data['from'])
    msg['To'] = ', '.join(format_email_header(a) for a in data['to'])
    msg['Cc'] = ', '.join(format_email_header(a) for a in data['cc'])
    msg['Bcc'] = ', '.join(format_email_header(a) for a in data['bcc'])
    msg['Subject'] = data['subject']
    msg['Date'] = format_datetime(data['msgdate'])
    for header, val in data['headers'].items():
        msg[header] = val
    if 'replyTo' in data:
        msg['replyTo'] = data['replyTo']

    if 'textBody' in data:
        text = data['textBody']
    else:
        text = htmltotext(data['htmlBody'])
    msg.add_header('Content-Type', 'text/plain')
    msg.set_content(text)

    if 'htmlBody' in data:
        htmlpart = EmailMessage()
        htmlpart.add_header('Content-Type', 'text/html')
        htmlpart.set_content(data['htmlBody'])
        msg.make_alternative()
        msg.add_alternative(htmlpart)

    for att in data.get('attachments', ()):
        msg.add_attachment(make_attachment(att, blobs))

    return msg.as_bytes()
