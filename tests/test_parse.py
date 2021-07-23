import email
from email.policy import default
import datetime

from jmap.parse import asAddresses, asMessageIds, asGroupedAddresses, asDate, asURLs, asRaw, asCommaList, bodystructure


def test_asAddresses():
    assert asAddresses(None) is None
    assert asAddresses('') is None
    assert asAddresses(' ') in (None, [])
    assert asAddresses('joe@example.com') == [{'name': None, 'email': 'joe@example.com'}]
    assert asAddresses('<joe@example.com>') == [{'name': None, 'email': 'joe@example.com'}]
    assert asAddresses('"Joe Doe" <joe@example.com>') == [{'name': 'Joe Doe', 'email': 'joe@example.com'}]
    assert asAddresses('  Joe   Doe   <joe@example.com>') == [{'name': 'Joe Doe', 'email': 'joe@example.com'}]
    assert asAddresses('"Joe Doe"   <joe@example.com>') == [{'name': 'Joe Doe', 'email': 'joe@example.com'}]
    assert asAddresses('杨孝宇 <xiaoyu@example.com>') == [{'name': '杨孝宇', 'email': 'xiaoyu@example.com'}]
    assert asAddresses('=?utf-8?q?Joe_Doe?= <joe@example.com>') == [{'name': 'Joe Doe', 'email': 'joe@example.com'}]
    assert asAddresses('"A B C" < a@b.c> , d@e') == [
        {'name': 'A B C', 'email': 'a@b.c'},
        {'name': None, 'email': 'd@e'},
    ]
    assert asAddresses('Brothers: abel@example.com, cain@example.com;') == [
        {'name': None, 'email': 'abel@example.com'},
        {'name': None, 'email': 'cain@example.com'},
    ]
    assert asAddresses('''"  James Smythe" <james@example.com>, Friends:
  jane@example.com, =?UTF-8?Q?John_Sm=C3=AEth?=
  <john@example.com>;''') == [
        {"name": "James Smythe", "email": "james@example.com"},
        {"name": None, "email": "jane@example.com"},
        {"name": "John Smîth", "email": "john@example.com"}
    ]


def test_asGroupedAddresses():
    assert asGroupedAddresses(None) is None
    assert asGroupedAddresses('') is None
    assert asGroupedAddresses(' ') in ([], None)
    assert asGroupedAddresses('''"  James Smythe" <james@example.com>, Friends:
  jane@example.com, =?UTF-8?Q?John_Sm=C3=AEth?=
  <john@example.com>;''') == [
        {"name": None, "addresses": [
            {"name": "James Smythe", "email": "james@example.com"}
        ]},
        {"name": "Friends", "addresses": [
            {"name": None, "email": "jane@example.com"},
            {"name": "John Smîth", "email": "john@example.com"}
        ]}
    ]


def test_asMessageIds():
    assert asMessageIds(None) is None
    assert asMessageIds('') is None
    assert asMessageIds(' ') in (None, [])
    assert asMessageIds('<msgid@example.com>') == ['msgid@example.com']
    assert asMessageIds('<msgid@example.com>,<msgid2@example.com>') == ['msgid@example.com', 'msgid2@example.com']
    assert asMessageIds(' <"msgid@example.com"> , <"msgid2@example.com">  ') == ['msgid@example.com', 'msgid2@example.com']
    # very rare: assert asMessageIds(' <"msgid"@example.com> , <"msgid2"@example.com>  ') == ['msgid@example.com', 'msgid2@example.com']


def test_asDate():
    assert asDate(None) is None
    assert asDate('') is None
    assert asDate(' ') is None
    assert asDate('Thu, 08 Sep 2020 17:31:45 +0200') == datetime.datetime(2020, 9, 8, 17, 31, 45, tzinfo=datetime.timezone(datetime.timedelta(seconds=7200)))


def test_asCommaList():
    assert asCommaList(None) is None
    assert asCommaList('') is None
    assert asCommaList(' ') == ['']
    assert asCommaList(' , ') == ['','']
    assert asCommaList(' hello, beautiful , world') == ['hello', 'beautiful', 'world']


def test_asURLs():
    assert asURLs(None) is None
    assert asURLs('') is None
    assert asURLs(' ') in (None, [])
    assert asURLs(' <mailto: unsubscribe@example.com?subject=unsubscribe>') == [
        'mailto: unsubscribe@example.com?subject=unsubscribe',
    ]
    assert asURLs('''<mailto:unsubscribe@example.com?subject=unsubscribe>,
        <http://www.example.com/unsubscribe.html>''') == [
        'mailto:unsubscribe@example.com?subject=unsubscribe',
        'http://www.example.com/unsubscribe.html',
    ]


def test_asRaw():
    assert asRaw(None) is None
    assert asRaw('') == ''
    assert asRaw(' ') == ' '
    assert asRaw(' <mailto: unsubscribe@example.com?subject=unsubscribe>') == ' <mailto: unsubscribe@example.com?subject=unsubscribe>'


def test_bodystructure():
    body = b''''''
    blobId = 'blobId'
    part = email.message_from_bytes(body, policy=default)
    bodyValues, bodyStructure = bodystructure(blobId, part)