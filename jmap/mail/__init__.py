from .email import Email
from .mailbox import Mailbox
from .thread import Thread
from .searchsnippet import SearchSnippet

class Mail(Email, Mailbox, Thread, SearchSnippet):
    capabilityValue = {}
