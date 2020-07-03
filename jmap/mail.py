import jmap.email
import jmap.mailbox
import jmap.thread
import jmap.searchsnippet


capabilityValue = {}


def register_methods(api):
    jmap.email.register_methods(api)
    jmap.mailbox.register_methods(api)
    jmap.thread.register_methods(api)
    jmap.searchsnippet.register_methods(api)

