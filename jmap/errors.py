class accountNotFound(Exception):
    pass

class anchorNotFound(Exception):
    pass

class cannotCalculateChanges(Exception):
    pass

class serverUnavailable(Exception):
    "Some internal server resource was temporarily unavailable. Attempting the same operation later (perhaps after a backoff with a random factor) may succeed."
    pass

class serverFail(Exception):
    "An unexpected or unknown error occurred during the processing of the call. A description property should provide more details about the error. The method call made no changes to the server’s state. Attempting the same operation again is expected to fail again. Contacting the service administrator is likely necessary to resolve this problem if it is persistent."
    pass

class serverPartialFail(Exception):
    "Some, but not all, expected changes described by the method occurred. The client MUST resynchronise impacted data to determine server state. Use of this error is strongly discouraged."
    pass

class unknownMethod(Exception):
    "The server does not recognise this method name."
    pass

class invalidArguments(Exception):
    "One of the arguments is of the wrong type or is otherwise invalid, or a required argument is missing."
    pass

class invalidResultReference(Exception):
    "The method used a result reference for one of its arguments (see Section 3.7), but this failed to resolve."
    pass

class forbidden(Exception):
    "The method and arguments are valid, but executing the method would violate an Access Control List (ACL) or other permissions policy."
    pass

class accountNotFound(Exception):
    "The accountId does not correspond to a valid account."
    pass

class accountNotSupportedByMethod(Exception):
    "The accountId given corresponds to a valid account, but the account does not support this method or data type."
    pass

class accountReadOnly(Exception):
    "This method modifies state, but the account is read-only (as returned on the corresponding Account object in the JMAP Session resource)."
    pass
