class JmapError(Exception):
    "Base JMAP Exception"

    def to_dict(self):
        out = {'type': self.__class__.__name__}
        desc = str(self)
        if desc:
            out['description'] = desc
        return out

class accountNotFound(JmapError):
    "The accountId does not correspond to a valid account."

class fromAccountNotFound(JmapError):
    "The fromAccountId does not correspond to a valid account."

class fromAccountNotSupportedByMethod(JmapError):
    "The fromAccountId given corresponds to a valid account, but the account does not support this data type."

class anchorNotFound(JmapError):
    "An anchor argument was supplied, but it cannot be found in the results of the query."

class notJSON(JmapError):
    "The content type of the request was not application/json or the request did not parse as I-JSON."

class notRequest(JmapError):
    "The request parsed as JSON but did not match the type signature of the Request object."

class cannotCalculateChanges(JmapError):
    "The server cannot calculate the changes from the state string given by the client."

class serverUnavailable(JmapError):
    "Some internal server resource was temporarily unavailable. Attempting the same operation later (perhaps after a backoff with a random factor) may succeed."

class serverFail(JmapError):
    "An unexpected or unknown error occurred during the processing of the call. A description property should provide more details about the error. The method call made no changes to the server’s state. Attempting the same operation again is expected to fail again. Contacting the service administrator is likely necessary to resolve this problem if it is persistent."

class serverPartialFail(JmapError):
    "Some, but not all, expected changes described by the method occurred. The client MUST resynchronise impacted data to determine server state. Use of this error is strongly discouraged."

class invalidArguments(JmapError):
    "One of the arguments is of the wrong type or is otherwise invalid, or a required argument is missing."

class invalidResultReference(JmapError):
    "The method used a result reference for one of its arguments (see Section 3.7), but this failed to resolve."

class forbidden(JmapError):
    "The method and arguments are valid, but executing the method would violate an Access Control List (ACL) or other permissions policy."

class overQuota(JmapError):
    "The create would exceed a server-defined limit on the number or total size of objects of this type."

class tooLarge(JmapError):
    "The create/update would result in an object that exceeds a server-defined limit for the maximum size of a single object of this type."

class tooManyChanges(JmapError):
    "There are more changes than the client’s maxChanges argument."

class unknownCapability(JmapError):
    "The client included a capability in the “using” property of the request that the server does not support."

class unknownMethod(JmapError):
    "The server does not recognise this method name."

class unsupportedFilter(JmapError):
    "The filter is syntactically valid, but the server cannot process it."

class unsupportedSort(JmapError):
    "The sort is syntactically valid, but includes a property the server does not support sorting on, or a collation method it does not recognise."


class rateLimit(JmapError):
    "Too many objects of this type have been created recently, and a server-defined rate limit has been reached. It may work if tried again later."

class notFound(JmapError):
    "The id given cannot be found."

class invalidPatch(JmapError):
    "The PatchObject given to update the record was not a valid patch (see the patch description)."

class willDestroy(JmapError):
    "The client requested that an object be both updated and destroyed in the same /set request, and the server has decided to therefore ignore the update."

class invalidProperties(JmapError):
    """
    The record given is invalid in some way. For example:

    It contains properties that are invalid according to the type specification of this record type.
    It contains a property that may only be set by the server (e.g., “id”) and is different to the current value. Note, to allow clients to pass whole objects back, it is not an error to include a server-set property in an update as long as the value is identical to the current value on the server.
    There is a reference to another record (foreign key), and the given id does not correspond to a valid record.

    The SetError object SHOULD also have a property called properties of type String[] that lists all the properties that were invalid.

    Individual methods MAY specify more specific errors for certain conditions that would otherwise result in an invalidProperties error. If the condition of one of these is met, it MUST be returned instead of the invalidProperties error.
    """

class singleton(JmapError):
    "This is a singleton type, so you cannot create another one or destroy the existing one."

class accountNotFound(JmapError):
    "The accountId does not correspond to a valid account."

class accountNotSupportedByMethod(JmapError):
    "The accountId given corresponds to a valid account, but the account does not support this method or data type."

class accountReadOnly(JmapError):
    "This method modifies state, but the account is read-only (as returned on the corresponding Account object in the JMAP Session resource)."

class mailboxHasEmail(JmapError):
    "Mailbox has at least one Email assigned to it, and the onDestroyRemoveEmails argument was false."

class mailboxHasChild(JmapError):
    "Mailbox still has at least one child Mailbox. The client MUST remove these before it can delete the parent Mailbox."

class stateMismatch(JmapError):
    "An ifInState argument was supplied, and it does not match the current state."

class requestTooLarge(JmapError):
    "The total number of objects to create, update, or destroy exceeds the maximum number the server is willing to process in a single method call."

class blobNotFound(JmapError):
    "At least one blob id given for an EmailBodyPart doesn’t exist. An extra notFound property of type Id[] MUST be included in the SetError object containing every blobId referenced by an EmailBodyPart that could not be found on the server."

class tooManyKeywords(JmapError):
    "The change to the Email’s keywords would exceed a server-defined maximum."

class tooManyMailboxes(JmapError):
    "The change to the set of Mailboxes that this Email is in would exceed a server-defined maximum."

class invalidEmail(JmapError):
    "The Email to be sent is invalid in some way. The SetError SHOULD contain a property called properties of type String[] that lists all the properties of the Email that were invalid."

class tooManyRecipients(JmapError):
    "The envelope (supplied or generated) has more recipients than the server allows. A maxRecipients UnsignedInt property MUST also be present on the SetError specifying the maximum number of allowed recipients."

class noRecipients(JmapError):
    "The envelope (supplied or generated) does not have any rcptTo email addresses."

class invalidRecipients(JmapError):
    "The rcptTo property of the envelope (supplied or generated) contains at least one rcptTo value which is not a valid email address for sending to. An invalidRecipients String[] property MUST also be present on the SetError, which is a list of the invalid addresses."

class forbiddenMailFrom(JmapError):
    "The server does not permit the user to send a message with this envelope From address [@!RFC5321]."

class forbiddenFrom(JmapError):
    "The server does not permit the user to send a message with the From header field [@!RFC5322] of the message to be sent."

class forbiddenToSend(JmapError):
    "The user does not have permission to send at all right now for some reason. A description String property MAY be present on the SetError object to display to the user why they are not permitted."

class cannotUnsend(JmapError):
    "The client attempted to update the undoStatus of a valid EmailSubmission object from pending to canceled, but the message cannot be unsent."
