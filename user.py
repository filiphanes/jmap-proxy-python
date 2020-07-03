import binascii

from starlette.authentication import (
    AuthenticationBackend, AuthenticationError, BaseUser,
    UnauthenticatedUser, AuthCredentials
)

from jmap.account import ImapAccount


class User(BaseUser):
    """
    User is person with credentials.
    User can have access to multiple (shared) accounts.
    User has one personal account.
    """
    def __init__(self, username: str, password: str) -> None:
        self.username = username
        self.accounts = {
            username: ImapAccount(username, password),
        }

    @property
    def is_authenticated(self) -> bool:
        return True

    @property
    def display_name(self) -> str:
        return self.username


class BasicAuthBackend(AuthenticationBackend):
    def __init__(self):
        self.users = {}
    
    async def authenticate(self, request):
        if "Authorization" not in request.headers:
            return
        auth = request.headers["Authorization"]
        try:
            scheme, credentials = auth.split()
            if scheme.lower() != 'basic':
                return
            decoded = binascii.a2b_base64(credentials).decode("ascii")
        except (ValueError, UnicodeDecodeError, binascii.Error) as exc:
            raise AuthenticationError('Invalid basic auth credentials')

        if decoded not in self.users:
            username, _, password = decoded.partition(":")
            self.users[decoded] = User(username, password)

        return AuthCredentials(["authenticated"]), self.users[decoded]
