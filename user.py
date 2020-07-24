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
    @classmethod
    async def init(cls, username: str, password: str, loop=None) -> None:
        self = cls()
        self.username = username
        self.accounts = {
            username: await ImapAccount.init(username, password, loop=loop),
        }
        self.sessionState = '0'
        return self

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
            self.users[decoded] = await User.init(username, password)

        return AuthCredentials(["authenticated"]), self.users[decoded]
