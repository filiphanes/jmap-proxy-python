import binascii

from starlette.authentication import AuthenticationBackend, AuthenticationError, BaseUser, AuthCredentials

from jmap import errors
from jmap.account import UserAccount


class User(BaseUser):
    """
    User is person with credentials.
    User can have access to multiple (shared) accounts.
    User has one personal account.
    """
    def __init__(self, username: str, password: str, loop=None) -> None:
        self.username = username
        self.accounts = {
            username: UserAccount(username, password, loop=loop),
        }
        self.sessionState = '0'

    async def ainit(self):
        for account in self.accounts.values():
            try:
                await account.ainit()
            except AttributeError:
                continue

    def get_account(self, accountId):
        try:
            return self.accounts[accountId]
        except KeyError:
            raise errors.accountNotFound()

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
            user = User(username, password)
            await user.ainit()
            self.users[decoded] = user

        return AuthCredentials(["authenticated"]), self.users[decoded]
