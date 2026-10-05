"""One shared password over HTTP Basic, for when this runs somewhere with a public URL
(SPEC §1, §12 Phase 9).

SPEC §1 rules out user accounts, and this does not add any: there is one secret, it lives in
``WEB_PASSWORD``, and every request either carries it or is refused. That is the whole of the
access control, which is the right size for a tool with one reader — but it is also why the
secret has to be a real one (:data:`settings.MIN_WEB_PASSWORD_LENGTH`).

**Why it is needed at all.** Run on loopback the app is as private as the machine. Give it a
public URL and it becomes two things it was never meant to be: the user's own tracking notes,
ratings and bookmarks, readable and writable by anyone who finds the host; and a browsable
directory of several hundred email addresses that companies published for hiring contact. The
first is private data. The second is other people's — republished in bulk to the open web is
not the use those addresses were put up for, and SPEC §4's whole posture toward sources is that
this project takes what it is offered on the terms it is offered on.

**Unset means off**, and that is deliberate rather than lax: the local run is bound to
127.0.0.1 by its launchd agent and asking for a password there would be friction protecting
nothing. The deployment is what sets it, and the README's Railway section makes it a required
variable rather than an optional one.
"""

from __future__ import annotations

import base64
import secrets
from collections.abc import Awaitable, Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response
from starlette.status import HTTP_401_UNAUTHORIZED
from starlette.types import ASGIApp

#: Paths that answer before the password is checked. Only the health endpoint: a platform
#: health check cannot carry credentials, and a 401 there reads to Railway as a dead container
#: and takes the deployment down. It discloses nothing — ``{"status": "ok"}`` and no database
#: round trip (see :func:`web.app.create_app`).
PUBLIC_PATHS = frozenset({"/healthz"})

#: Sent on a refusal so a browser offers its own password prompt rather than showing a bare 401.
#: The realm names the app and nothing about the deployment.
_CHALLENGE = 'Basic realm="startup-tracker", charset="UTF-8"'


class BasicAuthMiddleware(BaseHTTPMiddleware):
    """Refuse every request that does not carry ``password`` over HTTP Basic.

    The username is ignored. A browser's Basic dialog always asks for both, so demanding a
    particular one would be a second secret to remember that protects nothing extra — type
    anything.

    The comparison is :func:`secrets.compare_digest`, not ``==``. A short-circuiting comparison
    leaks the length of the shared prefix through its timing, which over enough requests is
    enough to recover the secret character by character. That attack is remote-feasible against
    a public URL, which is the only situation this class is ever switched on in.
    """

    def __init__(self, app: ASGIApp, *, password: str) -> None:
        super().__init__(app)
        self._password = password

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        if request.url.path in PUBLIC_PATHS or self._authorized(request):
            return await call_next(request)
        return PlainTextResponse(
            "Not authorized.",
            status_code=HTTP_401_UNAUTHORIZED,
            headers={"WWW-Authenticate": _CHALLENGE},
        )

    def _authorized(self, request: Request) -> bool:
        """Whether this request's ``Authorization`` header carries the password.

        Everything that is not a well-formed Basic header is simply unauthorized — a malformed
        one is not worth a different status, and distinguishing "wrong shape" from "wrong
        password" in the response would tell an attacker which half to work on.
        """
        header = request.headers.get("Authorization", "")
        scheme, _, encoded = header.partition(" ")
        if scheme.lower() != "basic" or not encoded:
            return False
        try:
            # `validate=True`: a base64 string with junk in it is a malformed header, not a
            # password that happens to decode to something. Latin-1 is what RFC 7617 specifies
            # for the legacy case and never raises, so a weird byte is a failed comparison
            # rather than a 500.
            decoded = base64.b64decode(encoded, validate=True).decode("latin-1")
        except (ValueError, UnicodeDecodeError):
            return False
        _user, separator, password = decoded.partition(":")
        if not separator:
            return False
        return secrets.compare_digest(password, self._password)
