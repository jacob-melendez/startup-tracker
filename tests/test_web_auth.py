"""``web.auth`` — the one shared password that guards a deployed instance (SPEC §1, §12 Phase 9).

Tested against a bare Starlette app rather than through :func:`web.app.create_app`, because
``create_app`` only switches the middleware on for the production instance (the one that builds
its own session factory). That is deliberate — it is what stops a developer's own ``.env``
turning authentication on inside the suite and 401ing every web test — and it means the honest
place to exercise the middleware is directly.

What these protect is not an abstraction: deployed, this app serves the user's private tracking
notes and a browsable directory of several hundred email addresses that companies published for
hiring contact. SPEC §1 rules out accounts, so this one secret is the whole of the door.
"""

from __future__ import annotations

import base64

import httpx
import pytest
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route

from web.auth import BasicAuthMiddleware

PASSWORD = "correct-horse-battery"


def build(password: str = PASSWORD) -> Starlette:
    """A two-route app behind the middleware: one guarded page, one health endpoint."""

    async def page(request: object) -> PlainTextResponse:
        return PlainTextResponse("the private list")

    async def healthz(request: object) -> PlainTextResponse:
        return PlainTextResponse("ok")

    app = Starlette(routes=[Route("/", page), Route("/healthz", healthz)])
    app.add_middleware(BasicAuthMiddleware, password=password)
    return app


def header(password: str, user: str = "anything") -> dict[str, str]:
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


async def get(app: Starlette, path: str = "/", **kwargs: object) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        return await client.get(path, **kwargs)  # type: ignore[arg-type]


async def test_the_right_password_is_let_through() -> None:
    response = await get(build(), headers=header(PASSWORD))
    assert response.status_code == 200
    assert response.text == "the private list"


async def test_no_credentials_are_refused_with_a_prompt() -> None:
    """A browser needs the challenge header to offer its own password dialog; without it the
    reader gets a bare error page and no way to type anything."""
    response = await get(build())
    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"].startswith("Basic realm=")


@pytest.mark.parametrize(
    ("label", "headers"),
    [
        ("wrong password", header("wrong-horse-battery")),
        ("right password, no colon", {"Authorization": "Basic " + base64.b64encode(b"x").decode()}),
        ("not base64 at all", {"Authorization": "Basic not-base-64!!"}),
        ("bearer token", {"Authorization": "Bearer " + PASSWORD}),
        ("scheme only", {"Authorization": "Basic"}),
        ("empty header", {"Authorization": ""}),
        # The password in the *username* field, which is where a reader who misread the dialog
        # would put it. It must not work: the username is ignored, not treated as a second key.
        ("password as username", header("", user=PASSWORD)),
    ],
)
async def test_everything_that_is_not_the_password_is_refused(
    label: str, headers: dict[str, str]
) -> None:
    """Every malformed shape fails the same way, on purpose. Distinguishing "wrong shape" from
    "wrong password" in the response would tell someone probing it which half to work on."""
    response = await get(build(), headers=headers)
    assert response.status_code == 401, label


async def test_the_username_is_ignored() -> None:
    """A browser's Basic dialog always asks for both, so demanding a particular username would
    be a second secret that protects nothing extra. Documented in `web.auth`; pinned here."""
    for user in ("", "jacob", "admin", "🙂"):
        assert (await get(build(), headers=header(PASSWORD, user=user))).status_code == 200


async def test_the_health_endpoint_answers_without_the_password() -> None:
    """Railway's health check cannot carry credentials, and a 401 there reads to the platform as
    a dead container — it would roll the deployment back on a *working* app. Safe to exempt:
    ``/healthz`` returns a constant and touches no table (`web.app.create_app`)."""
    response = await get(build(), "/healthz")
    assert response.status_code == 200
    assert "the private list" not in response.text


async def test_a_password_is_compared_in_full_not_by_prefix() -> None:
    """``secrets.compare_digest``, not ``==``: a short-circuiting comparison leaks the length of
    the shared prefix through its timing, which is enough to recover the secret character by
    character over enough requests — remote-feasible against exactly the public URL this exists
    for. Timing cannot be asserted reliably in a test, so what is pinned is the behaviour that
    would be wrong if someone swapped the call: a prefix of the password is not the password."""
    for attempt in (PASSWORD[:-1], PASSWORD[:5], PASSWORD + "x", ""):
        assert (await get(build(), headers=header(attempt))).status_code == 401, attempt
