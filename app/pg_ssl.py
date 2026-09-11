"""Whether a Postgres connection uses TLS, decided by where it goes.

Every asyncpg pool used to pass ``ssl=ssl.create_default_context()``
unconditionally. That was right while every database was Neon, and it is wrong
on xhostd: an app's own Postgres is reached over the platform's private network
and does not speak TLS at all, so the handshake fails with "server does not
support SSL" and the pool never opens. (creteus hit exactly this on its first
xhostd boot.)

The rule: no TLS for the database the platform injected (DATABASE_HOST), for
loopback and private addresses, and for names that only resolve inside the
platform (no dot, or .internal / .local). TLS for everything else, which today
means Neon and db.xhostd.com's external endpoint. Unknown public hosts get TLS,
so a mistake fails loudly rather than sending credentials in the clear.
"""
import ipaddress
import os
import ssl
from urllib.parse import urlsplit

_INTERNAL_SUFFIXES = (".internal", ".local", ".localdomain")


def host_is_internal(host: str) -> bool:
    h = (host or "").strip().lower().strip("[]")
    if not h or h == "localhost":
        return True
    platform_db = (os.environ.get("DATABASE_HOST") or "").strip().lower()
    if platform_db and h == platform_db:
        return True
    try:
        ip = ipaddress.ip_address(h)
    except ValueError:
        return "." not in h or h.endswith(_INTERNAL_SUFFIXES)
    return ip.is_private or ip.is_loopback or ip.is_link_local


def asyncpg_ssl_for(dsn: str):
    """The value for asyncpg's ``ssl=``: False for internal hosts, a verifying context otherwise."""
    host = urlsplit((dsn or "").strip()).hostname or ""
    if host_is_internal(host):
        return False
    return ssl.create_default_context()
