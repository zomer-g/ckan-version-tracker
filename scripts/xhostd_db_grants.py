"""Re-assert what the public console's role may and may not do, on every xhostd boot.

Two things the platform's defaults get wrong for OVER, both found by running
the checks on the channel database after the copy:

1. Functions. xhostd's default privileges create functions executable by the
   owner only, so the read-only role could not call any of OVER's 20 lookup
   functions (over_parcels_near, over_settlement, over_address_parcel, ...):
   "permission denied for function over_parcels_near". None is SECURITY
   DEFINER, so granting EXECUTE gives the role nothing beyond its own SELECTs.

2. Hidden tables. The loader revoked SELECT on public.over_re_geocode and
   verified it; two deploys later the role held a direct grant again. Something
   re-grants all of `public` to the read-only role, most likely the platform.
   Hiding it is a curation decision (data_catalog._OVER_HIDDEN), and a revoke
   that does not survive a deploy has to be re-asserted at boot, the same way
   geocode_queue.ensure_tables() re-asserts it on Neon.

Runs only when the app and the archive share the channel database. Never fails
the boot: it logs what it did and what it could not.
"""
import asyncio
import os
import sys
import time
from urllib.parse import urlsplit

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

FUNCTION_SCHEMAS = ("public", "extensions", "idx", "knesset", "ocal", "ocoi", "odata")


def log(msg: str) -> None:
    print(f"[grants] {time.strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def _qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def statements(ro_role: str, present_schemas: set[str], hidden_tables: list[str]) -> list[str]:
    role = _qi(ro_role)
    out = []
    for s in FUNCTION_SCHEMAS:
        if s in present_schemas:
            out.append(f"GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {_qi(s)} TO {role}")
            out.append(f"ALTER DEFAULT PRIVILEGES IN SCHEMA {_qi(s)} GRANT EXECUTE ON FUNCTIONS TO {role}")
    for t in hidden_tables:
        out.append(f"REVOKE ALL ON public.{_qi(t)} FROM {role}")
    return out


async def main() -> None:
    import asyncpg

    from app.pg_ssl import asyncpg_ssl_for
    from app.services.data_catalog import _OVER_HIDDEN

    db = os.environ.get("DATABASE_URL", "").replace("postgresql+asyncpg://", "postgresql://")
    ro_role = urlsplit(os.environ.get("DATABASE_URL_READONLY", "")).username or ""
    if not db or not ro_role:
        log("DATABASE_URL or DATABASE_URL_READONLY missing; nothing to do")
        return
    conn = await asyncpg.connect(db, ssl=asyncpg_ssl_for(db), timeout=30)
    try:
        present = {r["nspname"] for r in await conn.fetch("SELECT nspname FROM pg_namespace")}
        hidden = [t for t in sorted(_OVER_HIDDEN)
                  if await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", f"public.{t}")]
        done = 0
        for stmt in statements(ro_role, present, hidden):
            try:
                await conn.execute(stmt)
                done += 1
            except Exception as e:  # noqa: BLE001
                log(f"FAILED {stmt[:120]}: {str(e)[:200]}")
        denied = await conn.fetchval(
            "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
            "WHERE n.nspname = ANY($1::text[]) AND NOT has_function_privilege($2, p.oid, 'EXECUTE')",
            list(FUNCTION_SCHEMAS), ro_role)
        readable_hidden = [t for t in hidden
                           if await conn.fetchval("SELECT has_table_privilege($1, $2, 'SELECT')", ro_role, f"public.{t}")]
        log(f"{done} statements applied; functions the console role cannot execute: {denied}; "
            f"hidden tables still readable: {readable_hidden or 'none'}")
    finally:
        await conn.close()


if __name__ == "__main__":
    try:
        asyncio.run(asyncio.wait_for(main(), timeout=60))
    except Exception as e:  # noqa: BLE001
        log(f"STOPPED: {str(e)[:400]}")
