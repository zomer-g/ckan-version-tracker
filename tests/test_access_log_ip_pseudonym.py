"""Client IPs in the API access log are replaced by a keyed pseudonym once a
row is older than api_access_log_ip_retention_days."""
import asyncio
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import app.database as D
from app.config import settings
from app.models.api_access_log import ApiAccessLog
from app.services import api_access_log as L


_ids = iter(range(1, 10_000))


def _row(ip, age_days):
    return ApiAccessLog(id=next(_ids), ts=datetime.now(timezone.utc) - timedelta(days=age_days),
                        method="GET", path="/api/x", area="v1", status=200,
                        duration_ms=1, bytes_out=0, ip=ip, channel="script",
                        actor_kind="anonymous")


def test_old_ips_become_stable_pseudonyms(monkeypatch):
    monkeypatch.setattr(settings, "jwt_secret_key", "k" * 40)
    monkeypatch.setattr(settings, "api_access_log_ip_retention_days", 7)
    engine = create_async_engine("sqlite+aiosqlite://")
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    monkeypatch.setattr(D, "async_session", Session)

    async def run():
        async with engine.begin() as conn:
            await conn.run_sync(D.Base.metadata.create_all, tables=[ApiAccessLog.__table__])
        async with Session() as s:
            s.add_all([_row("8.8.8.8", 10), _row("8.8.8.8", 9), _row("9.9.9.9", 8),
                       _row("1.1.1.1", 1)])
            await s.commit()
        n = await L.pseudonymize_old_ips(batch=1)
        async with Session() as s:
            ips = [r.ip for r in (await s.execute(
                select(ApiAccessLog).order_by(ApiAccessLog.ts))).scalars()]
        await engine.dispose()
        return n, ips

    n, ips = asyncio.run(run())
    assert n == 2
    assert ips[0] == ips[1] == L.ip_pseudonym("8.8.8.8")
    assert ips[2] == L.ip_pseudonym("9.9.9.9") != ips[0]
    assert ips[3] == "1.1.1.1"                      # recent rows keep the address
    assert all(not i.startswith("8.") for i in ips[:3])
