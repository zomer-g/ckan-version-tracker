#!/usr/bin/env python3
"""Outside-in monitor for over.org.il, run by .github/workflows/external-monitor.yml.

The system checks (app/services/health_checks.py) run INSIDE the app, so they
cannot report that the app is down, or that they themselves stopped running.
This runs from GitHub's machines and answers exactly those questions.

    --mode site    (every 15 min) /healthz answers ok, the scheduler runs, the
                   daily checks finished within 26 h and the weekly within 8 days,
                   the home page and /api/v1 answer. A failure is retried before
                   it counts, so a deploy restart is not an outage.
    --mode health  (daily) the verdict of the newest daily system-check run, as
                   /healthz reports it. Fails when that run found a fault.

Writes a Markdown report to --report (the workflow puts it in an issue) and
exits 1 when something failed. Standard library only.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:  # noqa: BLE001
    pass

UA = "Mozilla/5.0 (X11; Linux x86_64) over-external-monitor"
STATUS_HE = {"ok": "תקין", "warn": "אזהרה", "fail": "תקלה", "error": "נקטעה"}


def get(url: str, timeout: int = 30) -> tuple[int | str, bytes]:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read(200_000)
    except urllib.error.HTTPError as e:
        return e.code, b""
    except Exception as e:  # noqa: BLE001
        return f"{type(e).__name__}: {e}", b""


def age_hours(iso: str | None) -> float | None:
    if not iso:
        return None
    dt = datetime.fromisoformat(iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - dt).total_seconds() / 3600


def site_checks(base: str) -> list[tuple[bool, str, str]]:
    out: list[tuple[bool, str, str]] = []
    code, body = get(f"{base}/healthz")
    hz = {}
    if code == 200:
        try:
            hz = json.loads(body)
        except ValueError:
            pass
    out.append((code == 200 and hz.get("ok") is True, "/healthz", f"HTTP {code}"))
    if hz:
        out.append((hz.get("scheduler_running") is True, "המתזמן רץ",
                    f"scheduler_running={hz.get('scheduler_running')}"))
        out.append((not hz.get("maintenance_mode"), "לא במצב תחזוקה",
                    f"maintenance_mode={hz.get('maintenance_mode')}"))
        hc = hz.get("health_checks")
        if hc is None:
            out.append((False, "בדיקות המערכת", "/healthz לא מדווח על בדיקות המערכת"))
        else:
            for tier, limit, label in (("daily", 26, "הבדיקה היומית רצה ב-26 השעות האחרונות"),
                                       ("weekly", 8 * 24 + 6, "הבדיקה השבועית רצה ב-8 הימים האחרונים")):
                last = hc.get(tier) or {}
                h = age_hours(last.get("finished_at"))
                out.append((h is not None and h <= limit, label,
                            "אף פעם" if h is None else f"לפני {h:.1f} שעות ({last.get('status')})"))
    code, body = get(f"{base}/")
    out.append((code == 200 and b'id="root"' in body, "דף הבית", f"HTTP {code}"))
    code, body = get(f"{base}/api/v1/datasets?limit=1")
    out.append((code == 200, "/api/v1/datasets", f"HTTP {code}"))
    return out


def health_checks(base: str) -> list[tuple[bool, str, str]]:
    code, body = get(f"{base}/healthz")
    try:
        last = (json.loads(body).get("health_checks") or {}).get("daily") if code == 200 else None
    except ValueError:
        last = None
    if not last:
        return [(False, "הבדיקה היומית", f"אין נתון (HTTP {code})")]
    st = last.get("status")
    return [(st not in ("fail", "error"), "הבדיקה היומית האחרונה",
             f"{STATUS_HE.get(st, st)} · הסתיימה {last.get('finished_at')} · "
             f"{last.get('new_problems', 0)} בעיות חדשות")]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="https://over.org.il")
    ap.add_argument("--mode", choices=("site", "health"), required=True)
    ap.add_argument("--report", default="report.md")
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--retry-wait", type=int, default=40)
    a = ap.parse_args()
    base = a.base.rstrip("/")
    fn = site_checks if a.mode == "site" else health_checks

    results = fn(base)
    tries = 1
    while not all(ok for ok, _, _ in results) and tries < a.retries:
        time.sleep(a.retry_wait)   # a deploy restart is not an outage
        results = fn(base)
        tries += 1

    ok_all = all(ok for ok, _, _ in results)
    lines = [f"| | בדיקה | פרט |", "|---|---|---|"]
    for ok, label, detail in results:
        lines.append(f"| {'✅' if ok else '❌'} | {label} | {detail} |")
        print(f"[{'PASS' if ok else 'FAIL'}] {label}: {detail}")
    when = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    head = "הכל תקין" if ok_all else f"נכשל אחרי {tries} ניסיונות"
    extra = ""
    if a.mode == "health":
        extra = f"\n\nהפירוט המלא בטאב **בדיקות מערכת**: {base}/admin#health\n"
    with open(a.report, "w", encoding="utf-8") as f:
        f.write(f"**{when}** — {head}\n\n" + "\n".join(lines) + extra + "\n")
    return 0 if ok_all else 1


if __name__ == "__main__":
    sys.exit(main())
