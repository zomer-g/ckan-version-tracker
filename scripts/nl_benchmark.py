#!/usr/bin/env python
"""Run the free-text query benchmark against a deployment and report the result.

    python scripts/nl_benchmark.py --base-url https://www.over.org.il \
        --token "$OVER_ADMIN_JWT" --discover --save runs/2026-08-01.json

    # compare against a previous run to see what a change did
    python scripts/nl_benchmark.py ... --baseline runs/2026-08-01.json

WHAT IT ACTUALLY TELLS YOU. Three things, and it is worth being precise because
the fourth is the one people assume:

  1. COVERAGE — answered vs refused, per case.
  2. COST SHAPE — which stage answered each question. This is the number that
     moves money: every question that falls from `template` to `anthropic` is a
     real cost increase, and it is invisible without measuring.
  3. ASSERTIONS — the cases that state an expectation, checked.

  4. It does NOT tell you whether an answer is CORRECT. A query that picks the
     wrong dataset and returns a plausible number passes everything here. That
     requires gold answers written against the live catalog (the `gold` array in
     benchmarks/nl_queries.json, currently empty).

It runs through the admin-only /api/admin/nl/try endpoint, which bypasses the
question cache — a benchmark that reads the cache scores the cache, and would
report a perfect result for a model that had started failing.

A run COSTS MONEY: each case that reaches a model is a real paid call, charged
against the same daily budget as public traffic. Roughly len(cases) calls, more
with escalation. Benchmark runs are not written to the admin question log.
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SET = ROOT / "benchmarks" / "nl_queries.json"

# Every line this tool prints contains Hebrew, and the default Windows console
# encoding (cp1252) cannot encode it — the run dies mid-report with a
# UnicodeEncodeError after the model calls have already been paid for. Force
# UTF-8 on the streams rather than stripping the text.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):  # non-reconfigurable stream (a pipe in some shells)
        pass

# Stages that cost nothing. The share of cases landing here is the headline.
FREE_STAGES = {"cache", "template"}


def _post(base: str, path: str, token: str, payload: dict, timeout: int = 120) -> tuple[int, dict]:
    req = urllib.request.Request(
        f"{base.rstrip('/')}{path}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        try:
            return e.code, json.loads(body)
        except ValueError:
            return e.code, {"detail": body[:400]}
    except Exception as e:  # noqa: BLE001 — network failure is a result, not a crash
        return 0, {"detail": f"{type(e).__name__}: {e}"}


def _get(base: str, path: str, token: str = "") -> dict:
    req = urllib.request.Request(f"{base.rstrip('/')}{path}", method="GET")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:  # noqa: BLE001
        print(f"  ! {path} failed: {e}", file=sys.stderr)
        return {}


def build_cases(spec: dict, base: str, token: str, discover: bool) -> list[dict]:
    cases: list[dict] = []
    for part in ("structural", "regression", "capability"):
        for c in spec.get(part, []):
            cases.append({**c, "part": part})
    for c in spec.get("gold", []):
        cases.append({**c, "part": "gold", "expect": c.get("expect", "answered")})
    # `retrieval_title` is checked in retrieval-only mode; in the full pipeline
    # the equivalent assertion is on the returned entity, which the runner
    # cannot map to a title without the catalog.
    if discover:
        # Questions generated from the LIVE model, so the set reflects the
        # datasets this deployment actually has rather than ones we guessed at.
        ex = _get(base, "/api/nl/examples")
        for e in ex.get("examples", []):
            cases.append({"q": e["question"], "expect": "answered",
                          "expect_stage": "template", "part": "discovered",
                          "note": f"generated from {e['table']}"})
    return cases


def check(case: dict, res: dict, http: int) -> tuple[bool, str]:
    """Did this case meet its stated expectation?"""
    expect = case.get("expect", "any")
    if expect == "error":
        return (http >= 400, f"HTTP {http}")
    if http >= 400:
        return False, f"HTTP {http}: {res.get('detail', '')[:80]}"

    answered = bool(res.get("answered"))
    if expect == "answered" and not answered:
        return False, f"refused: {(res.get('reason') or '')[:80]}"
    if expect == "refused" and answered:
        return False, f"answered from {res.get('entity')} (should have refused)"

    want_stage = case.get("expect_stage")
    if want_stage and res.get("stage") != want_stage:
        # Not a correctness failure, but a cost regression — report it as one.
        return False, f"stage {res.get('stage')} (expected {want_stage})"

    want_entity = case.get("entity")
    if want_entity and res.get("entity") != want_entity:
        return False, f"entity {res.get('entity')} (expected {want_entity})"

    for field in case.get("must_filter", []):
        fields = [f.get("field") for f in (res.get("query") or {}).get("filters", [])]
        if field not in fields:
            return False, f"missing filter on {field}"
    return True, ""


def run_retrieval(args) -> dict:
    """OFFLINE mode: measure RETRIEVAL only, against the live catalog.

    Why this exists as a separate mode: retrieval is the layer that produced the
    worst failure this feature has had — questions answered confidently from an
    unrelated dataset — and it is the one layer that can be measured without
    spending a cent or having any model enabled. It pulls /api/tables (public),
    rebuilds the semantic model locally, and checks which entity each question
    would land on. Run it on every change to the scorer; run the full mode when
    you need the end-to-end number.

    A case passes if `retrieval_title` appears in the top-1 entity's title, or —
    for a case marked expect "refused" — if nothing clears the score floor."""
    import os as _os
    _os.environ.setdefault("JWT_SECRET_KEY", "benchmark")
    sys.path.insert(0, str(ROOT))
    from app.services import semantic_model as sm

    cat = _get(args.base_url, "/api/tables").get("tables") or []
    if not cat:
        print("could not fetch /api/tables", file=sys.stderr)
        return {}
    model = [e for e in (sm._entity_from(r, None) for r in cat
                         if not r["table"].startswith("over_")) if e]
    spec = json.loads(Path(args.set).read_text(encoding="utf-8"))
    cases = [c for c in spec.get("gold", []) if c.get("retrieval_title") or c.get("expect") == "refused"]
    cases += [{**c, "part": "structural"} for c in spec.get("structural", [])
              if c.get("expect") == "refused" and c.get("q")]

    print(f"retrieval-only: {len(cases)} cases against {len(model)} entities "
          f"(catalog from {args.base_url})\n")
    rows, passed = [], 0
    for c in cases:
        top = sm.retrieve(model, c["q"], k=3)
        names = [e["title"] for e in top]
        want = c.get("retrieval_title")
        ok = (not names) if c.get("expect") == "refused" else (
            bool(names) and bool(want) and want in names[0])
        passed += ok
        rows.append({"q": c["q"], "ok": ok, "want": want,
                     "got": names[0] if names else None, "candidates": names})
        print(f"  {'ok  ' if ok else 'FAIL'} {c['q'][:44]:<44} -> "
              + (names[0][:40] if names else "(refuses)"))
    print("\n" + "-" * 62)
    print(f"retrieval top-1 correct: {passed}/{len(cases)}"
          f"  ({round(100 * passed / len(cases)) if cases else 0}%)")
    out = {"summary": {"mode": "retrieval", "cases": len(cases), "passed": passed,
                       "entities": len(model)}, "results": rows}
    if args.save:
        Path(args.save).parent.mkdir(parents=True, exist_ok=True)
        Path(args.save).write_text(json.dumps(out, ensure_ascii=False, indent=2),
                                   encoding="utf-8")
        print(f"saved -> {args.save}")
    return out


def run(args) -> dict:
    spec = json.loads(Path(args.set).read_text(encoding="utf-8"))
    cases = build_cases(spec, args.base_url, args.token, args.discover)
    if not cases:
        print("no cases — did --discover fail and the set has no structural cases?")
        return {}

    print(f"running {len(cases)} cases against {args.base_url}\n")
    results = []
    for i, case in enumerate(cases, 1):
        t0 = time.monotonic()
        http, res = _post(args.base_url, "/api/admin/nl/try", args.token,
                          {"q": case["q"], "use_cache": False, "run_sql": args.run_sql})
        ok, why = check(case, res, http)
        row = {
            "q": case["q"], "part": case.get("part"), "expect": case.get("expect"),
            "ok": ok, "why": why, "http": http,
            "answered": res.get("answered"), "stage": res.get("stage"),
            "entity": res.get("entity"), "model": res.get("model"),
            "escalated": res.get("escalated"),
            "input_tokens": res.get("input_tokens") or 0,
            "output_tokens": res.get("output_tokens") or 0,
            "rows": res.get("rows"), "sql_error": res.get("sql_error"),
            "duration_ms": res.get("duration_ms") or int((time.monotonic() - t0) * 1000),
        }
        results.append(row)
        mark = "ok  " if ok else "FAIL"
        print(f"  [{i:>3}/{len(cases)}] {mark} {row['stage'] or '-':<10} {case['q'][:52]}"
              + (f"   ← {why}" if why else ""))

    # Cache-pair cases run twice on purpose: the second call MUST hit the cache,
    # or the fingerprint is splitting on punctuation and the hit rate is a lie.
    pairs = []
    for p in spec.get("cache_pairs", []):
        _post(args.base_url, "/api/admin/nl/try", args.token, {"q": p["a"], "use_cache": True})
        _, r2 = _post(args.base_url, "/api/admin/nl/try", args.token,
                      {"q": p["b"], "use_cache": True})
        hit = r2.get("stage") == "cache"
        pairs.append({"a": p["a"], "b": p["b"], "cache_hit": hit, "note": p.get("note")})
        print(f"  cache pair: {'ok' if hit else 'FAIL — variants did not share a key'}")

    stages: dict[str, int] = {}
    by_part: dict[str, dict] = {}
    for r in results:
        stages[r["stage"] or "?"] = stages.get(r["stage"] or "?", 0) + 1
        p_ = by_part.setdefault(r["part"] or "?", {"n": 0, "passed": 0, "answered": 0})
        p_["n"] += 1
        p_["passed"] += 1 if r["ok"] else 0
        p_["answered"] += 1 if r["answered"] else 0
    durations = [r["duration_ms"] for r in results if r["duration_ms"]]
    summary = {
        "base_url": args.base_url,
        "cases": len(results),
        "passed": sum(1 for r in results if r["ok"]),
        "answered": sum(1 for r in results if r["answered"]),
        "escalated": sum(1 for r in results if r.get("escalated")),
        "free_share": round(
            sum(1 for r in results if r["stage"] in FREE_STAGES) / len(results), 3),
        "by_stage": stages,
        "input_tokens": sum(r["input_tokens"] for r in results),
        "output_tokens": sum(r["output_tokens"] for r in results),
        "median_ms": int(statistics.median(durations)) if durations else None,
        "cache_pairs_ok": all(p["cache_hit"] for p in pairs) if pairs else None,
        "by_part": by_part,
    }

    print("\n" + "─" * 62)
    print(f"passed        {summary['passed']}/{summary['cases']}")
    print(f"answered      {summary['answered']}/{summary['cases']}")
    print(f"free stages   {summary['free_share'] * 100:.0f}%   {summary['by_stage']}")
    print(f"escalated     {summary['escalated']}")
    print(f"tokens        {summary['input_tokens']} in / {summary['output_tokens']} out")
    print(f"median        {summary['median_ms']} ms")
    for part, v in sorted(summary["by_part"].items()):
        print(f"  {part:<12} {v['passed']}/{v['n']} passed, {v['answered']} answered")
    if pairs:
        print(f"cache pairs   {'ok' if summary['cache_pairs_ok'] else 'FAIL'}")

    out = {"summary": summary, "results": results, "cache_pairs": pairs}
    if args.save:
        Path(args.save).parent.mkdir(parents=True, exist_ok=True)
        Path(args.save).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\nsaved → {args.save}")
    if args.baseline:
        _diff(json.loads(Path(args.baseline).read_text(encoding="utf-8")), out)
    return out


def _diff(old: dict, new: dict) -> None:
    """What changed since the baseline run. This is the point of saving runs:
    an absolute pass rate says little, a REGRESSION says everything."""
    print("\n" + "─" * 62)
    print("vs baseline")
    o, n = old["summary"], new["summary"]
    for k in ("passed", "answered", "escalated", "output_tokens", "median_ms"):
        if o.get(k) is None or n.get(k) is None:
            continue
        d = n[k] - o[k]
        arrow = "→" if d == 0 else ("↑" if d > 0 else "↓")
        print(f"  {k:<14} {o[k]} {arrow} {n[k]}" + (f"  ({d:+})" if d else ""))
    print(f"  free_share     {o['free_share']} → {n['free_share']}")

    by_q_old = {r["q"]: r for r in old["results"]}
    regressed = [r for r in new["results"]
                 if r["ok"] is False and by_q_old.get(r["q"], {}).get("ok") is True]
    fixed = [r for r in new["results"]
             if r["ok"] is True and by_q_old.get(r["q"], {}).get("ok") is False]
    # A case that moved from a free stage to a paid one costs money on every
    # future ask — worth surfacing even though it still "passes".
    costlier = [r for r in new["results"]
                if r["stage"] not in FREE_STAGES
                and by_q_old.get(r["q"], {}).get("stage") in FREE_STAGES]
    for label, rows in (("REGRESSED", regressed), ("fixed", fixed),
                        ("now costs money", costlier)):
        if rows:
            print(f"\n  {label}:")
            for r in rows:
                print(f"    - {r['q'][:60]}  ({r['stage']}) {r['why']}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--base-url", default="http://localhost:8000")
    p.add_argument("--token", default="", help="admin JWT (or set OVER_ADMIN_JWT)")
    p.add_argument("--set", default=str(DEFAULT_SET))
    p.add_argument("--discover", action="store_true",
                   help="add questions generated from the live catalog")
    p.add_argument("--run-sql", action="store_true",
                   help="also execute each compiled query (slower; catches SQL that "
                        "validates but fails on real data)")
    p.add_argument("--save", default="")
    p.add_argument("--baseline", default="", help="a previous --save file to diff against")
    p.add_argument("--retrieval-only", action="store_true",
                   help="measure RETRIEVAL offline against the live catalog. Free, needs "
                        "no admin token and no model enabled — this is the layer that "
                        "produced the worst failure the feature has had.")
    args = p.parse_args()

    if args.retrieval_only:
        out = run_retrieval(args)
        if not out:
            return 1
        return 0 if out["summary"]["passed"] == out["summary"]["cases"] else 1

    import os
    args.token = args.token or os.environ.get("OVER_ADMIN_JWT", "")
    if not args.token:
        print("an admin JWT is required (--token or OVER_ADMIN_JWT)", file=sys.stderr)
        return 2

    out = run(args)
    if not out:
        return 1
    # Non-zero when anything failed, so this can gate a deploy.
    return 0 if out["summary"]["passed"] == out["summary"]["cases"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
