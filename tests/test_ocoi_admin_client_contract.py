"""The TS admin client and the Python admin API must agree on body field names.

Three buttons shipped broken because nothing checked this: "מזג" sent
{entity_type, keep_id, drop_ids} to a model wanting {keep_type, keep_id,
merge_id}, "מזג הכל" sent the same to one wanting {entity_type, canonical_id,
member_ids}, and "החזר לתור" sent `ids` to a model reading `document_ids`. Each
failed at the click with a 422/400; each type-checked cleanly on both sides,
because the two sides are different languages and no test spanned them.

So this parses frontend/src/api/client.ts and, for every admin call that sends a
body, asserts the keys it sends satisfy the route's Pydantic model: every
required field is sent, and nothing is sent that the model would ignore.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from app.api import ocoi_admin

CLIENT_TS = Path(__file__).resolve().parents[1] / "frontend" / "src" / "api" / "client.ts"

# Entries are split on their `  name: (` header rather than matched by one big
# regex: the return types nest angle brackets (`request<A<B<C>>>`) and the param
# lists wrap over lines, so a single pattern silently matches nothing.
_ENTRY_HEAD = re.compile(r"^  (?P<name>\w+): \(", re.M)
_PATH = re.compile(r"`\$\{OB\}(?P<path>[^`]*)`")
_METHOD = re.compile(r'method:\s*"(\w+)"')
_JBODY_LITERAL = re.compile(r"jbody\(\{(?P<keys>.*?)\}\)", re.S)
_INLINE_BODY_TYPE = re.compile(r"body:\s*\{(?P<fields>.*?)\}", re.S)


def _admin_block() -> str:
    src = CLIENT_TS.read_text(encoding="utf-8")
    start = src.index("export const ocoiAdmin")
    return src[start : src.index("\n};", start)]


def _keys_from_object_literal(body: str) -> set[str]:
    """Keys of a JS object literal: `{ a, b: x, ...c }` -> {a, b}."""
    out = set()
    for part in re.split(r",(?![^{(\[]*[})\]])", body):
        part = part.strip()
        if not part or part.startswith("..."):
            continue
        out.add(part.split(":", 1)[0].strip())
    return out


def _keys_from_type_literal(fields: str) -> set[str]:
    """Field names of a TS type literal: `{ a: string; b?: number }`."""
    out = set()
    for part in fields.split(";"):
        name = part.split(":", 1)[0].strip()
        if name:
            out.add(name.rstrip("?"))
    return out


def _client_calls() -> list[tuple[str, str, str, set[str]]]:
    """(name, verb, path, keys sent) for every admin call carrying a body."""
    block = _admin_block()
    heads = list(_ENTRY_HEAD.finditer(block))
    calls = []
    for i, head in enumerate(heads):
        end = heads[i + 1].start() if i + 1 < len(heads) else len(block)
        chunk = block[head.start():end]
        verb_m = _METHOD.search(chunk)
        if verb_m is None or verb_m.group(1) == "GET" or "jbody" not in chunk:
            continue
        path_m = _PATH.search(chunk)
        if path_m is None:
            continue
        lit = _JBODY_LITERAL.search(chunk)
        if lit and lit.group("keys").strip():
            keys = _keys_from_object_literal(lit.group("keys"))
        else:
            # `...jbody(body)` — the shape is the param's declared type.
            t = _INLINE_BODY_TYPE.search(chunk)
            if not t:
                continue
            keys = _keys_from_type_literal(t.group("fields"))
        # `${OB}` paths interpolate ids; the router spells them {param}.
        path = ocoi_admin.router.prefix + re.sub(r"\$\{[^}]*\}", "{}", path_m.group("path"))
        calls.append((head.group("name"), verb_m.group(1), path.split("?")[0], keys))
    return calls


def _route_models() -> dict[tuple[str, str], object]:
    out = {}
    for r in ocoi_admin.router.routes:
        field = getattr(r, "body_field", None)
        if field is None:
            continue
        # FastAPI on pydantic v2 keeps the model on field_info.annotation;
        # `.type_` is the v1 spelling and is simply absent here.
        model = getattr(getattr(field, "field_info", None), "annotation", None)
        norm = re.sub(r"\{[^}]*\}", "{}", getattr(r, "path", ""))
        for verb in getattr(r, "methods", ()) or ():
            out[(verb, norm)] = model
    return out


CALLS = _client_calls()


def test_the_parser_actually_found_the_admin_calls():
    """A regex that silently matches nothing would make every test below pass."""
    names = {c[0] for c in CALLS}
    assert len(CALLS) >= 10, f"only parsed {len(CALLS)} calls — the regex drifted"
    for expected in ("mergeEntities", "mergeCluster", "resetDocumentStatus"):
        assert expected in names, f"{expected} not parsed from client.ts"


@pytest.mark.parametrize("name,verb,path,keys",
                         CALLS, ids=[c[0] for c in CALLS])
def test_client_body_matches_the_route_model(name, verb, path, keys):
    model = _route_models().get((verb, path))
    if model is None or not hasattr(model, "model_fields"):
        pytest.skip(f"{name}: {verb} {path} takes no pydantic body")

    fields = model.model_fields
    required = {n for n, f in fields.items() if f.is_required()}

    unknown = keys - set(fields)
    assert not unknown, (
        f"{name} sends {sorted(unknown)}, which {model.__name__} does not "
        f"declare — the server will ignore them. Known: {sorted(fields)}")

    missing = required - keys
    assert not missing, (
        f"{name} never sends required field(s) {sorted(missing)} of "
        f"{model.__name__} — the call 422s at the button")


def test_merge_accepts_the_multi_select_the_admin_list_offers():
    """The entities list is multi-select, so one request must fold many losers;
    N client-side calls would race the 20/minute limit and half-apply."""
    fields = ocoi_admin.MergeBody.model_fields
    assert "merge_ids" in fields
    assert not fields["merge_id"].is_required(), (
        "merge_id must be optional once merge_ids exists, or the multi form 422s")


def test_extraction_status_has_one_name_for_success():
    """'completed' and 'extracted' both meant success; the corpus and OCOI use
    'extracted', so the ingest path must not reintroduce the second name."""
    ingest = (Path(__file__).resolve().parents[1]
              / "app" / "services" / "ocoi_ingest.py").read_text(encoding="utf-8")
    assert "extraction_status='completed'" not in ingest
    assert "extraction_status='extracted'" in ingest
