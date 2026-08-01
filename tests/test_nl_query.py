"""Orchestration tests for the free-text pipeline.

The thing worth pinning here is the ORDERING, because the ordering is the cost
control: the cache and the deterministic matcher must answer before anything
paid runs, and must never charge budget when they do. A refactor that hoists the
budget reservation to the top of ``answer`` would look harmless, pass every
other test in the suite, and quietly bill every cached question.

The second thing is that the paid tier's output is not trusted: a model naming a
column that does not exist must raise, and a model saying it cannot answer must
produce a refusal rather than an empty result set.

No DB, no network — the model, the cache and the provider are all stubbed.
"""
import asyncio
import os

import pytest

os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.services import nl_query, semantic_model as sm
from app.services.semantic_model import SemanticError

# The autouse fixture below stubs nl_query.tiers(). The two tests that
# exercise the ladder CONSTRUCTION need the real one, so hold a reference
# taken before any patching.
_REAL_TIERS = nl_query.tiers

ENTITY = {
    "key": "append_business", "schema": "public", "title": "רישיונות עסק",
    "summary": "רישיונות עסק", "rows": 1000, "synonyms": ["עסקים"],
    "dimensions": [
        {"key": "יישוב", "kind": "text", "title": "יישוב", "entity_type": "locality",
         "samples": ["חיפה"], "groupable": True},
        {"key": "סוג_עסק", "kind": "text", "title": "סוג עסק",
         "samples": ["מסעדה"], "groupable": True},
    ],
    "measures": [{"key": "count", "title": "מספר שורות"}],
    "geo_dims": ["יישוב"], "source_url": "", "page_url": "",
}


class _Spy:
    """Records whether the paid path was reached, and whether it was paid for."""

    def __init__(self, allow=True, output=None):
        self.reserved = 0
        self.llm_calls = 0
        self.usage_recorded = []
        self._allow = allow
        self._output = output

    async def reserve(self):
        self.reserved += 1
        return self._allow

    async def on_usage(self, i, o):
        self.usage_recorded.append((i, o))

    async def ask(self, question, entities, model=None):
        self.llm_calls += 1
        return self._output, (1234, 567)


@pytest.fixture(autouse=True)
def _stub(monkeypatch):
    """Model from a literal, no cache, Anthropic configured but stubbed out."""
    async def _model(db, use_cache=True):
        return [ENTITY]

    async def _cache_get(db, fp):
        return None

    async def _cache_put(db, fp, q, res):
        return None

    async def _config(db):
        # Everything on, no admin overrides — the multi-tier tests below stub
        # tiers() directly, so the config only has to be permissive here.
        return nl_query._config_defaults()

    async def _log(db, **row):
        return None

    monkeypatch.setattr(sm, "build_model", _model)
    monkeypatch.setattr(nl_query.semantic_model, "build_model", _model)
    monkeypatch.setattr(nl_query, "cache_get", _cache_get)
    monkeypatch.setattr(nl_query, "cache_put", _cache_put)
    monkeypatch.setattr(nl_query, "get_config", _config)
    monkeypatch.setattr(nl_query, "log_query", _log)
    # A SINGLE-tier ladder: these tests pin the pipeline itself, not the
    # escalation. The multi-tier cases live in the escalation section below
    # and override this.
    monkeypatch.setattr(nl_query, "tiers",
                        lambda cfg=None: [("anthropic", "claude-opus-5")])


def _run(coro):
    return asyncio.run(coro)


# ── the free tiers must stay free ────────────────────────────────────────────

def test_a_template_hit_never_charges_budget_or_calls_the_model(monkeypatch):
    spy = _Spy()
    monkeypatch.setattr(nl_query, "_ask_anthropic", spy.ask)
    res = _run(nl_query.answer(None, "כמה רישיונות עסק לפי יישוב",
                               reserve=spy.reserve, on_usage=spy.on_usage))
    assert res["source"] == "template"
    assert spy.reserved == 0, "a deterministic match must not consume LLM quota"
    assert spy.llm_calls == 0


def test_a_cache_hit_never_charges_budget_or_calls_the_model(monkeypatch):
    spy = _Spy()
    monkeypatch.setattr(nl_query, "_ask_anthropic", spy.ask)

    async def _hit(db, fp):
        return {"entity": "append_business", "query": {}, "sql": "SELECT 1",
                "explanation": "מהמטמון", "source": "anthropic"}

    monkeypatch.setattr(nl_query, "cache_get", _hit)
    res = _run(nl_query.answer(None, "שאלה מסובכת שאין לה תבנית בכלל",
                               reserve=spy.reserve, on_usage=spy.on_usage))
    assert res["cached"] is True
    assert spy.reserved == 0 and spy.llm_calls == 0


# ── the paid tier ────────────────────────────────────────────────────────────

def test_budget_is_charged_before_the_call_and_usage_after(monkeypatch):
    spy = _Spy(output={"entity": "append_business", "measures": ["count"],
                       "dimensions": ["סוג_עסק"], "filters": [], "unanswerable": ""})
    monkeypatch.setattr(nl_query, "_ask_anthropic", spy.ask)
    res = _run(nl_query.answer(None, "כמה רישיונות עסק שנסגרו לפי סוג עסק",
                               reserve=spy.reserve, on_usage=spy.on_usage))
    assert spy.reserved == 1 and spy.llm_calls == 1
    assert spy.usage_recorded == [(1234, 567)]
    assert res["source"] == "anthropic"
    assert "GROUP BY" in res["sql"]


def test_exhausted_budget_refuses_without_calling_the_model(monkeypatch):
    spy = _Spy(allow=False)
    monkeypatch.setattr(nl_query, "_ask_anthropic", spy.ask)
    with pytest.raises(nl_query.OutOfScope):
        _run(nl_query.answer(None, "כמה רישיונות עסק שנסגרו לפי סוג עסק",
                             reserve=spy.reserve, on_usage=spy.on_usage))
    assert spy.reserved == 1
    assert spy.llm_calls == 0, "a refused reservation must not reach the provider"


def test_model_saying_it_cannot_answer_becomes_a_refusal(monkeypatch):
    spy = _Spy(output={"entity": "", "measures": [], "dimensions": [], "filters": [],
                       "unanswerable": "אין במאגר נתון על שכר"})
    monkeypatch.setattr(nl_query, "_ask_anthropic", spy.ask)
    with pytest.raises(nl_query.OutOfScope) as e:
        _run(nl_query.answer(None, "מה השכר הממוצע ברישיונות עסק", reserve=spy.reserve))
    assert "שכר" in e.value.message


def test_a_model_inventing_a_column_raises_rather_than_degrading(monkeypatch):
    """The whole safety argument rests on this: model output is validated, and
    an undeclared field is a hard failure, not a dropped clause."""
    spy = _Spy(output={"entity": "append_business", "measures": ["count"],
                       "dimensions": ["עמודה_שלא_קיימת"], "filters": [], "unanswerable": ""})
    monkeypatch.setattr(nl_query, "_ask_anthropic", spy.ask)
    with pytest.raises(SemanticError):
        _run(nl_query.answer(None, "כמה רישיונות עסק שנסגרו לפי אזור", reserve=spy.reserve))


def test_unparseable_model_output_becomes_a_refusal(monkeypatch):
    spy = _Spy(output="not json at all")
    monkeypatch.setattr(nl_query, "_ask_anthropic", spy.ask)
    with pytest.raises(nl_query.OutOfScope):
        _run(nl_query.answer(None, "כמה רישיונות עסק שנסגרו", reserve=spy.reserve))


def test_json_wrapped_in_prose_is_still_read(monkeypatch):
    """Models fenced or prefaced their JSON often enough that failing the whole
    request over it would be a self-inflicted error rate."""
    spy = _Spy(output='```json\n{"entity": "append_business", "measures": ["count"], '
                      '"dimensions": [], "filters": [], "unanswerable": ""}\n```')
    monkeypatch.setattr(nl_query, "_ask_anthropic", spy.ask)
    res = _run(nl_query.answer(None, "כמה רישיונות עסק שנסגרו", reserve=spy.reserve))
    assert res["entity"] == "append_business"


# ── degraded mode ────────────────────────────────────────────────────────────

def test_allow_llm_false_keeps_the_free_tiers_working(monkeypatch):
    """When the budget is gone the feature must degrade, not go dark — a
    template question still answers."""
    spy = _Spy()
    monkeypatch.setattr(nl_query, "_ask_anthropic", spy.ask)
    res = _run(nl_query.answer(None, "כמה רישיונות עסק לפי יישוב", allow_llm=False))
    assert res["source"] == "template"
    with pytest.raises(nl_query.OutOfScope):
        _run(nl_query.answer(None, "כמה רישיונות עסק שנסגרו לפי סוג עסק", allow_llm=False))


def test_fingerprint_ignores_punctuation_and_spacing():
    """Cache keys are normalized tokens, so trivially different phrasings of the
    same question share an entry. This is the main cost lever on a public page."""
    a = nl_query.fingerprint("כמה רישיונות עסק לפי יישוב?", "sig")
    b = nl_query.fingerprint("  כמה רישיונות עסק לפי יישוב  ", "sig")
    assert a == b
    assert a != nl_query.fingerprint("כמה רישיונות עסק לפי יישוב", "other-sig")


# ── the escalation ladder ────────────────────────────────────────────────────
# Cheap model first, expensive one only when the cheap one fails DETECTABLY.
# The tests below pin what counts as "detectably" — and, just as importantly,
# that a cheap SUCCESS is never second-guessed, because escalating on a valid
# answer would make the ladder cost double for nothing.

class _Tier:
    def __init__(self, output, raises=None):
        self.calls = 0
        self._output = output
        self._raises = raises

    async def ask(self, question, entities, model=None):
        self.calls += 1
        if self._raises:
            raise self._raises
        return self._output, (100, 20)


GOOD = {"entity": "append_business", "measures": ["count"], "dimensions": [],
        "filters": [], "unanswerable": ""}


def _ladder(monkeypatch, cheap, dear, **flags):
    monkeypatch.setattr(nl_query, "tiers",
                        lambda cfg=None: [("deepseek", "deepseek-chat"), ("anthropic", "claude-opus-5")])
    monkeypatch.setattr(nl_query, "_ask_deepseek", cheap.ask)
    monkeypatch.setattr(nl_query, "_ask_anthropic", dear.ask)
    for k, v in flags.items():
        monkeypatch.setattr(nl_query.settings, k, v)


Q = "כמה רישיונות עסק שנסגרו"


def test_a_cheap_success_never_reaches_the_expensive_tier(monkeypatch):
    cheap, dear = _Tier(GOOD), _Tier(GOOD)
    _ladder(monkeypatch, cheap, dear)
    res = _run(nl_query.answer(None, Q))
    assert res["source"] == "deepseek" and res["escalated"] is False
    assert cheap.calls == 1 and dear.calls == 0


def test_unparseable_cheap_output_escalates(monkeypatch):
    cheap, dear = _Tier("not json"), _Tier(GOOD)
    _ladder(monkeypatch, cheap, dear)
    res = _run(nl_query.answer(None, Q))
    assert res["source"] == "anthropic" and res["escalated"] is True
    assert cheap.calls == 1 and dear.calls == 1


def test_an_invalid_query_from_the_cheap_tier_escalates(monkeypatch):
    """Naming a column that is not in the declared model is the clearest
    capability signal available — it is exactly what a weaker model gets wrong."""
    bad = {**GOOD, "dimensions": ["עמודה_מומצאת"]}
    cheap, dear = _Tier(bad), _Tier(GOOD)
    _ladder(monkeypatch, cheap, dear)
    assert _run(nl_query.answer(None, Q))["source"] == "anthropic"


def test_a_provider_outage_escalates_instead_of_failing(monkeypatch):
    cheap, dear = _Tier(None, raises=RuntimeError("503")), _Tier(GOOD)
    _ladder(monkeypatch, cheap, dear)
    assert _run(nl_query.answer(None, Q))["source"] == "anthropic"


def test_the_last_tier_failing_raises_rather_than_looping(monkeypatch):
    cheap, dear = _Tier("not json"), _Tier("also not json")
    _ladder(monkeypatch, cheap, dear)
    with pytest.raises(nl_query.OutOfScope):
        _run(nl_query.answer(None, Q))
    assert cheap.calls == 1 and dear.calls == 1


def test_unanswerable_escalates_when_enabled(monkeypatch):
    """A weak model's 'I can't' must not become the site's coverage ceiling."""
    no = {**GOOD, "unanswerable": "אין לי את הנתון"}
    cheap, dear = _Tier(no), _Tier(GOOD)
    _ladder(monkeypatch, cheap, dear, nl_query_escalate_on_unanswerable=True)
    assert _run(nl_query.answer(None, Q))["source"] == "anthropic"


def test_unanswerable_is_final_when_escalation_on_it_is_off(monkeypatch):
    """The cost switch: out-of-scope is a COMMON outcome here, so escalating on
    it is the expensive choice and has to be turn-off-able."""
    no = {**GOOD, "unanswerable": "אין לי את הנתון"}
    cheap, dear = _Tier(no), _Tier(GOOD)
    _ladder(monkeypatch, cheap, dear, nl_query_escalate_on_unanswerable=False)
    with pytest.raises(nl_query.OutOfScope) as e:
        _run(nl_query.answer(None, Q))
    assert "אין לי את הנתון" in e.value.message
    assert dear.calls == 0


def test_each_tier_reserves_budget_separately(monkeypatch):
    """An escalated question costs two calls, so it must consume two units of
    quota. Reserving once would let escalation double real spend invisibly."""
    cheap, dear = _Tier("not json"), _Tier(GOOD)
    _ladder(monkeypatch, cheap, dear)
    spy = _Spy()
    res = _run(nl_query.answer(None, Q, reserve=spy.reserve, on_usage=spy.on_usage))
    assert res["escalated"] is True
    assert spy.reserved == 2
    assert spy.usage_recorded == [(100, 20), (100, 20)]


def test_quota_running_out_mid_ladder_stops_before_the_expensive_call(monkeypatch):
    cheap, dear = _Tier("not json"), _Tier(GOOD)
    _ladder(monkeypatch, cheap, dear)

    class _OneShot:
        def __init__(self): self.n = 0
        async def reserve(self):
            self.n += 1
            return self.n == 1  # first tier allowed, second refused

    r = _OneShot()
    with pytest.raises(nl_query.OutOfScope):
        _run(nl_query.answer(None, Q, reserve=r.reserve))
    assert dear.calls == 0


def test_free_tiers_still_precede_the_whole_ladder(monkeypatch):
    """The ladder must not disturb the ordering that keeps the bill down: a
    template question reaches no paid tier at all."""
    cheap, dear = _Tier(GOOD), _Tier(GOOD)
    _ladder(monkeypatch, cheap, dear)
    spy = _Spy()
    res = _run(nl_query.answer(None, "כמה רישיונות עסק לפי יישוב", reserve=spy.reserve))
    assert res["source"] == "template"
    assert cheap.calls == 0 and dear.calls == 0 and spy.reserved == 0


def test_a_single_configured_key_means_no_escalation(monkeypatch):
    monkeypatch.setattr(nl_query.settings, "deepseek_api_key", "k")
    monkeypatch.setattr(nl_query.settings, "anthropic_api_key", "")
    monkeypatch.setattr(nl_query.settings, "nl_query_escalate", True)
    assert _REAL_TIERS() == [("deepseek", "deepseek-chat")]


def test_escalation_can_be_switched_off_entirely(monkeypatch):
    monkeypatch.setattr(nl_query.settings, "deepseek_api_key", "k")
    monkeypatch.setattr(nl_query.settings, "anthropic_api_key", "k")
    monkeypatch.setattr(nl_query.settings, "nl_query_escalate", False)
    assert [p for p, _ in _REAL_TIERS()] == ["deepseek"]


# ── example suggestions ──────────────────────────────────────────────────────
# These are the first thing anyone sees on the page, and the first live run
# produced "כמה KNS_PlenumVoteResult לפי תיאור סוג הפריט (למשל 'הצעת חוק')"
# four times over the same dataset family. Bad suggestions make the feature look
# broken before a user types anything, so they get tests.

def test_title_drops_the_restated_half():
    from app.api.nl_query import _clean_title
    assert _clean_title("תיקופי מסלקה — מאגר תיקופי מסלקה 2021") == "תיקופי מסלקה"
    assert _clean_title("רישיונות עסק") == "רישיונות עסק"


def test_year_variants_share_a_family_so_one_source_gives_one_suggestion():
    from app.api.nl_query import _family_key
    assert _family_key("תיקופי מסלקה 2021") == _family_key("תיקופי מסלקה 2026")
    assert _family_key("רישיונות עסק") != _family_key("החלטות ממשלה")


def test_a_sentence_description_is_not_used_as_a_label():
    from app.api.nl_query import _dim_label
    assert _dim_label({"key": "item_type",
                       "title": "תיאור סוג הפריט (למשל 'הצעת חוק', 'שאילתה')"}) == "item_type"
    assert _dim_label({"key": "mahoz", "title": "מחוז"}) == "מחוז"


def test_a_raw_identifier_title_is_not_hebrew():
    from app.api.nl_query import _HEB_RE
    assert not _HEB_RE.search("KNS_PlenumVoteResult")
    assert _HEB_RE.search("רישיונות עסק")


# ── the one-retry correction loop ────────────────────────────────────────────
# Highest-value single accuracy change available: removing the equivalent step
# cost -4.63 points in the MAC-SQL BIRD ablation, more than removing schema
# selection. The error text is what does the work — self-correction WITHOUT the
# error measures at 1-3 points — so these tests pin that the validator's actual
# complaint reaches the model, and that it happens exactly once.

class _Retrying:
    """First answer invalid, second valid. Records what the retry was told."""

    def __init__(self, first, second):
        self.calls = 0
        self.corrections = []
        self._first, self._second = first, second

    async def ask(self, question, entities, model=None, *, correction=None, previous=None):
        self.calls += 1
        self.corrections.append(correction)
        return (self._first if self.calls == 1 else self._second), (100, 20)


BAD = {"entity": "append_business", "measures": ["count"],
       "dimensions": ["עמודה_מומצאת"], "filters": [], "unanswerable": ""}


def test_an_invalid_query_is_retried_once_with_the_validator_error(monkeypatch):
    t = _Retrying(BAD, GOOD)
    monkeypatch.setattr(nl_query, "tiers", lambda cfg=None: [("anthropic", "claude-opus-5")])
    monkeypatch.setattr(nl_query, "_ask_anthropic", t.ask)
    res = _run(nl_query.answer(None, Q))
    assert res["answered"] if "answered" in res else res["entity"] == "append_business"
    assert t.calls == 2
    assert t.corrections[0] is None
    # The retry must carry the validator's real complaint, not a generic nudge.
    assert "עמודה_מומצאת" in (t.corrections[1] or "")


def test_the_retry_happens_at_most_once_per_question(monkeypatch):
    """Two tiers each retrying would be four paid calls for one question."""
    t = _Retrying(BAD, BAD)
    monkeypatch.setattr(nl_query, "tiers",
                        lambda cfg=None: [("deepseek", "deepseek-chat"),
                                          ("anthropic", "claude-opus-5")])
    monkeypatch.setattr(nl_query, "_ask_deepseek", t.ask)
    monkeypatch.setattr(nl_query, "_ask_anthropic", t.ask)
    with pytest.raises((nl_query.OutOfScope, SemanticError)):
        _run(nl_query.answer(None, Q))
    assert t.calls == 3, f"expected cheap + retry + expensive, got {t.calls}"


def test_the_retry_is_paid_for(monkeypatch):
    t = _Retrying(BAD, GOOD)
    monkeypatch.setattr(nl_query, "tiers", lambda cfg=None: [("anthropic", "claude-opus-5")])
    monkeypatch.setattr(nl_query, "_ask_anthropic", t.ask)
    spy = _Spy()
    res = _run(nl_query.answer(None, Q, reserve=spy.reserve, on_usage=spy.on_usage))
    assert spy.reserved == 2          # the correction call is a real call
    assert res["usage"] == (200, 40)  # and its tokens are attributed


def test_a_correction_prompt_shows_the_rejected_output_and_the_reason():
    from app.services.nl_query import _user_prompt
    p = _user_prompt("שאלה", [ENTITY], correction="העמודה 'x' אינה קיימת",
                     previous={"entity": "append_business", "dimensions": ["x"]})
    assert "העמודה 'x' אינה קיימת" in p
    assert '"dimensions"' in p or "dimensions" in p
