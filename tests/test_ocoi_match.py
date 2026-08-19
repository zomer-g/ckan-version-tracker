"""Guards for the OCOI duplicate matcher.

This is a FAITHFUL port, verified head-to-head against the original on 1,200
real corpus names and 12,015 scored pairs with zero divergence. These cases pin
the behaviour that verification established, so a later "improvement" has to be
a deliberate decision rather than a silent drift.
"""
import pytest

from app.services import ocoi_match as m


class TestNormalisation:
    def test_strips_gershayim_and_collapses_dashes(self):
        assert m.normalize('עו"ד  משה') == "עוד משה"
        assert m.normalize("אבו-מוך") == "אבו מוך"

    def test_person_tokens_drop_honorifics_and_single_chars(self):
        assert m.tokens('עו"ד אלעד מן', kind="person") == ["אלעד", "מן"]
        assert m.tokens("ד״ר משה כחלון", kind="person") == ["כחלון", "משה"]

    def test_org_tokens_also_drop_suffixes_and_construct_prefixes(self):
        assert m.tokens("עמותת הצלחה", kind="company") == ["הצלחה"]
        assert m.tokens("תנועת הצלחה", kind="company") == ["הצלחה"]

    def test_tokens_are_sorted_so_word_order_cannot_matter(self):
        assert m.tokens("משה כחלון") == m.tokens("כחלון משה")


class TestBlockingKey:
    def test_is_two_chars_of_the_longest_token(self):
        assert m.blocking_key("משה כחלון", kind="person") == "כח"

    def test_word_order_does_not_change_the_bucket(self):
        assert m.blocking_key("משה כחלון") == m.blocking_key("כחלון משה")

    def test_empty_when_nothing_informative_survives(self):
        assert m.blocking_key("מר", kind="person") == ""


class TestSimilarity:
    def test_exact_after_normalisation_is_one(self):
        s, why = m.similarity('עו"ד משה', "עוד משה", kind="person")
        assert s == 1.0 and "exact_normalised" in why

    def test_reordered_tokens_score_just_below_exact(self):
        """0.97, deliberately under 1.0 so true exact strings sort first."""
        s, why = m.similarity("משה כחלון", "כחלון משה", kind="person")
        assert s == 0.97 and "tokens_identical" in why

    def test_org_construct_prefixes_collapse_to_identical(self):
        s, _ = m.similarity("עמותת הצלחה", "תנועת הצלחה", kind="company")
        assert s == 0.97

    def test_token_subset_is_organisations_only(self):
        """A subset rule for people chains unrelated names through shared first
        names into 'everyone called X' mega-clusters — the reason it is org-only."""
        long_org = "הצלחה התנועה הצרכנית לקידום חברה"
        short_org = "הצלחה לקידום חברה"
        s_org, why = m.similarity(short_org, long_org, kind="company")
        assert s_org >= 0.88 and any("token_subset" in r for r in why)
        s_person, why_p = m.similarity(short_org, long_org, kind="person")
        assert not any("token_subset" in r for r in why_p)

    def test_unrelated_names_stay_below_the_threshold(self):
        s, _ = m.similarity("משה כחלון", "יאיר לפיד", kind="person")
        assert s < m.SCORE_THRESHOLD

    @pytest.mark.parametrize("a,b", [("", "משה"), ("משה", ""), ("מר", "גב")])
    def test_empty_or_all_noise_scores_zero(self, a, b):
        assert m.similarity(a, b, kind="person")[0] == 0.0


class TestPortInvariants:
    def test_threshold_matches_the_original(self):
        assert m.SCORE_THRESHOLD == 0.85

    def test_domain_is_never_scanned(self):
        """Domains are short topical labels; fuzzy-merging them is unsafe."""
        assert "domain" not in m.SCAN_KINDS

    def test_no_prefix_or_substring_rule_crept_back_in(self):
        """Both were removed upstream after producing 95-member clusters of
        legitimately different branches. A chain-store name must NOT match."""
        s, _ = m.similarity("ארומה תל אביב", "ארומה חיפה", kind="company")
        assert s < m.SCORE_THRESHOLD, (
            "a shared brand token must not make two branches duplicates")
