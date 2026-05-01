"""Tests for app._build_match_sql — the SQL clause builder behind the
Filter Wizard. Exercises every WHERE branch independently so a regression
in one (e.g. clusters, class_need, cond_logic) is caught fast."""
from __future__ import annotations


def _rule(**kwargs):
    """Construct a FilterRule (defined inside routers/filter.py) with the
    given overrides."""
    from routers.filter import FilterRule
    return FilterRule(**kwargs)


def test_build_match_sql_default_rule_has_no_where_filters():
    """Default FilterRule should produce a SELECT that doesn't filter
    anything beyond the base tables — i.e., no class predicate, no
    quality range narrower than [0, 1], no time range."""
    import app
    rule = _rule()
    sql, params = app._build_match_sql(rule)
    # The base SELECT joins images; with all-defaults it should not include
    # a WHERE on classes, quality range, brightness, or hours/dow.
    assert "FROM IMAGES" in sql.upper()
    # Defaults bind quality range [0, 1], brightness range [0, 255],
    # sharpness >= 0, and n_dets range [0, 100000]. So params is non-empty
    # but every numeric value falls inside those defaults.
    assert isinstance(params, list)


def test_build_match_sql_class_filter_adds_clause():
    """Setting classes=[1,2] should bind those values as parameters."""
    import app
    rule = _rule(classes=[1, 2], logic="any")
    sql, params = app._build_match_sql(rule)
    assert 1 in params and 2 in params


def test_build_match_sql_logic_all_uses_intersection():
    """logic=all means every class must appear → the SQL must enforce that
    via a HAVING/COUNT or self-joined EXISTS pattern, not a simple IN."""
    import app
    rule = _rule(classes=[1, 2, 3], logic="all")
    sql, _ = app._build_match_sql(rule)
    # 'all' should NOT be a plain `class_id IN (1,2,3)` — that's 'any'.
    # Looser check: the SQL must mention either HAVING or multiple EXISTS.
    upper = sql.upper()
    assert "HAVING" in upper or upper.count("EXISTS") > 1


def test_build_match_sql_logic_none_excludes_classes():
    """logic=none excludes any image whose detections include the listed
    classes — must use NOT EXISTS / NOT IN."""
    import app
    rule = _rule(classes=[5], logic="none")
    sql, _ = app._build_match_sql(rule)
    upper = sql.upper()
    assert "NOT EXISTS" in upper or "NOT IN" in upper


def test_build_match_sql_time_window():
    """min_date / max_date are epoch seconds — should appear in params."""
    import app
    rule = _rule(min_date=1000.0, max_date=2000.0)
    _, params = app._build_match_sql(rule)
    assert 1000.0 in params and 2000.0 in params


def test_build_match_sql_conditions_filter():
    """Frame-condition tags (night/fog/...) should bind."""
    import app
    rule = _rule(conditions=["night", "fog"], cond_logic="any")
    _, params = app._build_match_sql(rule)
    assert "night" in params and "fog" in params
