from __future__ import annotations

from dataclasses import dataclass

from roll_terminal_qt.incremental_views import diff_by_identity, keyed_row_delta


@dataclass(frozen=True)
class _Row:
    key: str
    state: str


def test_diff_by_identity_separates_changed_and_unchanged_rows() -> None:
    delta = diff_by_identity(
        [_Row("a", "live"), _Row("b", "live")],
        [_Row("a", "filled"), _Row("b", "live"), _Row("c", "live")],
        identity=lambda row: row.key,
        fingerprint=lambda row: row.state,
    )

    assert [row.key for row in delta.updated] == ["a"]
    assert [row.key for row in delta.unchanged] == ["b"]
    assert [row.key for row in delta.added] == ["c"]
    assert delta.removed == ()


def test_keyed_row_delta_skips_unchanged_rows() -> None:
    delta = keyed_row_delta(
        [("a", ("one", "two")), ("b", ("three",))],
        [("a", ("one", "two")), ("b", ("four",))],
    )

    assert delta.structure_changed is False
    assert delta.changed_keys == ("b",)


def test_keyed_row_delta_reports_insertions_and_reordering_as_structure_change() -> None:
    delta = keyed_row_delta(
        [("a", ("one",))],
        [("b", ("two",)), ("a", ("one",))],
    )

    assert delta.structure_changed is True
    assert delta.changed_keys == ("b",)
