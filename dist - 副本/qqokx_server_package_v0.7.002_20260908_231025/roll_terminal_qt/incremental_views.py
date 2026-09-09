from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Generic, Hashable, Sequence, TypeVar


T = TypeVar("T")


@dataclass(frozen=True)
class KeyedRowDelta:
    structure_changed: bool
    changed_keys: tuple[Hashable, ...]


@dataclass(frozen=True)
class ViewDelta(Generic[T]):
    added: tuple[T, ...]
    updated: tuple[T, ...]
    removed: tuple[T, ...]
    unchanged: tuple[T, ...]


def diff_by_identity(
    previous: Sequence[T],
    current: Sequence[T],
    *,
    identity: Callable[[T], Hashable],
    fingerprint: Callable[[T], object],
) -> ViewDelta[T]:
    """Partition a snapshot into deterministic identity-based UI changes."""
    old_by_id = {identity(item): item for item in previous}
    new_by_id = {identity(item): item for item in current}
    if len(old_by_id) != len(previous) or len(new_by_id) != len(current):
        raise ValueError("duplicate row identity")
    added = tuple(item for item in current if identity(item) not in old_by_id)
    unchanged = tuple(
        item
        for item in current
        if identity(item) in old_by_id and fingerprint(item) == fingerprint(old_by_id[identity(item)])
    )
    updated = tuple(
        item
        for item in current
        if identity(item) in old_by_id and fingerprint(item) != fingerprint(old_by_id[identity(item)])
    )
    removed = tuple(item for item in previous if identity(item) not in new_by_id)
    return ViewDelta(added=added, updated=updated, removed=removed, unchanged=unchanged)


def keyed_row_delta(
    previous: Sequence[tuple[Hashable, tuple[str, ...]]],
    current: Sequence[tuple[Hashable, tuple[str, ...]]],
) -> KeyedRowDelta:
    """Describe whether a keyed table can be updated in place."""
    previous_keys = tuple(key for key, _values in previous)
    current_keys = tuple(key for key, _values in current)
    previous_by_key = dict(previous)
    changed = tuple(key for key, values in current if previous_by_key.get(key) != values)
    return KeyedRowDelta(structure_changed=previous_keys != current_keys, changed_keys=changed)
