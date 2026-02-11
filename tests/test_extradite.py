"""Integration tests for the extradite runtime."""

import importlib
import sys
from collections.abc import Iterator

import pytest

from extradite import ExtraditeModuleLeakError
from extradite import UnsupportedInteractionError
from extradite import extradite

TARGET_MODULE: str = "tests.fixtures.sandbox_target"
TARGET_CLASS: str = "IsolatedCounter"
TARGET: str = f"{TARGET_MODULE}:{TARGET_CLASS}"
PEER_MODULE: str = "tests.fixtures.sandbox_peer"
PEER_CLASS: str = "PeerCounter"
PEER_TARGET: str = f"{PEER_MODULE}:{PEER_CLASS}"


def _purge_module(module_name: str) -> None:
    """Remove a module tree from ``sys.modules``.

    :param module_name: Root module name.
    """
    module_prefix: str = f"{module_name}."
    loaded_names: list[str] = list(sys.modules.keys())
    for loaded_name in loaded_names:
        is_exact_match: bool = loaded_name == module_name
        is_submodule_match: bool = loaded_name.startswith(module_prefix)
        if is_exact_match is True or is_submodule_match is True:
            sys.modules.pop(loaded_name, None)


@pytest.fixture(autouse=True)
def _clean_target_module_tree() -> Iterator[None]:
    """Keep the target module absent from root-process imports.

    :yields: Control to the active test.
    """
    _purge_module(TARGET_MODULE)
    _purge_module(PEER_MODULE)
    yield
    _purge_module(TARGET_MODULE)
    _purge_module(PEER_MODULE)


def test_extradited_usage_is_transparent_for_supported_interactions() -> None:
    """Verify basic proxy behavior and root-process module isolation."""
    counter_cls: type = extradite(TARGET)
    counter = None
    peer = None
    try:
        module_loaded_locally: bool = TARGET_MODULE in sys.modules
        assert module_loaded_locally is False

        counter = counter_cls(3, tag="remote")
        peer = counter_cls(5)

        updated: object = counter.increment(4)
        assert updated == 7
        assert counter.tag == "remote"

        counter.tag = "updated"
        assert counter.tag == "updated"

        class_level: object = counter_cls.class_level
        assert class_level == 7

        class_name: object = counter_cls.cls_name()
        assert class_name == TARGET_CLASS

        peer_sum: object = counter.sum_with_peer(peer)
        assert peer_sum == 12

        current_length: int = len(counter)
        assert current_length == 7

        representation: str = repr(counter)
        contains_name: bool = "IsolatedCounter" in representation
        assert contains_name is True

        module_loaded_after_calls: bool = TARGET_MODULE in sys.modules
        assert module_loaded_after_calls is False
    finally:
        if counter is not None:
            counter.close()
        if peer is not None:
            peer.close()
        counter_cls.close()


def test_unpicklable_return_values_raise() -> None:
    """Verify that unpicklable return values are rejected."""
    counter_cls: type = extradite(TARGET)
    counter = None
    try:
        counter = counter_cls(1)
        with pytest.raises(UnsupportedInteractionError):
            counter.make_unpicklable()
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_module_origin_values_raise() -> None:
    """Verify that values originating from the isolated module are rejected."""
    counter_cls: type = extradite(TARGET)
    counter = None
    try:
        counter = counter_cls(2)
        with pytest.raises(UnsupportedInteractionError):
            counter.make_module_value()
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_module_leak_detection_blocks_new_session() -> None:
    """Verify that a local import leak blocks session startup."""
    importlib.import_module(TARGET_MODULE)
    with pytest.raises(ExtraditeModuleLeakError):
        extradite(TARGET)


def test_share_key_reuses_session_for_same_target() -> None:
    """Verify that same-target calls with the same share key reuse one process."""
    share_key: str = "same-target"
    first_cls: type = extradite(TARGET, share_key=share_key)
    second_cls: type = extradite(TARGET, share_key=share_key)
    first = None
    second = None
    try:
        first = first_cls(10)
        second = second_cls(20)

        first_pid: object = first.worker_pid()
        second_pid: object = second.worker_pid()
        assert first_pid == second_pid

        first_cls.close()
        updated: object = second.increment(3)
        assert updated == 23
    finally:
        if first is not None:
            first.close()
        if second is not None:
            second.close()
        first_cls.close()
        second_cls.close()


def test_share_key_reuses_session_across_different_targets() -> None:
    """Verify that one share key can co-locate different modules in one process."""
    share_key: str = "cross-target"
    counter_cls: type = extradite(TARGET, share_key=share_key)
    peer_cls: type = extradite(PEER_TARGET, share_key=share_key)
    counter = None
    peer = None
    try:
        counter = counter_cls(3)
        peer = peer_cls(7)

        counter_pid: object = counter.worker_pid()
        peer_pid: object = peer.worker_pid()
        assert counter_pid == peer_pid

        counter_cls.close()
        peer_updated: object = peer.increment(5)
        assert peer_updated == 12

        target_loaded: bool = TARGET_MODULE in sys.modules
        peer_loaded: bool = PEER_MODULE in sys.modules
        assert target_loaded is False
        assert peer_loaded is False
    finally:
        if counter is not None:
            counter.close()
        if peer is not None:
            peer.close()
        counter_cls.close()
        peer_cls.close()
