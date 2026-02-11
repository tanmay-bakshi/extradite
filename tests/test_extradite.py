"""Integration tests for the extradite runtime."""

import concurrent.futures
import importlib
import os
import signal
import sys
import threading
import time
import traceback
from collections.abc import Callable
from collections.abc import Iterator

import pytest

import extradite.runtime as runtime
from extradite import ExtraditeModuleLeakError
from extradite import ExtraditeProtocolError
from extradite import ExtraditeRemoteError
from extradite import UnsupportedInteractionError
from extradite import extradite

TARGET_MODULE: str = "tests.fixtures.sandbox_target"
TARGET_CLASS: str = "IsolatedCounter"
TARGET: str = f"{TARGET_MODULE}:{TARGET_CLASS}"
PEER_MODULE: str = "tests.fixtures.sandbox_peer"
PEER_CLASS: str = "PeerCounter"
PEER_TARGET: str = f"{PEER_MODULE}:{PEER_CLASS}"
_INTERPRETER_POOL_EXECUTOR: object = getattr(concurrent.futures, "InterpreterPoolExecutor", None)


class PicklablePayload:
    """Top-level picklable payload used for value-mode identity tests."""

    value: int

    def __init__(self, value: int) -> None:
        """Initialize payload.

        :param value: Stored value.
        """
        self.value = value


class BaseLocalClass:
    """Base class used for class-handle semantics tests."""


class DerivedLocalClass(BaseLocalClass):
    """Derived class used for class-handle semantics tests."""


class AlternatePayload:
    """Picklable payload class used for policy-fallback tests."""

    value: int

    def __init__(self, value: int) -> None:
        """Initialize payload.

        :param value: Stored value.
        """
        self.value = value


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
    """Keep fixture target modules absent from root-process imports.

    :yields: Control to the active test.
    """
    _purge_module(TARGET_MODULE)
    _purge_module(PEER_MODULE)
    yield
    _purge_module(TARGET_MODULE)
    _purge_module(PEER_MODULE)


def _integration_probe(tag: str) -> dict[str, object]:
    """Run a compact integration probe used for parity checks.

    :param tag: Probe tag used to isolate share keys.
    :returns: Result payload with deterministic fields.
    """
    _purge_module(TARGET_MODULE)
    share_key: str = f"parity-{tag}"
    counter_cls: type = extradite(TARGET, share_key=share_key)
    counter = None
    lock_handle = None

    class LocalPayload:
        """Function-local payload class used to force handle transport."""

        marker: str

        def __init__(self, marker: str) -> None:
            """Initialize local payload.

            :param marker: Marker text.
            """
            self.marker = marker

    try:
        counter = counter_cls(4)

        payload = LocalPayload("probe")

        addend: int = 11

        def callback(value: int) -> int:
            """Compute deterministic callback result.

            :param value: Input value.
            :returns: Incremented value.
            """
            return value + addend

        marker: object = counter.inspect_marker(payload)
        callback_result: object = counter.callback_value(callback, 5)
        identity_result: object = counter.compare_identity(payload, payload)

        lock_handle = counter.make_non_picklable_native_value()
        lock_repr_before: str = repr(lock_handle)
        lock_handle.close()

        release_error_seen: bool = False
        try:
            repr(lock_handle)
        except ExtraditeProtocolError:
            release_error_seen = True

        value_after: object = counter.increment(3)
        return {
            "marker": marker,
            "callback_result": callback_result,
            "identity_result": identity_result,
            "release_error_seen": release_error_seen,
            "lock_repr_nonempty": len(lock_repr_before) > 0,
            "value_after": value_after,
        }
    finally:
        if lock_handle is not None:
            try:
                lock_handle.close()
            except (ExtraditeProtocolError, ExtraditeRemoteError, OSError):
                pass
        if counter is not None:
            counter.close()
        counter_cls.close()


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


def test_non_picklable_argument_as_handle() -> None:
    """Pass local-only objects and closures as handles to the isolated side."""
    counter_cls: type = extradite(TARGET)
    counter = None

    class LocalPayload:
        """Function-local payload class used for handle transport validation."""

        marker: str

        def __init__(self, marker: str) -> None:
            """Initialize payload.

            :param marker: Marker text.
            """
            self.marker = marker

    try:
        counter = counter_cls(0)
        payload = LocalPayload("marker-value")

        marker_result: object = counter.inspect_marker(payload)
        assert marker_result == "marker-value"

        base: int = 40

        def closure(value: int) -> int:
            """Function-local closure to force reference transport.

            :param value: Input value.
            :returns: Deterministic result.
            """
            return value + base + 2

        callback_result: object = counter.callback_value(closure, 0)
        assert callback_result == 42
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_parent_class_handle_repr_behaves_like_type() -> None:
    """Verify ``repr`` behavior for parent-origin class handles."""
    counter_cls: type = extradite(TARGET)
    counter = None
    try:
        counter = counter_cls(1)
        expected_repr: str = repr(BaseLocalClass)
        remote_repr: object = counter.class_handle_repr(BaseLocalClass)
        assert remote_repr == expected_repr
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_parent_class_handle_isinstance_semantics() -> None:
    """Verify ``isinstance`` semantics for parent-origin class handles."""
    counter_cls: type = extradite(TARGET)
    counter = None
    try:
        counter = counter_cls(1)
        value = DerivedLocalClass()
        result: object = counter.class_handle_isinstance(value, BaseLocalClass)
        assert result is True
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_parent_class_handle_issubclass_semantics() -> None:
    """Verify ``issubclass`` semantics for parent-origin class handles."""
    counter_cls: type = extradite(TARGET)
    counter = None
    try:
        counter = counter_cls(1)
        result: object = counter.class_handle_issubclass(DerivedLocalClass, BaseLocalClass)
        assert result is True
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_parent_class_handle_satisfies_real_type_requirement() -> None:
    """Parent-origin class handles should satisfy ``type(...)`` base checks."""
    counter_cls: type = extradite(TARGET)
    counter = None
    try:
        counter = counter_cls(1)
        result: object = counter.class_handle_requires_real_type(BaseLocalClass)
        assert result is True
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_parent_class_handle_schema_like_payload_accepts_real_type() -> None:
    """Nested payloads containing parent class handles should work in type-required paths."""
    counter_cls: type = extradite(TARGET)
    counter = None
    try:
        counter = counter_cls(1)

        class LocalModel:
            """Function-local model used to force parent-handle transport."""

            x: int

            def __init__(self, x: object) -> None:
                """Initialize model value.

                :param x: Raw value to convert.
                """
                self.x = int(x)

        schema_like_payload: dict[str, object] = {
            "cls": LocalModel,
            "kwargs": {"x": "1"},
        }
        instance: object = counter.class_handle_schema_like_constructor(schema_like_payload)
        assert instance.__class__ is LocalModel
        assert getattr(instance, "x") == 1
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_parent_class_handle_hasattr_missing_attribute_is_safe() -> None:
    """Missing attributes on class handles should surface as ``AttributeError`` semantics."""
    counter_cls: type = extradite(TARGET)
    counter = None
    try:
        counter = counter_cls(1)

        class LocalWithoutSlots:
            """Class without ``__slots__`` to exercise ``hasattr`` behavior."""

        result: object = counter.class_handle_missing_attribute_uses_attribute_error(LocalWithoutSlots)
        assert result is False
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_parent_handle_roundtrip_returns_same_object_without_release_error() -> None:
    """Roundtripping a parent-origin non-picklable handle should preserve identity."""
    counter_cls: type = extradite(TARGET)
    counter = None
    try:
        counter = counter_cls(1)
        lock_object = threading.Lock()
        roundtripped: object = counter.echo(lock_object)
        assert roundtripped is lock_object
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_transport_policy_force_reference_preserves_identity() -> None:
    """Policy ``reference`` should preserve identity for picklable user objects."""
    counter_cls: type = extradite(TARGET, transport_policy="reference")
    counter = None
    try:
        counter = counter_cls(1)
        payload = PicklablePayload(21)
        returned: object = counter.echo(payload)
        assert returned is payload
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_default_value_mode_copies_identity_as_documented() -> None:
    """Default policy should transfer picklable user objects by value."""
    counter_cls: type = extradite(TARGET)
    counter = None
    try:
        counter = counter_cls(1)
        payload = PicklablePayload(99)
        returned: object = counter.echo(payload)
        assert returned is not payload
        assert isinstance(returned, PicklablePayload) is True
        assert returned.value == payload.value
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_large_picklable_container_roundtrip_in_value_mode() -> None:
    """Large list payloads should roundtrip with value semantics."""
    counter_cls: type = extradite(TARGET)
    counter = None
    try:
        counter = counter_cls(1)
        payload: list[int] = list(range(4_096))
        returned_obj: object = counter.echo(payload)
        assert isinstance(returned_obj, list) is True
        returned: list[object] = returned_obj
        assert returned == payload
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_nested_type_rule_reference_semantics_without_call_override() -> None:
    """Nested type-rule matches should still preserve identity by reference."""
    counter_cls: type = extradite(
        TARGET,
        transport_policy="value",
        transport_type_rules={PicklablePayload: "reference"},
    )
    counter = None
    try:
        counter = counter_cls(1)
        payload_items: list[PicklablePayload] = [PicklablePayload(10), PicklablePayload(20)]
        payload: dict[str, object] = {"items": payload_items}

        updated_value: int = 999
        result_obj: object = counter.set_nested_item_value(payload, updated_value)
        assert result_obj == updated_value
        assert payload_items[0].value == updated_value
        assert payload_items[1].value == 20
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_call_override_value_applies_to_nested_type_rule_payloads() -> None:
    """Per-call value override should copy nested values even with type rules configured."""
    counter_cls: type = extradite(
        TARGET,
        transport_policy="value",
        transport_type_rules={PicklablePayload: "reference"},
    )
    counter = None
    try:
        counter = counter_cls(1)
        payload_items: list[PicklablePayload] = [PicklablePayload(31), PicklablePayload(41)]
        payload: dict[str, object] = {"items": payload_items}

        updated_value: int = 222
        result_obj: object = counter.set_nested_item_value(
            payload,
            updated_value,
            __extradite_policy__="value",
        )
        assert result_obj == updated_value
        assert payload_items[0].value == 31
        assert payload_items[1].value == 41
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_large_container_fallback_roundtrips_unpicklable_nested_values() -> None:
    """Large containers with unpicklable nested values should fall back to recursive encoding."""
    counter_cls: type = extradite(TARGET)
    counter = None
    try:
        counter = counter_cls(1)
        offset: int = 13

        def callback(value: int) -> int:
            """Compute one callback score.

            :param value: Input value.
            :returns: Incremented score.
            """
            return value + offset

        payload: list[object] = list(range(40))
        payload.append(callback)
        result_obj: object = counter.callback_from_payload(payload, 5)
        assert result_obj == 18
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_invalid_transport_policy_fails_fast() -> None:
    """Invalid transport policy configuration should fail fast in the caller."""
    with pytest.raises(ValueError, match="transport_policy"):
        extradite(TARGET, transport_policy="invalid-policy")


def test_share_key_rejects_conflicting_transport_policy() -> None:
    """A shared session key must not mix conflicting transport policies."""
    share_key: str = "policy-conflict"
    counter_cls: type = extradite(TARGET, share_key=share_key, transport_policy="value")
    try:
        with pytest.raises(ValueError, match="share_key already exists"):
            extradite(TARGET, share_key=share_key, transport_policy="reference")
    finally:
        counter_cls.close()


def test_share_key_rejects_conflicting_transport_type_rules() -> None:
    """A shared session key must not mix conflicting per-type rules."""
    share_key: str = "type-rule-conflict"
    counter_cls: type = extradite(
        TARGET,
        share_key=share_key,
        transport_policy="value",
        transport_type_rules={PicklablePayload: "reference"},
    )
    try:
        with pytest.raises(ValueError, match="transport_type_rules"):
            extradite(
                TARGET,
                share_key=share_key,
                transport_policy="value",
                transport_type_rules={AlternatePayload: "reference"},
            )
    finally:
        counter_cls.close()


def test_policy_precedence_call_override_then_type_then_session() -> None:
    """Validate precedence: per-call override > per-type rule > session default."""
    counter_cls: type = extradite(
        TARGET,
        transport_policy="value",
        transport_type_rules={PicklablePayload: "reference"},
    )
    counter = None
    try:
        counter = counter_cls(1)

        type_rule_payload = PicklablePayload(1)
        type_rule_result: object = counter.echo(type_rule_payload)
        assert type_rule_result is type_rule_payload

        call_override_payload = PicklablePayload(2)
        call_override_result: object = counter.echo(
            call_override_payload,
            __extradite_policy__="value",
        )
        assert call_override_result is not call_override_payload
        assert isinstance(call_override_result, PicklablePayload) is True
        assert call_override_result.value == call_override_payload.value

        fallback_payload = AlternatePayload(3)
        fallback_result: object = counter.echo(fallback_payload)
        assert fallback_result is not fallback_payload
        assert isinstance(fallback_result, AlternatePayload) is True
        assert fallback_result.value == fallback_payload.value

        explicit_reference_payload = AlternatePayload(4)
        explicit_reference_result: object = counter.echo(
            explicit_reference_payload,
            __extradite_policy__="reference",
        )
        assert explicit_reference_result is explicit_reference_payload
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_get_effective_policy_trace_precedence_and_fallback() -> None:
    """Verify effective-policy tracing for override, type-rule, and fallback cases."""
    counter_cls: type = extradite(
        TARGET,
        transport_policy="value",
        transport_type_rules={PicklablePayload: "reference"},
    )
    counter = None
    try:
        counter = counter_cls(1)

        from_type_rule: str = counter_cls.get_effective_policy(PicklablePayload(1))
        assert from_type_rule == "reference"

        type_trace: dict[str, str | None] = counter_cls.get_effective_policy_trace(PicklablePayload(1))
        assert type_trace["effective_policy"] == "reference"
        assert type_trace["source"] == "type_rule"
        assert type_trace["matched_type"] == f"{PicklablePayload.__module__}.{PicklablePayload.__qualname__}"

        override_trace: dict[str, str | None] = counter.get_effective_policy_trace(
            PicklablePayload(2),
            call_policy="value",
        )
        assert override_trace["effective_policy"] == "value"
        assert override_trace["source"] == "call_override"
        assert override_trace["matched_type"] is None

        fallback_trace: dict[str, str | None] = counter.get_effective_policy_trace(AlternatePayload(3))
        assert fallback_trace["effective_policy"] == "value"
        assert fallback_trace["source"] == "session_default"
        assert fallback_trace["matched_type"] is None
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_invalid_transport_type_rules_fail_fast() -> None:
    """Invalid per-type rules should fail fast during proxy creation."""
    with pytest.raises(TypeError, match="transport_type_rules keys must be type objects"):
        extradite(  # type: ignore[arg-type]
            TARGET,
            transport_type_rules={"not-a-type": "reference"},
        )
    with pytest.raises(ValueError, match="transport_policy"):
        extradite(
            TARGET,
            transport_type_rules={PicklablePayload: "bad-policy"},
        )


def test_method_batch_calls_preserve_order_and_results() -> None:
    """Batch method calls should preserve call order and returned values."""
    counter_cls: type = extradite(TARGET)
    counter = None
    try:
        counter = counter_cls(1)
        increment_obj: object = counter.increment
        increment_batch_obj: object = getattr(increment_obj, "batch")
        increment_batch_is_callable: bool = callable(increment_batch_obj)
        if increment_batch_is_callable is False:
            raise TypeError("increment.batch must be callable")
        increment_batch: Callable[[list[tuple[list[object], dict[str, object]]]], object] = increment_batch_obj  # type: ignore[assignment]

        results_obj: object = increment_batch(
            [
                ([1], {}),
                ([2], {}),
                ([3], {}),
            ]
        )
        assert isinstance(results_obj, list) is True
        assert results_obj == [2, 4, 7]
        assert counter.value == 7
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_method_batch_calls_short_circuit_on_first_error() -> None:
    """Batch method calls should stop on first failing call and preserve prior state."""
    counter_cls: type = extradite(TARGET)
    counter = None
    try:
        counter = counter_cls(1)
        increment_obj: object = counter.increment
        increment_batch_obj: object = getattr(increment_obj, "batch")
        increment_batch_is_callable: bool = callable(increment_batch_obj)
        if increment_batch_is_callable is False:
            raise TypeError("increment.batch must be callable")
        increment_batch: Callable[[list[tuple[list[object], dict[str, object]]]], object] = increment_batch_obj  # type: ignore[assignment]

        with pytest.raises(ExtraditeRemoteError) as exc_info:
            increment_batch(
                [
                    ([1], {}),
                    (["bad"], {}),
                    ([1], {}),
                ]
            )
        error_obj: ExtraditeRemoteError = exc_info.value
        assert error_obj.remote_type_name == "TypeError"
        assert counter.value == 2
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_method_batch_call_policy_override() -> None:
    """Batch calls should honor explicit call-policy override semantics."""
    counter_cls: type = extradite(
        TARGET,
        transport_policy="value",
        transport_type_rules={PicklablePayload: "reference"},
    )
    counter = None
    try:
        counter = counter_cls(1)
        echo_obj: object = counter.echo
        echo_batch_obj: object = getattr(echo_obj, "batch")
        echo_batch_is_callable: bool = callable(echo_batch_obj)
        if echo_batch_is_callable is False:
            raise TypeError("echo.batch must be callable")
        echo_batch: Callable[[list[tuple[list[object], dict[str, object]]], str | None], object] = echo_batch_obj  # type: ignore[assignment]

        first_payload = PicklablePayload(11)
        second_payload = PicklablePayload(22)

        default_results_obj: object = echo_batch(
            [
                ([first_payload], {}),
                ([second_payload], {}),
            ],
            None,
        )
        assert isinstance(default_results_obj, list) is True
        assert len(default_results_obj) == 2
        assert default_results_obj[0] is first_payload
        assert default_results_obj[1] is second_payload

        override_results_obj: object = echo_batch(
            [
                ([first_payload], {}),
                ([second_payload], {}),
            ],
            "value",
        )
        assert isinstance(override_results_obj, list) is True
        assert len(override_results_obj) == 2
        first_result_obj: object = override_results_obj[0]
        second_result_obj: object = override_results_obj[1]
        assert isinstance(first_result_obj, PicklablePayload) is True
        assert isinstance(second_result_obj, PicklablePayload) is True
        assert first_result_obj is not first_payload
        assert second_result_obj is not second_payload
        assert first_result_obj.value == first_payload.value
        assert second_result_obj.value == second_payload.value
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_callback_roundtrip_reentrant() -> None:
    """Ensure callback success and failure paths propagate through re-entrant RPC."""
    counter_cls: type = extradite(TARGET)
    counter = None
    try:
        counter = counter_cls(1)

        def ok_callback(value: int) -> int:
            """Return callback success value.

            :param value: Input value.
            :returns: Result value.
            """
            return value + 5

        ok_result: object = counter.callback_value(ok_callback, 10)
        assert ok_result == 15

        def failing_callback(value: int) -> int:
            """Raise one deterministic callback exception.

            :param value: Input value.
            :returns: Never returns.
            :raises RuntimeError: Always.
            """
            raise RuntimeError(f"callback-failed-{value}")

        with pytest.raises(ExtraditeRemoteError) as exc_info:
            counter.callback_value(failing_callback, 7)

        error_obj: ExtraditeRemoteError = exc_info.value
        assert error_obj.remote_type_name == "RuntimeError"
        assert error_obj.remote_message == "callback-failed-7"
        traceback_contains_name: bool = "failing_callback" in error_obj.remote_traceback
        assert traceback_contains_name is True
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_nested_rpc_no_deadlock() -> None:
    """Ensure nested caller->isolated->caller->isolated flow completes without deadlock."""
    counter_cls: type = extradite(TARGET)
    counter = None
    try:
        counter = counter_cls(2)

        def callback(delta: int) -> int:
            """Perform nested call back into the isolated object.

            :param delta: Increment amount.
            :returns: Updated counter value.
            """
            result_obj: object = counter.increment(delta)
            if isinstance(result_obj, int) is False:
                raise TypeError("Nested increment did not return int")
            return result_obj

        result_holder: dict[str, object] = {}

        def invoke() -> None:
            """Invoke callback_value and capture result."""
            try:
                result_holder["result"] = counter.callback_value(callback, 3)
            except Exception as exc:
                result_holder["error"] = exc
                result_holder["error_traceback"] = traceback.format_exc()

        thread = threading.Thread(target=invoke)
        thread.start()
        thread.join(timeout=5.0)

        is_alive: bool = thread.is_alive()
        assert is_alive is False

        has_error: bool = "error" in result_holder
        assert has_error is False
        assert result_holder.get("result") == 5
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_identity_preserved_for_handles() -> None:
    """Ensure repeated references to one local object preserve identity remotely."""
    counter_cls: type = extradite(TARGET)
    counter = None

    class LocalObject:
        """Function-local object used to force handle mode."""

    try:
        counter = counter_cls(0)
        shared_obj = LocalObject()
        different_obj = LocalObject()

        shared_identity: object = counter.compare_identity(shared_obj, shared_obj)
        different_identity: object = counter.compare_identity(shared_obj, different_obj)

        assert shared_identity is True
        assert different_identity is False
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_release_semantics() -> None:
    """Release a remote handle and ensure further access raises deterministic errors."""
    counter_cls: type = extradite(TARGET)
    counter = None
    lock_handle = None
    try:
        counter = counter_cls(1)
        lock_handle = counter.make_non_picklable_native_value()

        before_close_repr: str = repr(lock_handle)
        assert len(before_close_repr) > 0

        lock_handle.close()

        with pytest.raises(ExtraditeProtocolError, match="Unknown or released remote object id"):
            repr(lock_handle)
    finally:
        if lock_handle is not None:
            try:
                lock_handle.close()
            except (ExtraditeProtocolError, ExtraditeRemoteError, OSError):
                pass
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_exception_fidelity() -> None:
    """Validate remote exception wrapper fidelity fields."""
    counter_cls: type = extradite(TARGET)
    counter = None
    try:
        counter = counter_cls(1)

        with pytest.raises(ExtraditeRemoteError) as exc_info:
            counter.raise_custom_error("boom-value")

        error_obj: ExtraditeRemoteError = exc_info.value
        assert error_obj.remote_type_name == "SandboxRaisedError"
        assert error_obj.remote_message == "boom-value"
        traceback_contains_method: bool = "raise_custom_error" in error_obj.remote_traceback
        assert traceback_contains_method is True
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_protected_module_leak_block() -> None:
    """Values from the protected module tree must not cross the value-transfer barrier."""
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


def test_protected_module_import_before_bootstrap_fails_fast() -> None:
    """Importing the protected module before bootstrap must fail fast."""
    importlib.import_module(TARGET_MODULE)
    with pytest.raises(ExtraditeModuleLeakError):
        extradite(TARGET)


def test_protected_module_import_after_bootstrap_fails_on_next_boundary_call() -> None:
    """Importing the protected module after bootstrap must fail at the next call boundary."""
    counter_cls: type = extradite(TARGET)
    counter = None
    try:
        counter = counter_cls(3)
        baseline: object = counter.increment(1)
        assert baseline == 4

        importlib.import_module(TARGET_MODULE)
        with pytest.raises(ExtraditeModuleLeakError):
            counter.increment(1)
    finally:
        _purge_module(TARGET_MODULE)
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_protected_module_scan_fast_path_when_import_state_is_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Skip repeated leak scans when root-process import state has not changed."""
    counter_cls: type = extradite(TARGET)
    counter = None
    find_calls: int = 0
    original_find_module_leaks: object = runtime._find_module_leaks

    def _counting_find(module_name: str) -> list[str]:
        """Count leak-scan invocations while delegating to the real scanner.

        :param module_name: Protected module name.
        :returns: Matching leaked module list.
        """
        nonlocal find_calls
        find_calls += 1
        if callable(original_find_module_leaks) is False:
            raise TypeError("original leak scanner must be callable")
        return original_find_module_leaks(module_name)  # type: ignore[operator]

    monkeypatch.setattr(runtime, "_find_module_leaks", _counting_find)
    try:
        counter = counter_cls(1)
        warmup: object = counter.increment(0)
        assert warmup == 1
        find_calls_after_warmup: int = find_calls

        for _ in range(50):
            counter.increment(1)

        assert find_calls == find_calls_after_warmup
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_bootstrap_path_never_imports_protected_module_in_root() -> None:
    """Bootstrapping and using proxies should not import protected modules in root."""
    module_loaded_before: bool = TARGET_MODULE in sys.modules
    assert module_loaded_before is False

    counter_cls: type = extradite(TARGET)
    counter = None
    try:
        counter = counter_cls(3)
        counter.increment(2)
        module_loaded_after: bool = TARGET_MODULE in sys.modules
        assert module_loaded_after is False
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


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


def test_threaded_concurrency() -> None:
    """Concurrent caller threads should remain safe and consistent."""
    counter_cls: type = extradite(TARGET, share_key="threaded")
    counter = None
    try:
        counter = counter_cls(0)
        thread_count: int = 8
        increments_per_thread: int = 30
        errors: list[BaseException] = []
        error_traces: list[str] = []

        def run_increments() -> None:
            """Apply repeated increments and capture failures."""
            try:
                for _index in range(increments_per_thread):
                    counter.increment(1)
            except Exception as exc:
                errors.append(exc)
                error_traces.append(traceback.format_exc())

        threads: list[threading.Thread] = []
        for _index in range(thread_count):
            thread = threading.Thread(target=run_increments)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join(timeout=10.0)
            still_alive: bool = thread.is_alive()
            assert still_alive is False

        assert len(errors) == 0
        assert len(error_traces) == 0
        final_value: object = counter.value
        assert final_value == thread_count * increments_per_thread
    finally:
        if counter is not None:
            counter.close()
        counter_cls.close()


def test_worker_termination_recovery() -> None:
    """Killing the worker mid-request should yield fast protocol errors and clean teardown."""
    counter_cls: type = extradite(TARGET)
    counter = None
    try:
        counter = counter_cls(1)
        worker_pid_obj: object = counter.worker_pid()
        if isinstance(worker_pid_obj, int) is False:
            raise TypeError("worker_pid() did not return int")
        worker_pid: int = worker_pid_obj

        result_holder: dict[str, object] = {}

        def invoke_sleep_call() -> None:
            """Invoke a slow remote method and capture the outcome."""
            try:
                result_holder["result"] = counter.sleep_then_increment(10.0, 1)
            except Exception as exc:
                result_holder["error"] = exc
                result_holder["error_traceback"] = traceback.format_exc()

        thread = threading.Thread(target=invoke_sleep_call)
        thread.start()

        time.sleep(0.4)
        os.kill(worker_pid, signal.SIGKILL)

        thread.join(timeout=5.0)
        still_alive: bool = thread.is_alive()
        assert still_alive is False

        has_error: bool = "error" in result_holder
        assert has_error is True
        error_obj: object = result_holder.get("error")
        assert isinstance(error_obj, ExtraditeProtocolError) is True

        counter_cls.close()
    finally:
        if counter is not None:
            try:
                counter.close()
            except (ExtraditeProtocolError, ExtraditeRemoteError, OSError):
                pass
        counter_cls.close()


def test_main_and_subinterpreter_parity() -> None:
    """Run the same integration probe in main and subinterpreter contexts."""
    executor_class_obj: object = _INTERPRETER_POOL_EXECUTOR
    has_interpreter_pool: bool = executor_class_obj is not None
    if has_interpreter_pool is False:
        pytest.skip("InterpreterPoolExecutor is unavailable in this Python runtime")

    main_result: dict[str, object] = _integration_probe("main")

    executor_class = executor_class_obj
    with executor_class(max_workers=1) as executor:  # type: ignore[operator]
        future: concurrent.futures.Future[dict[str, object]] = executor.submit(_integration_probe, "sub")
        sub_result: dict[str, object] = future.result(timeout=30.0)

    assert main_result == sub_result
