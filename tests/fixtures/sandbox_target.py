"""Target module imported only in extradite child processes during tests."""

import os
import threading
import time
from collections.abc import Callable


class ModuleOnlyValue:
    """Local class that must never cross the extradite pickle barrier."""

    payload: int

    def __init__(self, payload: int) -> None:
        """Initialize the value.

        :param payload: Integer payload.
        """
        self.payload = payload


class SandboxRaisedError(Exception):
    """Custom exception class raised by fixture methods."""


class IsolatedCounter:
    """Simple class used to validate extradite behavior."""

    class_level: int = 7
    value: int
    tag: str

    def __init__(self, value: int, tag: str = "counter") -> None:
        """Initialize the counter.

        :param value: Initial numeric value.
        :param tag: Human-readable label.
        """
        self.value = value
        self.tag = tag

    @classmethod
    def cls_name(cls) -> str:
        """Return the class name.

        :returns: Class name.
        """
        return cls.__name__

    def increment(self, delta: int = 1) -> int:
        """Increment the internal counter.

        :param delta: Amount to add.
        :returns: Updated value.
        """
        self.value += delta
        return self.value

    def sum_with_peer(self, peer: "IsolatedCounter") -> int:
        """Combine this counter with another instance.

        :param peer: Other counter instance.
        :returns: Sum of values.
        """
        return self.value + peer.value

    def callback_value(self, callback: object, value: int) -> int:
        """Invoke a callback and return its integer result.

        :param callback: Callback object supplied by the caller process.
        :param value: Input value passed to the callback.
        :returns: Integer callback result.
        :raises TypeError: If ``callback`` is not callable or result is not ``int``.
        """
        is_callable: bool = callable(callback)
        if is_callable is False:
            raise TypeError("callback must be callable")
        result: object = callback(value)  # type: ignore[operator]
        if isinstance(result, int) is False:
            raise TypeError("callback must return int")
        return result

    def callback_accumulate(self, callback: object, item_count: int) -> int:
        """Call ``callback`` repeatedly and accumulate integer results.

        :param callback: Callback object supplied by the caller process.
        :param item_count: Number of callback invocations.
        :returns: Sum of callback return values.
        :raises TypeError: If callback is not callable or returns non-integer values.
        :raises ValueError: If ``item_count`` is negative.
        """
        if item_count < 0:
            raise ValueError("item_count must be >= 0")

        callback_is_callable: bool = callable(callback)
        if callback_is_callable is False:
            raise TypeError("callback must be callable")

        batch_attr_obj: object = getattr(callback, "batch", None)
        batch_is_callable: bool = callable(batch_attr_obj)

        total: int = 0
        if batch_is_callable is False:
            callback_method: Callable[[int], object] = callback  # type: ignore[assignment]
            for index in range(item_count):
                result_obj: object = callback_method(index)
                if isinstance(result_obj, int) is False:
                    raise TypeError("callback must return int")
                total += result_obj
            return total

        batch_method: Callable[[list[tuple[list[object], dict[str, object]]]], object] = batch_attr_obj  # type: ignore[assignment]
        batch_size: int = 32
        completed: int = 0
        while completed < item_count:
            remaining: int = item_count - completed
            current_batch_size: int = batch_size
            if remaining < batch_size:
                current_batch_size = remaining

            call_specs: list[tuple[list[object], dict[str, object]]] = []
            for offset in range(current_batch_size):
                index: int = completed + offset
                call_specs.append(([index], {}))

            result_list_obj: object = batch_method(call_specs)
            if isinstance(result_list_obj, list) is False:
                raise TypeError("callback.batch must return list")
            if len(result_list_obj) != current_batch_size:
                raise TypeError("callback.batch must return one result per call")

            for result_obj in result_list_obj:
                if isinstance(result_obj, int) is False:
                    raise TypeError("callback must return int")
                total += result_obj
            completed += current_batch_size
        return total

    def callback_from_payload(self, payload: list[object], value: int) -> int:
        """Invoke the last payload item as a callback and return its integer result.

        :param payload: Payload list whose final item must be callable.
        :param value: Input value passed to the callback.
        :returns: Integer callback result.
        :raises ValueError: If ``payload`` is empty.
        :raises TypeError: If callback is not callable or result is not integer.
        """
        payload_length: int = len(payload)
        if payload_length == 0:
            raise ValueError("payload must contain at least one item")

        callback_obj: object = payload[payload_length - 1]
        callback_is_callable: bool = callable(callback_obj)
        if callback_is_callable is False:
            raise TypeError("payload last item must be callable")

        result_obj: object = callback_obj(value)  # type: ignore[operator]
        if isinstance(result_obj, int) is False:
            raise TypeError("payload callback must return int")
        return result_obj

    def set_nested_item_value(self, payload: dict[str, object], updated_value: int) -> int:
        """Set ``payload['items'][0].value`` and return the updated integer value.

        :param payload: Payload containing key ``items`` mapped to a list-like value.
        :param updated_value: Integer assigned to ``items[0].value``.
        :returns: The resulting integer value from ``items[0].value``.
        :raises TypeError: If payload structure is invalid.
        """
        items_obj: object = payload.get("items")
        if isinstance(items_obj, list) is False:
            raise TypeError("payload['items'] must be a list")
        if len(items_obj) < 1:
            raise TypeError("payload['items'] must contain at least one item")

        first_obj: object = items_obj[0]
        setattr(first_obj, "value", updated_value)
        value_obj: object = getattr(first_obj, "value")
        if isinstance(value_obj, int) is False:
            raise TypeError("payload['items'][0].value must be int")
        return value_obj

    def echo(self, value: object) -> object:
        """Return one value unchanged.

        :param value: Input value.
        :returns: The same value.
        """
        return value

    def class_handle_repr(self, class_handle: object) -> str:
        """Return ``repr`` for a class handle.

        :param class_handle: Class-like object.
        :returns: Representation string.
        """
        return repr(class_handle)

    def class_handle_isinstance(self, value: object, class_handle: object) -> bool:
        """Evaluate ``isinstance`` with a parent-origin class handle.

        :param value: Candidate instance.
        :param class_handle: Class-like object.
        :returns: ``True`` when ``value`` is an instance of ``class_handle``.
        """
        return isinstance(value, class_handle)  # type: ignore[arg-type]

    def class_handle_issubclass(self, subclass: object, class_handle: object) -> bool:
        """Evaluate ``issubclass`` with a parent-origin class handle.

        :param subclass: Candidate subclass.
        :param class_handle: Class-like object.
        :returns: ``True`` when ``subclass`` is a subclass of ``class_handle``.
        """
        return issubclass(subclass, class_handle)  # type: ignore[arg-type]

    def class_handle_requires_real_type(self, class_handle: object) -> bool:
        """Exercise a code path that requires ``class_handle`` to be a real type object.

        :param class_handle: Candidate class handle.
        :returns: ``True`` when dynamic subclass creation succeeds.
        :raises TypeError: If ``class_handle`` is not a real type object.
        """
        derived: type = type("DerivedFromHandle", (class_handle,), {})  # type: ignore[arg-type]
        return issubclass(derived, class_handle)  # type: ignore[arg-type]

    def class_handle_schema_like_constructor(self, schema: dict[str, object]) -> object:
        """Exercise schema-like payload handling that expects a real type object.

        :param schema: Schema-like dictionary containing ``cls`` and ``kwargs``.
        :returns: Constructed instance from ``schema['cls'](**schema['kwargs'])``.
        :raises TypeError: If ``schema`` does not contain the expected payload types.
        """
        class_handle: object = schema["cls"]
        kwargs_obj: object = schema["kwargs"]
        if isinstance(kwargs_obj, dict) is False:
            raise TypeError("schema['kwargs'] must be a dict")

        _ = type("DerivedFromSchema", (class_handle,), {})  # type: ignore[arg-type]
        return class_handle(**kwargs_obj)  # type: ignore[arg-type,operator]

    def class_handle_missing_attribute_uses_attribute_error(self, class_handle: object) -> bool:
        """Evaluate ``hasattr`` against parent-origin class handles.

        :param class_handle: Candidate class handle.
        :returns: Result of ``hasattr(class_handle, "__slots__")``.
        """
        return hasattr(class_handle, "__slots__")

    def inspect_marker(self, payload: object) -> str:
        """Read ``payload.marker`` and return it as a string.

        :param payload: Arbitrary object supplied by the caller process.
        :returns: Marker string.
        :raises AttributeError: If ``payload`` does not expose ``marker``.
        """
        marker_obj: object = getattr(payload, "marker")
        if isinstance(marker_obj, str) is True:
            return marker_obj
        return str(marker_obj)

    def compare_identity(self, first: object, second: object) -> bool:
        """Return whether two values are identical.

        :param first: First value.
        :param second: Second value.
        :returns: ``True`` when both values are the same object.
        """
        return first is second

    def raise_custom_error(self, message: str) -> None:
        """Raise a fixture-defined exception.

        :param message: Error message text.
        :raises SandboxRaisedError: Always.
        """
        raise SandboxRaisedError(message)

    def make_non_picklable_native_value(self) -> object:
        """Return an unpicklable value that does not originate from this module tree.

        :returns: Native lock object.
        """
        return threading.Lock()

    def sleep_then_increment(self, delay_seconds: float, delta: int = 1) -> int:
        """Sleep and then increment.

        :param delay_seconds: Sleep duration in seconds.
        :param delta: Increment amount.
        :returns: Updated value.
        """
        time.sleep(delay_seconds)
        self.value += delta
        return self.value

    def make_unpicklable(self) -> object:
        """Return an unpicklable value.

        :returns: Lambda value.
        """
        return lambda x: x + 1

    def make_module_value(self) -> ModuleOnlyValue:
        """Return a value that originates from this module.

        :returns: Module-only value instance.
        """
        return ModuleOnlyValue(self.value)

    def __len__(self) -> int:
        """Return a deterministic length.

        :returns: Current counter value.
        """
        return self.value

    def __repr__(self) -> str:
        """Return representation string.

        :returns: Representation string.
        """
        return f"IsolatedCounter(value={self.value}, tag={self.tag!r})"

    def worker_pid(self) -> int:
        """Return the current worker process identifier.

        :returns: Worker process identifier.
        """
        return os.getpid()
