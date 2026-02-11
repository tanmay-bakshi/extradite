"""Target module imported only in extradite child processes during tests."""

import os
import threading
import time


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
