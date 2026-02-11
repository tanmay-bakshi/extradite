"""Target module imported only in extradite child processes during tests."""

import os


class ModuleOnlyValue:
    """Local class that must never cross the extradite pickle barrier."""

    payload: int

    def __init__(self, payload: int) -> None:
        """Initialize the value.

        :param payload: Integer payload.
        """
        self.payload = payload


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
