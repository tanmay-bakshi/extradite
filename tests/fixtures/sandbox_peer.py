"""Additional target module used for shared-session tests."""

import os


class PeerCounter:
    """Simple peer counter used to validate shared sessions."""

    value: int

    def __init__(self, value: int) -> None:
        """Initialize the counter.

        :param value: Initial numeric value.
        """
        self.value = value

    def increment(self, delta: int = 1) -> int:
        """Increment the internal value.

        :param delta: Amount to add.
        :returns: Updated value.
        """
        self.value += delta
        return self.value

    def worker_pid(self) -> int:
        """Return the current worker process identifier.

        :returns: Worker process identifier.
        """
        return os.getpid()
