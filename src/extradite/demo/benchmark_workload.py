"""Benchmark workload module for direct-vs-extradite performance comparisons."""

import hashlib
import readline
from typing import ClassVar


class BenchmarkWorkload:
    """Deterministic workload with diverse operational profiles for benchmarking."""

    MODULUS: ClassVar[int] = 1_000_000_007

    _salt_bytes: bytes
    _rounds: int
    _value: int

    def __init__(self, seed: str, rounds: int = 2_000) -> None:
        """Initialize benchmark state.

        :param seed: Salt seed used for deterministic digest derivation.
        :param rounds: PBKDF2 rounds used by hash-heavy methods.
        :raises ValueError: If ``rounds`` is smaller than ``1``.
        """
        if rounds < 1:
            raise ValueError("rounds must be >= 1")
        self._salt_bytes = seed.encode("utf-8")
        self._rounds = rounds
        self._value = 0

    def ping(self) -> int:
        """Return a tiny deterministic value for micro-call overhead tests.

        :returns: Deterministic integer based on seed length.
        """
        return len(self._salt_bytes)

    def set_value(self, value: int) -> int:
        """Set the internal integer state.

        :param value: New state value.
        :returns: Stored value.
        """
        self._value = int(value)
        return self._value

    def current_value(self) -> int:
        """Return the current integer state.

        :returns: Current state value.
        """
        return self._value

    def increment(self, delta: int = 1) -> int:
        """Increment the internal integer state.

        :param delta: Increment amount.
        :returns: Updated state value.
        """
        self._value += delta
        return self._value

    def payload_checksum(self, payload: dict[str, int | float | str]) -> int:
        """Compute a deterministic checksum for structured payloads.

        :param payload: Mapping with primitive scalar values.
        :returns: Deterministic checksum modulo ``MODULUS``.
        """
        checksum: int = 0
        ordered_keys: list[str] = sorted(payload.keys())
        for key in ordered_keys:
            value_obj: int | float | str = payload[key]
            serialized: str = f"{key}={value_obj}"
            digest: bytes = hashlib.sha256(serialized.encode("utf-8")).digest()
            score: int = int.from_bytes(digest[0:8], "big")
            checksum = (checksum + score) % self.MODULUS
        return checksum

    def sum_values(self, values: list[int]) -> int:
        """Sum integer values using Python-level iteration.

        :param values: Integer payload.
        :returns: Arithmetic sum.
        """
        total: int = 0
        for item in values:
            total += item
        return total

    def echo_bytes(self, payload: bytes) -> bytes:
        """Return a bytes payload unchanged.

        :param payload: Bytes payload.
        :returns: The same payload bytes.
        """
        return payload

    def callback_accumulate(self, callback: object, item_count: int) -> int:
        """Call a callback repeatedly and accumulate integer results.

        :param callback: Callback that accepts one ``int`` and returns one ``int``.
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
            for index in range(item_count):
                result_obj: object = callback(index)  # type: ignore[operator]
                if isinstance(result_obj, int) is False:
                    raise TypeError("callback must return int")
                total += result_obj
            return total

        batch_method: object = batch_attr_obj
        batch_size: int = 64
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
            result_list_obj: object = batch_method(call_specs)  # type: ignore[operator]
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

    def derive_score(self, label: str, index: int) -> int:
        """Compute a deterministic PBKDF2-derived score.

        :param label: Label used in score derivation.
        :param index: Item index in the score stream.
        :returns: Deterministic score modulo ``MODULUS``.
        """
        payload_text: str = f"{label}:{index}"
        payload_bytes: bytes = payload_text.encode("utf-8")
        digest: bytes = hashlib.pbkdf2_hmac(
            "sha256",
            payload_bytes,
            self._salt_bytes,
            self._rounds,
            dklen=32,
        )
        left: int = int.from_bytes(digest[0:8], "big")
        right: int = int.from_bytes(digest[8:16], "big")
        return (left ^ right) % self.MODULUS

    def compute_checksum(self, label: str, start_index: int, item_count: int) -> int:
        """Compute a checksum over a contiguous score range.

        :param label: Label used in score derivation.
        :param start_index: First index in the range.
        :param item_count: Number of items in the range.
        :returns: Deterministic checksum modulo ``MODULUS``.
        :raises ValueError: If ``item_count`` is negative.
        """
        if item_count < 0:
            raise ValueError("item_count must be >= 0")

        checksum: int = 0
        for offset in range(item_count):
            current_index: int = start_index + offset
            checksum = (checksum + self.derive_score(label, current_index)) % self.MODULUS
        return checksum

    def readline_history_digest(self, label: str, start_index: int, item_count: int) -> str:
        """Exercise ``readline`` state and return a deterministic digest.

        :param label: Label used in history entries.
        :param start_index: First history index.
        :param item_count: Number of history items to create.
        :returns: Hex digest summarizing history state.
        :raises ValueError: If ``item_count`` is negative.
        """
        if item_count < 0:
            raise ValueError("item_count must be >= 0")

        original_delimiters: str = readline.get_completer_delims()
        augmented_delimiters: str = f"{original_delimiters}|"
        readline.set_completer_delims(augmented_delimiters)
        readline.parse_and_bind("tab: complete")
        readline.clear_history()
        try:
            for offset in range(item_count):
                current_index: int = start_index + offset
                entry: str = f"{label}:{current_index}"
                readline.add_history(entry)

            history_length: int = readline.get_current_history_length()
            first_item: str | None = None
            last_item: str | None = None
            if history_length > 0:
                first_obj: object = readline.get_history_item(1)
                if isinstance(first_obj, str) is True:
                    first_item = first_obj
                last_obj: object = readline.get_history_item(history_length)
                if isinstance(last_obj, str) is True:
                    last_item = last_obj

            active_delimiters: str = readline.get_completer_delims()
            digest_source: str = (
                f"{history_length}|"
                + f"{first_item}|"
                + f"{last_item}|"
                + active_delimiters
            )
            return hashlib.sha256(digest_source.encode("utf-8")).hexdigest()
        finally:
            readline.set_completer_delims(original_delimiters)
            readline.clear_history()

    def mixed_pipeline(
        self,
        label: str,
        start_index: int,
        item_count: int,
        payload_values: list[int],
    ) -> dict[str, object]:
        """Run a mixed real-world pipeline in a single coarse-grained call.

        :param label: Label used for hashing and readline payloads.
        :param start_index: First index in checksum/history loops.
        :param item_count: Number of items used by checksum/history loops.
        :param payload_values: Integer payload used in list aggregation.
        :returns: Structured result summary.
        """
        checksum: int = self.compute_checksum(label, start_index, item_count)
        payload_sum: int = self.sum_values(payload_values)

        readline_item_count: int = item_count
        if item_count > 64:
            readline_item_count = 64
        readline_digest: str = self.readline_history_digest(
            label,
            start_index,
            readline_item_count,
        )

        current_value: int = self.increment(item_count)
        return {
            "checksum": checksum,
            "payload_sum": payload_sum,
            "readline_digest": readline_digest,
            "current_value": current_value,
        }
