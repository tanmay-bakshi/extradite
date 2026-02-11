"""Workload module that depends on a native extension with subinterpreter limits."""

import hashlib
import readline
from typing import ClassVar


class NativeDependencyWorkload:
    """Deterministic CPU workload backed by native modules."""

    MODULUS: ClassVar[int] = 1_000_000_007

    _salt_bytes: bytes
    _rounds: int

    def __init__(self, salt: str, rounds: int = 12_000) -> None:
        """Initialize the workload instance.

        :param salt: Salt used for deterministic digest derivation.
        :param rounds: PBKDF2 rounds per score derivation.
        :raises ValueError: If ``rounds`` is less than ``1``.
        """
        if rounds < 1:
            raise ValueError("rounds must be >= 1")
        self._salt_bytes = salt.encode("utf-8")
        self._rounds = rounds

    @classmethod
    def native_dependency_name(cls) -> str:
        """Return the native dependency that blocks direct subinterpreter imports.

        :returns: Native module name.
        """
        dependency_name: str = readline.__name__
        return dependency_name

    def derive_score(self, label: str, index: int) -> int:
        """Compute a deterministic integer score.

        :param label: Logical label for the score stream.
        :param index: Item index inside the score stream.
        :returns: Deterministic integer score.
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
        score: int = (left ^ right) % self.MODULUS
        return score

    def readline_history_snapshot(self, label: str, start_index: int, item_count: int) -> dict[str, object]:
        """Exercise readline APIs and return a deterministic summary.

        :param label: Logical label for history items.
        :param start_index: First index to include.
        :param item_count: Number of history items to add.
        :returns: Snapshot of readline state derived from inserted history.
        :raises ValueError: If ``item_count`` is negative.
        """
        if item_count < 0:
            raise ValueError("item_count must be >= 0")

        original_delimiters: str = readline.get_completer_delims()
        augmented_delimiters: str = f"{original_delimiters}|"
        readline.set_completer_delims(augmented_delimiters)
        readline.parse_and_bind("tab: complete")

        readline.clear_history()
        for offset in range(item_count):
            current_index: int = start_index + offset
            history_entry: str = f"{label}:{current_index}"
            readline.add_history(history_entry)

        history_length: int = readline.get_current_history_length()
        first_entry: str | None = None
        last_entry: str | None = None
        if history_length > 0:
            first_obj: object = readline.get_history_item(1)
            if isinstance(first_obj, str) is True:
                first_entry = first_obj
            last_obj: object = readline.get_history_item(history_length)
            if isinstance(last_obj, str) is True:
                last_entry = last_obj

        active_delimiters: str = readline.get_completer_delims()
        has_pipe_delimiter: bool = "|" in active_delimiters
        digest_source: str = (
            f"{history_length}|"
            + f"{first_entry}|"
            + f"{last_entry}|"
            + active_delimiters
        )
        digest: str = hashlib.sha256(digest_source.encode("utf-8")).hexdigest()

        readline.set_completer_delims(original_delimiters)
        readline.clear_history()

        return {
            "history_length": history_length,
            "first_entry": first_entry,
            "last_entry": last_entry,
            "has_pipe_delimiter": has_pipe_delimiter,
            "digest": digest,
        }

    def compute_checksum(self, label: str, start_index: int, item_count: int) -> int:
        """Compute a checksum across a contiguous score range.

        :param label: Logical label for the score stream.
        :param start_index: First index to include.
        :param item_count: Number of items to include.
        :returns: Deterministic checksum modulo ``MODULUS``.
        :raises ValueError: If ``item_count`` is negative.
        """
        if item_count < 0:
            raise ValueError("item_count must be >= 0")

        checksum: int = 0
        for offset in range(item_count):
            current_index: int = start_index + offset
            score: int = self.derive_score(label, current_index)
            checksum = (checksum + score) % self.MODULUS
        return checksum
