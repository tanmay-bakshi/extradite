"""Comprehensive performance benchmark for direct vs extradite execution."""

import argparse
import hashlib
import importlib
import json
import os
import pathlib
import statistics
import subprocess
import sys
import time
from collections.abc import Callable
from typing import Literal

BENCHMARK_TARGET_MODULE: str = "extradite.demo.benchmark_workload"
BENCHMARK_TARGET_CLASS: str = "BenchmarkWorkload"
BENCHMARK_TARGET: str = f"{BENCHMARK_TARGET_MODULE}:{BENCHMARK_TARGET_CLASS}"
BenchmarkMode = Literal["direct", "extradite"]
CaseRunner = Callable[[object, int], dict[str, object]]
REPO_ROOT: pathlib.Path = pathlib.Path(__file__).resolve().parent.parent
SCRIPT_PATH: pathlib.Path = pathlib.Path(__file__).resolve()
_SEED: str = "extradite-benchmark-seed"

_CASE_ORDER: list[str] = [
    "cold_start_first_call",
    "tiny_ping",
    "increment_small",
    "payload_small_checksum",
    "payload_large_sum",
    "bytes_roundtrip_512kb",
    "callback_chatter",
    "readline_digest",
    "cpu_checksum",
    "mixed_pipeline",
]

_CASE_DESCRIPTIONS: dict[str, str] = {
    "cold_start_first_call": (
        "Cold start: construct workload and run one tiny call per iteration (startup-inclusive)."
    ),
    "tiny_ping": "Tiny call overhead with scalar return values.",
    "increment_small": "State mutation via many small integer method calls.",
    "payload_small_checksum": "Small structured payload marshaling plus light compute.",
    "payload_large_sum": "Large list payload marshaling and list traversal.",
    "bytes_roundtrip_512kb": "Large bytes round-trip transfer (512 KiB).",
    "callback_chatter": "Callback-heavy, re-entrant cross-boundary interaction.",
    "readline_digest": "Native dependency (`readline`) stateful operations.",
    "cpu_checksum": "CPU-heavy PBKDF2 checksum operations.",
    "mixed_pipeline": "Coarse-grained real-world pipeline (hashing + payload + readline).",
}

_CASE_DEFAULT_ITERATIONS: dict[str, int] = {
    "cold_start_first_call": 20,
    "tiny_ping": 20_000,
    "increment_small": 15_000,
    "payload_small_checksum": 5_000,
    "payload_large_sum": 500,
    "bytes_roundtrip_512kb": 120,
    "callback_chatter": 400,
    "readline_digest": 400,
    "cpu_checksum": 80,
    "mixed_pipeline": 180,
}

_CASE_QUICK_ITERATIONS: dict[str, int] = {
    "cold_start_first_call": 4,
    "tiny_ping": 2_000,
    "increment_small": 1_500,
    "payload_small_checksum": 500,
    "payload_large_sum": 50,
    "bytes_roundtrip_512kb": 20,
    "callback_chatter": 40,
    "readline_digest": 40,
    "cpu_checksum": 10,
    "mixed_pipeline": 20,
}

_CASE_WARMUP_ITERATIONS: dict[str, int] = {
    "cold_start_first_call": 0,
    "tiny_ping": 500,
    "increment_small": 250,
    "payload_small_checksum": 200,
    "payload_large_sum": 20,
    "bytes_roundtrip_512kb": 5,
    "callback_chatter": 10,
    "readline_digest": 10,
    "cpu_checksum": 4,
    "mixed_pipeline": 6,
}

_CASE_INCLUDE_SETUP_IN_TIMING: dict[str, bool] = {
    "cold_start_first_call": True,
    "tiny_ping": False,
    "increment_small": False,
    "payload_small_checksum": False,
    "payload_large_sum": False,
    "bytes_roundtrip_512kb": False,
    "callback_chatter": False,
    "readline_digest": False,
    "cpu_checksum": False,
    "mixed_pipeline": False,
}


def _ensure_repo_paths() -> None:
    """Ensure repository-local imports are resolvable for this process."""
    src_path: str = str(REPO_ROOT / "src")
    root_path: str = str(REPO_ROOT)
    has_src_path: bool = src_path in sys.path
    if has_src_path is False:
        sys.path.insert(0, src_path)
    has_root_path: bool = root_path in sys.path
    if has_root_path is False:
        sys.path.insert(0, root_path)


def _summary_fingerprint(summary: dict[str, object]) -> int:
    """Compute a stable integer fingerprint for summary dictionaries.

    :param summary: Summary payload.
    :returns: Stable integer fingerprint.
    """
    encoded: bytes = json.dumps(summary, sort_keys=True).encode("utf-8")
    digest: bytes = hashlib.sha256(encoded).digest()
    return int.from_bytes(digest[0:8], "big")


def _create_workload(
    mode: BenchmarkMode,
    rounds: int,
) -> tuple[object, type | None]:
    """Create one benchmark workload object in the requested mode.

    :param mode: Execution mode.
    :param rounds: Hash rounds used by the workload.
    :returns: Tuple of ``(workload_instance, proxy_type_or_none)``.
    :raises TypeError: If the configured workload class cannot be resolved.
    """
    if mode == "direct":
        imported_module = importlib.import_module(BENCHMARK_TARGET_MODULE)
        workload_type_obj: object = getattr(imported_module, BENCHMARK_TARGET_CLASS)
        if isinstance(workload_type_obj, type) is False:
            raise TypeError(f"{BENCHMARK_TARGET} is not a class")
        workload_obj: object = workload_type_obj(_SEED, rounds=rounds)  # type: ignore[call-arg]
        return workload_obj, None

    from extradite import extradite

    proxy_type: type = extradite(BENCHMARK_TARGET)
    workload_obj = proxy_type(_SEED, rounds=rounds)
    return workload_obj, proxy_type


def _close_workload(workload: object, proxy_type: type | None) -> None:
    """Close benchmark resources for one workload object.

    :param workload: Workload instance.
    :param proxy_type: Extradite proxy type or ``None`` for direct mode.
    """
    close_method_obj: object = getattr(workload, "close", None)
    close_method_is_callable: bool = callable(close_method_obj)
    if close_method_is_callable is True:
        close_method_obj()  # type: ignore[operator]

    if proxy_type is None:
        return

    close_class_method_obj: object = getattr(proxy_type, "close", None)
    close_class_is_callable: bool = callable(close_class_method_obj)
    if close_class_is_callable is True:
        close_class_method_obj()  # type: ignore[operator]


def _require_int(value: object, field_name: str) -> int:
    """Require an integer value.

    :param value: Candidate value.
    :param field_name: Field label used in error messages.
    :returns: Integer value.
    :raises TypeError: If ``value`` is not ``int``.
    """
    if isinstance(value, int) is False:
        raise TypeError(f"{field_name} must be int")
    return value


def _require_str(value: object, field_name: str) -> str:
    """Require a string value.

    :param value: Candidate value.
    :param field_name: Field label used in error messages.
    :returns: String value.
    :raises TypeError: If ``value`` is not ``str``.
    """
    if isinstance(value, str) is False:
        raise TypeError(f"{field_name} must be str")
    return value


def _invoke_callable_batch_if_supported(
    callable_obj: object,
    call_specs: list[tuple[list[object], dict[str, object]]],
) -> list[object]:
    """Invoke a callable via its batch API when available.

    :param callable_obj: Callable object to invoke.
    :param call_specs: Ordered call specifications.
    :returns: Ordered list of call results.
    :raises TypeError: If ``callable_obj`` is not callable or batch result shape is invalid.
    """
    callable_flag: bool = callable(callable_obj)
    if callable_flag is False:
        raise TypeError("callable_obj must be callable")

    batch_attr_obj: object = getattr(callable_obj, "batch", None)
    batch_callable: bool = callable(batch_attr_obj)
    if batch_callable is True:
        batch_method: Callable[[list[tuple[list[object], dict[str, object]]]], object] = batch_attr_obj  # type: ignore[assignment]
        batched_result_obj: object = batch_method(call_specs)
        if isinstance(batched_result_obj, list) is False:
            raise TypeError("batch() must return list")
        return batched_result_obj

    result_list: list[object] = []
    for call_args, call_kwargs in call_specs:
        result_obj: object = callable_obj(*call_args, **call_kwargs)  # type: ignore[operator]
        result_list.append(result_obj)
    return result_list


def _run_case_tiny_ping(workload: object, iterations: int) -> dict[str, object]:
    """Run the tiny ping-call regime.

    :param workload: Benchmark workload object.
    :param iterations: Number of iterations.
    :returns: Deterministic summary payload.
    """
    ping_obj: object = getattr(workload, "ping")
    ping_is_callable: bool = callable(ping_obj)
    if ping_is_callable is False:
        raise TypeError("workload.ping must be callable")
    ping_method: Callable[[], object] = ping_obj  # type: ignore[assignment]
    batch_size: int = 1024
    completed: int = 0
    accumulator: int = 0
    while completed < iterations:
        remaining: int = iterations - completed
        current_batch_size: int = batch_size
        if remaining < batch_size:
            current_batch_size = remaining
        call_specs: list[tuple[list[object], dict[str, object]]] = [
            ([], {}) for _ in range(current_batch_size)
        ]
        batch_results: list[object] = _invoke_callable_batch_if_supported(ping_method, call_specs)
        if len(batch_results) != current_batch_size:
            raise ValueError("batch result length mismatch for ping")
        for result_obj in batch_results:
            accumulator ^= _require_int(result_obj, "ping result")
        completed += current_batch_size
    return {"xor": accumulator}


def _run_case_increment_small(workload: object, iterations: int) -> dict[str, object]:
    """Run repeated small integer increments.

    :param workload: Benchmark workload object.
    :param iterations: Number of iterations.
    :returns: Deterministic summary payload.
    """
    set_value_obj: object = getattr(workload, "set_value")
    increment_obj: object = getattr(workload, "increment")
    set_value_is_callable: bool = callable(set_value_obj)
    increment_is_callable: bool = callable(increment_obj)
    if set_value_is_callable is False:
        raise TypeError("workload.set_value must be callable")
    if increment_is_callable is False:
        raise TypeError("workload.increment must be callable")
    set_value_method: Callable[[int], object] = set_value_obj  # type: ignore[assignment]
    increment_method: Callable[[int], object] = increment_obj  # type: ignore[assignment]

    seed_obj: object = set_value_method(0)
    _ = _require_int(seed_obj, "set_value result")

    batch_size: int = 1024
    completed: int = 0
    final_value: int = 0
    while completed < iterations:
        remaining: int = iterations - completed
        current_batch_size: int = batch_size
        if remaining < batch_size:
            current_batch_size = remaining
        call_specs: list[tuple[list[object], dict[str, object]]] = [
            ([1], {}) for _ in range(current_batch_size)
        ]
        batch_results: list[object] = _invoke_callable_batch_if_supported(
            increment_method,
            call_specs,
        )
        if len(batch_results) != current_batch_size:
            raise ValueError("batch result length mismatch for increment")
        for result_obj in batch_results:
            final_value = _require_int(result_obj, "increment result")
        completed += current_batch_size
    return {"final_value": final_value}


def _run_case_payload_small_checksum(workload: object, iterations: int) -> dict[str, object]:
    """Run repeated small structured payload checksums.

    :param workload: Benchmark workload object.
    :param iterations: Number of iterations.
    :returns: Deterministic summary payload.
    """
    checksum_obj: object = getattr(workload, "payload_checksum")
    checksum_is_callable: bool = callable(checksum_obj)
    if checksum_is_callable is False:
        raise TypeError("workload.payload_checksum must be callable")
    checksum_method: Callable[[dict[str, int | float | str]], object] = checksum_obj  # type: ignore[assignment]

    accumulator: int = 0
    for index in range(iterations):
        payload: dict[str, int | float | str] = {
            "index": index,
            "scaled": index / 10.0,
            "label": f"label-{index % 13}",
        }
        result_obj: object = checksum_method(payload)
        accumulator ^= _require_int(result_obj, "payload_checksum result")
    return {"xor": accumulator}


def _run_case_payload_large_sum(workload: object, iterations: int) -> dict[str, object]:
    """Run repeated large-list payload summations.

    :param workload: Benchmark workload object.
    :param iterations: Number of iterations.
    :returns: Deterministic summary payload.
    """
    sum_values_obj: object = getattr(workload, "sum_values")
    sum_values_is_callable: bool = callable(sum_values_obj)
    if sum_values_is_callable is False:
        raise TypeError("workload.sum_values must be callable")
    sum_values_method: Callable[[list[int]], object] = sum_values_obj  # type: ignore[assignment]

    payload: list[int] = list(range(8_192))
    expected_sum: int = sum(payload)
    accumulator: int = 0
    for _ in range(iterations):
        result_obj: object = sum_values_method(payload)
        result_value: int = _require_int(result_obj, "sum_values result")
        if result_value != expected_sum:
            raise ValueError("sum_values returned unexpected value")
        accumulator ^= result_value
    return {"xor": accumulator, "expected_sum": expected_sum}


def _run_case_bytes_roundtrip_512kb(workload: object, iterations: int) -> dict[str, object]:
    """Run large bytes round-trip transfers.

    :param workload: Benchmark workload object.
    :param iterations: Number of iterations.
    :returns: Deterministic summary payload.
    """
    echo_obj: object = getattr(workload, "echo_bytes")
    echo_is_callable: bool = callable(echo_obj)
    if echo_is_callable is False:
        raise TypeError("workload.echo_bytes must be callable")
    echo_method: Callable[[bytes], object] = echo_obj  # type: ignore[assignment]

    payload: bytes = b"x" * (512 * 1024)
    expected_digest: str = hashlib.sha256(payload).hexdigest()
    accumulator: int = 0
    for _ in range(iterations):
        result_obj: object = echo_method(payload)
        if isinstance(result_obj, bytes) is False:
            raise TypeError("echo_bytes result must be bytes")
        digest: str = hashlib.sha256(result_obj).hexdigest()
        if digest != expected_digest:
            raise ValueError("echo_bytes returned unexpected payload")
        accumulator ^= len(result_obj)
    return {"xor_len": accumulator, "payload_bytes": len(payload), "digest": expected_digest}


def _run_case_callback_chatter(workload: object, iterations: int) -> dict[str, object]:
    """Run callback-heavy re-entrant interactions.

    :param workload: Benchmark workload object.
    :param iterations: Number of iterations.
    :returns: Deterministic summary payload.
    """
    callback_accumulate_obj: object = getattr(workload, "callback_accumulate")
    callback_accumulate_is_callable: bool = callable(callback_accumulate_obj)
    if callback_accumulate_is_callable is False:
        raise TypeError("workload.callback_accumulate must be callable")
    callback_accumulate_method: Callable[[object, int], object] = callback_accumulate_obj  # type: ignore[assignment]

    offset: int = 17
    callback_calls_per_iteration: int = 40

    def callback(index: int) -> int:
        """Return one deterministic callback score.

        :param index: Callback index.
        :returns: Deterministic score.
        """
        return (index * 3) + offset

    accumulator: int = 0
    for _ in range(iterations):
        result_obj: object = callback_accumulate_method(callback, callback_calls_per_iteration)
        accumulator ^= _require_int(result_obj, "callback_accumulate result")
    return {"xor": accumulator, "callbacks_per_iteration": callback_calls_per_iteration}


def _run_case_readline_digest(workload: object, iterations: int) -> dict[str, object]:
    """Run readline-backed stateful digest operations.

    :param workload: Benchmark workload object.
    :param iterations: Number of iterations.
    :returns: Deterministic summary payload.
    """
    readline_obj: object = getattr(workload, "readline_history_digest")
    readline_is_callable: bool = callable(readline_obj)
    if readline_is_callable is False:
        raise TypeError("workload.readline_history_digest must be callable")
    readline_method: Callable[[str, int, int], object] = readline_obj  # type: ignore[assignment]

    batch_size: int = 64
    completed: int = 0
    digest_accumulator: int = 0
    while completed < iterations:
        remaining: int = iterations - completed
        current_batch_size: int = batch_size
        if remaining < batch_size:
            current_batch_size = remaining
        call_specs: list[tuple[list[object], dict[str, object]]] = []
        for offset in range(current_batch_size):
            index: int = completed + offset
            call_specs.append((["bench", index * 2, 24], {}))
        batch_results: list[object] = _invoke_callable_batch_if_supported(readline_method, call_specs)
        if len(batch_results) != current_batch_size:
            raise ValueError("batch result length mismatch for readline_history_digest")
        for digest_obj in batch_results:
            digest: str = _require_str(digest_obj, "readline_history_digest result")
            digest_head: str = digest[0:16]
            digest_piece: int = int(digest_head, 16)
            digest_accumulator ^= digest_piece
        completed += current_batch_size
    return {"xor": digest_accumulator}


def _run_case_cpu_checksum(workload: object, iterations: int) -> dict[str, object]:
    """Run PBKDF2-heavy checksum operations.

    :param workload: Benchmark workload object.
    :param iterations: Number of iterations.
    :returns: Deterministic summary payload.
    """
    checksum_obj: object = getattr(workload, "compute_checksum")
    checksum_is_callable: bool = callable(checksum_obj)
    if checksum_is_callable is False:
        raise TypeError("workload.compute_checksum must be callable")
    checksum_method: Callable[[str, int, int], object] = checksum_obj  # type: ignore[assignment]

    accumulator: int = 0
    for index in range(iterations):
        result_obj: object = checksum_method("cpu", index * 3, 20)
        accumulator ^= _require_int(result_obj, "compute_checksum result")
    return {"xor": accumulator}


def _run_case_mixed_pipeline(workload: object, iterations: int) -> dict[str, object]:
    """Run a coarse-grained mixed pipeline.

    :param workload: Benchmark workload object.
    :param iterations: Number of iterations.
    :returns: Deterministic summary payload.
    """
    mixed_obj: object = getattr(workload, "mixed_pipeline")
    mixed_is_callable: bool = callable(mixed_obj)
    if mixed_is_callable is False:
        raise TypeError("workload.mixed_pipeline must be callable")
    mixed_method: Callable[[str, int, int, list[int]], object] = mixed_obj  # type: ignore[assignment]

    payload_values: list[int] = list(range(512))
    accumulator: int = 0
    for index in range(iterations):
        result_obj: object = mixed_method("pipeline", index * 5, 18, payload_values)
        if isinstance(result_obj, dict) is False:
            raise TypeError("mixed_pipeline result must be dict")
        checksum_value: int = _require_int(result_obj.get("checksum"), "mixed_pipeline.checksum")
        payload_sum_value: int = _require_int(result_obj.get("payload_sum"), "mixed_pipeline.payload_sum")
        current_value: int = _require_int(result_obj.get("current_value"), "mixed_pipeline.current_value")
        digest_value: str = _require_str(result_obj.get("readline_digest"), "mixed_pipeline.readline_digest")
        digest_piece: int = int(digest_value[0:16], 16)
        accumulator ^= checksum_value
        accumulator ^= payload_sum_value
        accumulator ^= current_value
        accumulator ^= digest_piece
    return {"xor": accumulator}


_CASE_RUNNERS: dict[str, CaseRunner] = {
    "cold_start_first_call": _run_case_tiny_ping,
    "tiny_ping": _run_case_tiny_ping,
    "increment_small": _run_case_increment_small,
    "payload_small_checksum": _run_case_payload_small_checksum,
    "payload_large_sum": _run_case_payload_large_sum,
    "bytes_roundtrip_512kb": _run_case_bytes_roundtrip_512kb,
    "callback_chatter": _run_case_callback_chatter,
    "readline_digest": _run_case_readline_digest,
    "cpu_checksum": _run_case_cpu_checksum,
    "mixed_pipeline": _run_case_mixed_pipeline,
}


def _run_worker_case(
    mode: BenchmarkMode,
    case_name: str,
    iterations: int,
    warmup_iterations: int,
    rounds: int,
) -> dict[str, object]:
    """Run one benchmark case in a single worker process.

    :param mode: Execution mode.
    :param case_name: Benchmark case name.
    :param iterations: Timed iterations.
    :param warmup_iterations: Warmup iterations used for hot cases.
    :param rounds: Hash rounds used by the workload class.
    :returns: Measurement payload.
    :raises ValueError: If ``case_name`` is unknown.
    """
    known_case: bool = case_name in _CASE_RUNNERS
    if known_case is False:
        raise ValueError(f"Unknown case: {case_name}")

    runner: CaseRunner = _CASE_RUNNERS[case_name]
    include_setup: bool = _CASE_INCLUDE_SETUP_IN_TIMING[case_name]

    if include_setup is True:
        summary_xor: int = 0
        start_time: float = time.perf_counter()
        for _ in range(iterations):
            workload_obj: object
            proxy_type: type | None
            workload_obj, proxy_type = _create_workload(mode, rounds)
            try:
                summary: dict[str, object] = runner(workload_obj, 1)
                summary_xor ^= _summary_fingerprint(summary)
            finally:
                _close_workload(workload_obj, proxy_type)
        elapsed_seconds: float = time.perf_counter() - start_time
        return {
            "case_name": case_name,
            "mode": mode,
            "iterations": iterations,
            "operations": iterations,
            "elapsed_seconds": elapsed_seconds,
            "summary": {"fingerprint_xor": summary_xor},
        }

    workload_obj, proxy_type = _create_workload(mode, rounds)
    try:
        warmup_count: int = warmup_iterations
        if warmup_count > 0:
            _ = runner(workload_obj, warmup_count)

        start_time = time.perf_counter()
        summary = runner(workload_obj, iterations)
        elapsed_seconds = time.perf_counter() - start_time
        return {
            "case_name": case_name,
            "mode": mode,
            "iterations": iterations,
            "operations": iterations,
            "elapsed_seconds": elapsed_seconds,
            "summary": summary,
        }
    finally:
        _close_workload(workload_obj, proxy_type)


def _run_worker_from_cli(args: argparse.Namespace) -> int:
    """Run worker mode from CLI arguments.

    :param args: Parsed CLI arguments.
    :returns: Process exit code.
    """
    _ensure_repo_paths()

    mode_obj: object = args.mode
    case_name_obj: object = args.case
    iterations_obj: object = args.iterations
    warmup_obj: object = args.warmup_iterations
    rounds_obj: object = args.rounds
    if isinstance(mode_obj, str) is False:
        raise TypeError("mode must be a string")
    if mode_obj != "direct" and mode_obj != "extradite":
        raise ValueError("mode must be direct or extradite")
    if isinstance(case_name_obj, str) is False:
        raise TypeError("case must be a string")
    if isinstance(iterations_obj, int) is False:
        raise TypeError("iterations must be int")
    if isinstance(warmup_obj, int) is False:
        raise TypeError("warmup_iterations must be int")
    if isinstance(rounds_obj, int) is False:
        raise TypeError("rounds must be int")
    if iterations_obj < 1:
        raise ValueError("iterations must be >= 1")
    if warmup_obj < 0:
        raise ValueError("warmup_iterations must be >= 0")

    result: dict[str, object] = _run_worker_case(
        mode_obj,
        case_name_obj,
        iterations_obj,
        warmup_obj,
        rounds_obj,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


def _build_case_list(cases_arg: str | None) -> list[str]:
    """Build the ordered case list from user input.

    :param cases_arg: Optional comma-separated case names.
    :returns: Ordered case names.
    :raises ValueError: If unknown case names are requested.
    """
    if cases_arg is None:
        return list(_CASE_ORDER)

    requested: list[str] = []
    raw_parts: list[str] = cases_arg.split(",")
    for raw_part in raw_parts:
        candidate: str = raw_part.strip()
        if len(candidate) == 0:
            continue
        known_case: bool = candidate in _CASE_RUNNERS
        if known_case is False:
            raise ValueError(f"Unknown case: {candidate}")
        already_seen: bool = candidate in requested
        if already_seen is False:
            requested.append(candidate)
    if len(requested) == 0:
        raise ValueError("No benchmark cases selected")
    return requested


def _scaled_iterations(base_iterations: int, scale: float) -> int:
    """Scale iteration counts while preserving a minimum of one iteration.

    :param base_iterations: Baseline iteration count.
    :param scale: Positive scale multiplier.
    :returns: Scaled iteration count.
    """
    scaled: int = int(base_iterations * scale)
    if scaled < 1:
        return 1
    return scaled


def _invoke_worker(
    python_executable: str,
    mode: BenchmarkMode,
    case_name: str,
    iterations: int,
    warmup_iterations: int,
    rounds: int,
) -> dict[str, object]:
    """Invoke one worker subprocess and parse its JSON payload.

    :param python_executable: Python executable path.
    :param mode: Execution mode.
    :param case_name: Benchmark case name.
    :param iterations: Timed iterations.
    :param warmup_iterations: Warmup iterations.
    :param rounds: Hash rounds used by workload class.
    :returns: Parsed worker payload.
    :raises RuntimeError: If the worker process fails.
    """
    command: list[str] = [
        python_executable,
        str(SCRIPT_PATH),
        "--worker",
        "--mode",
        mode,
        "--case",
        case_name,
        "--iterations",
        str(iterations),
        "--warmup-iterations",
        str(warmup_iterations),
        "--rounds",
        str(rounds),
    ]
    environment: dict[str, str] = dict(os.environ)
    existing_pythonpath: str = environment.get("PYTHONPATH", "")
    pythonpath_parts: list[str] = [str(REPO_ROOT / "src"), str(REPO_ROOT)]
    existing_present: bool = len(existing_pythonpath) > 0
    if existing_present is True:
        pythonpath_parts.append(existing_pythonpath)
    environment["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)

    completed: subprocess.CompletedProcess[str] = subprocess.run(
        command,
        capture_output=True,
        text=True,
        env=environment,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "Worker process failed for "
            + f"case={case_name} mode={mode}\n"
            + f"stdout:\n{completed.stdout}\n"
            + f"stderr:\n{completed.stderr}"
        )
    output: str = completed.stdout.strip()
    if len(output) == 0:
        raise RuntimeError(f"Worker produced no output for case={case_name} mode={mode}")
    return json.loads(output)


def _parse_worker_timing(payload: dict[str, object]) -> tuple[float, int, dict[str, object]]:
    """Extract timing fields from worker payloads.

    :param payload: Worker payload.
    :returns: Tuple of elapsed seconds, operation count, and summary.
    :raises TypeError: If payload fields have unexpected types.
    """
    elapsed_obj: object = payload.get("elapsed_seconds")
    operations_obj: object = payload.get("operations")
    summary_obj: object = payload.get("summary")
    if isinstance(elapsed_obj, float) is False:
        raise TypeError("worker elapsed_seconds must be float")
    if isinstance(operations_obj, int) is False:
        raise TypeError("worker operations must be int")
    if isinstance(summary_obj, dict) is False:
        raise TypeError("worker summary must be dict")
    return elapsed_obj, operations_obj, summary_obj


def _render_table(
    case_names: list[str],
    iteration_map: dict[str, int],
    direct_stats: dict[str, dict[str, float]],
    extradite_stats: dict[str, dict[str, float]],
) -> str:
    """Render a summary table of benchmark results.

    :param case_names: Ordered case names.
    :param iteration_map: Iteration counts by case.
    :param direct_stats: Direct-mode aggregated stats by case.
    :param extradite_stats: Extradite-mode aggregated stats by case.
    :returns: Rendered table text.
    """
    header: str = (
        "Case                         Iter   Direct(ms)   Extradite(ms)   "
        "Slowdown   Direct(us/op)   Extradite(us/op)"
    )
    separator: str = "-" * len(header)
    lines: list[str] = [header, separator]

    for case_name in case_names:
        iterations: int = iteration_map[case_name]
        direct_median_ms: float = direct_stats[case_name]["median_seconds"] * 1_000.0
        extradite_median_ms: float = extradite_stats[case_name]["median_seconds"] * 1_000.0
        direct_us_per_op: float = direct_stats[case_name]["median_us_per_op"]
        extradite_us_per_op: float = extradite_stats[case_name]["median_us_per_op"]
        slowdown_x: float = extradite_stats[case_name]["median_seconds"] / direct_stats[case_name]["median_seconds"]
        line: str = (
            f"{case_name:26} {iterations:6d} "
            + f"{direct_median_ms:12.3f} {extradite_median_ms:15.3f} "
            + f"{slowdown_x:9.2f}x {direct_us_per_op:14.3f} {extradite_us_per_op:18.3f}"
        )
        lines.append(line)
    return "\n".join(lines)


def _run_driver(args: argparse.Namespace) -> int:
    """Run orchestrator mode from CLI arguments.

    :param args: Parsed CLI arguments.
    :returns: Process exit code.
    """
    repetitions_obj: object = args.repetitions
    quick_obj: object = args.quick
    rounds_obj: object = args.rounds
    iteration_scale_obj: object = args.iteration_scale
    python_executable_obj: object = args.python_executable
    if isinstance(repetitions_obj, int) is False:
        raise TypeError("repetitions must be int")
    if isinstance(quick_obj, bool) is False:
        raise TypeError("quick must be bool")
    if isinstance(rounds_obj, int) is False:
        raise TypeError("rounds must be int")
    if isinstance(iteration_scale_obj, float) is False:
        raise TypeError("iteration_scale must be float")
    if isinstance(python_executable_obj, str) is False:
        raise TypeError("python_executable must be str")
    if repetitions_obj < 1:
        raise ValueError("repetitions must be >= 1")
    if rounds_obj < 1:
        raise ValueError("rounds must be >= 1")
    if iteration_scale_obj <= 0.0:
        raise ValueError("iteration_scale must be > 0")

    selected_cases: list[str] = _build_case_list(args.cases)

    iteration_map: dict[str, int] = {}
    warmup_map: dict[str, int] = {}
    for case_name in selected_cases:
        base_iterations: int
        if quick_obj is True:
            base_iterations = _CASE_QUICK_ITERATIONS[case_name]
        else:
            base_iterations = _CASE_DEFAULT_ITERATIONS[case_name]
        scaled_iterations: int = _scaled_iterations(base_iterations, iteration_scale_obj)
        iteration_map[case_name] = scaled_iterations

        base_warmup: int = _CASE_WARMUP_ITERATIONS[case_name]
        warmup_iterations: int = _scaled_iterations(base_warmup, iteration_scale_obj)
        include_setup: bool = _CASE_INCLUDE_SETUP_IN_TIMING[case_name]
        if include_setup is True:
            warmup_iterations = 0
        warmup_map[case_name] = warmup_iterations

    raw_results: dict[str, dict[str, list[dict[str, object]]]] = {
        case_name: {"direct": [], "extradite": []}
        for case_name in selected_cases
    }

    direct_stats: dict[str, dict[str, float]] = {}
    extradite_stats: dict[str, dict[str, float]] = {}
    summary_by_mode: dict[str, dict[str, object]] = {"direct": {}, "extradite": {}}

    for case_name in selected_cases:
        case_iterations: int = iteration_map[case_name]
        warmup_iterations_case: int = warmup_map[case_name]

        for mode in ("direct", "extradite"):
            timings: list[float] = []
            operation_counts: list[int] = []
            first_summary: dict[str, object] | None = None
            for _ in range(repetitions_obj):
                payload: dict[str, object] = _invoke_worker(
                    python_executable_obj,
                    mode,
                    case_name,
                    case_iterations,
                    warmup_iterations_case,
                    rounds_obj,
                )
                raw_results[case_name][mode].append(payload)
                elapsed_seconds, operations, summary = _parse_worker_timing(payload)
                timings.append(elapsed_seconds)
                operation_counts.append(operations)
                if first_summary is None:
                    first_summary = summary

            expected_operations: int = case_iterations
            for operations_value in operation_counts:
                if operations_value != expected_operations:
                    raise ValueError(
                        f"Worker operations mismatch for {case_name}/{mode}: "
                        + f"{operations_value} != {expected_operations}"
                    )

            median_seconds: float = statistics.median(timings)
            mean_seconds: float = statistics.mean(timings)
            min_seconds: float = min(timings)
            max_seconds: float = max(timings)
            median_us_per_op: float = (median_seconds / expected_operations) * 1_000_000.0
            mean_us_per_op: float = (mean_seconds / expected_operations) * 1_000_000.0

            stats_payload: dict[str, float] = {
                "median_seconds": median_seconds,
                "mean_seconds": mean_seconds,
                "min_seconds": min_seconds,
                "max_seconds": max_seconds,
                "median_us_per_op": median_us_per_op,
                "mean_us_per_op": mean_us_per_op,
            }
            if mode == "direct":
                direct_stats[case_name] = stats_payload
            else:
                extradite_stats[case_name] = stats_payload

            if first_summary is None:
                raise ValueError(f"Missing summary for {case_name}/{mode}")
            summary_by_mode[mode][case_name] = first_summary

        direct_summary: object = summary_by_mode["direct"][case_name]
        extradite_summary: object = summary_by_mode["extradite"][case_name]
        if direct_summary != extradite_summary:
            raise ValueError(
                f"Result mismatch for case {case_name}\n"
                + f"direct={direct_summary}\n"
                + f"extradite={extradite_summary}"
            )

    print("Extradite Performance Benchmark")
    print(f"Python executable: {python_executable_obj}")
    print(f"Repetitions per mode: {repetitions_obj}")
    print(f"PBKDF2 rounds parameter: {rounds_obj}")
    print(f"Quick mode: {quick_obj}")
    print(f"Iteration scale: {iteration_scale_obj}")
    print("")
    print(
        _render_table(
            selected_cases,
            iteration_map,
            direct_stats,
            extradite_stats,
        )
    )
    print("")
    print("Regime descriptions:")
    for case_name in selected_cases:
        description: str = _CASE_DESCRIPTIONS[case_name]
        print(f"- {case_name}: {description}")

    json_output_obj: object = args.json_output
    if isinstance(json_output_obj, str) is True:
        results_payload: dict[str, object] = {
            "python_executable": python_executable_obj,
            "repetitions": repetitions_obj,
            "rounds": rounds_obj,
            "quick": quick_obj,
            "iteration_scale": iteration_scale_obj,
            "cases": selected_cases,
            "iterations": iteration_map,
            "warmups": warmup_map,
            "direct_stats": direct_stats,
            "extradite_stats": extradite_stats,
            "raw_results": raw_results,
        }
        json_path: pathlib.Path = pathlib.Path(json_output_obj)
        json_path.write_text(json.dumps(results_payload, indent=2, sort_keys=True), encoding="utf-8")
        print("")
        print(f"Wrote raw benchmark JSON: {json_path}")

    return 0


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments.

    :returns: Parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Compare direct execution against extradite across diverse workload regimes."
        )
    )
    parser.add_argument("--worker", action="store_true", help="Run one worker case and print JSON.")
    parser.add_argument("--mode", choices=("direct", "extradite"), help="Worker execution mode.")
    parser.add_argument("--case", help="Worker case name.")
    parser.add_argument("--iterations", type=int, default=1, help="Worker timed iterations.")
    parser.add_argument(
        "--warmup-iterations",
        type=int,
        default=0,
        help="Worker warmup iterations for hot cases.",
    )
    parser.add_argument("--repetitions", type=int, default=5, help="Driver repetitions per mode/case.")
    parser.add_argument("--quick", action="store_true", help="Run lower-iteration quick benchmark settings.")
    parser.add_argument(
        "--cases",
        type=str,
        default=None,
        help="Comma-separated subset of cases to run.",
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=2_000,
        help="PBKDF2 rounds used by benchmark workload instances.",
    )
    parser.add_argument(
        "--iteration-scale",
        type=float,
        default=1.0,
        help="Multiply per-case iteration/warmup counts by this scale.",
    )
    parser.add_argument(
        "--python-executable",
        type=str,
        default=sys.executable,
        help="Python executable used to spawn worker subprocesses.",
    )
    parser.add_argument(
        "--json-output",
        type=str,
        default=None,
        help="Optional path to write full raw benchmark output as JSON.",
    )
    return parser.parse_args()


def main() -> int:
    """Run the benchmark driver or worker entrypoint.

    :returns: Process exit code.
    """
    args: argparse.Namespace = _parse_args()
    worker_mode: object = args.worker
    if isinstance(worker_mode, bool) is False:
        raise TypeError("worker flag must be bool")

    if worker_mode is True:
        return _run_worker_from_cli(args)
    return _run_driver(args)


if __name__ == "__main__":
    raise SystemExit(main())
