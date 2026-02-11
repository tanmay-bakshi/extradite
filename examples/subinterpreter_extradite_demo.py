"""Show that extradite keeps subinterpreter workloads working with native deps."""

import argparse
import hashlib
import pathlib
import sys
import time
import traceback
from concurrent.futures import Future
from concurrent.futures import InterpreterPoolExecutor

TARGET_MODULE: str = "extradite.demo.native_dependency_workload"
TARGET_CLASS: str = "NativeDependencyWorkload"
TARGET: str = f"{TARGET_MODULE}:{TARGET_CLASS}"
MODULUS: int = 1_000_000_007
SUBINTERPRETER_IMPORT_ERROR_FRAGMENT: str = "does not support loading in subinterpreters"
WorkerResult = dict[str, object]


def _ensure_src_path(src_path: str) -> None:
    """Ensure ``src`` is importable in the current interpreter.

    :param src_path: Absolute path to the repository ``src`` directory.
    """
    exists: bool = src_path in sys.path
    if exists is False:
        sys.path.insert(0, src_path)


def _run_direct_import_attempt(
    module_name: str,
    class_name: str,
    worker_id: int,
    rounds: int,
    start_index: int,
    item_count: int,
) -> WorkerResult:
    """Try direct native-dependent import and report the outcome.

    :param module_name: Module to import in the subinterpreter.
    :param class_name: Class name inside the module.
    :param worker_id: Logical worker identifier.
    :param rounds: PBKDF2 rounds for the workload.
    :param start_index: First index for the checksum range.
    :param item_count: Number of items in the checksum range.
    :returns: Structured result dictionary.
    """
    try:
        imported_module = __import__(module_name, fromlist=[class_name])
        workload_type: object = getattr(imported_module, class_name)
        type_is_callable: bool = callable(workload_type)
        if type_is_callable is False:
            raise TypeError(f"{module_name}:{class_name} is not callable")

        workload: object = workload_type(f"worker-{worker_id}", rounds=rounds)
        checksum_method: object = getattr(workload, "compute_checksum")
        method_is_callable: bool = callable(checksum_method)
        if method_is_callable is False:
            raise TypeError(f"{module_name}:{class_name}.compute_checksum is not callable")

        checksum: object = checksum_method(
            f"payload-{worker_id}",
            start_index,
            item_count,
        )
        return {
            "worker_id": worker_id,
            "status": "unexpected-success",
            "checksum": checksum,
        }
    except Exception as exc:
        return {
            "worker_id": worker_id,
            "status": "expected-failure",
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "traceback": traceback.format_exc(),
        }


def _run_extradited_attempt(
    target: str,
    worker_id: int,
    rounds: int,
    start_index: int,
    item_count: int,
) -> WorkerResult:
    """Run the workload through extradite from inside a subinterpreter.

    :param target: Extradite target in ``module.path:ClassName`` form.
    :param worker_id: Logical worker identifier.
    :param rounds: PBKDF2 rounds for the workload.
    :param start_index: First index for the checksum range.
    :param item_count: Number of items in the checksum range.
    :returns: Structured result dictionary.
    """
    start_time: float = time.perf_counter()
    remote_type: type | None = None
    remote_workload: object | None = None

    try:
        from extradite import extradite

        remote_type = extradite(target)
        remote_workload = remote_type(f"worker-{worker_id}", rounds=rounds)

        checksum_method: object = getattr(remote_workload, "compute_checksum")
        checksum_method_is_callable: bool = callable(checksum_method)
        if checksum_method_is_callable is False:
            raise TypeError("Remote workload does not expose callable compute_checksum")
        checksum_obj: object = checksum_method(
            f"payload-{worker_id}",
            start_index,
            item_count,
        )
        dependency_method: object = getattr(remote_type, "native_dependency_name")
        dependency_method_is_callable: bool = callable(dependency_method)
        if dependency_method_is_callable is False:
            raise TypeError("Remote type does not expose callable native_dependency_name")
        dependency_obj: object = dependency_method()

        snapshot_method: object = getattr(remote_workload, "readline_history_snapshot")
        snapshot_method_is_callable: bool = callable(snapshot_method)
        if snapshot_method_is_callable is False:
            raise TypeError("Remote workload does not expose callable readline_history_snapshot")
        snapshot_obj: object = snapshot_method(
            f"payload-{worker_id}",
            start_index,
            item_count,
        )

        elapsed_seconds: float = time.perf_counter() - start_time
        return {
            "worker_id": worker_id,
            "status": "ok",
            "checksum": checksum_obj,
            "dependency": dependency_obj,
            "readline_snapshot": snapshot_obj,
            "elapsed_seconds": elapsed_seconds,
        }
    except Exception as exc:
        elapsed_seconds = time.perf_counter() - start_time
        return {
            "worker_id": worker_id,
            "status": "error",
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "traceback": traceback.format_exc(),
            "elapsed_seconds": elapsed_seconds,
        }
    finally:
        if remote_workload is not None:
            close_method: object = getattr(remote_workload, "close", None)
            close_is_callable: bool = callable(close_method)
            if close_is_callable is True:
                close_method()

        if remote_type is not None:
            close_class_method: object = getattr(remote_type, "close", None)
            close_class_is_callable: bool = callable(close_class_method)
            if close_class_is_callable is True:
                close_class_method()


def _expected_checksum(
    worker_id: int,
    rounds: int,
    start_index: int,
    item_count: int,
) -> int:
    """Compute the expected checksum in-process for validation.

    :param worker_id: Logical worker identifier.
    :param rounds: PBKDF2 rounds for the workload.
    :param start_index: First index for the checksum range.
    :param item_count: Number of items in the checksum range.
    :returns: Expected checksum modulo ``MODULUS``.
    """
    salt_text: str = f"worker-{worker_id}"
    salt_bytes: bytes = salt_text.encode("utf-8")
    label: str = f"payload-{worker_id}"

    checksum: int = 0
    for offset in range(item_count):
        current_index: int = start_index + offset
        payload_text: str = f"{label}:{current_index}"
        payload_bytes: bytes = payload_text.encode("utf-8")
        digest: bytes = hashlib.pbkdf2_hmac(
            "sha256",
            payload_bytes,
            salt_bytes,
            rounds,
            dklen=32,
        )
        left: int = int.from_bytes(digest[0:8], "big")
        right: int = int.from_bytes(digest[8:16], "big")
        score: int = (left ^ right) % MODULUS
        checksum = (checksum + score) % MODULUS
    return checksum


def _collect_results(futures: list[Future[WorkerResult]]) -> list[WorkerResult]:
    """Collect results from interpreter futures.

    :param futures: Submitted futures.
    :returns: Materialized result payloads.
    """
    results: list[WorkerResult] = []
    for future in futures:
        try:
            result: WorkerResult = future.result()
            results.append(result)
        except Exception as exc:
            results.append(
                {
                    "worker_id": -1,
                    "status": "future-error",
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "traceback": traceback.format_exc(),
                }
            )
    return results


def _worker_id(result: WorkerResult) -> int:
    """Extract the worker id used for sorting.

    :param result: Result dictionary.
    :returns: Worker identifier or fallback value.
    """
    worker_id_obj: object = result.get("worker_id")
    if isinstance(worker_id_obj, int) is True:
        return worker_id_obj
    return 9_999_999


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments.

    :returns: Parsed CLI arguments.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Demonstrate that subinterpreters can execute native-dependent workloads "
            "through extradite even when direct imports fail."
        )
    )
    parser.add_argument("--workers", type=int, default=4, help="Number of subinterpreter workers.")
    parser.add_argument("--items-per-worker", type=int, default=6, help="Items processed by each worker.")
    parser.add_argument("--rounds", type=int, default=16_000, help="PBKDF2 rounds per item.")
    return parser.parse_args()


def main() -> int:
    """Run the full demonstration.

    :returns: Process exit code where ``0`` indicates success.
    """
    args: argparse.Namespace = _parse_args()
    workers: int = int(args.workers)
    items_per_worker: int = int(args.items_per_worker)
    rounds: int = int(args.rounds)

    if workers < 1:
        print("workers must be >= 1")
        return 1
    if items_per_worker < 1:
        print("items-per-worker must be >= 1")
        return 1
    if rounds < 1:
        print("rounds must be >= 1")
        return 1

    repo_root: pathlib.Path = pathlib.Path(__file__).resolve().parent.parent
    src_path: pathlib.Path = repo_root / "src"
    src_path_str: str = str(src_path)
    _ensure_src_path(src_path_str)

    print("Subinterpreter + Extradite Demo")
    print(f"python={sys.version.split()[0]}")
    print(f"workers={workers} items_per_worker={items_per_worker} rounds={rounds}")
    print(f"target={TARGET}")
    print("")

    direct_start: float = time.perf_counter()
    with InterpreterPoolExecutor(
        max_workers=workers,
        initializer=_ensure_src_path,
        initargs=(src_path_str,),
    ) as executor:
        direct_futures: list[Future[WorkerResult]] = []
        for worker_index in range(workers):
            start_index: int = worker_index * items_per_worker
            direct_futures.append(
                executor.submit(
                    _run_direct_import_attempt,
                    TARGET_MODULE,
                    TARGET_CLASS,
                    worker_index,
                    rounds,
                    start_index,
                    items_per_worker,
                )
            )
        direct_results: list[WorkerResult] = _collect_results(direct_futures)
    direct_elapsed: float = time.perf_counter() - direct_start

    print("Phase 1: direct subinterpreter import attempt")
    direct_expected_failures: int = 0
    direct_failure_messages_match: int = 0
    for result in sorted(direct_results, key=_worker_id):
        worker_index: int = _worker_id(result)
        status_obj: object = result.get("status")
        status: str = str(status_obj)

        if status == "expected-failure":
            direct_expected_failures += 1
            error_message_obj: object = result.get("error_message")
            error_message: str = str(error_message_obj)
            contains_fragment: bool = SUBINTERPRETER_IMPORT_ERROR_FRAGMENT in error_message
            if contains_fragment is True:
                direct_failure_messages_match += 1
            print(f"  worker={worker_index} status={status} error={error_message}")
            continue

        print(f"  worker={worker_index} status={status} payload={result}")

    print(f"  elapsed_seconds={direct_elapsed:.3f}")
    print("")

    extradite_start: float = time.perf_counter()
    with InterpreterPoolExecutor(
        max_workers=workers,
        initializer=_ensure_src_path,
        initargs=(src_path_str,),
    ) as executor:
        extradite_futures: list[Future[WorkerResult]] = []
        for worker_index in range(workers):
            start_index = worker_index * items_per_worker
            extradite_futures.append(
                executor.submit(
                    _run_extradited_attempt,
                    TARGET,
                    worker_index,
                    rounds,
                    start_index,
                    items_per_worker,
                )
            )
        extradite_results: list[WorkerResult] = _collect_results(extradite_futures)
    extradite_elapsed: float = time.perf_counter() - extradite_start

    print("Phase 2: extradited calls from subinterpreters")
    extradite_success_count: int = 0
    extradite_dependency_match_count: int = 0
    extradite_checksum_match_count: int = 0
    readline_snapshot_match_count: int = 0
    for result in sorted(extradite_results, key=_worker_id):
        worker_index = _worker_id(result)
        status_obj = result.get("status")
        status = str(status_obj)

        if status != "ok":
            print(f"  worker={worker_index} status={status} payload={result}")
            continue

        extradite_success_count += 1
        dependency_obj: object = result.get("dependency")
        dependency: str = str(dependency_obj)
        dependency_matches: bool = dependency == "readline"
        if dependency_matches is True:
            extradite_dependency_match_count += 1

        checksum_obj: object = result.get("checksum")
        checksum: int = int(checksum_obj)
        expected_checksum: int = _expected_checksum(
            worker_index,
            rounds,
            worker_index * items_per_worker,
            items_per_worker,
        )
        checksum_matches: bool = checksum == expected_checksum
        if checksum_matches is True:
            extradite_checksum_match_count += 1

        snapshot_ok: bool = False
        snapshot_obj: object = result.get("readline_snapshot")
        snapshot_history_length: object = None
        snapshot_first_entry: object = None
        snapshot_last_entry: object = None
        snapshot_has_pipe: object = None
        if isinstance(snapshot_obj, dict) is True:
            snapshot_history_length = snapshot_obj.get("history_length")
            snapshot_first_entry = snapshot_obj.get("first_entry")
            snapshot_last_entry = snapshot_obj.get("last_entry")
            snapshot_has_pipe = snapshot_obj.get("has_pipe_delimiter")
            snapshot_digest: object = snapshot_obj.get("digest")

            expected_first_entry: str | None = None
            expected_last_entry: str | None = None
            if items_per_worker > 0:
                expected_first_entry = f"payload-{worker_index}:{worker_index * items_per_worker}"
                expected_last_entry = (
                    f"payload-{worker_index}:"
                    + f"{worker_index * items_per_worker + items_per_worker - 1}"
                )

            history_length_matches: bool = snapshot_history_length == items_per_worker
            first_entry_matches: bool = snapshot_first_entry == expected_first_entry
            last_entry_matches: bool = snapshot_last_entry == expected_last_entry
            has_pipe_matches: bool = snapshot_has_pipe is True
            digest_is_hex: bool = isinstance(snapshot_digest, str) and len(snapshot_digest) == 64
            snapshot_ok = (
                history_length_matches is True
                and first_entry_matches is True
                and last_entry_matches is True
                and has_pipe_matches is True
                and digest_is_hex is True
            )
        if snapshot_ok is True:
            readline_snapshot_match_count += 1

        elapsed_seconds_obj: object = result.get("elapsed_seconds")
        elapsed_seconds: float = float(elapsed_seconds_obj)
        print(
            "  "
            + f"worker={worker_index} status={status} dependency={dependency} "
            + f"checksum={checksum} expected={expected_checksum} "
            + f"history_len={snapshot_history_length} first={snapshot_first_entry} "
            + f"last={snapshot_last_entry} pipe_delim={snapshot_has_pipe} "
            + f"elapsed={elapsed_seconds:.3f}s"
        )

    total_items: int = workers * items_per_worker
    throughput: float = 0.0
    if extradite_elapsed > 0.0:
        throughput = total_items / extradite_elapsed
    print(f"  elapsed_seconds={extradite_elapsed:.3f}")
    print(f"  throughput_items_per_second={throughput:.2f}")
    print("")

    direct_phase_ok: bool = direct_expected_failures == workers
    direct_messages_ok: bool = direct_failure_messages_match == workers
    extradite_phase_ok: bool = extradite_success_count == workers
    extradite_dependency_ok: bool = extradite_dependency_match_count == workers
    extradite_checksums_ok: bool = extradite_checksum_match_count == workers
    readline_snapshot_ok: bool = readline_snapshot_match_count == workers

    demo_passes: bool = (
        direct_phase_ok is True
        and direct_messages_ok is True
        and extradite_phase_ok is True
        and extradite_dependency_ok is True
        and extradite_checksums_ok is True
        and readline_snapshot_ok is True
    )
    if demo_passes is True:
        print("DEMO RESULT: PASS")
        return 0

    print("DEMO RESULT: FAIL")
    print(
        "  "
        + f"direct_phase_ok={direct_phase_ok} "
        + f"direct_messages_ok={direct_messages_ok} "
        + f"extradite_phase_ok={extradite_phase_ok} "
        + f"extradite_dependency_ok={extradite_dependency_ok} "
        + f"extradite_checksums_ok={extradite_checksums_ok} "
        + f"readline_snapshot_ok={readline_snapshot_ok}"
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
