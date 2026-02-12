# Extradite Optimization Plan (Living Document)

Last updated: 2026-02-12
Owner: Codex + Tanmay
Status: Phases 1-6 implemented

## Purpose

This document tracks the optimization roadmap for Extradite based on benchmark and profiling evidence. It is intended to be updated continuously as phases are implemented, validated, and iterated.

## Baseline Performance Snapshot

Source run:

```bash
PYTHONPATH=src /usr/bin/time -p python3 benchmarks/extradite_performance_benchmark.py --repetitions 3 --json-output /tmp/extradite-benchmark-results.json
```

Total benchmark wall time:

- `real 30.21s`

Baseline median results:

| Regime | Direct (us/op) | Extradite (us/op) | Slowdown |
| --- | ---: | ---: | ---: |
| `cold_start_first_call` | 298.340 | 47188.565 | 158.17x |
| `tiny_ping` | 0.048 | 26.543 | 551.87x |
| `increment_small` | 0.055 | 33.876 | 614.28x |
| `payload_small_checksum` | 1.735 | 43.911 | 25.31x |
| `payload_large_sum` | 79.930 | 7959.455 | 99.58x |
| `bytes_roundtrip_512kb` | 153.380 | 1010.402 | 6.59x |
| `callback_chatter` | 1.778 | 1090.998 | 613.71x |
| `readline_digest` | 8.793 | 43.698 | 4.97x |
| `cpu_checksum` | 3049.527 | 3087.198 | 1.01x |
| `mixed_pipeline` | 2798.425 | 3336.957 | 1.19x |

## Profiling Summary (What Dominates Today)

- Chatty regimes are dominated by boundary overhead: parent/child `send`/`recv` loops and per-request protocol work.
- `tiny_ping`/`increment_small` also spend significant time in per-request module leak checks (`_find_module_leaks` path).
- `payload_large_sum` is dominated by recursive encode/decode + pickle per element (`_encode_for_child`, `_decode_from_parent`).
- `callback_chatter` is dominated by re-entrant callback roundtrips (`call_parent_attr` / parent request servicing).
- `bytes_roundtrip_512kb` is dominated by IPC I/O (`posix.read`/`posix.write`).
- `cpu_checksum` and `mixed_pipeline` are compute dominated in worker (`pbkdf2_hmac`), so protocol overhead is amortized.

## Phase Tracker

| Phase | Name | Status | Primary Regimes Impacted |
| --- | --- | --- | --- |
| 1 | Remove Per-Request Leak-Scan Overhead | Completed (2026-02-11) | `tiny_ping`, `increment_small`, `payload_small_checksum`, `readline_digest` |
| 2 | Structured Payload Transport Fast-Path | Completed (2026-02-11) | `payload_large_sum`, `payload_small_checksum`, `mixed_pipeline` |
| 3 | Batched RPC for Chatty Calls | Completed (2026-02-11) | `tiny_ping`, `increment_small`, `readline_digest` |
| 4 | Callback-Batch Bridge | Completed (2026-02-11) | `callback_chatter` |
| 5 | Shared-Memory Binary Transport | Completed (2026-02-11) | `bytes_roundtrip_512kb`, large binary workloads |
| 6 | Warm Session Lifecycle / Pooling | Completed (2026-02-12) | `cold_start_first_call` |

## Phase 1: Remove Per-Request Leak-Scan Overhead

### Objective

Reduce cost from repeatedly scanning `sys.modules` on every request while preserving isolation guarantees.

### Current Hotspots

- `src/extradite/runtime.py:_assert_protected_modules_not_loaded`
- `src/extradite/runtime.py:_assert_module_not_loaded`
- `src/extradite/runtime.py:_find_module_leaks`

### Planned Change

- Keep strict validation on target registration/session startup.
- Replace per-request full scans with a low-cost fast-path:
  - session-level invariants + protected module fingerprint/version checks, or
  - explicit optional audit mode for expensive full scans.

### Acceptance Criteria

- All existing isolation tests pass unchanged.
- Add tests proving protected-module leaks are still detected in required paths.
- Bench target:
  - `tiny_ping` and `increment_small` median `us/op` improve by at least 20%.

### Implementation Notes (2026-02-11)

- Added an import-activity epoch tracker in `src/extradite/runtime.py` using a process audit hook.
- Changed per-request leak checks to:
  - fast-path skip when import epoch and `sys.modules` size are unchanged;
  - full leak scan only when import state changed.
- Preserved strict registration/startup leak validation.
- Added deterministic tests:
  - `test_protected_module_import_after_bootstrap_fails_on_next_boundary_call`
  - `test_protected_module_scan_fast_path_when_import_state_is_unchanged`

### Phase 1 Result Snapshot (Targeted Regimes)

Source runs:

```bash
PYTHONPATH=src python3 benchmarks/extradite_performance_benchmark.py --repetitions 5 --cases tiny_ping,increment_small,payload_small_checksum,readline_digest --json-output /tmp/extradite-phase1-before.json
PYTHONPATH=src python3 benchmarks/extradite_performance_benchmark.py --repetitions 5 --cases tiny_ping,increment_small,payload_small_checksum,readline_digest --json-output /tmp/extradite-phase1-after.json
```

Extradite median `us/op` before vs after:

| Regime | Before | After | Change |
| --- | ---: | ---: | ---: |
| `tiny_ping` | 27.434 | 24.572 | -10.43% |
| `increment_small` | 33.322 | 21.595 | -35.19% |
| `payload_small_checksum` | 38.868 | 38.495 | -0.96% |
| `readline_digest` | 49.911 | 38.689 | -22.48% |

Summary:

- Relevant regimes improved in all 4/4 cases.
- The strongest gains were in `increment_small` and `readline_digest`.
- `tiny_ping` improved materially but did not meet the original 20% stretch target.

---

## Phase 2: Structured Payload Transport Fast-Path

### Objective

Avoid recursive per-element encoding for large picklable container payloads.

### Current Hotspots

- `src/extradite/runtime.py:_encode_for_child`
- `src/extradite/worker.py:_decode_from_parent`
- `_pickle.dumps` / `_pickle.loads` called at very high frequency for large lists.

### Planned Change

- Add whole-object bulk transport path when payload is safely picklable and does not require handle semantics.
- Preserve fallback recursive behavior for:
  - proxy/handle-containing payloads,
  - type-rule/call-policy scenarios requiring reference behavior.

### Acceptance Criteria

- Deterministic parity tests for nested container payloads.
- Policy precedence tests remain green.
- Bench target:
  - `payload_large_sum` median `us/op` improves by at least 40%.

### Implementation Notes (2026-02-11)

- Added bulk pickle fast-path for large container payloads in:
  - `src/extradite/runtime.py` (parent -> child)
  - `src/extradite/worker.py` (child -> parent)
- Fast-path constraints:
  - only for large containers (`list`, `tuple`, `set`, `frozenset`, `dict`);
  - only in effective `value` policy mode;
  - for parent -> child, disabled when per-type rules are active and no per-call override is present (to preserve nested type-rule semantics).
- Preserved recursive fallback behavior for mixed/handle-containing containers when bulk pickle fails.
- Added deterministic tests:
  - `test_large_picklable_container_roundtrip_in_value_mode`
  - `test_nested_type_rule_reference_semantics_without_call_override`
  - `test_call_override_value_applies_to_nested_type_rule_payloads`
  - `test_large_container_fallback_roundtrips_unpicklable_nested_values`

### Phase 2 Result Snapshot (Targeted Regimes)

Source runs:

```bash
PYTHONPATH=src python3 benchmarks/extradite_performance_benchmark.py --repetitions 5 --cases payload_large_sum,payload_small_checksum,mixed_pipeline --json-output /tmp/extradite-phase2-before.json
PYTHONPATH=src python3 benchmarks/extradite_performance_benchmark.py --repetitions 5 --cases payload_large_sum,payload_small_checksum,mixed_pipeline --json-output /tmp/extradite-phase2-after.json
```

Extradite median `us/op` before vs after:

| Regime | Before | After | Change |
| --- | ---: | ---: | ---: |
| `payload_large_sum` | 7875.700 | 248.377 | -96.85% |
| `payload_small_checksum` | 38.370 | 37.606 | -1.99% |
| `mixed_pipeline` | 3272.039 | 2837.936 | -13.27% |

Summary:

- `payload_large_sum` exceeded the phase target by a wide margin.
- All three targeted Phase 2 regimes improved.

---

## Phase 3: Batched RPC for Chatty Calls

### Objective

Collapse many tiny parent->child calls into fewer protocol roundtrips.

### Current Hotspots

- `src/extradite/runtime.py:_send_request`
- `src/extradite/runtime.py:_wait_for_response`
- `multiprocessing.connection` send/recv loops.

### Planned Change

- Add batch protocol action for repeated method invocation (`call_attr_batch` style).
- Add API surface that allows users/workloads to request batching without breaking existing semantics.

### Acceptance Criteria

- New tests for order, return shape, and error behavior in batch execution.
- Bench target:
  - `tiny_ping` and `increment_small` each improve by at least 2x versus baseline Extradite.

### Implementation Notes (2026-02-11)

- Added a new protocol action `call_attr_batch` in:
  - `src/extradite/runtime.py`
  - `src/extradite/worker.py`
- Added a callable-proxy batch API:
  - `counter.method.batch([(args, kwargs), ...], call_policy=...)`
- Added deterministic tests for:
  - ordered batch results;
  - short-circuit behavior on first failing call;
  - per-call policy override behavior in batch mode.
- Updated benchmark regimes (`tiny_ping`, `increment_small`, `readline_digest`) to use the batch path when the callable exposes `.batch`, with loop fallback otherwise.

### Phase 3 Result Snapshot (Targeted Regimes)

Source runs:

```bash
PYTHONPATH=src python3 benchmarks/extradite_performance_benchmark.py --repetitions 5 --cases tiny_ping,increment_small,readline_digest --json-output /tmp/extradite-phase3-before.json
PYTHONPATH=src python3 benchmarks/extradite_performance_benchmark.py --repetitions 5 --cases tiny_ping,increment_small,readline_digest --json-output /tmp/extradite-phase3-after.json
```

Extradite median `us/op` before vs after:

| Regime | Before | After | Change |
| --- | ---: | ---: | ---: |
| `tiny_ping` | 25.713 | 2.742 | -89.34% |
| `increment_small` | 28.909 | 4.029 | -86.06% |
| `readline_digest` | 37.811 | 15.897 | -57.96% |

Summary:

- All targeted Phase 3 regimes improved substantially.
- `tiny_ping` and `increment_small` each improved by far more than 2x, meeting the phase target.

---

## Phase 4: Callback-Batch Bridge

### Objective

Reduce callback chatter caused by re-entrant per-item parent callback calls.

### Current Hotspots

- `src/extradite/worker.py:call_parent_attr`
- `src/extradite/worker.py:_send_request_to_parent`
- Parent re-entrant handling in `src/extradite/runtime.py:_handle_incoming_request`.

### Planned Change

- Add callback batch request path for worker->parent callback fanout.
- Update callback-heavy workflows to use batch callback invocation where semantically valid.

### Acceptance Criteria

- Re-entrant safety tests pass (no deadlock, correct exception propagation).
- Bench target:
  - `callback_chatter` median `us/op` improves by at least 3x.

### Implementation Notes (2026-02-11)

- Added worker->parent callback batch bridge:
  - worker-side parent proxies (`_ParentRemoteHandle`, `_ParentCallProxy`) now expose `.batch(...)`;
  - worker runtime added `call_parent_attr_batch(...)`;
  - parent runtime now handles child-origin `call_attr_batch` requests.
- Updated callback-heavy workload paths to opportunistically use callback batch:
  - `src/extradite/demo/benchmark_workload.py:callback_accumulate`
  - `tests/fixtures/sandbox_target.py:callback_accumulate`
- Added deterministic tests for callback-batch behavior:
  - batch path usage (counts `call_attr_batch` actions, no per-item `call_attr`);
  - short-circuit on first callback error with expected call count.

### Phase 4 Result Snapshot (Targeted Regime)

Source runs:

```bash
PYTHONPATH=src python3 benchmarks/extradite_performance_benchmark.py --repetitions 5 --cases callback_chatter --json-output /tmp/extradite-phase4-before.json
PYTHONPATH=src python3 benchmarks/extradite_performance_benchmark.py --repetitions 5 --cases callback_chatter --json-output /tmp/extradite-phase4-after.json
```

Extradite median `us/op` before vs after:

| Regime | Before | After | Change |
| --- | ---: | ---: | ---: |
| `callback_chatter` | 915.248 | 225.853 | -75.32% |

Summary:

- `callback_chatter` improved by over 4x, exceeding the phase target.

---

## Phase 5: Shared-Memory Binary Transport

### Objective

Reduce overhead for large binary payload transfers.

### Current Hotspots

- `posix.read` / `posix.write` dominated IPC in large-byte regime.

### Planned Change

- Add thresholded transport mode for large `bytes`/`bytearray` via `multiprocessing.shared_memory`.
- Keep existing pipe transport for small payloads.
- Ensure explicit cleanup semantics for shared memory blocks on success/failure.

### Acceptance Criteria

- Tests for correctness and cleanup under normal and exceptional paths.
- Bench target:
  - `bytes_roundtrip_512kb` median `us/op` improves by at least 25%.

### Implementation Notes (2026-02-11)

- Added thresholded shared-memory transport for large binary payloads (`bytes`/`bytearray`) in:
  - `src/extradite/runtime.py`
  - `src/extradite/worker.py`
- Added decode/cleanup behavior that unlinks shared-memory blocks after read on the receiving side.
- Added sender-side best-effort cleanup on request send failure and post-send cleanup.
- Kept pipe/pickle transport as fallback for smaller payloads and non-binary values.
- Added deterministic tests:
  - `test_large_bytes_roundtrip_in_value_mode`
  - `test_large_bytearray_roundtrip_in_value_mode`
  - `test_shared_memory_cleanup_on_send_failure`
  - `test_shared_memory_decode_unlinks_block`

### Phase 5 Result Snapshot (Targeted Regimes)

Source runs:

```bash
cd /tmp/extradite-phase5-before
PYTHONPATH=src /Users/tanmaybakshi/extradite/.venv/bin/python benchmarks/extradite_performance_benchmark.py --repetitions 5 --cases bytes_roundtrip_512kb,mixed_pipeline --json-output /tmp/extradite-phase5-before.json
cd /Users/tanmaybakshi/extradite
PYTHONPATH=src .venv/bin/python benchmarks/extradite_performance_benchmark.py --repetitions 5 --cases bytes_roundtrip_512kb,mixed_pipeline --json-output /tmp/extradite-phase5-after.json
```

Extradite median `us/op` before vs after:

| Regime | Before | After | Change |
| --- | ---: | ---: | ---: |
| `bytes_roundtrip_512kb` | 1002.676 | 349.686 | -65.12% |
| `mixed_pipeline` | 2772.909 | 2746.350 | -0.96% |

Summary:

- `bytes_roundtrip_512kb` exceeded the phase target by a wide margin.
- Guard regime `mixed_pipeline` remained effectively flat.

---

## Phase 6: Warm Session Lifecycle / Pooling

### Objective

Amortize startup/teardown costs for workflows with repeated short-lived calls.

### Current Hotspots

- `runtime.start`, process spawn, startup handshake, and session close/join in cold regime.

### Planned Change

- Add explicit prewarm/keepalive strategy on top of `share_key`.
- Optional TTL or lifecycle control for long-lived shared workers.
- Keep default behavior unchanged for backward compatibility.

### Acceptance Criteria

- Lifecycle/refcount tests for shared sessions pass.
- Bench target:
  - `cold_start_first_call` median `us/op` improves by at least 50% in warm-session mode.

### Implementation Notes (2026-02-12)

- Added shared-session keepalive support:
  - new `share_keepalive_seconds` argument on `extradite(...)` for `share_key` sessions;
  - default behavior remains unchanged (`share_key` without keepalive still closes immediately when idle).
- Added explicit lifecycle APIs:
  - `prewarm_extradite_session(...)` for eager child startup;
  - `close_extradite_session(share_key)` for explicit shutdown.
- Added shared-session idle close timer mechanics in `src/extradite/runtime.py`:
  - cancel pending idle-close timer on reuse;
  - schedule close on last-handle release when keepalive is enabled;
  - preserve compatibility/conflict checks across `transport_policy`, `transport_type_rules`, and explicit keepalive values.
- Added deterministic tests:
  - `test_share_keepalive_requires_share_key`
  - `test_share_key_rejects_conflicting_share_keepalive_seconds`
  - `test_prewarm_and_close_shared_session_controls_lifecycle`
  - `test_share_key_keepalive_reuses_running_session_after_class_close`
  - `test_share_key_keepalive_timeout_closes_idle_session`
- Added benchmark regime:
  - `cold_start_first_call_warm_session`

### Phase 6 Result Snapshot (Targeted Regimes)

Source runs:

```bash
cd /tmp/extradite-phase6-before
PYTHONPATH=src /Users/tanmaybakshi/extradite/.venv/bin/python benchmarks/extradite_performance_benchmark.py --repetitions 7 --cases cold_start_first_call --json-output /tmp/extradite-phase6-before.json
cd /Users/tanmaybakshi/extradite
PYTHONPATH=src .venv/bin/python benchmarks/extradite_performance_benchmark.py --repetitions 7 --cases cold_start_first_call,cold_start_first_call_warm_session --json-output /tmp/extradite-phase6-after.json
```

Extradite median `us/op` before vs after:

| Regime | Before | After | Change |
| --- | ---: | ---: | ---: |
| `cold_start_first_call` | 49356.544 | 49455.954 | +0.20% |
| `cold_start_first_call_warm_session` | 49356.544 (baseline cold) | 3094.819 | -93.73% |

Summary:

- Warm-session mode achieved a `15.95x` speedup (`-93.73%`) versus baseline cold start.
- The phase target (`>= 50%` improvement in warm-session mode) was exceeded by a wide margin.
- Default cold behavior stayed effectively flat.

## Rollout and Validation Strategy

- Implement one phase at a time.
- After each phase:
  - run full tests,
  - run benchmark with same settings used for baseline,
  - update this document with before/after numbers and decision notes.
- Defer phase merges if regression appears in:
  - isolation guarantees,
  - protocol correctness,
  - error fidelity.

## Open Risks

- Fast-path serialization changes may accidentally bypass policy/reference semantics if not carefully gated.
- Batch RPC and callback batch features can complicate error propagation ordering.
- Shared memory introduces lifecycle complexity (dangling blocks/leaks) if cleanup paths are incomplete.
- Session warming must not weaken isolation invariants or leak module state into root interpreter.

## Update Log

### 2026-02-11

- Created initial 6-phase optimization plan from benchmark + profiling evidence.
- Recorded baseline metrics and phase-level success criteria.
- Completed Phase 5 shared-memory binary transport and recorded benchmark deltas.

### 2026-02-12

- Completed Phase 6 warm-session lifecycle/pooling, including keepalive + prewarm APIs and benchmark validation.
