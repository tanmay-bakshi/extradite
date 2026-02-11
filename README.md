# extradite

`extradite` lets you use classes from modules that must never be imported in your main Python interpreter.

It launches a separate interpreter process, imports the target class in that process, and exposes a proxy class in the root process. Class/instance interactions are executed remotely over IPC.

## Why this exists

Some dependencies are expensive, unsafe, or context-specific in your root process. `extradite` provides a strict isolation boundary where:

- the target module is imported only in the child interpreter;
- the root process refuses to start if the target module is already present in `sys.modules`;
- all method/attribute interactions are proxied over a request/response protocol.

## Install

```bash
pip install extradite
```

For local development:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
```

## Quick start

```python
from extradite import extradite

RemoteCounter = extradite("my_pkg.heavy_module:Counter")

counter = RemoteCounter(3)
print(counter.increment(2))
print(counter.value)

counter.close()       # release remote instance
RemoteCounter.close() # stop child process
```

## Transport contract

`extradite` supports two transport modes for arguments and return values:

- value mode: picklable values are transferred as serialized payloads;
- reference mode: non-picklable values are transferred as opaque handles.

Handles are bi-directional:

- parent -> child: local closures, function-local classes/instances, bound methods, and other non-picklable objects are passed by handle;
- child -> parent: non-picklable values that do not violate isolation are returned by handle.

Handle identity is preserved per session. Passing the same object repeatedly yields the same remote handle identity.

## Isolation contract

`extradite` enforces strict module isolation:

- the target module tree must never be loaded in the root interpreter;
- the root process refuses to start a target if that module tree is already present in `sys.modules`;
- child->parent value-mode transfers are blocked if they originate from the protected module tree.

This prevents protected-module leakage through marshal/unmarshal transfer paths.

## API

### `extradite(target: str, share_key: str | None = None) -> type`

- `target` format: `"module.path:ClassName"`.
- `share_key` is optional; when provided, calls with the same key reuse one backing child process.
- returns a dynamic proxy class.
- constructing that proxy creates remote instances in the child process.
- default behavior is unchanged: without `share_key`, each call gets its own child process.

## Re-entrant callbacks

The protocol is re-entrant: while one side is waiting on a response, it can service incoming nested requests from the other side.

This enables callback chains such as:

- caller -> isolated method -> caller callback -> isolated method.

Nested request routing is handled in-process and avoids deadlock in supported interaction patterns.

## Exceptions

Remote exceptions are propagated with stable local wrappers:

- known protocol/isolation exceptions map to local `ExtraditeProtocolError`, `ExtraditeModuleLeakError`, and `UnsupportedInteractionError`;
- unknown remote exceptions map to `ExtraditeRemoteError`.

`ExtraditeRemoteError` exposes fidelity fields:

- `remote_type_name`
- `remote_message`
- `remote_traceback`

These preserve the original remote exception type, message, and traceback text.

## Constraints and caveats

- The target module must not be imported in the root process before creating the proxy.
- Passing proxy objects between different proxy sessions is disallowed.
- Some advanced Python interactions may be unsupported and raise `UnsupportedInteractionError`.
- If you use `share_key`, `Class.close()` releases that class handle; the shared child process exits when the last handle for that key is closed.
- Handle proxies support explicit `close()` and deterministic use-after-release protocol errors.

## Running tests

```bash
PYTHONPATH=src pytest
```

## Subinterpreter demonstration (Python 3.14)

This project includes a runnable showcase that proves two points:

- direct subinterpreter imports can fail for native extensions that are not subinterpreter-safe;
- the same workload succeeds from subinterpreters when routed through `extradite`.

Run:

```bash
python3.14 examples/subinterpreter_extradite_demo.py
```

The demo intentionally targets `readline` through `extradite.demo.native_dependency_workload`, because `readline` is a native module that reports unsupported subinterpreter loading in Python 3.14. The extradited phase exercises real `readline` APIs (`clear_history`, `add_history`, `get_history_item`, and completer-delimiter configuration), then validates those outputs in the subinterpreter driver.

## License

Apache License 2.0 (`LICENSE`).
