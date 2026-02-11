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

## Serialization contract

`extradite` uses pickled payloads over IPC. Two categories are explicitly rejected:

- unpicklable return values;
- return values that originate from the isolated module tree.

When either happens, an exception is raised and the value is not transferred to the root process.

This is intentional and enforces a hard barrier so the root process never imports classes/functions from the isolated module.

## API

### `extradite(target: str, share_key: str | None = None) -> type`

- `target` format: `"module.path:ClassName"`.
- `share_key` is optional; when provided, calls with the same key reuse one backing child process.
- returns a dynamic proxy class.
- constructing that proxy creates remote instances in the child process.
- default behavior is unchanged: without `share_key`, each call gets its own child process.

## Constraints and caveats

- The target module must not be imported in the root process before creating the proxy.
- Passing objects between different proxy sessions is disallowed.
- Some advanced Python interactions may be unsupported and raise `UnsupportedInteractionError`.
- If you use `share_key`, `Class.close()` releases that class handle; the shared child process exits when the last handle for that key is closed.

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
