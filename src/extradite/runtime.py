"""Parent-process runtime for extradite proxies."""

import atexit
import io
import multiprocessing
import pickle
import sys
import threading
import traceback
import weakref
from collections.abc import Callable
from multiprocessing.connection import Connection
from typing import Any
from typing import ClassVar
from typing import Literal

from extradite.errors import ExtraditeModuleLeakError
from extradite.errors import ExtraditeProtocolError
from extradite.errors import ExtraditeRemoteError
from extradite.errors import UnsupportedInteractionError
from extradite.worker import OWNER_CHILD
from extradite.worker import OWNER_PARENT
from extradite.worker import WIRE_PICKLE_TAG
from extradite.worker import WIRE_REF_TAG
from extradite.worker import worker_entry

_SHARED_SESSION_LOCK: threading.RLock = threading.RLock()
_SHARED_SESSIONS_BY_KEY: dict[str, "ExtraditeSession"] = {}
_IMPORT_HOOK_LOCK: threading.Lock = threading.Lock()
_IMPORT_HOOK_INSTALLED: bool = False
_IMPORT_EPOCH: int = 0
_BULK_PICKLE_MIN_ITEMS: int = 32
TransportPolicy = Literal["value", "reference"]
PolicySource = Literal["call_override", "type_rule", "session_default"]
CALL_POLICY_KWARG: str = "__extradite_policy__"


def _is_protected_module(module_name: str, protected_module_name: str) -> bool:
    """Check whether ``module_name`` matches the protected module tree.

    :param module_name: Candidate module path.
    :param protected_module_name: Protected module root.
    :returns: ``True`` when candidate belongs to the protected module tree.
    """
    is_exact_match: bool = module_name == protected_module_name
    if is_exact_match is True:
        return True
    protected_prefix: str = f"{protected_module_name}."
    return module_name.startswith(protected_prefix)


def _is_protected_by_any_module(module_name: str, protected_module_names: set[str]) -> bool:
    """Check whether ``module_name`` belongs to any protected module tree.

    :param module_name: Candidate module path.
    :param protected_module_names: Protected module roots.
    :returns: ``True`` when candidate belongs to any protected module tree.
    """
    for protected_module_name in protected_module_names:
        is_protected: bool = _is_protected_module(module_name, protected_module_name)
        if is_protected is True:
            return True
    return False


def _is_bulk_pickle_container_candidate(value: object) -> bool:
    """Report whether ``value`` is a container eligible for bulk pickle transport.

    :param value: Candidate runtime value.
    :returns: ``True`` when ``value`` is a supported container type.
    """
    if isinstance(value, list) is True:
        return True
    if isinstance(value, tuple) is True:
        return True
    if isinstance(value, set) is True:
        return True
    if isinstance(value, frozenset) is True:
        return True
    if isinstance(value, dict) is True:
        return True
    return False


def _container_item_count(value: object) -> int:
    """Return the number of top-level items in one supported container value.

    :param value: Container value.
    :returns: Top-level item count.
    :raises TypeError: If ``value`` is not a supported container.
    """
    if isinstance(value, list) is True:
        return len(value)
    if isinstance(value, tuple) is True:
        return len(value)
    if isinstance(value, set) is True:
        return len(value)
    if isinstance(value, frozenset) is True:
        return len(value)
    if isinstance(value, dict) is True:
        return len(value)
    raise TypeError("value must be a supported container")


def _try_pickle_payload(value: object) -> bytes | None:
    """Try to pickle one value and return payload bytes on success.

    :param value: Runtime value to pickle.
    :returns: Pickle payload bytes or ``None`` when pickling fails.
    """
    try:
        return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
    except (pickle.PicklingError, TypeError, AttributeError, ValueError):
        return None


def _parse_target(target: str) -> tuple[str, str]:
    """Parse ``module.path:ClassName`` targets.

    :param target: Raw target string.
    :returns: Tuple of ``(module_name, class_qualname)``.
    :raises ValueError: If the target format is invalid.
    """
    parts: list[str] = target.split(":")
    if len(parts) != 2:
        raise ValueError("Target must use module.path:ClassName format")

    module_name: str = parts[0].strip()
    class_qualname: str = parts[1].strip()
    if len(module_name) == 0:
        raise ValueError("Module path in target cannot be empty")
    if len(class_qualname) == 0:
        raise ValueError("Class name in target cannot be empty")
    return module_name, class_qualname


def _find_module_leaks(module_name: str) -> list[str]:
    """Find matching module entries present in ``sys.modules``.

    :param module_name: Fully qualified module path to protect.
    :returns: Sorted list of matching module entries.
    """
    leaks: list[str] = []
    module_prefix: str = f"{module_name}."
    for loaded_name in sys.modules:
        is_exact_match: bool = loaded_name == module_name
        is_submodule_match: bool = loaded_name.startswith(module_prefix)
        if is_exact_match is True or is_submodule_match is True:
            leaks.append(loaded_name)
    leaks.sort()
    return leaks


def _on_audit_event(event: str, args: tuple[object, ...]) -> None:
    """Track interpreter import activity.

    :param event: Audit hook event name.
    :param args: Event payload tuple.
    """
    _ = args
    if event != "import":
        return

    global _IMPORT_EPOCH
    _IMPORT_EPOCH += 1


def _ensure_import_hook_installed() -> None:
    """Install the process-level import activity hook exactly once."""
    global _IMPORT_HOOK_INSTALLED
    with _IMPORT_HOOK_LOCK:
        if _IMPORT_HOOK_INSTALLED is True:
            return
        sys.addaudithook(_on_audit_event)
        _IMPORT_HOOK_INSTALLED = True


def _get_import_epoch() -> int:
    """Return the current import activity counter.

    :returns: Monotonic import-activity counter.
    """
    return _IMPORT_EPOCH


class _RootSafeUnpickler(pickle.Unpickler):
    """Unpickler that blocks imports from protected module trees."""

    _protected_module_names: set[str]

    def __init__(self, payload_stream: io.BytesIO, protected_module_names: set[str]) -> None:
        """Initialize the guarded unpickler.

        :param payload_stream: Byte stream to decode.
        :param protected_module_names: Protected module roots.
        """
        super().__init__(payload_stream)
        self._protected_module_names = set(protected_module_names)

    def find_class(self, module: str, name: str) -> Any:
        """Resolve global references during unpickling.

        :param module: Module name referenced by pickle.
        :param name: Attribute name referenced by pickle.
        :returns: Resolved class/function object.
        :raises ExtraditeModuleLeakError: If protected-module import is requested.
        """
        is_protected: bool = _is_protected_by_any_module(module, self._protected_module_names)
        if is_protected is True:
            raise ExtraditeModuleLeakError(
                "Refusing to unpickle payload that references isolated module "
                + f"{module}.{name}"
            )
        return super().find_class(module, name)


def _safe_unpickle(payload: bytes, protected_module_names: set[str]) -> object:
    """Unpickle bytes with protected-module import blocking.

    :param payload: Pickled payload bytes.
    :param protected_module_names: Protected module roots.
    :returns: Unpickled value.
    :raises ExtraditeProtocolError: If payload cannot be unpickled.
    :raises ExtraditeModuleLeakError: If protected-module import is requested.
    """
    stream: io.BytesIO = io.BytesIO(payload)
    unpickler = _RootSafeUnpickler(stream, protected_module_names)
    try:
        return unpickler.load()
    except ExtraditeModuleLeakError:
        raise
    except (pickle.UnpicklingError, EOFError, AttributeError, ValueError) as exc:
        raise ExtraditeProtocolError("Failed to unpickle child payload") from exc


def _validate_transport_policy(transport_policy: str) -> TransportPolicy:
    """Validate and normalize transport policy values.

    :param transport_policy: Requested transport policy.
    :returns: Validated transport policy.
    :raises ValueError: If policy is unsupported.
    """
    if transport_policy == "value":
        return "value"
    if transport_policy == "reference":
        return "reference"
    raise ValueError("transport_policy must be one of: reference, value")


def _extract_call_policy(kwargs: dict[str, object]) -> TransportPolicy | None:
    """Extract and validate a per-call policy override from kwargs.

    :param kwargs: Mutable keyword-argument dictionary.
    :returns: Validated per-call policy or ``None``.
    :raises ValueError: If override value is not a valid policy string.
    """
    has_override: bool = CALL_POLICY_KWARG in kwargs
    if has_override is False:
        return None

    override_obj: object = kwargs.pop(CALL_POLICY_KWARG)
    if isinstance(override_obj, str) is False:
        raise ValueError(f"{CALL_POLICY_KWARG} must be a string policy name")
    return _validate_transport_policy(override_obj)


def _normalize_transport_type_rules(
    transport_type_rules: dict[type[object], str] | None,
) -> list[tuple[type[object], TransportPolicy]]:
    """Validate and normalize per-type transport rules.

    :param transport_type_rules: Optional mapping of ``type -> policy``.
    :returns: Ordered normalized rule list.
    :raises TypeError: If rule keys are not type objects.
    :raises ValueError: If a policy value is unsupported.
    """
    if transport_type_rules is None:
        return []

    normalized: list[tuple[type[object], TransportPolicy]] = []
    for candidate_type, policy_name in transport_type_rules.items():
        if isinstance(candidate_type, type) is False:
            raise TypeError("transport_type_rules keys must be type objects")
        normalized_policy: TransportPolicy = _validate_transport_policy(policy_name)
        normalized.append((candidate_type, normalized_policy))
    return normalized


def _type_name(type_object: type[object]) -> str:
    """Build a stable dotted name for a type object.

    :param type_object: Type object.
    :returns: Dotted ``module.qualname`` string.
    """
    module_name: str = type_object.__module__
    qualname: str = type_object.__qualname__
    return f"{module_name}.{qualname}"


def _match_transport_rule(
    value: object,
    transport_type_rules: list[tuple[type[object], TransportPolicy]],
) -> tuple[TransportPolicy | None, type[object] | None]:
    """Resolve the best matching per-type policy rule for ``value``.

    Rules are matched using ``isinstance`` semantics and ranked by MRO distance.
    Lower distance wins; ties preserve rule declaration order.

    :param value: Candidate runtime value.
    :param transport_type_rules: Ordered ``(type, policy)`` rules.
    :returns: Tuple of ``(policy, matched_type)`` when matched, otherwise ``(None, None)``.
    """
    value_type: type[object] = type(value)
    best_policy: TransportPolicy | None = None
    best_type: type[object] | None = None
    best_distance: int | None = None

    for candidate_type, policy_name in transport_type_rules:
        matches: bool = isinstance(value, candidate_type)
        if matches is False:
            continue

        mro: tuple[type[object], ...] = value_type.__mro__
        candidate_distance: int | None = None
        for index, entry in enumerate(mro):
            if entry is candidate_type:
                candidate_distance = index
                break

        if candidate_distance is None:
            candidate_distance = len(mro)

        if best_distance is None:
            best_distance = candidate_distance
            best_policy = policy_name
            best_type = candidate_type
            continue

        if candidate_distance < best_distance:
            best_distance = candidate_distance
            best_policy = policy_name
            best_type = candidate_type

    return best_policy, best_type


def _is_force_reference_candidate(value: object) -> bool:
    """Decide whether policy ``reference`` should force handle transport.

    :param value: Candidate runtime value.
    :returns: ``True`` when value should be sent by reference.
    """
    if isinstance(value, (type(None), bool, int, float, complex, str, bytes)) is True:
        return False
    return True


class _LocalObjectRegistry:
    """Store parent-side objects exposed to the child via handles."""

    _by_object_id: dict[int, object]
    _by_identity: dict[int, int]
    _next_object_id: int

    def __init__(self) -> None:
        """Initialize an empty object registry."""
        self._by_object_id = {}
        self._by_identity = {}
        self._next_object_id = 1

    def store(self, value: object) -> int:
        """Store a value and return its object identifier.

        :param value: Object to store.
        :returns: Integer identifier for the object.
        """
        identity: int = id(value)
        existing: int | None = self._by_identity.get(identity)
        if existing is not None:
            return existing

        object_id: int = self._next_object_id
        self._next_object_id += 1
        self._by_object_id[object_id] = value
        self._by_identity[identity] = object_id
        return object_id

    def get(self, object_id: int) -> object:
        """Return one stored object.

        :param object_id: Stored object identifier.
        :returns: Stored object.
        :raises ExtraditeProtocolError: If the identifier is unknown or released.
        """
        exists: bool = object_id in self._by_object_id
        if exists is False:
            raise ExtraditeProtocolError(f"Unknown or released remote object id: {object_id}")
        return self._by_object_id[object_id]

    def release(self, object_id: int) -> None:
        """Release one stored object.

        :param object_id: Stored object identifier.
        """
        exists: bool = object_id in self._by_object_id
        if exists is False:
            return

        value: object = self._by_object_id.pop(object_id)
        identity: int = id(value)
        self._by_identity.pop(identity, None)

    def clear(self) -> None:
        """Clear all entries."""
        self._by_object_id.clear()
        self._by_identity.clear()


def _finalize_remote_object(session_ref: "weakref.ReferenceType[ExtraditeSession]", object_id: int) -> None:
    """Finalize a proxy by releasing its remote object.

    :param session_ref: Weak reference to session.
    :param object_id: Remote object identifier.
    """
    session: ExtraditeSession | None = session_ref()
    if session is None:
        return
    session.release_object_safely(object_id)


class _RemoteCallProxy:
    """Call wrapper for remote callable attributes."""

    _session: "ExtraditeSession"
    _object_id: int
    _attr_name: str

    def __init__(self, session: "ExtraditeSession", object_id: int, attr_name: str) -> None:
        """Initialize a remote call wrapper.

        :param session: Owning session.
        :param object_id: Remote object identifier.
        :param attr_name: Callable attribute name.
        """
        self._session = session
        self._object_id = object_id
        self._attr_name = attr_name

    def __call__(self, *args: object, **kwargs: object) -> object:
        """Invoke the wrapped remote callable.

        :param args: Positional arguments.
        :param kwargs: Keyword arguments.
        :returns: Decoded remote result.
        """
        call_policy: TransportPolicy | None = _extract_call_policy(kwargs)
        return self._session.call_attr(
            self._object_id,
            self._attr_name,
            list(args),
            kwargs,
            call_policy=call_policy,
        )


class _ClassCloseProxy:
    """Call wrapper that releases one class proxy handle."""

    _session: "ExtraditeSession"
    _remote_target: str

    def __init__(self, session: "ExtraditeSession", remote_target: str) -> None:
        """Initialize a class close wrapper.

        :param session: Owning session.
        :param remote_target: Remote target in ``module.path:ClassName`` format.
        """
        self._session = session
        self._remote_target = remote_target

    def __call__(self) -> None:
        """Release this class proxy handle."""
        self._session.release_target(self._remote_target)


class _RemoteProxyOps:
    """Shared protocol-level operations for remote object proxies."""

    _session: "ExtraditeSession"
    _remote_object_id: int
    _finalizer: weakref.finalize

    @property
    def remote_object_id(self) -> int:
        """Return the remote object identifier.

        :returns: Remote object identifier.
        """
        return self._remote_object_id

    def close(self) -> None:
        """Release this remote object immediately."""
        was_alive: bool = self._finalizer.alive
        if was_alive is True:
            self._finalizer()

    def __getattr__(self, attr_name: str) -> object:
        """Resolve remote attributes.

        :param attr_name: Instance attribute name.
        :returns: Attribute value or callable wrapper.
        """
        return self._session.get_attr(self._remote_object_id, attr_name)

    def __setattr__(self, attr_name: str, value: object) -> None:
        """Set remote attributes.

        :param attr_name: Attribute name.
        :param value: Attribute value.
        """
        is_internal: bool = attr_name.startswith("_")
        if is_internal is True:
            object.__setattr__(self, attr_name, value)
            return
        self._session.set_attr(self._remote_object_id, attr_name, value)

    def __delattr__(self, attr_name: str) -> None:
        """Delete remote attributes.

        :param attr_name: Attribute name.
        """
        is_internal: bool = attr_name.startswith("_")
        if is_internal is True:
            object.__delattr__(self, attr_name)
            return
        self._session.del_attr(self._remote_object_id, attr_name)

    def _call_dunder(self, attr_name: str, *args: object) -> object:
        """Call one dunder method remotely.

        :param attr_name: Dunder method name.
        :param args: Positional arguments.
        :returns: Decoded remote result.
        """
        return self._session.call_attr(
            self._remote_object_id,
            attr_name,
            list(args),
            {},
        )

    def __repr__(self) -> str:
        """Return the remote ``repr`` string.

        :returns: Representation string.
        """
        value: object = self._call_dunder("__repr__")
        if isinstance(value, str) is False:
            raise ExtraditeProtocolError("Remote __repr__ did not return a string")
        return value

    def __str__(self) -> str:
        """Return the remote ``str`` string.

        :returns: String value.
        """
        value: object = self._call_dunder("__str__")
        if isinstance(value, str) is False:
            raise ExtraditeProtocolError("Remote __str__ did not return a string")
        return value

    def __bytes__(self) -> bytes:
        """Return remote ``bytes`` conversion.

        :returns: Bytes value.
        """
        value: object = self._call_dunder("__bytes__")
        if isinstance(value, bytes) is False:
            raise ExtraditeProtocolError("Remote __bytes__ did not return bytes")
        return value

    def __bool__(self) -> bool:
        """Return remote truth value.

        :returns: Truth value.
        """
        value: object = self._call_dunder("__bool__")
        if isinstance(value, bool) is False:
            raise ExtraditeProtocolError("Remote __bool__ did not return bool")
        return value

    def __len__(self) -> int:
        """Return remote length.

        :returns: Integer length.
        """
        value: object = self._call_dunder("__len__")
        if isinstance(value, int) is False:
            raise ExtraditeProtocolError("Remote __len__ did not return int")
        return value

    def __iter__(self) -> object:
        """Return remote iterator.

        :returns: Iterator-like value.
        """
        return self._call_dunder("__iter__")

    def __next__(self) -> object:
        """Return next value from remote iterator.

        :returns: Next value.
        """
        return self._call_dunder("__next__")

    def __getitem__(self, key: object) -> object:
        """Get remote item by key.

        :param key: Item key.
        :returns: Item value.
        """
        return self._call_dunder("__getitem__", key)

    def __setitem__(self, key: object, value: object) -> None:
        """Set remote item by key.

        :param key: Item key.
        :param value: Item value.
        """
        self._call_dunder("__setitem__", key, value)

    def __delitem__(self, key: object) -> None:
        """Delete remote item by key.

        :param key: Item key.
        """
        self._call_dunder("__delitem__", key)

    def __contains__(self, item: object) -> bool:
        """Check remote membership.

        :param item: Candidate item.
        :returns: Membership result.
        """
        value: object = self._call_dunder("__contains__", item)
        if isinstance(value, bool) is False:
            raise ExtraditeProtocolError("Remote __contains__ did not return bool")
        return value

    def __eq__(self, other: object) -> bool:
        """Evaluate remote equality.

        :param other: Comparator value.
        :returns: Equality result.
        """
        value: object = self._call_dunder("__eq__", other)
        if isinstance(value, bool) is False:
            raise ExtraditeProtocolError("Remote __eq__ did not return bool")
        return value

    def __ne__(self, other: object) -> bool:
        """Evaluate remote inequality.

        :param other: Comparator value.
        :returns: Inequality result.
        """
        value: object = self._call_dunder("__ne__", other)
        if isinstance(value, bool) is False:
            raise ExtraditeProtocolError("Remote __ne__ did not return bool")
        return value

    def __lt__(self, other: object) -> bool:
        """Evaluate remote less-than.

        :param other: Comparator value.
        :returns: Comparison result.
        """
        value: object = self._call_dunder("__lt__", other)
        if isinstance(value, bool) is False:
            raise ExtraditeProtocolError("Remote __lt__ did not return bool")
        return value

    def __le__(self, other: object) -> bool:
        """Evaluate remote less-or-equal.

        :param other: Comparator value.
        :returns: Comparison result.
        """
        value: object = self._call_dunder("__le__", other)
        if isinstance(value, bool) is False:
            raise ExtraditeProtocolError("Remote __le__ did not return bool")
        return value

    def __gt__(self, other: object) -> bool:
        """Evaluate remote greater-than.

        :param other: Comparator value.
        :returns: Comparison result.
        """
        value: object = self._call_dunder("__gt__", other)
        if isinstance(value, bool) is False:
            raise ExtraditeProtocolError("Remote __gt__ did not return bool")
        return value

    def __ge__(self, other: object) -> bool:
        """Evaluate remote greater-or-equal.

        :param other: Comparator value.
        :returns: Comparison result.
        """
        value: object = self._call_dunder("__ge__", other)
        if isinstance(value, bool) is False:
            raise ExtraditeProtocolError("Remote __ge__ did not return bool")
        return value

    def __call__(self, *args: object, **kwargs: object) -> object:
        """Call the remote object if callable.

        :param args: Positional arguments.
        :param kwargs: Keyword arguments.
        :returns: Remote call result.
        """
        call_policy: TransportPolicy | None = _extract_call_policy(kwargs)
        return self._session.call_attr(
            self._remote_object_id,
            "__call__",
            list(args),
            kwargs,
            call_policy=call_policy,
        )

    def get_effective_policy(self, value: object, call_policy: str | None = None) -> TransportPolicy:
        """Return the effective policy used for encoding ``value``.

        :param value: Candidate runtime value.
        :param call_policy: Optional per-call override to evaluate.
        :returns: Effective transport policy.
        """
        return self._session.get_effective_policy(value, call_policy=call_policy)

    def get_effective_policy_trace(self, value: object, call_policy: str | None = None) -> dict[str, str | None]:
        """Return a trace describing effective-policy resolution.

        :param value: Candidate runtime value.
        :param call_policy: Optional per-call override to evaluate.
        :returns: Trace with effective policy and precedence source.
        """
        return self._session.get_effective_policy_trace(value, call_policy=call_policy)

    def __enter__(self) -> object:
        """Enter remote context manager.

        :returns: Context value.
        """
        return self._call_dunder("__enter__")

    def __exit__(self, exc_type: object, exc_value: object, exc_traceback: object) -> object:
        """Exit remote context manager.

        :param exc_type: Exception type.
        :param exc_value: Exception value.
        :param exc_traceback: Exception traceback.
        :returns: Exit handler result.
        """
        return self._call_dunder("__exit__", exc_type, exc_value, exc_traceback)

    def __reduce__(self) -> object:
        """Block local pickling of proxies.

        :raises UnsupportedInteractionError: Always.
        """
        raise UnsupportedInteractionError("Proxy instances cannot be pickled in the root process")

    def __reduce_ex__(self, protocol: int) -> object:
        """Block local pickling of proxies.

        :param protocol: Pickle protocol version.
        :raises UnsupportedInteractionError: Always.
        """
        raise UnsupportedInteractionError("Proxy instances cannot be pickled in the root process")


class ExtraditedMeta(type):
    """Metaclass for dynamically generated extradited proxy classes."""

    _session: ClassVar["ExtraditeSession"]
    _class_object_id: ClassVar[int]
    _remote_target: ClassVar[str]

    def __call__(cls, *args: object, **kwargs: object) -> object:
        """Construct a remote instance and return its local proxy.

        :param args: Positional constructor arguments.
        :param kwargs: Keyword constructor arguments.
        :returns: Local proxy to the remote instance.
        """
        session: ExtraditeSession = cls._session
        class_object_id: int = cls._class_object_id
        call_policy: TransportPolicy | None = _extract_call_policy(kwargs)
        object_id: int = session.construct_instance(
            class_object_id,
            list(args),
            kwargs,
            call_policy=call_policy,
        )
        return session.get_or_create_instance_proxy(object_id, cls)

    def __getattribute__(cls, attr_name: str) -> object:
        """Resolve special metaclass attributes.

        :param attr_name: Attribute name requested on the class object.
        :returns: Resolved attribute value.
        """
        if attr_name == "close":
            session: ExtraditeSession = type.__getattribute__(cls, "_session")
            remote_target: str = type.__getattribute__(cls, "_remote_target")
            return _ClassCloseProxy(session, remote_target)
        if attr_name == "get_effective_policy":
            session = type.__getattribute__(cls, "_session")
            return session.get_effective_policy
        if attr_name == "get_effective_policy_trace":
            session = type.__getattribute__(cls, "_session")
            return session.get_effective_policy_trace
        return super().__getattribute__(attr_name)

    def __getattr__(cls, attr_name: str) -> object:
        """Resolve class attributes via the remote class object.

        :param attr_name: Class attribute name.
        :returns: Class attribute value or callable wrapper.
        """
        session: ExtraditeSession = cls._session
        class_object_id: int = cls._class_object_id
        return session.get_attr(class_object_id, attr_name)


class ExtraditedObjectBase(_RemoteProxyOps, metaclass=ExtraditedMeta):
    """Base instance proxy that forwards operations to the child process."""

    _session: ClassVar["ExtraditeSession"]
    _class_object_id: ClassVar[int]
    _remote_target: ClassVar[str]

    def __init__(self) -> None:
        """Prevent direct initialization from root process code.

        :raises UnsupportedInteractionError: Always.
        """
        raise UnsupportedInteractionError(
            "Proxy instances are created by the extradited class constructor only"
        )

    @classmethod
    def _bind_remote(cls, instance: "ExtraditedObjectBase", object_id: int) -> "ExtraditedObjectBase":
        """Bind a just-allocated instance to a remote object identifier.

        :param instance: Newly allocated instance.
        :param object_id: Remote object identifier.
        :returns: Bound instance.
        """
        session: ExtraditeSession = cls._session
        session_ref: "weakref.ReferenceType[ExtraditeSession]" = weakref.ref(session)
        object.__setattr__(instance, "_remote_object_id", object_id)
        finalizer: weakref.finalize = weakref.finalize(instance, _finalize_remote_object, session_ref, object_id)
        object.__setattr__(instance, "_finalizer", finalizer)
        return instance


class _RemoteHandleProxy(_RemoteProxyOps):
    """Generic proxy for remote handle values returned from the child."""

    _session: "ExtraditeSession"
    _remote_object_id: int
    _finalizer: weakref.finalize

    def __init__(self, session: "ExtraditeSession", object_id: int) -> None:
        """Initialize one generic remote handle proxy.

        :param session: Owning session.
        :param object_id: Remote object identifier.
        """
        object.__setattr__(self, "_session", session)
        object.__setattr__(self, "_remote_object_id", object_id)
        session_ref: "weakref.ReferenceType[ExtraditeSession]" = weakref.ref(session)
        finalizer: weakref.finalize = weakref.finalize(self, _finalize_remote_object, session_ref, object_id)
        object.__setattr__(self, "_finalizer", finalizer)


class ExtraditeSession:
    """Manage one child interpreter and its IPC channel."""

    _share_key: str | None
    _transport_policy: TransportPolicy
    _transport_type_rules: list[tuple[type[object], TransportPolicy]]
    _transport_type_rule_signature: tuple[tuple[type[object], TransportPolicy], ...]
    _close_callback: Callable[[], None] | None
    _connection: Connection | None
    _process: multiprocessing.Process | None
    _next_request_id: int
    _is_closed: bool
    _lock: threading.RLock
    _instance_proxy_cache: "weakref.WeakValueDictionary[int, ExtraditedObjectBase]"
    _handle_proxy_cache: "weakref.WeakValueDictionary[int, _RemoteHandleProxy]"
    _local_object_registry: _LocalObjectRegistry
    _protected_module_names: set[str]
    _class_object_ids_by_target: dict[str, int]
    _target_reference_counts: dict[str, int]
    _last_checked_import_epoch: int
    _last_checked_sys_modules_size: int

    def __init__(
        self,
        share_key: str | None = None,
        close_callback: Callable[[], None] | None = None,
        transport_policy: TransportPolicy = "value",
        transport_type_rules: list[tuple[type[object], TransportPolicy]] | None = None,
    ) -> None:
        """Initialize a session.

        :param share_key: Optional share key used by the session registry.
        :param close_callback: Optional callback invoked once when the session closes.
        :param transport_policy: Wire transport policy for picklable values.
        :param transport_type_rules: Optional per-type transport rules.
        """
        _ensure_import_hook_installed()
        self._share_key = share_key
        self._transport_policy = transport_policy
        if transport_type_rules is None:
            self._transport_type_rules = []
        else:
            self._transport_type_rules = list(transport_type_rules)
        self._transport_type_rule_signature = tuple(self._transport_type_rules)
        self._close_callback = close_callback
        self._connection = None
        self._process = None
        self._next_request_id = 1
        self._is_closed = False
        self._lock = threading.RLock()
        self._instance_proxy_cache = weakref.WeakValueDictionary()
        self._handle_proxy_cache = weakref.WeakValueDictionary()
        self._local_object_registry = _LocalObjectRegistry()
        self._protected_module_names = set()
        self._class_object_ids_by_target = {}
        self._target_reference_counts = {}
        self._last_checked_import_epoch = _get_import_epoch()
        self._last_checked_sys_modules_size = len(sys.modules)

    @property
    def is_closed(self) -> bool:
        """Report whether this session has been closed.

        :returns: ``True`` when the session is closed.
        """
        with self._lock:
            return self._is_closed

    def start(self) -> None:
        """Start the child interpreter process."""
        with self._lock:
            is_started: bool = self._connection is not None
            if is_started is True:
                return

            context = multiprocessing.get_context("spawn")
            parent_connection, child_connection = context.Pipe(duplex=True)
            process = context.Process(
                target=worker_entry,
                args=(child_connection, self._transport_policy),
            )
            process.daemon = True
            process.start()
            child_connection.close()

            self._connection = parent_connection
            self._process = process

            response: dict[str, object] = self._wait_for_response(expected_request_id=0)
            payload: object = response.get("payload")
            if isinstance(payload, dict) is False:
                self.close()
                raise ExtraditeProtocolError("Startup payload must be a dict")

            ready: object = payload.get("ready")
            if ready is not True:
                self.close()
                raise ExtraditeProtocolError("Startup payload missing ready marker")

            atexit.register(self.close)

    def register_target(self, module_name: str, class_qualname: str) -> int:
        """Register or reference one remote target class.

        :param module_name: Module path.
        :param class_qualname: Class qualname.
        :returns: Remote class object identifier.
        :raises ExtraditeModuleLeakError: If target module is loaded in root process.
        """
        remote_target: str = f"{module_name}:{class_qualname}"
        with self._lock:
            self.start()

            existing_object_id: int | None = self._class_object_ids_by_target.get(remote_target)
            if existing_object_id is not None:
                existing_count: int = self._target_reference_counts.get(remote_target, 0)
                self._target_reference_counts[remote_target] = existing_count + 1
                return existing_object_id

            self._assert_module_not_loaded(module_name)
            self._protected_module_names.add(module_name)

            try:
                response: dict[str, object] = self._send_request(
                    "load_class",
                    {
                        "module_name": module_name,
                        "class_qualname": class_qualname,
                    },
                )
            except Exception:
                traceback.format_exc()
                self._protected_module_names.discard(module_name)
                raise

            payload: object = response.get("payload")
            if isinstance(payload, dict) is False:
                raise ExtraditeProtocolError("load_class payload must be a dict")

            class_object_id: object = payload.get("class_object_id")
            if isinstance(class_object_id, int) is False:
                raise ExtraditeProtocolError("load_class payload missing int class_object_id")

            self._class_object_ids_by_target[remote_target] = class_object_id
            self._target_reference_counts[remote_target] = 1
            return class_object_id

    def release_target(self, remote_target: str) -> None:
        """Release one class proxy handle associated with this session.

        :param remote_target: Remote target in ``module.path:ClassName`` format.
        """
        should_close: bool = False
        with self._lock:
            current_count: int | None = self._target_reference_counts.get(remote_target)
            if current_count is None:
                return

            if current_count > 1:
                self._target_reference_counts[remote_target] = current_count - 1
                return

            self._target_reference_counts.pop(remote_target, None)
            self._class_object_ids_by_target.pop(remote_target, None)
            should_close = len(self._target_reference_counts) == 0

        if should_close is True:
            self.close()

    def _assert_module_not_loaded(self, module_name: str) -> None:
        """Ensure a protected module is absent from root-process imports.

        :param module_name: Fully-qualified module path.
        :raises ExtraditeModuleLeakError: If the module is present in ``sys.modules``.
        """
        leaks: list[str] = _find_module_leaks(module_name)
        if len(leaks) > 0:
            leak_list: str = ", ".join(leaks)
            raise ExtraditeModuleLeakError(
                "Isolated module leaked into root process: " + leak_list
            )

    def _assert_protected_modules_not_loaded(self) -> None:
        """Ensure all protected modules are absent from root-process imports."""
        protected_module_count: int = len(self._protected_module_names)
        if protected_module_count == 0:
            self._refresh_import_state_snapshot()
            return

        current_epoch: int = _get_import_epoch()
        current_sys_modules_size: int = len(sys.modules)
        import_epoch_unchanged: bool = current_epoch == self._last_checked_import_epoch
        sys_modules_size_unchanged: bool = (
            current_sys_modules_size == self._last_checked_sys_modules_size
        )
        if import_epoch_unchanged is True and sys_modules_size_unchanged is True:
            return

        while True:
            scan_start_epoch: int = _get_import_epoch()
            scan_start_sys_modules_size: int = len(sys.modules)
            protected_module_names: list[str] = sorted(self._protected_module_names)
            for module_name in protected_module_names:
                self._assert_module_not_loaded(module_name)

            scan_end_epoch: int = _get_import_epoch()
            scan_end_sys_modules_size: int = len(sys.modules)
            same_epoch: bool = scan_end_epoch == scan_start_epoch
            same_sys_modules_size: bool = (
                scan_end_sys_modules_size == scan_start_sys_modules_size
            )
            if same_epoch is True and same_sys_modules_size is True:
                self._last_checked_import_epoch = scan_end_epoch
                self._last_checked_sys_modules_size = scan_end_sys_modules_size
                return

    def _refresh_import_state_snapshot(self) -> None:
        """Capture current root-process import state for fast-path checks."""
        self._last_checked_import_epoch = _get_import_epoch()
        self._last_checked_sys_modules_size = len(sys.modules)

    def _require_connection(self) -> Connection:
        """Return the active IPC connection.

        :returns: Active connection object.
        :raises ExtraditeProtocolError: If session is closed or not started.
        """
        if self._is_closed is True:
            raise ExtraditeProtocolError("Session is closed")

        connection: Connection | None = self._connection
        if connection is None:
            raise ExtraditeProtocolError("Session is not started")
        return connection

    def _raise_child_error(self, payload: dict[str, object]) -> None:
        """Raise a local exception based on child error payload.

        :param payload: Error payload dictionary.
        :raises ExtraditeRemoteError: For unknown child error types.
        :raises ExtraditeProtocolError: For protocol errors from child.
        :raises UnsupportedInteractionError: For disallowed barrier interactions.
        """
        error_type_obj: object = payload.get("error_type", "Exception")
        error_message_obj: object = payload.get("error_message", "")
        stacktrace_obj: object = payload.get("stacktrace", "")

        error_type: str = "Exception"
        if isinstance(error_type_obj, str) is True:
            error_type = error_type_obj
        error_message: str = ""
        if isinstance(error_message_obj, str) is True:
            error_message = error_message_obj
        stacktrace: str = ""
        if isinstance(stacktrace_obj, str) is True:
            stacktrace = stacktrace_obj

        formatted: str = (
            f"Child process raised {error_type}: {error_message}\n"
            + f"Remote traceback:\n{stacktrace}"
        )

        if error_type == "UnsupportedInteractionError":
            raise UnsupportedInteractionError(formatted)
        if error_type == "ExtraditeProtocolError":
            raise ExtraditeProtocolError(formatted)
        if error_type == "ExtraditeModuleLeakError":
            raise ExtraditeModuleLeakError(formatted)
        raise ExtraditeRemoteError(error_type, error_message, stacktrace)

    def _decode_from_child(self, value: object) -> object:
        """Decode one wire value received from the child.

        :param value: Wire value.
        :returns: Runtime value.
        :raises ExtraditeProtocolError: If the payload shape is invalid.
        """
        if isinstance(value, tuple) is True:
            tuple_length: int = len(value)
            if tuple_length == 2:
                tag_obj: object = value[0]
                payload_obj: object = value[1]
                if tag_obj == WIRE_PICKLE_TAG:
                    if isinstance(payload_obj, bytes) is False:
                        raise ExtraditeProtocolError("Encoded pickle payload must be bytes")
                    return _safe_unpickle(payload_obj, self._protected_module_names)
            if tuple_length == 3:
                tag_obj = value[0]
                owner_obj: object = value[1]
                object_id_obj: object = value[2]
                if tag_obj == WIRE_REF_TAG:
                    if isinstance(owner_obj, str) is False:
                        raise ExtraditeProtocolError("Reference owner must be a string")
                    if isinstance(object_id_obj, int) is False:
                        raise ExtraditeProtocolError("Reference object id must be an int")
                    owner: str = owner_obj
                    object_id: int = object_id_obj
                    if owner == OWNER_CHILD:
                        return self.get_or_create_handle_proxy(object_id)
                    if owner == OWNER_PARENT:
                        return self._local_object_registry.get(object_id)
                    raise ExtraditeProtocolError(f"Unknown reference owner: {owner!r}")
            return tuple(self._decode_from_child(item) for item in value)

        if isinstance(value, list) is True:
            return [self._decode_from_child(item) for item in value]

        if isinstance(value, set) is True:
            decoded_items: set[object] = set()
            for item in value:
                decoded_item: object = self._decode_from_child(item)
                decoded_items.add(decoded_item)
            return decoded_items

        if isinstance(value, frozenset) is True:
            decoded_frozen_items: set[object] = set()
            for item in value:
                decoded_item = self._decode_from_child(item)
                decoded_frozen_items.add(decoded_item)
            return frozenset(decoded_frozen_items)

        if isinstance(value, dict) is True:
            decoded_dict: dict[object, object] = {}
            for key, item in value.items():
                decoded_key: object = self._decode_from_child(key)
                try:
                    hash(decoded_key)
                except TypeError as exc:
                    raise UnsupportedInteractionError("Decoded dict keys must stay hashable") from exc
                decoded_item: object = self._decode_from_child(item)
                decoded_dict[decoded_key] = decoded_item
            return decoded_dict

        return value

    def _resolve_effective_policy(
        self,
        value: object,
        call_policy: str | None = None,
    ) -> tuple[TransportPolicy, PolicySource, type[object] | None]:
        """Resolve effective transport policy for one value.

        Precedence order:
        1. Explicit per-call override.
        2. Per-type rule.
        3. Session default.

        :param value: Candidate runtime value.
        :param call_policy: Optional per-call override.
        :returns: Tuple of policy, source, and matched type.
        """
        if call_policy is not None:
            call_policy_normalized: TransportPolicy = _validate_transport_policy(call_policy)
            return call_policy_normalized, "call_override", None

        matched_policy, matched_type = _match_transport_rule(value, self._transport_type_rules)
        if matched_policy is not None:
            return matched_policy, "type_rule", matched_type

        return self._transport_policy, "session_default", None

    def get_effective_policy(self, value: object, call_policy: str | None = None) -> TransportPolicy:
        """Return the effective policy for encoding ``value``.

        :param value: Candidate runtime value.
        :param call_policy: Optional per-call override.
        :returns: Effective transport policy.
        """
        effective_policy: TransportPolicy
        source: PolicySource
        matched_type: type[object] | None
        effective_policy, source, matched_type = self._resolve_effective_policy(
            value,
            call_policy=call_policy,
        )
        _ = source
        _ = matched_type
        return effective_policy

    def get_effective_policy_trace(self, value: object, call_policy: str | None = None) -> dict[str, str | None]:
        """Return a trace describing policy precedence for ``value``.

        :param value: Candidate runtime value.
        :param call_policy: Optional per-call override.
        :returns: Trace with effective policy, source, and matched type.
        """
        effective_policy: TransportPolicy
        source: PolicySource
        matched_type: type[object] | None
        effective_policy, source, matched_type = self._resolve_effective_policy(
            value,
            call_policy=call_policy,
        )
        matched_type_name: str | None = None
        if matched_type is not None:
            matched_type_name = _type_name(matched_type)
        return {
            "effective_policy": effective_policy,
            "source": source,
            "matched_type": matched_type_name,
        }

    def _should_try_bulk_pickle_for_child(
        self,
        value: object,
        effective_policy: TransportPolicy,
        call_policy: str | None = None,
    ) -> bool:
        """Decide whether container ``value`` should use bulk pickle transport.

        :param value: Candidate runtime value.
        :param effective_policy: Resolved policy for ``value``.
        :param call_policy: Optional per-call override.
        :returns: ``True`` when bulk pickle should be attempted first.
        """
        if effective_policy != "value":
            return False

        has_type_rules: bool = len(self._transport_type_rules) > 0
        if call_policy is None and has_type_rules is True:
            return False

        container_item_count: int = _container_item_count(value)
        if container_item_count < _BULK_PICKLE_MIN_ITEMS:
            return False
        return True

    def _encode_for_child(self, value: object, call_policy: str | None = None) -> object:
        """Encode one runtime value for transport to the child.

        :param value: Runtime value.
        :param call_policy: Optional per-call override.
        :returns: Wire value.
        :raises UnsupportedInteractionError: If transfer cannot be performed safely.
        """
        if isinstance(value, ExtraditedObjectBase) is True:
            same_session: bool = value._session is self
            if same_session is False:
                raise UnsupportedInteractionError(
                    "Cannot pass proxy values between different extradite sessions"
                )
            return (WIRE_REF_TAG, OWNER_CHILD, value.remote_object_id)

        if isinstance(value, _RemoteHandleProxy) is True:
            same_session = value._session is self
            if same_session is False:
                raise UnsupportedInteractionError(
                    "Cannot pass proxy values between different extradite sessions"
                )
            return (WIRE_REF_TAG, OWNER_CHILD, value.remote_object_id)

        effective_policy: TransportPolicy
        source: PolicySource
        matched_type: type[object] | None
        effective_policy, source, matched_type = self._resolve_effective_policy(
            value,
            call_policy=call_policy,
        )
        _ = source
        _ = matched_type
        force_reference: bool = (
            effective_policy == "reference"
            and _is_force_reference_candidate(value) is True
        )
        if force_reference is True:
            object_id_force_ref: int = self._local_object_registry.store(value)
            return (WIRE_REF_TAG, OWNER_PARENT, object_id_force_ref)

        is_container_candidate: bool = _is_bulk_pickle_container_candidate(value)
        if is_container_candidate is True:
            should_try_bulk_pickle: bool = self._should_try_bulk_pickle_for_child(
                value,
                effective_policy=effective_policy,
                call_policy=call_policy,
            )
            if should_try_bulk_pickle is True:
                bulk_payload: bytes | None = _try_pickle_payload(value)
                if bulk_payload is not None:
                    return (WIRE_PICKLE_TAG, bulk_payload)

        if isinstance(value, list) is True:
            return [self._encode_for_child(item, call_policy=call_policy) for item in value]

        if isinstance(value, tuple) is True:
            return tuple(self._encode_for_child(item, call_policy=call_policy) for item in value)

        if isinstance(value, set) is True:
            encoded_items: set[object] = set()
            for item in value:
                encoded_item: object = self._encode_for_child(item, call_policy=call_policy)
                encoded_items.add(encoded_item)
            return encoded_items

        if isinstance(value, frozenset) is True:
            encoded_frozen_items: set[object] = set()
            for item in value:
                encoded_item = self._encode_for_child(item, call_policy=call_policy)
                encoded_frozen_items.add(encoded_item)
            return frozenset(encoded_frozen_items)

        if isinstance(value, dict) is True:
            encoded_dict: dict[object, object] = {}
            for key, item in value.items():
                encoded_key: object = self._encode_for_child(key, call_policy=call_policy)
                try:
                    hash(encoded_key)
                except TypeError as exc:
                    raise UnsupportedInteractionError("Encoded dict keys must stay hashable") from exc
                encoded_item: object = self._encode_for_child(item, call_policy=call_policy)
                encoded_dict[encoded_key] = encoded_item
            return encoded_dict

        payload: bytes | None = _try_pickle_payload(value)
        if payload is not None:
            return (WIRE_PICKLE_TAG, payload)

        object_id: int = self._local_object_registry.store(value)
        return (WIRE_REF_TAG, OWNER_PARENT, object_id)

    def _decode_args_from_child(self, message: dict[str, object]) -> tuple[list[object], dict[str, object]]:
        """Decode args/kwargs from child request message.

        :param message: Request message.
        :returns: Positional and keyword argument values.
        :raises ExtraditeProtocolError: If argument payload types are invalid.
        """
        raw_args: object = message.get("args", [])
        if isinstance(raw_args, list) is False:
            raise ExtraditeProtocolError("args must be a list")

        raw_kwargs: object = message.get("kwargs", {})
        if isinstance(raw_kwargs, dict) is False:
            raise ExtraditeProtocolError("kwargs must be a dict")

        args: list[object] = [self._decode_from_child(item) for item in raw_args]
        kwargs: dict[str, object] = {}
        for key, item in raw_kwargs.items():
            if isinstance(key, str) is False:
                raise ExtraditeProtocolError("kwargs keys must be strings")
            kwargs[key] = self._decode_from_child(item)
        return args, kwargs

    def _execute_local_request(self, message: dict[str, object]) -> dict[str, object]:
        """Execute one child-origin request against parent-owned objects.

        :param message: Request message.
        :returns: Response payload.
        :raises ExtraditeProtocolError: If request fields are invalid.
        """
        action_obj: object = message.get("action")
        if isinstance(action_obj, str) is False:
            raise ExtraditeProtocolError("action must be a string")
        action: str = action_obj

        if action == "get_attr":
            object_id_obj: object = message.get("object_id")
            if isinstance(object_id_obj, int) is False:
                raise ExtraditeProtocolError("object_id must be an integer")
            attr_name_obj: object = message.get("attr_name")
            if isinstance(attr_name_obj, str) is False:
                raise ExtraditeProtocolError("attr_name must be a string")

            target: object = self._local_object_registry.get(object_id_obj)
            attr_value: object = getattr(target, attr_name_obj)
            is_callable: bool = callable(attr_value)
            if is_callable is True:
                return {"callable": True}
            return {"callable": False, "value": self._encode_for_child(attr_value)}

        if action == "set_attr":
            object_id_obj = message.get("object_id")
            if isinstance(object_id_obj, int) is False:
                raise ExtraditeProtocolError("object_id must be an integer")
            attr_name_obj = message.get("attr_name")
            if isinstance(attr_name_obj, str) is False:
                raise ExtraditeProtocolError("attr_name must be a string")
            raw_value: object = message.get("value")
            value: object = self._decode_from_child(raw_value)

            target = self._local_object_registry.get(object_id_obj)
            setattr(target, attr_name_obj, value)
            return {}

        if action == "del_attr":
            object_id_obj = message.get("object_id")
            if isinstance(object_id_obj, int) is False:
                raise ExtraditeProtocolError("object_id must be an integer")
            attr_name_obj = message.get("attr_name")
            if isinstance(attr_name_obj, str) is False:
                raise ExtraditeProtocolError("attr_name must be a string")

            target = self._local_object_registry.get(object_id_obj)
            delattr(target, attr_name_obj)
            return {}

        if action == "call_attr":
            object_id_obj = message.get("object_id")
            if isinstance(object_id_obj, int) is False:
                raise ExtraditeProtocolError("object_id must be an integer")
            attr_name_obj = message.get("attr_name")
            if isinstance(attr_name_obj, str) is False:
                raise ExtraditeProtocolError("attr_name must be a string")

            args, kwargs = self._decode_args_from_child(message)
            target = self._local_object_registry.get(object_id_obj)
            callable_obj: object = getattr(target, attr_name_obj)
            callable_flag: bool = callable(callable_obj)
            if callable_flag is False:
                raise ExtraditeProtocolError(f"Attribute {attr_name_obj!r} is not callable")

            target_is_type: bool = isinstance(target, type)
            repr_or_str_call: bool = (
                target_is_type is True
                and (attr_name_obj == "__repr__" or attr_name_obj == "__str__")
                and len(args) == 0
                and len(kwargs) == 0
            )
            if repr_or_str_call is True:
                if attr_name_obj == "__repr__":
                    result = repr(target)
                else:
                    result = str(target)
            else:
                result = callable_obj(*args, **kwargs)  # type: ignore[operator]
            return {"value": self._encode_for_child(result)}

        if action == "describe_object":
            object_id_obj = message.get("object_id")
            if isinstance(object_id_obj, int) is False:
                raise ExtraditeProtocolError("object_id must be an integer")

            target: object = self._local_object_registry.get(object_id_obj)
            is_type: bool = isinstance(target, type)
            if is_type is False:
                return {"is_type": False}

            target_type: type[object] = target
            encoded_bases: list[object] = []
            for base in target_type.__bases__:
                encoded_bases.append(self._encode_for_child(base))
            return {
                "is_type": True,
                "type_name": target_type.__name__,
                "type_qualname": target_type.__qualname__,
                "type_module": target_type.__module__,
                "bases": encoded_bases,
            }

        if action == "release_object":
            object_id_obj = message.get("object_id")
            if isinstance(object_id_obj, int) is False:
                raise ExtraditeProtocolError("object_id must be an integer")
            self._local_object_registry.release(object_id_obj)
            return {}

        raise ExtraditeProtocolError(f"Unsupported action from child: {action}")

    def _handle_incoming_request(self, request_message: dict[str, object]) -> None:
        """Handle one child-origin request and send its response.

        :param request_message: Child-origin request dictionary.
        """
        request_id_obj: object = request_message.get("request_id")
        request_id: int = -1
        if isinstance(request_id_obj, int) is True:
            request_id = request_id_obj

        try:
            if isinstance(request_id_obj, int) is False:
                raise ExtraditeProtocolError("request_id must be an integer")

            payload: dict[str, object] = self._execute_local_request(request_message)
            response: dict[str, object] = {
                "request_id": request_id,
                "status": "ok",
                "payload": payload,
            }
            connection: Connection = self._require_connection()
            connection.send(response)
        except Exception as exc:
            error_response: dict[str, object] = {
                "request_id": request_id,
                "status": "error",
                "payload": {
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "stacktrace": traceback.format_exc(),
                },
            }
            connection_for_error: Connection = self._require_connection()
            try:
                connection_for_error.send(error_response)
            except (BrokenPipeError, EOFError, OSError):
                return

    def _wait_for_response(self, expected_request_id: int) -> dict[str, object]:
        """Wait for one correlated child response while servicing re-entrant requests.

        :param expected_request_id: Request id this side is waiting for.
        :returns: Response dictionary.
        :raises ExtraditeRemoteError: If child reports an unknown error type.
        :raises ExtraditeProtocolError: If response shape is invalid.
        """
        while True:
            connection: Connection = self._require_connection()
            incoming: object
            try:
                incoming = connection.recv()
            except (EOFError, BrokenPipeError, OSError) as exc:
                raise ExtraditeProtocolError("Failed to receive message from child process") from exc

            if isinstance(incoming, dict) is False:
                raise ExtraditeProtocolError("Child message must be a dict")

            message: dict[str, object] = incoming
            has_status: bool = "status" in message
            has_action: bool = "action" in message

            if has_status is True:
                request_id_obj: object = message.get("request_id")
                if isinstance(request_id_obj, int) is False:
                    raise ExtraditeProtocolError("Child response request_id must be an int")
                if request_id_obj != expected_request_id:
                    raise ExtraditeProtocolError(
                        f"Unexpected response request_id {request_id_obj}; expected {expected_request_id}"
                    )

                status_obj: object = message.get("status")
                if isinstance(status_obj, str) is False:
                    raise ExtraditeProtocolError("Child response status must be a string")
                status: str = status_obj

                if status == "ok":
                    return message
                if status != "error":
                    raise ExtraditeProtocolError(f"Unknown child response status: {status!r}")

                payload_obj: object = message.get("payload")
                if isinstance(payload_obj, dict) is False:
                    raise ExtraditeProtocolError("Error response payload must be a dict")
                self._raise_child_error(payload_obj)
                raise ExtraditeProtocolError("Unreachable child error state")

            if has_action is True:
                self._handle_incoming_request(message)
                continue

            raise ExtraditeProtocolError("Child message must include either status or action")

    def _send_request(self, action: str, payload: dict[str, object]) -> dict[str, object]:
        """Send one request and return its decoded top-level response.

        :param action: Action name.
        :param payload: Action payload.
        :returns: Response dictionary.
        """
        with self._lock:
            self._assert_protected_modules_not_loaded()
            connection: Connection = self._require_connection()

            request_id: int = self._next_request_id
            self._next_request_id += 1

            request: dict[str, object] = {
                "request_id": request_id,
                "action": action,
            }
            request.update(payload)

            try:
                connection.send(request)
            except (BrokenPipeError, EOFError, OSError) as exc:
                raise ExtraditeProtocolError("Failed to send request to child process") from exc

            return self._wait_for_response(expected_request_id=request_id)

    def construct_instance(
        self,
        class_object_id: int,
        args: list[object],
        kwargs: dict[str, object],
        call_policy: str | None = None,
    ) -> int:
        """Construct a remote class instance.

        :param class_object_id: Remote class object identifier.
        :param args: Constructor positional arguments.
        :param kwargs: Constructor keyword arguments.
        :param call_policy: Optional per-call override.
        :returns: Remote object identifier.
        """
        encoded_args: list[object] = [
            self._encode_for_child(item, call_policy=call_policy)
            for item in args
        ]
        encoded_kwargs: dict[str, object] = {}
        for key, value in kwargs.items():
            encoded_kwargs[key] = self._encode_for_child(value, call_policy=call_policy)

        response: dict[str, object] = self._send_request(
            "construct",
            {
                "class_object_id": class_object_id,
                "args": encoded_args,
                "kwargs": encoded_kwargs,
            },
        )

        payload: object = response.get("payload")
        if isinstance(payload, dict) is False:
            raise ExtraditeProtocolError("construct payload must be a dict")
        object_id: object = payload.get("object_id")
        if isinstance(object_id, int) is False:
            raise ExtraditeProtocolError("construct payload missing int object_id")
        return object_id

    def get_attr(self, object_id: int, attr_name: str) -> object:
        """Fetch an attribute from a remote object.

        :param object_id: Remote object identifier.
        :param attr_name: Attribute name.
        :returns: Attribute value or callable wrapper.
        """
        response: dict[str, object] = self._send_request(
            "get_attr",
            {
                "object_id": object_id,
                "attr_name": attr_name,
            },
        )

        payload: object = response.get("payload")
        if isinstance(payload, dict) is False:
            raise ExtraditeProtocolError("get_attr payload must be a dict")

        callable_field: object = payload.get("callable")
        if isinstance(callable_field, bool) is False:
            raise ExtraditeProtocolError("get_attr payload missing bool callable")

        if callable_field is True:
            return _RemoteCallProxy(self, object_id, attr_name)

        encoded_value: object = payload.get("value")
        return self._decode_from_child(encoded_value)

    def call_attr(
        self,
        object_id: int,
        attr_name: str,
        args: list[object],
        kwargs: dict[str, object],
        call_policy: str | None = None,
    ) -> object:
        """Call a callable attribute on a remote object.

        :param object_id: Remote object identifier.
        :param attr_name: Callable attribute name.
        :param args: Positional arguments.
        :param kwargs: Keyword arguments.
        :param call_policy: Optional per-call override.
        :returns: Decoded call result.
        """
        encoded_args: list[object] = [
            self._encode_for_child(item, call_policy=call_policy)
            for item in args
        ]
        encoded_kwargs: dict[str, object] = {}
        for key, value in kwargs.items():
            encoded_kwargs[key] = self._encode_for_child(value, call_policy=call_policy)

        response: dict[str, object] = self._send_request(
            "call_attr",
            {
                "object_id": object_id,
                "attr_name": attr_name,
                "args": encoded_args,
                "kwargs": encoded_kwargs,
            },
        )

        payload: object = response.get("payload")
        if isinstance(payload, dict) is False:
            raise ExtraditeProtocolError("call_attr payload must be a dict")

        encoded_value: object = payload.get("value")
        return self._decode_from_child(encoded_value)

    def set_attr(
        self,
        object_id: int,
        attr_name: str,
        value: object,
        call_policy: str | None = None,
    ) -> None:
        """Set an attribute on a remote object.

        :param object_id: Remote object identifier.
        :param attr_name: Attribute name.
        :param value: New value.
        :param call_policy: Optional per-call override.
        """
        encoded_value: object = self._encode_for_child(value, call_policy=call_policy)
        self._send_request(
            "set_attr",
            {
                "object_id": object_id,
                "attr_name": attr_name,
                "value": encoded_value,
            },
        )

    def del_attr(self, object_id: int, attr_name: str) -> None:
        """Delete an attribute on a remote object.

        :param object_id: Remote object identifier.
        :param attr_name: Attribute name.
        """
        self._send_request(
            "del_attr",
            {
                "object_id": object_id,
                "attr_name": attr_name,
            },
        )

    def release_object_safely(self, object_id: int) -> None:
        """Best-effort remote object release used by finalizers.

        :param object_id: Remote object identifier.
        """
        try:
            self.release_object(object_id)
        except (ExtraditeProtocolError, ExtraditeRemoteError, OSError):
            return

    def release_object(self, object_id: int) -> None:
        """Release a remote object.

        :param object_id: Remote object identifier.
        """
        self._instance_proxy_cache.pop(object_id, None)
        self._handle_proxy_cache.pop(object_id, None)
        self._send_request(
            "release_object",
            {
                "object_id": object_id,
            },
        )

    def get_or_create_instance_proxy(
        self,
        object_id: int,
        proxy_type: type[ExtraditedObjectBase],
    ) -> ExtraditedObjectBase:
        """Get a cached class-instance proxy or create a new one.

        :param object_id: Remote object identifier.
        :param proxy_type: Desired proxy class.
        :returns: Bound proxy object.
        """
        cached: ExtraditedObjectBase | None = self._instance_proxy_cache.get(object_id)
        if cached is not None:
            return cached

        instance: ExtraditedObjectBase = object.__new__(proxy_type)
        bound: ExtraditedObjectBase = proxy_type._bind_remote(instance, object_id)
        self._instance_proxy_cache[object_id] = bound
        return bound

    def get_or_create_handle_proxy(self, object_id: int) -> _RemoteHandleProxy:
        """Get a cached generic handle proxy or create a new one.

        :param object_id: Remote object identifier.
        :returns: Generic handle proxy.
        """
        cached: _RemoteHandleProxy | None = self._handle_proxy_cache.get(object_id)
        if cached is not None:
            return cached

        created: _RemoteHandleProxy = _RemoteHandleProxy(self, object_id)
        self._handle_proxy_cache[object_id] = created
        return created

    def close(self) -> None:
        """Close the session and terminate the child process."""
        close_callback: Callable[[], None] | None = None
        with self._lock:
            already_closed: bool = self._is_closed
            if already_closed is True:
                return

            self._is_closed = True
            connection: Connection | None = self._connection
            process: multiprocessing.Process | None = self._process

            if connection is not None:
                try:
                    request: dict[str, object] = {
                        "request_id": self._next_request_id,
                        "action": "shutdown",
                    }
                    connection.send(request)
                    self._next_request_id += 1
                except (BrokenPipeError, EOFError, OSError):
                    pass

                try:
                    connection.close()
                except OSError:
                    pass

            if process is not None:
                process.join(timeout=2.0)
                is_alive: bool = process.is_alive()
                if is_alive is True:
                    process.terminate()
                    process.join(timeout=2.0)

            self._connection = None
            self._process = None
            self._instance_proxy_cache.clear()
            self._handle_proxy_cache.clear()
            self._local_object_registry.clear()
            self._class_object_ids_by_target.clear()
            self._target_reference_counts.clear()
            self._protected_module_names.clear()
            close_callback = self._close_callback
            self._close_callback = None

        if close_callback is not None:
            close_callback()


def _remove_shared_session_if_current(share_key: str, session: ExtraditeSession) -> None:
    """Remove a shared session only when it is still the current registry entry.

    :param share_key: Shared-session key.
    :param session: Candidate session to remove.
    """
    with _SHARED_SESSION_LOCK:
        current_session: ExtraditeSession | None = _SHARED_SESSIONS_BY_KEY.get(share_key)
        if current_session is session:
            _SHARED_SESSIONS_BY_KEY.pop(share_key, None)


def _get_or_create_shared_session(
    share_key: str,
    transport_policy: TransportPolicy,
    transport_type_rules: list[tuple[type[object], TransportPolicy]],
) -> tuple[ExtraditeSession, bool]:
    """Get or create a shared session for ``share_key``.

    :param share_key: Shared-session key.
    :param transport_policy: Requested transport policy.
    :param transport_type_rules: Requested per-type transport rules.
    :returns: Tuple of ``(session, was_created)``.
    :raises ValueError: If an active shared session has conflicting policy settings.
    """
    with _SHARED_SESSION_LOCK:
        existing: ExtraditeSession | None = _SHARED_SESSIONS_BY_KEY.get(share_key)
        if existing is not None:
            is_closed: bool = existing.is_closed
            if is_closed is False:
                same_policy: bool = existing._transport_policy == transport_policy
                if same_policy is False:
                    raise ValueError(
                        "share_key already exists with transport_policy="
                        + f"{existing._transport_policy!r}; requested {transport_policy!r}"
                    )
                same_type_rules: bool = (
                    existing._transport_type_rule_signature
                    == tuple(transport_type_rules)
                )
                if same_type_rules is False:
                    raise ValueError(
                        "share_key already exists with conflicting transport_type_rules"
                    )
                return existing, False
            _SHARED_SESSIONS_BY_KEY.pop(share_key, None)

        session = ExtraditeSession(
            share_key=share_key,
            close_callback=lambda: _remove_shared_session_if_current(share_key, session),
            transport_policy=transport_policy,
            transport_type_rules=transport_type_rules,
        )
        _SHARED_SESSIONS_BY_KEY[share_key] = session
        return session, True


def _build_proxy_class(
    module_name: str,
    class_qualname: str,
    session: ExtraditeSession,
    class_object_id: int,
) -> type:
    """Build the dynamic proxy class.

    :param module_name: Isolated module name.
    :param class_qualname: Remote class qualname.
    :param session: Active session.
    :returns: Dynamic proxy class.
    """
    class_name: str = class_qualname.split(".")[-1]
    remote_target: str = f"{module_name}:{class_qualname}"

    namespace: dict[str, object] = {
        "__module__": "extradite.runtime",
        "__doc__": f"Proxy class for remote target {remote_target}.",
        "_session": session,
        "_class_object_id": class_object_id,
        "_remote_target": remote_target,
    }
    proxy_class: type = ExtraditedMeta(class_name, (ExtraditedObjectBase,), namespace)
    return proxy_class


def create_extradited_class(
    target: str,
    share_key: str | None = None,
    transport_policy: TransportPolicy = "value",
    transport_type_rules: dict[type[object], str] | None = None,
) -> type:
    """Create a class proxy for an isolated import target.

    :param target: Target in ``module.path:ClassName`` format.
    :param share_key: Optional key that reuses a child process across calls.
    :param transport_policy: Wire transport policy for picklable values.
    :param transport_type_rules: Optional per-type transport rules.
    :returns: Dynamic proxy class for that target.
    :raises ExtraditeModuleLeakError: If target module already leaked into root process.
    """
    validated_transport_policy: TransportPolicy = _validate_transport_policy(transport_policy)
    normalized_transport_type_rules: list[tuple[type[object], TransportPolicy]] = _normalize_transport_type_rules(
        transport_type_rules
    )
    module_name, class_qualname = _parse_target(target)
    session: ExtraditeSession
    is_new_session: bool
    if share_key is None:
        session = ExtraditeSession(
            transport_policy=validated_transport_policy,
            transport_type_rules=normalized_transport_type_rules,
        )
        is_new_session = True
    else:
        session, is_new_session = _get_or_create_shared_session(
            share_key,
            validated_transport_policy,
            normalized_transport_type_rules,
        )

    try:
        session.start()
        class_object_id: int = session.register_target(module_name, class_qualname)
    except Exception:
        traceback.format_exc()
        if is_new_session is True:
            session.close()
        raise

    return _build_proxy_class(module_name, class_qualname, session, class_object_id)
