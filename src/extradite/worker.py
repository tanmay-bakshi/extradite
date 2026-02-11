"""Child interpreter worker for extradite."""

import importlib
import pickle
import traceback
import weakref
from multiprocessing.connection import Connection
from typing import Any
from typing import Literal

from extradite.errors import ExtraditeProtocolError
from extradite.errors import UnsupportedInteractionError

WIRE_PICKLE_TAG: str = "__extradite_transport_pickle_v1__"
WIRE_REF_TAG: str = "__extradite_transport_ref_v1__"
OWNER_PARENT: str = "parent"
OWNER_CHILD: str = "child"
TransportPolicy = Literal["value", "reference"]
_BULK_PICKLE_MIN_ITEMS: int = 32
_PARENT_TYPE_PROXY_RUNTIME_ATTR: str = "__extradite_parent_type_proxy_runtime__"
_PARENT_TYPE_PROXY_OBJECT_ID_ATTR: str = "__extradite_parent_type_proxy_object_id__"


def _is_protected_module(module_name: str, protected_module_name: str) -> bool:
    """Check whether ``module_name`` matches the protected module tree.

    :param module_name: Candidate module name.
    :param protected_module_name: Protected module root.
    :returns: ``True`` when candidate belongs to the protected module tree.
    """
    is_exact_match: bool = module_name == protected_module_name
    if is_exact_match is True:
        return True
    protected_prefix: str = f"{protected_module_name}."
    return module_name.startswith(protected_prefix)


def _is_protected_by_any_module(module_name: str, protected_module_names: set[str]) -> bool:
    """Check whether ``module_name`` matches any protected module tree.

    :param module_name: Candidate module name.
    :param protected_module_names: Protected module roots.
    :returns: ``True`` when candidate belongs to any protected module tree.
    """
    for protected_module_name in protected_module_names:
        is_protected: bool = _is_protected_module(module_name, protected_module_name)
        if is_protected is True:
            return True
    return False


def _value_originates_from_module(
    value: object,
    protected_module_names: set[str],
    seen_ids: set[int],
) -> bool:
    """Recursively detect references to values from protected modules.

    :param value: Value to inspect.
    :param protected_module_names: Protected module roots.
    :param seen_ids: Cycle-breaker set.
    :returns: ``True`` when a protected-module reference is found.
    """
    value_module: object = getattr(value, "__module__", None)
    if isinstance(value_module, str) is True:
        is_protected: bool = _is_protected_by_any_module(value_module, protected_module_names)
        if is_protected is True:
            return True

    value_type: type = type(value)
    type_module: str = value_type.__module__
    type_is_protected: bool = _is_protected_by_any_module(type_module, protected_module_names)
    if type_is_protected is True:
        return True

    if isinstance(value, (type(None), bool, int, float, complex, str, bytes)) is True:
        return False

    identity: int = id(value)
    seen_before: bool = identity in seen_ids
    if seen_before is True:
        return False
    seen_ids.add(identity)

    if isinstance(value, (list, tuple, set, frozenset)) is True:
        for item in value:
            item_is_protected: bool = _value_originates_from_module(item, protected_module_names, seen_ids)
            if item_is_protected is True:
                return True
        return False

    if isinstance(value, dict) is True:
        for key, item in value.items():
            key_is_protected: bool = _value_originates_from_module(key, protected_module_names, seen_ids)
            if key_is_protected is True:
                return True
            item_is_protected = _value_originates_from_module(item, protected_module_names, seen_ids)
            if item_is_protected is True:
                return True
        return False

    if isinstance(value, slice) is True:
        start_is_protected: bool = _value_originates_from_module(value.start, protected_module_names, seen_ids)
        if start_is_protected is True:
            return True
        stop_is_protected: bool = _value_originates_from_module(value.stop, protected_module_names, seen_ids)
        if stop_is_protected is True:
            return True
        step_is_protected: bool = _value_originates_from_module(value.step, protected_module_names, seen_ids)
        if step_is_protected is True:
            return True
        return False

    instance_dict: object = getattr(value, "__dict__", None)
    if isinstance(instance_dict, dict) is True:
        for item in instance_dict.values():
            item_is_protected = _value_originates_from_module(item, protected_module_names, seen_ids)
            if item_is_protected is True:
                return True

    slots: object = getattr(value_type, "__slots__", ())
    slot_names: list[str] = []
    if isinstance(slots, str) is True:
        slot_names = [slots]
    if isinstance(slots, tuple) is True:
        slot_names = [name for name in slots if isinstance(name, str) is True]
    if isinstance(slots, list) is True:
        slot_names = [name for name in slots if isinstance(name, str) is True]

    for slot_name in slot_names:
        has_slot: bool = hasattr(value, slot_name)
        if has_slot is False:
            continue
        slot_value: object = getattr(value, slot_name)
        slot_is_protected: bool = _value_originates_from_module(slot_value, protected_module_names, seen_ids)
        if slot_is_protected is True:
            return True

    return False


def _is_force_reference_candidate(value: object) -> bool:
    """Decide whether policy ``reference`` should force handle transport.

    :param value: Candidate runtime value.
    :returns: ``True`` when value should be sent by reference.
    """
    if isinstance(value, (type(None), bool, int, float, complex, str, bytes)) is True:
        return False
    return True


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


def _extract_parent_type_proxy_object_id(value: object) -> int | None:
    """Return parent object id when ``value`` is a parent type proxy.

    :param value: Candidate value.
    :returns: Parent object identifier or ``None``.
    """
    if isinstance(value, type) is False:
        return None

    try:
        object_id_obj: object = type.__getattribute__(value, _PARENT_TYPE_PROXY_OBJECT_ID_ATTR)
        runtime_obj: object = type.__getattribute__(value, _PARENT_TYPE_PROXY_RUNTIME_ATTR)
    except AttributeError:
        return None

    if isinstance(object_id_obj, int) is False:
        return None
    if isinstance(runtime_obj, WorkerRuntime) is False:
        return None
    return object_id_obj


def _extract_parent_type_proxy_instance_class_id(value: object) -> int | None:
    """Return parent class object id when ``value`` is a parent type-proxy instance.

    :param value: Candidate value.
    :returns: Parent class object identifier or ``None``.
    """
    value_type: type[object] = type(value)
    return _extract_parent_type_proxy_object_id(value_type)


class ObjectRegistry:
    """Store worker-side objects under stable integer identifiers."""

    _by_object_id: dict[int, object]
    _by_identity: dict[int, int]
    _pinned_object_ids: set[int]
    _next_object_id: int

    def __init__(self) -> None:
        """Initialize an empty object registry."""
        self._by_object_id = {}
        self._by_identity = {}
        self._pinned_object_ids = set()
        self._next_object_id = 1

    def store(self, value: object, pinned: bool = False) -> int:
        """Store a value and return its object identifier.

        :param value: Object to store.
        :param pinned: Whether the object should be protected from release.
        :returns: Integer identifier for the object.
        """
        identity: int = id(value)
        existing: int | None = self._by_identity.get(identity)
        if existing is not None:
            if pinned is True:
                self._pinned_object_ids.add(existing)
            return existing

        object_id: int = self._next_object_id
        self._next_object_id += 1
        self._by_object_id[object_id] = value
        self._by_identity[identity] = object_id
        if pinned is True:
            self._pinned_object_ids.add(object_id)
        return object_id

    def get(self, object_id: int) -> object:
        """Get a stored object.

        :param object_id: Identifier of the object.
        :returns: Stored object.
        :raises ExtraditeProtocolError: If the identifier is unknown or released.
        """
        exists: bool = object_id in self._by_object_id
        if exists is False:
            raise ExtraditeProtocolError(f"Unknown or released remote object id: {object_id}")
        return self._by_object_id[object_id]

    def release(self, object_id: int) -> None:
        """Release an object unless pinned.

        :param object_id: Identifier of the object.
        """
        is_pinned: bool = object_id in self._pinned_object_ids
        if is_pinned is True:
            return

        exists: bool = object_id in self._by_object_id
        if exists is False:
            return

        value: object = self._by_object_id.pop(object_id)
        identity: int = id(value)
        self._by_identity.pop(identity, None)

    def clear(self) -> None:
        """Clear all stored objects."""
        self._by_object_id.clear()
        self._by_identity.clear()
        self._pinned_object_ids.clear()


class _RemotePeerError(Exception):
    """Carry remote exception details across nested request boundaries."""

    remote_type_name: str
    remote_message: str
    remote_traceback: str

    def __init__(
        self,
        remote_type_name: str,
        remote_message: str,
        remote_traceback: str,
    ) -> None:
        """Initialize a remote peer error value.

        :param remote_type_name: Remote exception type name.
        :param remote_message: Remote exception message.
        :param remote_traceback: Remote exception traceback text.
        """
        self.remote_type_name = remote_type_name
        self.remote_message = remote_message
        self.remote_traceback = remote_traceback
        message: str = (
            f"Remote side raised {remote_type_name}: {remote_message}\n"
            + f"Remote traceback:\n{remote_traceback}"
        )
        super().__init__(message)


def _send_ok(connection: Connection, request_id: int, payload: dict[str, object]) -> None:
    """Send a success response.

    :param connection: IPC connection.
    :param request_id: Request identifier.
    :param payload: Response payload.
    """
    message: dict[str, object] = {
        "request_id": request_id,
        "status": "ok",
        "payload": payload,
    }
    try:
        connection.send(message)
    except (BrokenPipeError, EOFError, OSError):
        return


def _send_error(connection: Connection, request_id: int, error_type: str, error_message: str, stacktrace: str) -> None:
    """Send an error response.

    :param connection: IPC connection.
    :param request_id: Request identifier.
    :param error_type: Name of the exception class.
    :param error_message: Exception message.
    :param stacktrace: Formatted stacktrace.
    """
    message: dict[str, object] = {
        "request_id": request_id,
        "status": "error",
        "payload": {
            "error_type": error_type,
            "error_message": error_message,
            "stacktrace": stacktrace,
        },
    }
    try:
        connection.send(message)
    except (BrokenPipeError, EOFError, OSError):
        return


def _resolve_qualname(root: object, qualname: str) -> object:
    """Resolve a dotted qualname against a root object.

    :param root: Root object.
    :param qualname: Dotted qualname, such as ``Outer.Inner``.
    :returns: Resolved object.
    """
    current: object = root
    pieces: list[str] = qualname.split(".")
    for piece in pieces:
        current = getattr(current, piece)
    return current


def _require_request_id(message: dict[str, object]) -> int:
    """Extract and validate the request identifier.

    :param message: Request message.
    :returns: Request identifier.
    :raises ExtraditeProtocolError: If ``request_id`` is missing or invalid.
    """
    request_id: object = message.get("request_id")
    if isinstance(request_id, int) is False:
        raise ExtraditeProtocolError("request_id must be an integer")
    return request_id


def _require_action(message: dict[str, object]) -> str:
    """Extract and validate the action string.

    :param message: Request message.
    :returns: Action string.
    :raises ExtraditeProtocolError: If ``action`` is missing or invalid.
    """
    action: object = message.get("action")
    if isinstance(action, str) is False:
        raise ExtraditeProtocolError("action must be a string")
    return action


def _require_int_field(message: dict[str, object], key: str) -> int:
    """Extract and validate an integer field.

    :param message: Request message.
    :param key: Field name.
    :returns: Integer field value.
    :raises ExtraditeProtocolError: If the field is missing or invalid.
    """
    value: object = message.get(key)
    if isinstance(value, int) is False:
        raise ExtraditeProtocolError(f"{key} must be an integer")
    return value


def _require_str_field(message: dict[str, object], key: str) -> str:
    """Extract and validate a string field.

    :param message: Request message.
    :param key: Field name.
    :returns: String field value.
    :raises ExtraditeProtocolError: If the field is missing or invalid.
    """
    value: object = message.get(key)
    if isinstance(value, str) is False:
        raise ExtraditeProtocolError(f"{key} must be a string")
    return value


class _ParentTypeCloseProxy:
    """Callable close wrapper for parent-origin type proxies."""

    _runtime: "WorkerRuntime"
    _object_id: int

    def __init__(self, runtime: "WorkerRuntime", object_id: int) -> None:
        """Initialize a close wrapper.

        :param runtime: Worker runtime.
        :param object_id: Parent-owned object identifier.
        """
        self._runtime = runtime
        self._object_id = object_id

    def __call__(self) -> None:
        """Release one parent-owned type handle."""
        self._runtime.release_parent_object(self._object_id)


class _ParentTypeProxyMeta(type):
    """Metaclass used for parent-origin type handles."""

    def __getattribute__(cls, attr_name: str) -> object:
        """Resolve attributes from local metadata or parent process.

        :param attr_name: Attribute name.
        :returns: Attribute value.
        """
        if attr_name == "close":
            runtime_obj: object = type.__getattribute__(cls, _PARENT_TYPE_PROXY_RUNTIME_ATTR)
            object_id_obj: object = type.__getattribute__(cls, _PARENT_TYPE_PROXY_OBJECT_ID_ATTR)
            if isinstance(runtime_obj, WorkerRuntime) is False:
                raise ExtraditeProtocolError("Parent type proxy runtime metadata is invalid")
            if isinstance(object_id_obj, int) is False:
                raise ExtraditeProtocolError("Parent type proxy object id metadata is invalid")
            return _ParentTypeCloseProxy(runtime_obj, object_id_obj)

        try:
            return super().__getattribute__(attr_name)
        except AttributeError:
            runtime_obj = type.__getattribute__(cls, _PARENT_TYPE_PROXY_RUNTIME_ATTR)
            object_id_obj = type.__getattribute__(cls, _PARENT_TYPE_PROXY_OBJECT_ID_ATTR)
            if isinstance(runtime_obj, WorkerRuntime) is False:
                raise ExtraditeProtocolError("Parent type proxy runtime metadata is invalid")
            if isinstance(object_id_obj, int) is False:
                raise ExtraditeProtocolError("Parent type proxy object id metadata is invalid")
            return runtime_obj.get_parent_attr(object_id_obj, attr_name)

    def __setattr__(cls, attr_name: str, value: object) -> None:
        """Set attributes in the parent process.

        :param attr_name: Attribute name.
        :param value: Attribute value.
        """
        is_internal: bool = attr_name.startswith("_")
        if is_internal is True:
            super().__setattr__(attr_name, value)
            return

        runtime_obj: object = type.__getattribute__(cls, _PARENT_TYPE_PROXY_RUNTIME_ATTR)
        object_id_obj: object = type.__getattribute__(cls, _PARENT_TYPE_PROXY_OBJECT_ID_ATTR)
        if isinstance(runtime_obj, WorkerRuntime) is False:
            raise ExtraditeProtocolError("Parent type proxy runtime metadata is invalid")
        if isinstance(object_id_obj, int) is False:
            raise ExtraditeProtocolError("Parent type proxy object id metadata is invalid")
        runtime_obj.set_parent_attr(object_id_obj, attr_name, value)

    def __delattr__(cls, attr_name: str) -> None:
        """Delete attributes in the parent process.

        :param attr_name: Attribute name.
        """
        is_internal: bool = attr_name.startswith("_")
        if is_internal is True:
            super().__delattr__(attr_name)
            return

        runtime_obj: object = type.__getattribute__(cls, _PARENT_TYPE_PROXY_RUNTIME_ATTR)
        object_id_obj: object = type.__getattribute__(cls, _PARENT_TYPE_PROXY_OBJECT_ID_ATTR)
        if isinstance(runtime_obj, WorkerRuntime) is False:
            raise ExtraditeProtocolError("Parent type proxy runtime metadata is invalid")
        if isinstance(object_id_obj, int) is False:
            raise ExtraditeProtocolError("Parent type proxy object id metadata is invalid")
        runtime_obj.del_parent_attr(object_id_obj, attr_name)

    def __call__(cls, *args: object, **kwargs: object) -> object:
        """Instantiate the parent-origin class handle.

        :param args: Positional arguments.
        :param kwargs: Keyword arguments.
        :returns: Decoded constructor result.
        """
        runtime_obj: object = type.__getattribute__(cls, _PARENT_TYPE_PROXY_RUNTIME_ATTR)
        object_id_obj: object = type.__getattribute__(cls, _PARENT_TYPE_PROXY_OBJECT_ID_ATTR)
        if isinstance(runtime_obj, WorkerRuntime) is False:
            raise ExtraditeProtocolError("Parent type proxy runtime metadata is invalid")
        if isinstance(object_id_obj, int) is False:
            raise ExtraditeProtocolError("Parent type proxy object id metadata is invalid")
        return runtime_obj.call_parent_attr(object_id_obj, "__call__", list(args), kwargs)

    def __instancecheck__(cls, instance: object) -> bool:
        """Evaluate ``isinstance(instance, cls)`` via parent semantics.

        :param instance: Candidate instance.
        :returns: ``True`` when parent-side ``isinstance`` succeeds.
        """
        runtime_obj: object = type.__getattribute__(cls, _PARENT_TYPE_PROXY_RUNTIME_ATTR)
        object_id_obj: object = type.__getattribute__(cls, _PARENT_TYPE_PROXY_OBJECT_ID_ATTR)
        if isinstance(runtime_obj, WorkerRuntime) is False:
            raise ExtraditeProtocolError("Parent type proxy runtime metadata is invalid")
        if isinstance(object_id_obj, int) is False:
            raise ExtraditeProtocolError("Parent type proxy object id metadata is invalid")
        result: object = runtime_obj.call_parent_attr(object_id_obj, "__instancecheck__", [instance], {})
        if isinstance(result, bool) is False:
            raise ExtraditeProtocolError("Remote __instancecheck__ did not return bool")
        return result

    def __subclasscheck__(cls, subclass: object) -> bool:
        """Evaluate ``issubclass(subclass, cls)`` via parent semantics.

        :param subclass: Candidate subclass.
        :returns: ``True`` when parent-side ``issubclass`` succeeds.
        """
        runtime_obj: object = type.__getattribute__(cls, _PARENT_TYPE_PROXY_RUNTIME_ATTR)
        object_id_obj: object = type.__getattribute__(cls, _PARENT_TYPE_PROXY_OBJECT_ID_ATTR)
        if isinstance(runtime_obj, WorkerRuntime) is False:
            raise ExtraditeProtocolError("Parent type proxy runtime metadata is invalid")
        if isinstance(object_id_obj, int) is False:
            raise ExtraditeProtocolError("Parent type proxy object id metadata is invalid")
        result: object = runtime_obj.call_parent_attr(object_id_obj, "__subclasscheck__", [subclass], {})
        if isinstance(result, bool) is False:
            raise ExtraditeProtocolError("Remote __subclasscheck__ did not return bool")
        return result

    def __repr__(cls) -> str:
        """Return parent ``repr`` for this type proxy.

        :returns: Parent representation string.
        """
        runtime_obj: object = type.__getattribute__(cls, _PARENT_TYPE_PROXY_RUNTIME_ATTR)
        object_id_obj: object = type.__getattribute__(cls, _PARENT_TYPE_PROXY_OBJECT_ID_ATTR)
        if isinstance(runtime_obj, WorkerRuntime) is False:
            raise ExtraditeProtocolError("Parent type proxy runtime metadata is invalid")
        if isinstance(object_id_obj, int) is False:
            raise ExtraditeProtocolError("Parent type proxy object id metadata is invalid")
        value: object = runtime_obj.call_parent_attr(object_id_obj, "__repr__", [], {})
        if isinstance(value, str) is False:
            raise ExtraditeProtocolError("Remote __repr__ did not return a string")
        return value

    def __str__(cls) -> str:
        """Return parent ``str`` for this type proxy.

        :returns: Parent string representation.
        """
        runtime_obj: object = type.__getattribute__(cls, _PARENT_TYPE_PROXY_RUNTIME_ATTR)
        object_id_obj: object = type.__getattribute__(cls, _PARENT_TYPE_PROXY_OBJECT_ID_ATTR)
        if isinstance(runtime_obj, WorkerRuntime) is False:
            raise ExtraditeProtocolError("Parent type proxy runtime metadata is invalid")
        if isinstance(object_id_obj, int) is False:
            raise ExtraditeProtocolError("Parent type proxy object id metadata is invalid")
        value: object = runtime_obj.call_parent_attr(object_id_obj, "__str__", [], {})
        if isinstance(value, str) is False:
            raise ExtraditeProtocolError("Remote __str__ did not return a string")
        return value


class _ParentCallProxy:
    """Call wrapper for parent-owned callable attributes."""

    _runtime: "WorkerRuntime"
    _object_id: int
    _attr_name: str

    def __init__(self, runtime: "WorkerRuntime", object_id: int, attr_name: str) -> None:
        """Initialize a parent call wrapper.

        :param runtime: Worker runtime.
        :param object_id: Parent-owned object identifier.
        :param attr_name: Attribute to call.
        """
        self._runtime = runtime
        self._object_id = object_id
        self._attr_name = attr_name

    def __call__(self, *args: object, **kwargs: object) -> object:
        """Invoke the wrapped parent callable attribute.

        :param args: Positional arguments.
        :param kwargs: Keyword arguments.
        :returns: Decoded parent response value.
        """
        return self._runtime.call_parent_attr(self._object_id, self._attr_name, list(args), kwargs)


class _ParentRemoteHandle:
    """Proxy object for a parent-owned handle exposed to the worker."""

    _runtime: "WorkerRuntime"
    _remote_object_id: int
    _is_closed: bool

    def __init__(self, runtime: "WorkerRuntime", object_id: int) -> None:
        """Initialize a parent remote handle proxy.

        :param runtime: Worker runtime.
        :param object_id: Parent-owned object identifier.
        """
        object.__setattr__(self, "_runtime", runtime)
        object.__setattr__(self, "_remote_object_id", object_id)
        object.__setattr__(self, "_is_closed", False)

    @property
    def remote_object_id(self) -> int:
        """Return the parent-owned object identifier.

        :returns: Parent-owned object identifier.
        """
        return object.__getattribute__(self, "_remote_object_id")

    def close(self) -> None:
        """Release this parent-owned handle."""
        already_closed: bool = self._is_closed
        if already_closed is True:
            return
        object.__setattr__(self, "_is_closed", True)
        self._runtime.release_parent_object(self._remote_object_id)

    def __getattr__(self, attr_name: str) -> object:
        """Resolve attributes from the parent process.

        :param attr_name: Attribute name.
        :returns: Remote attribute value or call proxy.
        """
        is_internal: bool = attr_name.startswith("_")
        if is_internal is True:
            raise AttributeError(attr_name)
        return self._runtime.get_parent_attr(self._remote_object_id, attr_name)

    def __setattr__(self, attr_name: str, value: object) -> None:
        """Set attributes in the parent process.

        :param attr_name: Attribute name.
        :param value: Attribute value.
        """
        is_internal: bool = attr_name.startswith("_")
        if is_internal is True:
            object.__setattr__(self, attr_name, value)
            return
        self._runtime.set_parent_attr(self._remote_object_id, attr_name, value)

    def __delattr__(self, attr_name: str) -> None:
        """Delete attributes in the parent process.

        :param attr_name: Attribute name.
        """
        is_internal: bool = attr_name.startswith("_")
        if is_internal is True:
            object.__delattr__(self, attr_name)
            return
        self._runtime.del_parent_attr(self._remote_object_id, attr_name)

    def _call_dunder(self, attr_name: str, *args: object) -> object:
        """Call one dunder method on the parent-owned object.

        :param attr_name: Dunder method name.
        :param args: Dunder arguments.
        :returns: Remote return value.
        """
        return self._runtime.call_parent_attr(self._remote_object_id, attr_name, list(args), {})

    def __repr__(self) -> str:
        """Return remote ``repr``.

        :returns: Representation string.
        """
        value: object = self._call_dunder("__repr__")
        if isinstance(value, str) is False:
            raise ExtraditeProtocolError("Remote __repr__ did not return a string")
        return value

    def __str__(self) -> str:
        """Return remote ``str``.

        :returns: String value.
        """
        value: object = self._call_dunder("__str__")
        if isinstance(value, str) is False:
            raise ExtraditeProtocolError("Remote __str__ did not return a string")
        return value

    def __bytes__(self) -> bytes:
        """Return remote ``bytes`` value.

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
        """Return remote ``len`` value.

        :returns: Integer length.
        """
        value: object = self._call_dunder("__len__")
        if isinstance(value, int) is False:
            raise ExtraditeProtocolError("Remote __len__ did not return int")
        return value

    def __iter__(self) -> object:
        """Return remote iterator object.

        :returns: Iterator object.
        """
        return self._call_dunder("__iter__")

    def __next__(self) -> object:
        """Return next remote iterator value.

        :returns: Next value.
        """
        return self._call_dunder("__next__")

    def __getitem__(self, key: object) -> object:
        """Return one remote item.

        :param key: Item key.
        :returns: Item value.
        """
        return self._call_dunder("__getitem__", key)

    def __setitem__(self, key: object, value: object) -> None:
        """Set one remote item.

        :param key: Item key.
        :param value: Item value.
        """
        self._call_dunder("__setitem__", key, value)

    def __delitem__(self, key: object) -> None:
        """Delete one remote item.

        :param key: Item key.
        """
        self._call_dunder("__delitem__", key)

    def __contains__(self, item: object) -> bool:
        """Evaluate remote ``in`` membership.

        :param item: Candidate value.
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
        """Invoke remote ``__call__``.

        :param args: Positional arguments.
        :param kwargs: Keyword arguments.
        :returns: Remote return value.
        """
        return self._runtime.call_parent_attr(self._remote_object_id, "__call__", list(args), kwargs)

    def __instancecheck__(self, instance: object) -> bool:
        """Evaluate ``isinstance(instance, self)`` against parent class handles.

        :param instance: Candidate instance.
        :returns: ``True`` when parent-side ``isinstance`` succeeds.
        """
        value: object = self._runtime.call_parent_attr(
            self._remote_object_id,
            "__instancecheck__",
            [instance],
            {},
        )
        if isinstance(value, bool) is False:
            raise ExtraditeProtocolError("Remote __instancecheck__ did not return bool")
        return value

    def __subclasscheck__(self, subclass: object) -> bool:
        """Evaluate ``issubclass(subclass, self)`` against parent class handles.

        :param subclass: Candidate subclass.
        :returns: ``True`` when parent-side ``issubclass`` succeeds.
        """
        value: object = self._runtime.call_parent_attr(
            self._remote_object_id,
            "__subclasscheck__",
            [subclass],
            {},
        )
        if isinstance(value, bool) is False:
            raise ExtraditeProtocolError("Remote __subclasscheck__ did not return bool")
        return value

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
        """Block local pickling of remote handle proxies.

        :raises UnsupportedInteractionError: Always.
        """
        raise UnsupportedInteractionError("Parent-handle proxies cannot be pickled in the worker")

    def __reduce_ex__(self, protocol: int) -> object:
        """Block local pickling of remote handle proxies.

        :param protocol: Pickle protocol version.
        :raises UnsupportedInteractionError: Always.
        """
        raise UnsupportedInteractionError("Parent-handle proxies cannot be pickled in the worker")


class WorkerRuntime:
    """Own worker-side protocol handling and object dispatch."""

    _connection: Connection
    _registry: ObjectRegistry
    _protected_module_names: set[str]
    _transport_policy: TransportPolicy
    _next_request_id: int
    _parent_proxy_cache: "weakref.WeakValueDictionary[int, object]"
    _parent_proxy_is_type_cache: dict[int, bool]

    def __init__(self, connection: Connection, transport_policy: TransportPolicy = "value") -> None:
        """Initialize worker runtime state.

        :param connection: Bidirectional IPC connection to the parent process.
        :param transport_policy: Wire transport policy for picklable values.
        :raises ValueError: If transport policy is unsupported.
        """
        allowed_policies: set[str] = {"value", "reference"}
        is_allowed: bool = transport_policy in allowed_policies
        if is_allowed is False:
            raise ValueError(
                "transport_policy must be one of: "
                + ", ".join(sorted(allowed_policies))
            )
        self._connection = connection
        self._registry = ObjectRegistry()
        self._protected_module_names = set()
        self._transport_policy = transport_policy
        self._next_request_id = 1
        self._parent_proxy_cache = weakref.WeakValueDictionary()
        self._parent_proxy_is_type_cache = {}

    def run(self) -> None:
        """Run the worker message loop."""
        try:
            _send_ok(self._connection, 0, {"ready": True})
        except Exception as exc:
            _send_error(
                self._connection,
                0,
                type(exc).__name__,
                str(exc),
                traceback.format_exc(),
            )
            self._connection.close()
            return

        should_exit: bool = False
        while should_exit is False:
            try:
                incoming: object = self._connection.recv()
            except EOFError:
                break

            if isinstance(incoming, dict) is False:
                _send_error(
                    self._connection,
                    -1,
                    "ExtraditeProtocolError",
                    "Incoming message must be a dict",
                    "",
                )
                continue

            request_message: dict[str, object] = incoming
            shutdown_requested: bool = self._handle_incoming_request(request_message)
            if shutdown_requested is True:
                should_exit = True

        self._registry.clear()
        self._parent_proxy_cache.clear()
        self._parent_proxy_is_type_cache.clear()
        self._connection.close()

    def _handle_incoming_request(self, request_message: dict[str, object]) -> bool:
        """Handle one incoming request and emit a correlated response.

        :param request_message: Request dictionary.
        :returns: ``True`` when loop shutdown is requested.
        """
        request_id: int
        try:
            request_id = _require_request_id(request_message)
            payload: dict[str, object] = self._execute_request(request_message)
            _send_ok(self._connection, request_id, payload)
            shutdown_obj: object = payload.get("shutdown")
            shutdown_requested: bool = shutdown_obj is True
            return shutdown_requested
        except _RemotePeerError as exc:
            request_id_fallback: int = -1
            request_id_obj: object = request_message.get("request_id")
            if isinstance(request_id_obj, int) is True:
                request_id_fallback = request_id_obj
            _send_error(
                self._connection,
                request_id_fallback,
                exc.remote_type_name,
                exc.remote_message,
                exc.remote_traceback,
            )
            return False
        except Exception as exc:
            request_id_fallback = -1
            request_id_obj = request_message.get("request_id")
            if isinstance(request_id_obj, int) is True:
                request_id_fallback = request_id_obj
            _send_error(
                self._connection,
                request_id_fallback,
                type(exc).__name__,
                str(exc),
                traceback.format_exc(),
            )
            return False

    def _decode_from_parent(self, value: object) -> object:
        """Decode one wire value received from the parent.

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
                        raise ExtraditeProtocolError("Pickle payload must be bytes")
                    try:
                        return pickle.loads(payload_obj)
                    except (pickle.UnpicklingError, EOFError, AttributeError, ValueError) as exc:
                        raise ExtraditeProtocolError("Failed to unpickle parent payload") from exc
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
                        return self._registry.get(object_id)
                    if owner == OWNER_PARENT:
                        return self._get_or_create_parent_proxy(object_id)
                    raise ExtraditeProtocolError(f"Unknown reference owner: {owner!r}")
            return tuple(self._decode_from_parent(item) for item in value)

        if isinstance(value, list) is True:
            return [self._decode_from_parent(item) for item in value]

        if isinstance(value, set) is True:
            decoded_items: set[object] = set()
            for item in value:
                decoded_item: object = self._decode_from_parent(item)
                decoded_items.add(decoded_item)
            return decoded_items

        if isinstance(value, frozenset) is True:
            decoded_items_frozen: set[object] = set()
            for item in value:
                decoded_item = self._decode_from_parent(item)
                decoded_items_frozen.add(decoded_item)
            return frozenset(decoded_items_frozen)

        if isinstance(value, dict) is True:
            decoded_dict: dict[object, object] = {}
            for key, item in value.items():
                decoded_key: object = self._decode_from_parent(key)
                try:
                    hash(decoded_key)
                except TypeError as exc:
                    raise UnsupportedInteractionError("Decoded dict keys must stay hashable") from exc
                decoded_item: object = self._decode_from_parent(item)
                decoded_dict[decoded_key] = decoded_item
            return decoded_dict

        return value

    def _materialize_parent_instance_from_type_proxy(
        self,
        value: object,
        class_object_id: int,
    ) -> _ParentRemoteHandle:
        """Build a parent-origin instance from one child-local type-proxy instance.

        :param value: Child-local instance created from a parent type proxy.
        :param class_object_id: Parent class object identifier.
        :returns: Parent-origin instance handle.
        :raises ExtraditeProtocolError: If parent instance construction fails.
        :raises UnsupportedInteractionError: If instance state cannot be transferred.
        """
        class_proxy: object = type(value)
        created_obj: object = self.call_parent_attr(class_object_id, "__new__", [class_proxy], {})
        if isinstance(created_obj, _ParentRemoteHandle) is False:
            raise ExtraditeProtocolError("Parent __new__ did not produce a parent handle")
        parent_instance: _ParentRemoteHandle = created_obj

        instance_dict_obj: object = getattr(value, "__dict__", None)
        if isinstance(instance_dict_obj, dict) is False:
            return parent_instance

        for key, item in instance_dict_obj.items():
            if isinstance(key, str) is False:
                raise UnsupportedInteractionError("Type-proxy instance dict keys must be strings")
            setattr(parent_instance, key, item)
        return parent_instance

    def _should_try_bulk_pickle_for_parent(self, value: object) -> bool:
        """Decide whether container ``value`` should use bulk pickle transport.

        :param value: Candidate runtime value.
        :returns: ``True`` when bulk pickle should be attempted first.
        """
        if self._transport_policy != "value":
            return False

        container_item_count: int = _container_item_count(value)
        if container_item_count < _BULK_PICKLE_MIN_ITEMS:
            return False
        return True

    def _encode_for_parent(self, value: object) -> object:
        """Encode one runtime value for transport to the parent.

        :param value: Runtime value.
        :returns: Wire value.
        :raises UnsupportedInteractionError: If value transfer is disallowed.
        """
        if isinstance(value, _ParentRemoteHandle) is True:
            return (WIRE_REF_TAG, OWNER_PARENT, value.remote_object_id)
        parent_type_proxy_object_id: int | None = _extract_parent_type_proxy_object_id(value)
        if parent_type_proxy_object_id is not None:
            return (WIRE_REF_TAG, OWNER_PARENT, parent_type_proxy_object_id)
        parent_type_proxy_class_id: int | None = _extract_parent_type_proxy_instance_class_id(value)
        if parent_type_proxy_class_id is not None:
            parent_instance: _ParentRemoteHandle = self._materialize_parent_instance_from_type_proxy(
                value,
                parent_type_proxy_class_id,
            )
            return (WIRE_REF_TAG, OWNER_PARENT, parent_instance.remote_object_id)

        seen_ids: set[int] = set()
        contains_protected: bool = _value_originates_from_module(value, self._protected_module_names, seen_ids)
        if contains_protected is True:
            raise UnsupportedInteractionError(
                "Refusing to transfer values that originate from the protected isolated module"
            )

        force_reference: bool = (
            self._transport_policy == "reference"
            and _is_force_reference_candidate(value) is True
        )
        if force_reference is True:
            object_id_force_ref: int = self._registry.store(value)
            return (WIRE_REF_TAG, OWNER_CHILD, object_id_force_ref)

        is_container_candidate: bool = _is_bulk_pickle_container_candidate(value)
        if is_container_candidate is True:
            should_try_bulk_pickle: bool = self._should_try_bulk_pickle_for_parent(value)
            if should_try_bulk_pickle is True:
                bulk_payload: bytes | None = _try_pickle_payload(value)
                if bulk_payload is not None:
                    return (WIRE_PICKLE_TAG, bulk_payload)

        if isinstance(value, list) is True:
            return [self._encode_for_parent(item) for item in value]

        if isinstance(value, tuple) is True:
            return tuple(self._encode_for_parent(item) for item in value)

        if isinstance(value, set) is True:
            encoded_items: set[object] = set()
            for item in value:
                encoded_item: object = self._encode_for_parent(item)
                encoded_items.add(encoded_item)
            return encoded_items

        if isinstance(value, frozenset) is True:
            encoded_frozen_items: set[object] = set()
            for item in value:
                encoded_item = self._encode_for_parent(item)
                encoded_frozen_items.add(encoded_item)
            return frozenset(encoded_frozen_items)

        if isinstance(value, dict) is True:
            encoded_dict: dict[object, object] = {}
            for key, item in value.items():
                encoded_key: object = self._encode_for_parent(key)
                try:
                    hash(encoded_key)
                except TypeError as exc:
                    raise UnsupportedInteractionError("Encoded dict keys must stay hashable") from exc
                encoded_item: object = self._encode_for_parent(item)
                encoded_dict[encoded_key] = encoded_item
            return encoded_dict

        payload: bytes | None = _try_pickle_payload(value)
        if payload is not None:
            return (WIRE_PICKLE_TAG, payload)

        object_id: int = self._registry.store(value)
        return (WIRE_REF_TAG, OWNER_CHILD, object_id)

    def _decode_args_from_parent(self, message: dict[str, object]) -> tuple[list[object], dict[str, object]]:
        """Decode args/kwargs from a request message.

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

        args: list[object] = [self._decode_from_parent(item) for item in raw_args]
        kwargs: dict[str, object] = {}
        for key, item in raw_kwargs.items():
            if isinstance(key, str) is False:
                raise ExtraditeProtocolError("kwargs keys must be strings")
            kwargs[key] = self._decode_from_parent(item)
        return args, kwargs

    def _execute_request(self, message: dict[str, object]) -> dict[str, object]:
        """Execute one request from the parent.

        :param message: Request message.
        :returns: Response payload.
        :raises ExtraditeProtocolError: If request fields are invalid.
        """
        action: str = _require_action(message)

        if action == "load_class":
            module_name: str = _require_str_field(message, "module_name")
            class_qualname: str = _require_str_field(message, "class_qualname")
            imported_module = importlib.import_module(module_name)
            class_object: object = _resolve_qualname(imported_module, class_qualname)
            is_type: bool = isinstance(class_object, type)
            if is_type is False:
                raise ExtraditeProtocolError(f"Target {module_name}:{class_qualname} is not a class")
            class_object_id: int = self._registry.store(class_object, pinned=True)
            self._protected_module_names.add(module_name)
            return {"class_object_id": class_object_id}

        if action == "construct":
            class_object_id: int = _require_int_field(message, "class_object_id")
            args, kwargs = self._decode_args_from_parent(message)
            class_object: object = self._registry.get(class_object_id)
            instance: object = class_object(*args, **kwargs)  # type: ignore[operator]
            object_id: int = self._registry.store(instance)
            return {"object_id": object_id}

        if action == "get_attr":
            object_id: int = _require_int_field(message, "object_id")
            attr_name: str = _require_str_field(message, "attr_name")
            target: object = self._registry.get(object_id)
            attr_value: object = getattr(target, attr_name)
            is_callable: bool = callable(attr_value)
            if is_callable is True:
                return {"callable": True}
            return {"callable": False, "value": self._encode_for_parent(attr_value)}

        if action == "set_attr":
            object_id = _require_int_field(message, "object_id")
            attr_name = _require_str_field(message, "attr_name")
            raw_value: object = message.get("value")
            value: object = self._decode_from_parent(raw_value)
            target = self._registry.get(object_id)
            setattr(target, attr_name, value)
            return {}

        if action == "del_attr":
            object_id = _require_int_field(message, "object_id")
            attr_name = _require_str_field(message, "attr_name")
            target = self._registry.get(object_id)
            delattr(target, attr_name)
            return {}

        if action == "call_attr":
            object_id = _require_int_field(message, "object_id")
            attr_name = _require_str_field(message, "attr_name")
            args, kwargs = self._decode_args_from_parent(message)
            target = self._registry.get(object_id)
            callable_obj: object = getattr(target, attr_name)
            is_callable = callable(callable_obj)
            if is_callable is False:
                raise ExtraditeProtocolError(f"Attribute {attr_name!r} is not callable")
            result: object = callable_obj(*args, **kwargs)  # type: ignore[operator]
            return {"value": self._encode_for_parent(result)}

        if action == "release_object":
            object_id = _require_int_field(message, "object_id")
            self._registry.release(object_id)
            return {}

        if action == "shutdown":
            return {"shutdown": True}

        raise ExtraditeProtocolError(f"Unsupported action: {action}")

    def _describe_parent_object(self, object_id: int) -> dict[str, object]:
        """Describe one parent-owned object.

        :param object_id: Parent-owned object identifier.
        :returns: Description payload from the parent.
        :raises ExtraditeProtocolError: If payload shape is invalid.
        """
        payload: dict[str, object] = self._send_request_to_parent(
            "describe_object",
            {"object_id": object_id},
        )
        is_type_obj: object = payload.get("is_type")
        if isinstance(is_type_obj, bool) is False:
            raise ExtraditeProtocolError("describe_object payload missing bool is_type")
        return payload

    def _create_parent_type_proxy(self, object_id: int, description: dict[str, object]) -> type[object]:
        """Create one local type proxy for a parent-origin type handle.

        :param object_id: Parent-owned type object identifier.
        :param description: Parent description payload.
        :returns: Local class object that proxies to the parent type.
        :raises ExtraditeProtocolError: If description payload is malformed.
        """
        type_name_obj: object = description.get("type_name")
        if isinstance(type_name_obj, str) is False:
            raise ExtraditeProtocolError("describe_object payload missing str type_name")
        type_qualname_obj: object = description.get("type_qualname")
        if isinstance(type_qualname_obj, str) is False:
            raise ExtraditeProtocolError("describe_object payload missing str type_qualname")
        type_module_obj: object = description.get("type_module")
        if isinstance(type_module_obj, str) is False:
            raise ExtraditeProtocolError("describe_object payload missing str type_module")
        encoded_bases_obj: object = description.get("bases")
        if isinstance(encoded_bases_obj, list) is False:
            raise ExtraditeProtocolError("describe_object payload missing list bases")

        decoded_bases: list[type[object]] = []
        for encoded_base in encoded_bases_obj:
            decoded_base_obj: object = self._decode_from_parent(encoded_base)
            if isinstance(decoded_base_obj, type) is False:
                raise ExtraditeProtocolError("describe_object base entries must decode to type")
            decoded_bases.append(decoded_base_obj)

        bases_tuple: tuple[type[object], ...]
        if len(decoded_bases) == 0:
            bases_tuple = ()
        else:
            bases_tuple = tuple(decoded_bases)

        namespace: dict[str, object] = {
            "__module__": type_module_obj,
            "__qualname__": type_qualname_obj,
        }
        proxy_type: type[object] = _ParentTypeProxyMeta(type_name_obj, bases_tuple, namespace)
        type.__setattr__(proxy_type, _PARENT_TYPE_PROXY_RUNTIME_ATTR, self)
        type.__setattr__(proxy_type, _PARENT_TYPE_PROXY_OBJECT_ID_ATTR, object_id)
        return proxy_type

    def _get_or_create_parent_proxy(self, object_id: int) -> object:
        """Return cached parent proxy or create a new one.

        :param object_id: Parent-owned object identifier.
        :returns: Parent object proxy.
        """
        cached: object | None = self._parent_proxy_cache.get(object_id)
        if cached is not None:
            return cached

        is_type_cached: bool | None = self._parent_proxy_is_type_cache.get(object_id)
        is_type: bool
        description: dict[str, object] | None
        if is_type_cached is None:
            description = self._describe_parent_object(object_id)
            is_type_obj: object = description.get("is_type")
            if isinstance(is_type_obj, bool) is False:
                raise ExtraditeProtocolError("describe_object payload missing bool is_type")
            is_type = is_type_obj
            self._parent_proxy_is_type_cache[object_id] = is_type
        else:
            is_type = is_type_cached
            description = None

        if is_type is True:
            if description is None:
                description = self._describe_parent_object(object_id)
            created_type: type[object] = self._create_parent_type_proxy(object_id, description)
            self._parent_proxy_cache[object_id] = created_type
            return created_type

        created_handle: _ParentRemoteHandle = _ParentRemoteHandle(self, object_id)
        self._parent_proxy_cache[object_id] = created_handle
        return created_handle

    def _send_request_to_parent(self, action: str, payload: dict[str, object]) -> dict[str, object]:
        """Send one request to the parent and await the correlated response.

        :param action: Action name.
        :param payload: Action payload.
        :returns: Response payload dictionary.
        :raises ExtraditeProtocolError: If transport fails or response shape is invalid.
        :raises _RemotePeerError: If parent reports an exception.
        """
        request_id: int = self._next_request_id
        self._next_request_id += 1
        request: dict[str, object] = {
            "request_id": request_id,
            "action": action,
        }
        request.update(payload)

        try:
            self._connection.send(request)
        except (BrokenPipeError, EOFError, OSError) as exc:
            raise ExtraditeProtocolError("Failed to send request to parent process") from exc

        return self._wait_for_response(request_id)

    def _wait_for_response(self, expected_request_id: int) -> dict[str, object]:
        """Wait for one correlated response while servicing re-entrant requests.

        :param expected_request_id: Response request id to wait for.
        :returns: Response payload dictionary.
        :raises ExtraditeProtocolError: If transport fails or message shape is invalid.
        :raises _RemotePeerError: If parent reports an exception.
        """
        while True:
            incoming: object
            try:
                incoming = self._connection.recv()
            except (EOFError, BrokenPipeError, OSError) as exc:
                raise ExtraditeProtocolError("Failed to receive message from parent process") from exc

            if isinstance(incoming, dict) is False:
                raise ExtraditeProtocolError("Incoming parent message must be a dict")

            message: dict[str, object] = incoming
            has_status: bool = "status" in message
            has_action: bool = "action" in message

            if has_status is True:
                response_request_id: int = _require_request_id(message)
                if response_request_id != expected_request_id:
                    raise ExtraditeProtocolError(
                        "Unexpected response request_id "
                        + f"{response_request_id}; expected {expected_request_id}"
                    )

                status_obj: object = message.get("status")
                if isinstance(status_obj, str) is False:
                    raise ExtraditeProtocolError("Response status must be a string")
                status: str = status_obj

                payload_obj: object = message.get("payload")
                if isinstance(payload_obj, dict) is False:
                    raise ExtraditeProtocolError("Response payload must be a dict")
                payload: dict[str, object] = payload_obj

                if status == "ok":
                    return payload
                if status != "error":
                    raise ExtraditeProtocolError(f"Unknown response status: {status!r}")

                error_type_obj: object = payload.get("error_type", "Exception")
                error_message_obj: object = payload.get("error_message", "")
                traceback_obj: object = payload.get("stacktrace", "")

                error_type: str = "Exception"
                if isinstance(error_type_obj, str) is True:
                    error_type = error_type_obj
                error_message: str = ""
                if isinstance(error_message_obj, str) is True:
                    error_message = error_message_obj
                remote_traceback: str = ""
                if isinstance(traceback_obj, str) is True:
                    remote_traceback = traceback_obj

                if error_type == "AttributeError":
                    raise AttributeError(error_message)

                raise _RemotePeerError(error_type, error_message, remote_traceback)

            if has_action is True:
                self._handle_incoming_request(message)
                continue

            raise ExtraditeProtocolError("Incoming message must include either status or action")

    def get_parent_attr(self, object_id: int, attr_name: str) -> object:
        """Get one parent-side attribute from a referenced object.

        :param object_id: Parent-owned object identifier.
        :param attr_name: Attribute name.
        :returns: Attribute value or call proxy.
        """
        response_payload: dict[str, object] = self._send_request_to_parent(
            "get_attr",
            {
                "object_id": object_id,
                "attr_name": attr_name,
            },
        )
        callable_obj: object = response_payload.get("callable")
        if isinstance(callable_obj, bool) is False:
            raise ExtraditeProtocolError("get_attr payload missing bool callable")

        if callable_obj is True:
            return _ParentCallProxy(self, object_id, attr_name)

        encoded_value: object = response_payload.get("value")
        return self._decode_from_parent(encoded_value)

    def call_parent_attr(
        self,
        object_id: int,
        attr_name: str,
        args: list[object],
        kwargs: dict[str, object],
    ) -> object:
        """Call one parent-side attribute.

        :param object_id: Parent-owned object identifier.
        :param attr_name: Callable attribute name.
        :param args: Positional arguments.
        :param kwargs: Keyword arguments.
        :returns: Decoded return value.
        """
        encoded_args: list[object] = [self._encode_for_parent(item) for item in args]
        encoded_kwargs: dict[str, object] = {}
        for key, value in kwargs.items():
            encoded_kwargs[key] = self._encode_for_parent(value)

        response_payload = self._send_request_to_parent(
            "call_attr",
            {
                "object_id": object_id,
                "attr_name": attr_name,
                "args": encoded_args,
                "kwargs": encoded_kwargs,
            },
        )
        encoded_value: object = response_payload.get("value")
        return self._decode_from_parent(encoded_value)

    def set_parent_attr(self, object_id: int, attr_name: str, value: object) -> None:
        """Set one parent-side attribute.

        :param object_id: Parent-owned object identifier.
        :param attr_name: Attribute name.
        :param value: Attribute value.
        """
        encoded_value: object = self._encode_for_parent(value)
        self._send_request_to_parent(
            "set_attr",
            {
                "object_id": object_id,
                "attr_name": attr_name,
                "value": encoded_value,
            },
        )

    def del_parent_attr(self, object_id: int, attr_name: str) -> None:
        """Delete one parent-side attribute.

        :param object_id: Parent-owned object identifier.
        :param attr_name: Attribute name.
        """
        self._send_request_to_parent(
            "del_attr",
            {
                "object_id": object_id,
                "attr_name": attr_name,
            },
        )

    def release_parent_object(self, object_id: int) -> None:
        """Release one parent-owned object handle.

        :param object_id: Parent-owned object identifier.
        """
        self._parent_proxy_cache.pop(object_id, None)
        self._parent_proxy_is_type_cache.pop(object_id, None)
        self._send_request_to_parent(
            "release_object",
            {
                "object_id": object_id,
            },
        )


def worker_entry(connection: Connection, transport_policy: TransportPolicy = "value") -> None:
    """Run the child interpreter message loop.

    :param connection: IPC connection from parent process.
    :param transport_policy: Wire transport policy for picklable values.
    """
    runtime: WorkerRuntime = WorkerRuntime(connection, transport_policy=transport_policy)
    runtime.run()
