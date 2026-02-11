"""Child interpreter worker for extradite."""

import importlib
import pickle
import traceback
from multiprocessing.connection import Connection

from extradite.errors import ExtraditeProtocolError
from extradite.errors import UnsupportedInteractionError

REMOTE_REF_KEY: str = "__extradite_remote_ref__"


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
    """Recursively detect references to values from the protected module.

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
        if hasattr(value, slot_name) is False:
            continue
        slot_value: object = getattr(value, slot_name)
        slot_is_protected: bool = _value_originates_from_module(slot_value, protected_module_names, seen_ids)
        if slot_is_protected is True:
            return True

    return False


class ObjectRegistry:
    """Store child-process objects under stable integer identifiers."""

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
        :raises ExtraditeProtocolError: If the identifier is unknown.
        """
        value: object | None = self._by_object_id.get(object_id)
        if value is None:
            raise ExtraditeProtocolError(f"Unknown remote object id: {object_id}")
        return value

    def release(self, object_id: int) -> None:
        """Release an object unless pinned.

        :param object_id: Identifier of the object.
        """
        is_pinned: bool = object_id in self._pinned_object_ids
        if is_pinned is True:
            return

        value: object | None = self._by_object_id.pop(object_id, None)
        if value is None:
            return

        identity: int = id(value)
        self._by_identity.pop(identity, None)


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


def _is_remote_ref_payload(value: object) -> bool:
    """Check whether a value represents a remote object reference.

    :param value: Candidate value.
    :returns: ``True`` if it is a valid remote-reference payload.
    """
    if isinstance(value, dict) is False:
        return False
    if len(value) != 1:
        return False
    has_key: bool = REMOTE_REF_KEY in value
    if has_key is False:
        return False
    object_id: object = value[REMOTE_REF_KEY]
    if isinstance(object_id, int) is False:
        return False
    return True


def _decode_value(value: object, registry: ObjectRegistry) -> object:
    """Decode a parent-provided argument for local execution.

    :param value: Encoded value.
    :param registry: Child-process registry.
    :returns: Decoded runtime value.
    """
    is_ref: bool = _is_remote_ref_payload(value)
    if is_ref is True:
        payload: dict[str, object] = value  # type: ignore[assignment]
        object_id: int = payload[REMOTE_REF_KEY]  # type: ignore[assignment]
        return registry.get(object_id)

    if isinstance(value, list) is True:
        return [_decode_value(item, registry) for item in value]

    if isinstance(value, tuple) is True:
        return tuple(_decode_value(item, registry) for item in value)

    if isinstance(value, set) is True:
        return {_decode_value(item, registry) for item in value}

    if isinstance(value, dict) is True:
        decoded: dict[object, object] = {}
        for key, item in value.items():
            decoded_key: object = _decode_value(key, registry)
            decoded_item: object = _decode_value(item, registry)
            decoded[decoded_key] = decoded_item
        return decoded

    return value


def _encode_value(value: object, protected_module_names: set[str]) -> dict[str, object]:
    """Encode a local value for transfer back to the parent.

    :param value: Runtime value.
    :param protected_module_names: Protected module roots.
    :returns: Encoded value payload containing pickle bytes.
    :raises UnsupportedInteractionError: If transfer is disallowed.
    """
    seen_ids: set[int] = set()
    contains_protected: bool = _value_originates_from_module(value, protected_module_names, seen_ids)
    if contains_protected is True:
        raise UnsupportedInteractionError(
            "Refusing to transfer values that originate from the protected isolated module"
        )

    try:
        payload: bytes = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
    except (pickle.PicklingError, TypeError, AttributeError, ValueError) as exc:
        raise UnsupportedInteractionError(
            "Refusing to transfer unpicklable return value across the barrier"
        ) from exc

    return {"kind": "pickle", "payload": payload}


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


def _decode_args(message: dict[str, object], registry: ObjectRegistry) -> tuple[list[object], dict[str, object]]:
    """Decode ``args`` and ``kwargs`` from a request message.

    :param message: Request message.
    :param registry: Child-process object registry.
    :returns: Decoded positional and keyword arguments.
    :raises ExtraditeProtocolError: If argument payload types are invalid.
    """
    raw_args: object = message.get("args", [])
    if isinstance(raw_args, list) is False:
        raise ExtraditeProtocolError("args must be a list")

    raw_kwargs: object = message.get("kwargs", {})
    if isinstance(raw_kwargs, dict) is False:
        raise ExtraditeProtocolError("kwargs must be a dict")

    args: list[object] = [_decode_value(item, registry) for item in raw_args]
    kwargs: dict[str, object] = {}
    for key, item in raw_kwargs.items():
        if isinstance(key, str) is False:
            raise ExtraditeProtocolError("kwargs keys must be strings")
        kwargs[key] = _decode_value(item, registry)
    return args, kwargs


def _handle_request(
    message: dict[str, object],
    registry: ObjectRegistry,
    protected_module_names: set[str],
) -> dict[str, object]:
    """Handle a single request.

    :param message: Request message.
    :param registry: Child-process object registry.
    :param protected_module_names: Protected module roots.
    :returns: Success payload.
    :raises ExtraditeProtocolError: If the request is malformed.
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
        class_object_id: int = registry.store(class_object, pinned=True)
        protected_module_names.add(module_name)
        return {"class_object_id": class_object_id}

    if action == "construct":
        class_object_id: int = _require_int_field(message, "class_object_id")
        args, kwargs = _decode_args(message, registry)
        class_object: object = registry.get(class_object_id)
        instance: object = class_object(*args, **kwargs)  # type: ignore[operator]
        object_id: int = registry.store(instance)
        return {"object_id": object_id}

    if action == "get_attr":
        object_id = _require_int_field(message, "object_id")
        attr_name = _require_str_field(message, "attr_name")
        target: object = registry.get(object_id)
        attr_value: object = getattr(target, attr_name)
        is_callable: bool = callable(attr_value)
        if is_callable is True:
            return {"callable": True}
        return {"callable": False, "value": _encode_value(attr_value, protected_module_names)}

    if action == "set_attr":
        object_id = _require_int_field(message, "object_id")
        attr_name = _require_str_field(message, "attr_name")
        raw_value: object = message.get("value")
        value: object = _decode_value(raw_value, registry)
        target = registry.get(object_id)
        setattr(target, attr_name, value)
        return {}

    if action == "del_attr":
        object_id = _require_int_field(message, "object_id")
        attr_name = _require_str_field(message, "attr_name")
        target = registry.get(object_id)
        delattr(target, attr_name)
        return {}

    if action == "call_attr":
        object_id = _require_int_field(message, "object_id")
        attr_name = _require_str_field(message, "attr_name")
        args, kwargs = _decode_args(message, registry)
        target = registry.get(object_id)
        callable_obj: object = getattr(target, attr_name)
        is_callable = callable(callable_obj)
        if is_callable is False:
            raise ExtraditeProtocolError(f"Attribute {attr_name!r} is not callable")
        result: object = callable_obj(*args, **kwargs)  # type: ignore[operator]
        return {"value": _encode_value(result, protected_module_names)}

    if action == "release_object":
        object_id = _require_int_field(message, "object_id")
        registry.release(object_id)
        return {}

    if action == "shutdown":
        return {"shutdown": True}

    raise ExtraditeProtocolError(f"Unsupported action: {action}")


def worker_entry(connection: Connection) -> None:
    """Run the child interpreter message loop.

    :param connection: IPC connection from parent process.
    """
    registry = ObjectRegistry()
    protected_module_names: set[str] = set()

    try:
        _send_ok(connection, 0, {"ready": True})
    except Exception as exc:
        _send_error(
            connection,
            0,
            type(exc).__name__,
            str(exc),
            traceback.format_exc(),
        )
        connection.close()
        return

    should_exit: bool = False
    while should_exit is False:
        try:
            incoming: object = connection.recv()
        except EOFError:
            break

        if isinstance(incoming, dict) is False:
            _send_error(
                connection,
                -1,
                "ExtraditeProtocolError",
                "Incoming message must be a dict",
                "",
            )
            continue

        request_message: dict[str, object] = incoming
        request_id: int
        try:
            request_id = _require_request_id(request_message)
        except Exception as exc:
            _send_error(
                connection,
                -1,
                type(exc).__name__,
                str(exc),
                traceback.format_exc(),
            )
            continue

        try:
            payload: dict[str, object] = _handle_request(
                request_message,
                registry,
                protected_module_names,
            )
            _send_ok(connection, request_id, payload)
            shutdown_requested: object = payload.get("shutdown")
            if shutdown_requested is True:
                should_exit = True
        except Exception as exc:
            _send_error(
                connection,
                request_id,
                type(exc).__name__,
                str(exc),
                traceback.format_exc(),
            )

    connection.close()
