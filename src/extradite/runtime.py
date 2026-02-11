"""Parent-process runtime for extradite proxies."""

import atexit
import io
import multiprocessing
import pickle
import sys
import threading
import weakref
from multiprocessing.connection import Connection
from typing import Any
from typing import ClassVar

from extradite.errors import ExtraditeModuleLeakError
from extradite.errors import ExtraditeProtocolError
from extradite.errors import ExtraditeRemoteError
from extradite.errors import UnsupportedInteractionError
from extradite.worker import REMOTE_REF_KEY
from extradite.worker import worker_entry


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


def _parse_target(target: str) -> tuple[str, str]:
    """Parse ``module.path:ClassName`` targets.

    :param target: Raw target string.
    :returns: Tuple of ``(module_name, class_qualname)``.
    :raises ValueError: If the target format is invalid.
    """
    parts: list[str] = target.split(":")
    if len(parts) != 2:
        raise ValueError("Target must use 'module.path:ClassName' format")

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


class _RootSafeUnpickler(pickle.Unpickler):
    """Unpickler that blocks imports from the protected module tree."""

    _protected_module_name: str

    def __init__(self, payload_stream: io.BytesIO, protected_module_name: str) -> None:
        """Initialize the guarded unpickler.

        :param payload_stream: Byte stream to decode.
        :param protected_module_name: Protected module root.
        """
        super().__init__(payload_stream)
        self._protected_module_name = protected_module_name

    def find_class(self, module: str, name: str) -> Any:
        """Resolve global references during unpickling.

        :param module: Module name referenced by pickle.
        :param name: Attribute name referenced by pickle.
        :returns: Resolved class/function object.
        :raises ExtraditeModuleLeakError: If protected-module import is requested.
        """
        is_protected: bool = _is_protected_module(module, self._protected_module_name)
        if is_protected is True:
            raise ExtraditeModuleLeakError(
                "Refusing to unpickle payload that references isolated module "
                + f"{module}.{name}"
            )
        return super().find_class(module, name)


def _safe_unpickle(payload: bytes, protected_module_name: str) -> object:
    """Unpickle bytes with protected-module import blocking.

    :param payload: Pickled payload bytes.
    :param protected_module_name: Protected module root.
    :returns: Unpickled value.
    :raises ExtraditeProtocolError: If payload cannot be unpickled.
    :raises ExtraditeModuleLeakError: If protected-module import is requested.
    """
    stream: io.BytesIO = io.BytesIO(payload)
    unpickler = _RootSafeUnpickler(stream, protected_module_name)
    try:
        return unpickler.load()
    except ExtraditeModuleLeakError:
        raise
    except (pickle.UnpicklingError, EOFError, AttributeError, ValueError) as exc:
        raise ExtraditeProtocolError("Failed to unpickle child payload") from exc


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
        return self._session.call_attr(
            self._object_id,
            self._attr_name,
            list(args),
            kwargs,
        )


class ExtraditedMeta(type):
    """Metaclass for dynamically generated extradited proxy classes."""

    _session: ClassVar["ExtraditeSession"]

    def __call__(cls, *args: object, **kwargs: object) -> object:
        """Construct a remote instance and return its local proxy.

        :param args: Positional constructor arguments.
        :param kwargs: Keyword constructor arguments.
        :returns: Local proxy to the remote instance.
        """
        session: ExtraditeSession = cls._session
        object_id: int = session.construct_instance(list(args), kwargs)
        return session.get_or_create_proxy(object_id, cls)

    def __getattribute__(cls, attr_name: str) -> object:
        """Resolve special metaclass attributes.

        :param attr_name: Attribute name requested on the class object.
        :returns: Resolved attribute value.
        """
        if attr_name == "close":
            session: ExtraditeSession = type.__getattribute__(cls, "_session")
            return session.close
        return super().__getattribute__(attr_name)

    def __getattr__(cls, attr_name: str) -> object:
        """Resolve class attributes via the remote class object.

        :param attr_name: Class attribute name.
        :returns: Class attribute value or callable wrapper.
        """
        session: ExtraditeSession = cls._session
        class_object_id: int = session.class_object_id
        return session.get_attr(class_object_id, attr_name)


class ExtraditedObjectBase(metaclass=ExtraditedMeta):
    """Base instance proxy that forwards operations to the child process."""

    _session: ClassVar["ExtraditeSession"]
    _remote_target: ClassVar[str]
    _remote_object_id: int
    _finalizer: weakref.finalize

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
        """Resolve instance attributes via the child process.

        :param attr_name: Instance attribute name.
        :returns: Attribute value or callable wrapper.
        """
        return self._session.get_attr(self._remote_object_id, attr_name)

    def __setattr__(self, attr_name: str, value: object) -> None:
        """Set instance attributes remotely.

        :param attr_name: Attribute name.
        :param value: Attribute value.
        """
        is_internal: bool = attr_name.startswith("_")
        if is_internal is True:
            object.__setattr__(self, attr_name, value)
            return
        self._session.set_attr(self._remote_object_id, attr_name, value)

    def __delattr__(self, attr_name: str) -> None:
        """Delete instance attributes remotely.

        :param attr_name: Attribute name.
        """
        is_internal: bool = attr_name.startswith("_")
        if is_internal is True:
            object.__delattr__(self, attr_name)
            return
        self._session.del_attr(self._remote_object_id, attr_name)

    def _call_dunder(self, attr_name: str, *args: object) -> object:
        """Call a dunder method remotely.

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

        :returns: Repr string.
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

    def __iter__(self) -> Any:
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
        return self._session.call_attr(
            self._remote_object_id,
            "__call__",
            list(args),
            kwargs,
        )

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


class ExtraditeSession:
    """Manage one child interpreter and its IPC channel."""

    _module_name: str
    _class_qualname: str
    _remote_target: str
    _connection: Connection | None
    _process: multiprocessing.Process | None
    _class_object_id: int | None
    _next_request_id: int
    _is_closed: bool
    _lock: threading.RLock
    _proxy_cache: "weakref.WeakValueDictionary[int, ExtraditedObjectBase]"

    def __init__(self, module_name: str, class_qualname: str) -> None:
        """Initialize a session.

        :param module_name: Isolated module path.
        :param class_qualname: Remote class qualname.
        """
        self._module_name = module_name
        self._class_qualname = class_qualname
        self._remote_target = f"{module_name}:{class_qualname}"
        self._connection = None
        self._process = None
        self._class_object_id = None
        self._next_request_id = 1
        self._is_closed = False
        self._lock = threading.RLock()
        self._proxy_cache = weakref.WeakValueDictionary()

    @property
    def class_object_id(self) -> int:
        """Return the remote identifier of the class object.

        :returns: Remote class object identifier.
        :raises ExtraditeProtocolError: If session has not started.
        """
        class_id: int | None = self._class_object_id
        if class_id is None:
            raise ExtraditeProtocolError("Session has not been started")
        return class_id

    def start(self) -> None:
        """Start the child interpreter process.

        :raises ExtraditeModuleLeakError: If the protected module is loaded locally.
        :raises ExtraditeRemoteError: If startup in child process fails.
        """
        with self._lock:
            self._assert_module_not_loaded()
            is_started: bool = self._connection is not None
            if is_started is True:
                return

            context = multiprocessing.get_context("spawn")
            parent_connection, child_connection = context.Pipe(duplex=True)
            process = context.Process(
                target=worker_entry,
                args=(child_connection, self._module_name, self._class_qualname),
            )
            process.daemon = True
            process.start()
            child_connection.close()

            self._connection = parent_connection
            self._process = process

            response: dict[str, object] = self._receive_response(expected_request_id=0)
            payload: object = response.get("payload")
            if isinstance(payload, dict) is False:
                self.close()
                raise ExtraditeProtocolError("Startup payload must be a dict")

            class_object_id: object = payload.get("class_object_id")
            if isinstance(class_object_id, int) is False:
                self.close()
                raise ExtraditeProtocolError("Startup payload missing class_object_id")

            self._class_object_id = class_object_id
            atexit.register(self.close)

    def _assert_module_not_loaded(self) -> None:
        """Ensure the isolated module is absent from root-process imports.

        :raises ExtraditeModuleLeakError: If the module is present in ``sys.modules``.
        """
        leaks: list[str] = _find_module_leaks(self._module_name)
        if len(leaks) > 0:
            leak_list: str = ", ".join(leaks)
            raise ExtraditeModuleLeakError(
                "Isolated module leaked into root process: " + leak_list
            )

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
        error_type: object = payload.get("error_type", "Exception")
        error_message: object = payload.get("error_message", "")
        stacktrace: object = payload.get("stacktrace", "")

        if isinstance(error_type, str) is False:
            error_type = "Exception"
        if isinstance(error_message, str) is False:
            error_message = ""
        if isinstance(stacktrace, str) is False:
            stacktrace = ""

        formatted: str = (
            f"Child process raised {error_type}: {error_message}\n"
            f"Remote traceback:\n{stacktrace}"
        )

        if error_type == "UnsupportedInteractionError":
            raise UnsupportedInteractionError(formatted)
        if error_type == "ExtraditeProtocolError":
            raise ExtraditeProtocolError(formatted)
        if error_type == "ExtraditeModuleLeakError":
            raise ExtraditeModuleLeakError(formatted)
        raise ExtraditeRemoteError(formatted)

    def _receive_response(self, expected_request_id: int) -> dict[str, object]:
        """Receive and validate one response message.

        :param expected_request_id: Expected request identifier.
        :returns: Validated response message.
        :raises ExtraditeRemoteError: If child process reports an exception.
        :raises ExtraditeProtocolError: If message format is invalid.
        """
        connection: Connection = self._require_connection()
        incoming: object
        try:
            incoming = connection.recv()
        except (EOFError, BrokenPipeError, OSError) as exc:
            raise ExtraditeProtocolError("Failed to receive response from child process") from exc

        if isinstance(incoming, dict) is False:
            raise ExtraditeProtocolError("Child response must be a dict")
        response: dict[str, object] = incoming

        request_id: object = response.get("request_id")
        if isinstance(request_id, int) is False:
            raise ExtraditeProtocolError("Child response request_id must be an int")
        if request_id != expected_request_id:
            raise ExtraditeProtocolError(
                f"Unexpected response request_id {request_id}; expected {expected_request_id}"
            )

        status: object = response.get("status")
        if isinstance(status, str) is False:
            raise ExtraditeProtocolError("Child response status must be a string")

        if status == "ok":
            return response

        if status != "error":
            raise ExtraditeProtocolError(f"Unknown child response status: {status!r}")

        payload: object = response.get("payload")
        if isinstance(payload, dict) is False:
            raise ExtraditeProtocolError("Error response payload must be a dict")
        self._raise_child_error(payload)
        raise ExtraditeProtocolError("Unreachable error state")

    def _encode_argument(self, value: object) -> object:
        """Encode one outbound argument for child transport.

        :param value: Root-process value.
        :returns: Encoded value.
        :raises UnsupportedInteractionError: If value cannot be encoded safely.
        """
        if isinstance(value, ExtraditedObjectBase) is True:
            same_session: bool = value._session is self
            if same_session is False:
                raise UnsupportedInteractionError(
                    "Cannot pass proxy values between different extradite sessions"
                )
            return {REMOTE_REF_KEY: value.remote_object_id}

        if isinstance(value, list) is True:
            return [self._encode_argument(item) for item in value]

        if isinstance(value, tuple) is True:
            return tuple(self._encode_argument(item) for item in value)

        if isinstance(value, set) is True:
            return {self._encode_argument(item) for item in value}

        if isinstance(value, dict) is True:
            encoded: dict[object, object] = {}
            for key, item in value.items():
                encoded_key: object = self._encode_argument(key)
                is_hashable: bool = hasattr(encoded_key, "__hash__")
                if is_hashable is False:
                    raise UnsupportedInteractionError("Encoded dict keys must stay hashable")
                encoded_item: object = self._encode_argument(item)
                encoded[encoded_key] = encoded_item
            return encoded

        is_picklable: bool = True
        try:
            pickle.dumps(value)
        except (pickle.PicklingError, TypeError, AttributeError, ValueError):
            is_picklable = False

        if is_picklable is False:
            raise UnsupportedInteractionError(
                f"Argument {value!r} is not picklable and cannot be transferred"
            )
        return value

    def _decode_value(self, encoded: object) -> object:
        """Decode an inbound value from child payload.

        :param encoded: Encoded result payload.
        :returns: Decoded value.
        :raises ExtraditeProtocolError: If payload format is invalid.
        """
        if isinstance(encoded, dict) is False:
            raise ExtraditeProtocolError("Encoded result must be a dict")

        kind: object = encoded.get("kind")
        if kind != "pickle":
            raise ExtraditeProtocolError(f"Unknown encoded result kind: {kind!r}")

        payload: object = encoded.get("payload")
        if isinstance(payload, bytes) is False:
            raise ExtraditeProtocolError("Encoded pickle payload must be bytes")

        return _safe_unpickle(payload, self._module_name)

    def _send_request(self, action: str, payload: dict[str, object]) -> dict[str, object]:
        """Send one request and return its decoded top-level response.

        :param action: Action name.
        :param payload: Action payload.
        :returns: Response dictionary.
        """
        with self._lock:
            self._assert_module_not_loaded()
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
            except (BrokenPipeError, OSError) as exc:
                raise ExtraditeProtocolError("Failed to send request to child process") from exc

            return self._receive_response(expected_request_id=request_id)

    def construct_instance(self, args: list[object], kwargs: dict[str, object]) -> int:
        """Construct a remote class instance.

        :param args: Constructor positional arguments.
        :param kwargs: Constructor keyword arguments.
        :returns: Remote object identifier.
        """
        encoded_args: list[object] = [self._encode_argument(item) for item in args]
        encoded_kwargs: dict[str, object] = {}
        for key, value in kwargs.items():
            encoded_kwargs[key] = self._encode_argument(value)

        response: dict[str, object] = self._send_request(
            "construct",
            {
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
        return self._decode_value(encoded_value)

    def call_attr(
        self,
        object_id: int,
        attr_name: str,
        args: list[object],
        kwargs: dict[str, object],
    ) -> object:
        """Call a callable attribute on a remote object.

        :param object_id: Remote object identifier.
        :param attr_name: Callable attribute name.
        :param args: Positional arguments.
        :param kwargs: Keyword arguments.
        :returns: Decoded call result.
        """
        encoded_args: list[object] = [self._encode_argument(item) for item in args]
        encoded_kwargs: dict[str, object] = {}
        for key, value in kwargs.items():
            encoded_kwargs[key] = self._encode_argument(value)

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
        return self._decode_value(encoded_value)

    def set_attr(self, object_id: int, attr_name: str, value: object) -> None:
        """Set an attribute on a remote object.

        :param object_id: Remote object identifier.
        :param attr_name: Attribute name.
        :param value: New value.
        """
        encoded_value: object = self._encode_argument(value)
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
        self._proxy_cache.pop(object_id, None)
        self._send_request(
            "release_object",
            {
                "object_id": object_id,
            },
        )

    def get_or_create_proxy(
        self,
        object_id: int,
        proxy_type: type[ExtraditedObjectBase],
    ) -> ExtraditedObjectBase:
        """Get a cached proxy or create a new one.

        :param object_id: Remote object identifier.
        :param proxy_type: Desired proxy class.
        :returns: Bound proxy object.
        """
        cached: ExtraditedObjectBase | None = self._proxy_cache.get(object_id)
        if cached is not None:
            return cached

        instance: ExtraditedObjectBase = object.__new__(proxy_type)
        bound: ExtraditedObjectBase = proxy_type._bind_remote(instance, object_id)
        self._proxy_cache[object_id] = bound
        return bound

    def close(self) -> None:
        """Close the session and terminate the child process."""
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
                except (BrokenPipeError, OSError):
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
            self._class_object_id = None
            self._proxy_cache.clear()


def _build_proxy_class(module_name: str, class_qualname: str, session: ExtraditeSession) -> type:
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
        "_remote_target": remote_target,
    }
    proxy_class: type = ExtraditedMeta(class_name, (ExtraditedObjectBase,), namespace)
    return proxy_class


def create_extradited_class(target: str) -> type:
    """Create a class proxy for an isolated import target.

    :param target: Target in ``module.path:ClassName`` format.
    :returns: Dynamic proxy class for that target.
    :raises ExtraditeModuleLeakError: If target module already leaked into root process.
    """
    module_name, class_qualname = _parse_target(target)
    session = ExtraditeSession(module_name, class_qualname)
    session.start()
    return _build_proxy_class(module_name, class_qualname, session)
