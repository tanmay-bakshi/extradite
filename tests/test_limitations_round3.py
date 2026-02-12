import sys
import tempfile
import textwrap
import uuid
from pathlib import Path

import pytest

from extradite import extradite


def _make_bridge() -> tuple[tempfile.TemporaryDirectory[str], type, object, str]:
    """Create an extradited Bridge instance from a temporary protected module.

    The Bridge class lives in a temporary module so that:

    - the module is "protected" (it is the target of ``extradite(...)``), and
    - the root interpreter does not need to import it.

    :returns: Tuple of ``(tmpdir, BridgeProxyType, bridge_instance, sys_path_entry)``.
    """
    module_name: str = f"extradite_repro_bridge_{uuid.uuid4().hex}"
    tmp: tempfile.TemporaryDirectory[str] = tempfile.TemporaryDirectory()
    tmp_path: Path = Path(tmp.name)

    source: str = textwrap.dedent(
        """
        class Bridge:
            def echo(self, value: object) -> object:
                return value

            def return_in_dict(self, value: object) -> dict[str, object]:
                return {"value": value}

            def return_in_list(self, value: object) -> list[object]:
                return [value]

            def return_in_tuple(self, value: object) -> tuple[object, ...]:
                return (value,)

            def set_attr(self, obj: object, name: str, value: object) -> None:
                setattr(obj, name, value)

            def get_attr(self, obj: object, name: str) -> object:
                return getattr(obj, name, None)

            def set_in_dunder_dict(self, obj: object, key: str, value: object) -> None:
                obj.__dict__[key] = value

            def get_from_dunder_dict(self, obj: object, key: str) -> object:
                return obj.__dict__.get(key)
        """
    ).lstrip()

    (tmp_path / f"{module_name}.py").write_text(source, encoding="utf-8")

    sys.path.insert(0, tmp.name)
    bridge_type: type = extradite(f"{module_name}:Bridge", transport_policy="value")
    bridge: object = bridge_type()
    return tmp, bridge_type, bridge, tmp.name


def _cleanup_bridge(
    tmp: tempfile.TemporaryDirectory[str],
    bridge_type: type,
    bridge: object,
    sys_path_entry: str,
) -> None:
    """Tear down one temporary Bridge.

    :param tmp: Temporary directory handle.
    :param bridge_type: Extradite proxy type.
    :param bridge: Extradite proxy instance.
    :param sys_path_entry: sys.path entry to remove.
    """
    close_obj: object = getattr(bridge, "close", None)
    if callable(close_obj) is True:
        close_obj()

    close_type_obj: object = getattr(bridge_type, "close", None)
    if callable(close_type_obj) is True:
        close_type_obj()

    while sys_path_entry in sys.path:
        sys.path.remove(sys_path_entry)

    tmp.cleanup()


def test_control_parent_handle_roundtrips_when_returned_directly() -> None:
    """Control: returning the parent handle directly round-trips successfully."""
    tmp, bridge_type, bridge, sys_path_entry = _make_bridge()
    try:
        class ParentObj:
            """Local type to force parent-handle transport (not picklable)."""

        parent_obj: ParentObj = ParentObj()
        out: object = bridge.echo(parent_obj)
        assert out is parent_obj
    finally:
        _cleanup_bridge(tmp, bridge_type, bridge, sys_path_entry)


@pytest.mark.parametrize(
    "method_name,container_kind",
    [
        ("return_in_dict", "dict"),
        ("return_in_list", "list"),
        ("return_in_tuple", "tuple"),
    ],
)
def test_parent_handle_can_be_nested_in_child_containers_and_roundtrip(
    method_name: str,
    container_kind: str,
) -> None:
    """Parent handles should be nestable in child-owned containers."""
    tmp, bridge_type, bridge, sys_path_entry = _make_bridge()
    try:
        class ParentObj:
            """Local type to force parent-handle transport (not picklable)."""

        parent_obj: ParentObj = ParentObj()
        method = getattr(bridge, method_name)
        out: object = method(parent_obj)

        if container_kind == "dict":
            assert isinstance(out, dict)
            assert out["value"] is parent_obj
            return
        if container_kind == "list":
            assert isinstance(out, list)
            assert out[0] is parent_obj
            return
        if container_kind == "tuple":
            assert isinstance(out, tuple)
            assert out[0] is parent_obj
            return

        raise AssertionError(f"Unexpected container_kind: {container_kind}")
    finally:
        _cleanup_bridge(tmp, bridge_type, bridge, sys_path_entry)


def test_control_child_can_set_parent_public_attr_via_handle() -> None:
    """Control: non-underscore attribute writes should reach the parent."""
    tmp, bridge_type, bridge, sys_path_entry = _make_bridge()
    try:
        class ParentObj:
            """Local type to force parent-handle transport (not picklable)."""

        parent_obj: ParentObj = ParentObj()
        bridge.set_attr(parent_obj, "public", 123)
        assert getattr(parent_obj, "public", None) == 123
    finally:
        _cleanup_bridge(tmp, bridge_type, bridge, sys_path_entry)


def test_child_can_set_parent_dunder_attr_via_handle() -> None:
    """Dunder writes (e.g. ``__sentinel__``) should reach the parent."""
    tmp, bridge_type, bridge, sys_path_entry = _make_bridge()
    try:
        class ParentObj:
            """Local type to force parent-handle transport (not picklable)."""

        parent_obj: ParentObj = ParentObj()
        bridge.set_attr(parent_obj, "__sentinel__", 123)
        assert getattr(parent_obj, "__sentinel__", None) == 123
    finally:
        _cleanup_bridge(tmp, bridge_type, bridge, sys_path_entry)


def test_child_can_read_parent_dunder_attr_via_handle() -> None:
    """Dunder reads should resolve against the parent."""
    tmp, bridge_type, bridge, sys_path_entry = _make_bridge()
    try:
        class ParentObj:
            """Local type to force parent-handle transport (not picklable)."""

        parent_obj: ParentObj = ParentObj()
        setattr(parent_obj, "__sentinel__", 456)
        out: object = bridge.get_attr(parent_obj, "__sentinel__")
        assert out == 456
    finally:
        _cleanup_bridge(tmp, bridge_type, bridge, sys_path_entry)


def test_child_can_mutate_parent_dunder_dict_via_handle() -> None:
    """``obj.__dict__[k] = v`` in the child should mutate the parent."""
    tmp, bridge_type, bridge, sys_path_entry = _make_bridge()
    try:
        class ParentObj:
            """Local type to force parent-handle transport (not picklable)."""

        parent_obj: ParentObj = ParentObj()
        bridge.set_in_dunder_dict(parent_obj, "x", 1)
        assert parent_obj.__dict__.get("x") == 1
    finally:
        _cleanup_bridge(tmp, bridge_type, bridge, sys_path_entry)


def test_child_can_read_parent_dunder_dict_via_handle() -> None:
    """``obj.__dict__.get(k)`` in the child should read the parent."""
    tmp, bridge_type, bridge, sys_path_entry = _make_bridge()
    try:
        class ParentObj:
            """Local type to force parent-handle transport (not picklable)."""

        parent_obj: ParentObj = ParentObj()
        parent_obj.__dict__["x"] = 1
        out: object = bridge.get_from_dunder_dict(parent_obj, "x")
        assert out == 1
    finally:
        _cleanup_bridge(tmp, bridge_type, bridge, sys_path_entry)


def test_control_child_can_set_parent_type_public_attr_via_type_proxy() -> None:
    """Control: non-underscore attribute writes should reach parent types too."""
    tmp, bridge_type, bridge, sys_path_entry = _make_bridge()
    try:
        class ParentType:
            """Local type to force parent type-proxy transport (not picklable)."""

        bridge.set_attr(ParentType, "public", 123)
        assert getattr(ParentType, "public", None) == 123
    finally:
        _cleanup_bridge(tmp, bridge_type, bridge, sys_path_entry)


def test_child_can_set_parent_type_dunder_attr_via_type_proxy() -> None:
    """Dunder writes should reach parent types too."""
    tmp, bridge_type, bridge, sys_path_entry = _make_bridge()
    try:
        class ParentType:
            """Local type to force parent type-proxy transport (not picklable)."""

        bridge.set_attr(ParentType, "__sentinel__", 123)
        assert getattr(ParentType, "__sentinel__", None) == 123
    finally:
        _cleanup_bridge(tmp, bridge_type, bridge, sys_path_entry)

