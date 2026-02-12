"""User-facing API entrypoints for extradite."""

from extradite.builder import close_shared_session
from extradite.builder import create_extradited_class
from extradite.builder import prewarm_shared_session


def extradite(
    target: str,
    share_key: str | None = None,
    transport_policy: str = "value",
    transport_type_rules: dict[type[object], str] | None = None,
    share_keepalive_seconds: float | None = None,
) -> type:
    """Create a class-like proxy that runs in a separate interpreter process.

    :param target: Target in ``module.path:ClassName`` format.
    :param share_key: Optional key that reuses a child process across calls.
    :param transport_policy: Wire transport policy for picklable values.
    :param transport_type_rules: Optional per-type transport rules.
    :param share_keepalive_seconds: Optional idle keepalive timeout for shared sessions.
    :returns: A dynamically generated proxy class.
    """
    return create_extradited_class(
        target,
        share_key=share_key,
        transport_policy=transport_policy,
        transport_type_rules=transport_type_rules,
        share_keepalive_seconds=share_keepalive_seconds,
    )


def prewarm_extradite_session(
    share_key: str,
    transport_policy: str = "value",
    transport_type_rules: dict[type[object], str] | None = None,
    share_keepalive_seconds: float | None = None,
) -> bool:
    """Create or reuse one shared session and start its child process eagerly.

    :param share_key: Shared-session key.
    :param transport_policy: Requested transport policy.
    :param transport_type_rules: Optional per-type transport rules.
    :param share_keepalive_seconds: Optional idle keepalive timeout for this shared session.
        When omitted and a new session is created, a default of ``30.0`` seconds is used.
    :returns: ``True`` when a new shared session was created.
    """
    return prewarm_shared_session(
        share_key,
        transport_policy=transport_policy,
        transport_type_rules=transport_type_rules,
        share_keepalive_seconds=share_keepalive_seconds,
    )


def close_extradite_session(share_key: str) -> bool:
    """Close one active shared session by key.

    :param share_key: Shared-session key.
    :returns: ``True`` when an active session was found and closed.
    """
    return close_shared_session(share_key)
