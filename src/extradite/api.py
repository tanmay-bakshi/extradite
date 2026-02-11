"""User-facing API entrypoints for extradite."""

from extradite.builder import create_extradited_class


def extradite(
    target: str,
    share_key: str | None = None,
    transport_policy: str = "value",
    transport_type_rules: dict[type[object], str] | None = None,
) -> type:
    """Create a class-like proxy that runs in a separate interpreter process.

    :param target: Target in ``module.path:ClassName`` format.
    :param share_key: Optional key that reuses a child process across calls.
    :param transport_policy: Wire transport policy for picklable values.
    :param transport_type_rules: Optional per-type transport rules.
    :returns: A dynamically generated proxy class.
    """
    return create_extradited_class(
        target,
        share_key=share_key,
        transport_policy=transport_policy,
        transport_type_rules=transport_type_rules,
    )
