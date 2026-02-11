"""User-facing API entrypoints for extradite."""

from extradite.builder import create_extradited_class


def extradite(
    target: str,
    share_key: str | None = None,
    transport_policy: str = "value",
) -> type:
    """Create a class-like proxy that runs in a separate interpreter process.

    :param target: Target in ``module.path:ClassName`` format.
    :param share_key: Optional key that reuses a child process across calls.
    :param transport_policy: Wire transport policy for picklable values.
    :returns: A dynamically generated proxy class.
    """
    return create_extradited_class(
        target,
        share_key=share_key,
        transport_policy=transport_policy,
    )
