"""User-facing API entrypoints for extradite."""

from extradite.builder import create_extradited_class


def extradite(target: str, share_key: str | None = None) -> type:
    """Create a class-like proxy that runs in a separate interpreter process.

    :param target: Target in ``module.path:ClassName`` format.
    :param share_key: Optional key that reuses a child process across calls.
    :returns: A dynamically generated proxy class.
    """
    return create_extradited_class(target, share_key=share_key)
