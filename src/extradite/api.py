"""User-facing API entrypoints for extradite."""

from extradite.builder import create_extradited_class


def extradite(target: str) -> type:
    """Create a class-like proxy that runs in a separate interpreter process.

    :param target: Target in ``module.path:ClassName`` format.
    :returns: A dynamically generated proxy class.
    """
    return create_extradited_class(target)
