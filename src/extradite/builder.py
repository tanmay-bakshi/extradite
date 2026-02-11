"""Proxy class builder routines."""


def create_extradited_class(target: str) -> type:
    """Create a dynamic proxy class for the given target.

    :param target: Target in ``module.path:ClassName`` format.
    :returns: A dynamic class placeholder.
    :raises NotImplementedError: Always, until implementation exists.
    """
    raise NotImplementedError(f"extradite core is not implemented yet for target {target!r}")
