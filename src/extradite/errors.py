"""Custom error types for extradite."""


class ExtraditeError(Exception):
    """Base class for all extradite errors."""


class ExtraditeModuleLeakError(ExtraditeError):
    """Raised when the isolated module appears in root-process sys.modules."""


class ExtraditeProtocolError(ExtraditeError):
    """Raised for unexpected messages on the parent/child IPC channel."""


class ExtraditeRemoteError(ExtraditeError):
    """Raised when the child interpreter reports an exception."""


class UnsupportedInteractionError(ExtraditeError):
    """Raised when a root-process interaction cannot be proxied safely."""
