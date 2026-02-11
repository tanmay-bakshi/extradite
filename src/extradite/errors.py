"""Custom error types for extradite."""


class ExtraditeError(Exception):
    """Base class for all extradite errors."""


class ExtraditeModuleLeakError(ExtraditeError):
    """Raised when the isolated module appears in root-process sys.modules."""


class ExtraditeProtocolError(ExtraditeError):
    """Raised for unexpected messages on the parent/child IPC channel."""


class ExtraditeRemoteError(ExtraditeError):
    """Raised when the remote interpreter reports an exception."""

    remote_type_name: str
    remote_message: str
    remote_traceback: str

    def __init__(
        self,
        remote_type_name: str,
        remote_message: str,
        remote_traceback: str,
    ) -> None:
        """Initialize a remote exception wrapper.

        :param remote_type_name: Original remote exception type name.
        :param remote_message: Original remote exception message.
        :param remote_traceback: Original remote traceback text.
        """
        self.remote_type_name = remote_type_name
        self.remote_message = remote_message
        self.remote_traceback = remote_traceback
        formatted: str = (
            f"Remote side raised {remote_type_name}: {remote_message}\n"
            + f"Remote traceback:\n{remote_traceback}"
        )
        super().__init__(formatted)


class UnsupportedInteractionError(ExtraditeError):
    """Raised when a root-process interaction cannot be proxied safely."""
