from typing import Any

from fastapi import HTTPException, status


class OpenHandsError(HTTPException):
    """General Error"""

    def __init__(
        self,
        detail: Any = None,
        headers: dict[str, str] | None = None,
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
    ):
        super().__init__(status_code=status_code, detail=detail, headers=headers)


class AuthError(OpenHandsError):
    """Error in authentication."""

    def __init__(
        self,
        detail: Any = None,
        headers: dict[str, str] | None = None,
        status_code: int = status.HTTP_401_UNAUTHORIZED,
    ):
        super().__init__(status_code=status_code, detail=detail, headers=headers)


class PermissionsError(OpenHandsError):
    """Error in permissions."""

    def __init__(
        self,
        detail: Any = None,
        headers: dict[str, str] | None = None,
        status_code: int = status.HTTP_403_FORBIDDEN,
    ):
        super().__init__(status_code=status_code, detail=detail, headers=headers)


class SandboxError(OpenHandsError):
    """Error in Sandbox."""


class ConcurrencyLimitError(OpenHandsError):
    """Error when user has reached their concurrent sandbox limit."""

    def __init__(
        self,
        limit: int,
        current: int,
        detail: Any = None,
        headers: dict[str, str] | None = None,
    ):
        if detail is None:
            detail = {
                'error': 'CONCURRENCY_LIMIT_REACHED',
                'message': (
                    f'You have reached your limit of {limit} concurrent conversations. '
                    'Please close an existing conversation to start a new one.'
                ),
                'limit': limit,
                'current': current,
            }
        super().__init__(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=detail,
            headers=headers,
        )
