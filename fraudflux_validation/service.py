"""Reusable validation service and stable error representation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple

from pydantic import ValidationError

from .contracts import TransactionEvent


@dataclass(frozen=True)
class ValidationIssue:
    code: str
    path: str
    message: str

    def as_dict(self) -> Dict[str, str]:
        return {
            "code": self.code,
            "path": self.path,
            "message": self.message,
        }


class TransactionValidationError(ValueError):
    """Contract error safe to expose through API and dead-letter metadata."""

    def __init__(
        self,
        issues: Iterable[ValidationIssue],
        *,
        cause: Optional[ValidationError] = None,
    ) -> None:
        self.issues: Tuple[ValidationIssue, ...] = tuple(issues)
        self.cause = cause
        super().__init__(
            f"transaction event failed validation with {len(self.issues)} issue(s)"
        )

    def api_detail(self) -> Dict[str, Any]:
        """Return a stable HTTP-422-compatible response body."""
        return {
            "code": "transaction_validation_failed",
            "message": "Transaction event failed validation.",
            "errors": [issue.as_dict() for issue in self.issues],
        }


def validate_transaction_event(payload: Any) -> TransactionEvent:
    """Validate a mapping or JSON payload and return a typed immutable event."""
    try:
        if isinstance(payload, (str, bytes, bytearray)):
            return TransactionEvent.model_validate_json(payload)
        if isinstance(payload, Mapping):
            return TransactionEvent.model_validate(dict(payload))
        return TransactionEvent.model_validate(payload)
    except ValidationError as exc:
        issues = (
            ValidationIssue(
                code=error["type"],
                path=_format_path(error["loc"]),
                message=error["msg"],
            )
            for error in exc.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            )
        )
        raise TransactionValidationError(issues, cause=exc) from exc


def _format_path(location: Tuple[Any, ...]) -> str:
    if not location:
        return "$"

    result = ""
    for part in location:
        if isinstance(part, int):
            result += f"[{part}]"
        elif result:
            result += f".{part}"
        else:
            result = str(part)
    return result

