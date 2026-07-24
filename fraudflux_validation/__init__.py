"""Transaction contract validation for FraudFlux."""

from .contracts import (
    SUPPORTED_CURRENCIES,
    SUPPORTED_SCHEMA_VERSIONS,
    TransactionEvent,
)
from .dead_letter import DeadLetterEvent, DeadLetterSource, build_dead_letter_event
from .service import (
    TransactionValidationError,
    ValidationIssue,
    validate_transaction_event,
)

__all__ = [
    "DeadLetterEvent",
    "DeadLetterSource",
    "SUPPORTED_CURRENCIES",
    "SUPPORTED_SCHEMA_VERSIONS",
    "TransactionEvent",
    "TransactionValidationError",
    "ValidationIssue",
    "build_dead_letter_event",
    "validate_transaction_event",
]

