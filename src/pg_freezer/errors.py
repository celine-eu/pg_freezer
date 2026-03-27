"""Exception hierarchy for pg_freezer."""


class PgFreezerError(Exception):
    """Base exception for all pg_freezer errors."""


class ConfigError(PgFreezerError):
    """Invalid or missing configuration."""


class ExportError(PgFreezerError):
    """Error during data export from PostgreSQL."""


class VerificationError(PgFreezerError):
    """Row count mismatch between PG export and S3 Parquet."""


class DeleteSafetyError(PgFreezerError):
    """Row count mismatch or unexpected state during DELETE transaction."""


class InvalidTransitionError(PgFreezerError):
    """Attempted an illegal manifest state machine transition."""


class StorageError(PgFreezerError):
    """Error reading from or writing to S3-compatible storage."""
