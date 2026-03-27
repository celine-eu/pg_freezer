"""Configuration models for pg_freezer, loaded from YAML."""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Annotated, Any, Literal

import yaml
from pydantic import (
    AnyHttpUrl,
    BaseModel,
    SecretStr,
    field_validator,
    model_validator,
)
from pydantic.functional_validators import BeforeValidator

from pg_freezer.errors import ConfigError


def parse_timestring(value: Any) -> timedelta:
    """Parse timestrings like '15d', '1h', '30m', '10s' into timedelta."""
    if isinstance(value, timedelta):
        return value
    pattern = re.compile(r"^(\d+)(d|h|m|s)$")
    m = pattern.fullmatch(str(value).strip())
    if not m:
        raise ValueError(
            f"Invalid timestring {value!r}. Expected format: 15d, 1h, 30m, 10s"
        )
    n, unit = int(m.group(1)), m.group(2)
    mapping = {"d": "days", "h": "hours", "m": "minutes", "s": "seconds"}
    return timedelta(**{mapping[unit]: n})


Timestring = Annotated[timedelta, BeforeValidator(parse_timestring)]


class DatabaseConfig(BaseModel):
    dsn: SecretStr
    pool_size: int = 5
    statement_timeout: Timestring = timedelta(seconds=30)

    @field_validator("pool_size")
    @classmethod
    def _pool_size_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("pool_size must be >= 1")
        return v


class StorageConfig(BaseModel):
    protocol: Literal["s3"] = "s3"
    endpoint_url: str | None = None
    bucket: str
    access_key: SecretStr | None = None
    secret_key: SecretStr | None = None
    path_style: bool = False


class NotifyConfig(BaseModel):
    webhook_url: AnyHttpUrl | None = None


class GlobalConfig(BaseModel):
    max_table_concurrency: int = 3
    dry_run: bool = False
    check_interval: Timestring = timedelta(hours=1)
    notify: NotifyConfig = NotifyConfig()

    @field_validator("max_table_concurrency")
    @classmethod
    def _concurrency_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("max_table_concurrency must be >= 1")
        return v


class TableConfig(BaseModel):
    name: str
    schema_name: str = "public"
    timestamp_column: str
    partition_by: Literal["hourly", "daily", "monthly"] = "daily"
    lag: Timestring = timedelta(days=15)
    extra_filters: str | None = None

    model_config = {"populate_by_name": True}

    @model_validator(mode="before")
    @classmethod
    def _remap_schema(cls, data: Any) -> Any:
        """Allow 'schema' key in YAML (pydantic reserved word → schema_name)."""
        if isinstance(data, dict) and "schema" in data and "schema_name" not in data:
            data = dict(data)
            data["schema_name"] = data.pop("schema")
        return data

    @property
    def qualified_name(self) -> str:
        return f"{self.schema_name}.{self.name}"


class AppConfig(BaseModel):
    database: DatabaseConfig
    storage: StorageConfig
    tables: list[TableConfig]
    global_config: GlobalConfig = GlobalConfig()

    model_config = {"populate_by_name": True}

    @model_validator(mode="before")
    @classmethod
    def _remap_global(cls, data: Any) -> Any:
        """Allow 'global' key in YAML (Python reserved word → global_config)."""
        if isinstance(data, dict) and "global" in data and "global_config" not in data:
            data = dict(data)
            data["global_config"] = data.pop("global")
        return data

    @field_validator("tables")
    @classmethod
    def _tables_not_empty(cls, v: list[TableConfig]) -> list[TableConfig]:
        if not v:
            raise ValueError("At least one table must be configured")
        return v


_ENV_DEFAULT_RE = re.compile(r"\$\{(\w+):=([^}]*)\}")


def _expand_env(content: str) -> str:
    """
    Expand environment variables in a string.

    Supports three forms:
        ${VAR:=default}  — use VAR if set and non-empty, otherwise use default
        ${VAR}           — use VAR if set, otherwise leave as-is
        $VAR             — same as ${VAR}
    """
    import os

    # First pass: handle ${VAR:=default} (not supported by os.path.expandvars)
    def _replace_with_default(m: re.Match) -> str:
        name, default = m.group(1), m.group(2)
        return os.environ.get(name) or default

    content = _ENV_DEFAULT_RE.sub(_replace_with_default, content)

    # Second pass: handle ${VAR} and $VAR
    return os.path.expandvars(content)


def load_config(path: str) -> AppConfig:
    """
    Load and validate YAML config file. Raises ConfigError on any issue.

    Environment variables are expanded before parsing. Supported syntax:
        ${VAR:=default}  — use VAR env var, fall back to default if unset
        ${VAR}           — use VAR env var, leave as-is if unset
        $VAR             — same as ${VAR}

    Examples:
        dsn: "${PG_DSN:=postgresql://postgres:postgres@localhost/db}"
        access_key: "${S3_ACCESS_KEY}"
    """
    try:
        with open(path) as f:
            content = f.read()
    except FileNotFoundError:
        raise ConfigError(f"Config file not found: {path}")

    content = _expand_env(content)

    try:
        raw = yaml.safe_load(content)
    except yaml.YAMLError as e:
        raise ConfigError(f"Invalid YAML in {path}: {e}")

    try:
        return AppConfig.model_validate(raw)
    except Exception as e:
        raise ConfigError(f"Config validation failed: {e}") from e
