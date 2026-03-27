"""Tests for configuration loading and validation."""

from datetime import timedelta

import pytest

from pg_freezer.config import AppConfig, TableConfig, load_config, parse_timestring
from pg_freezer.errors import ConfigError


class TestParseTimestring:
    def test_days(self):
        assert parse_timestring("15d") == timedelta(days=15)

    def test_hours(self):
        assert parse_timestring("1h") == timedelta(hours=1)

    def test_minutes(self):
        assert parse_timestring("30m") == timedelta(minutes=30)

    def test_seconds(self):
        assert parse_timestring("10s") == timedelta(seconds=10)

    def test_passthrough_timedelta(self):
        td = timedelta(days=5)
        assert parse_timestring(td) is td

    def test_invalid_no_unit(self):
        with pytest.raises(ValueError, match="Invalid timestring"):
            parse_timestring("15")

    def test_invalid_unknown_unit(self):
        with pytest.raises(ValueError, match="Invalid timestring"):
            parse_timestring("15x")

    def test_invalid_empty(self):
        with pytest.raises(ValueError, match="Invalid timestring"):
            parse_timestring("")

    def test_invalid_double_unit(self):
        with pytest.raises(ValueError, match="Invalid timestring"):
            parse_timestring("15dd")

    def test_zero(self):
        assert parse_timestring("0d") == timedelta(days=0)


class TestTableConfig:
    def test_schema_alias(self):
        """'schema' in dict maps to schema_name field."""
        cfg = TableConfig.model_validate({
            "name": "readings",
            "schema": "sensors",
            "timestamp_column": "recorded_at",
        })
        assert cfg.schema_name == "sensors"

    def test_schema_name_direct(self):
        cfg = TableConfig.model_validate({
            "name": "readings",
            "schema_name": "sensors",
            "timestamp_column": "recorded_at",
        })
        assert cfg.schema_name == "sensors"

    def test_defaults(self):
        cfg = TableConfig.model_validate({
            "name": "readings",
            "timestamp_column": "ts",
        })
        assert cfg.schema_name == "public"
        assert cfg.partition_by == "daily"
        assert cfg.lag == timedelta(days=15)

    def test_qualified_name(self):
        cfg = TableConfig.model_validate({
            "name": "readings",
            "schema": "sensors",
            "timestamp_column": "ts",
        })
        assert cfg.qualified_name == "sensors.readings"


class TestAppConfig:
    _BASE = {
        "database": {"dsn": "postgresql://user:pass@host/db"},
        "storage": {"bucket": "test-bucket"},
        "tables": [{"name": "t", "timestamp_column": "ts"}],
    }

    def test_minimal_valid(self):
        cfg = AppConfig.model_validate(self._BASE)
        assert cfg.database.dsn.get_secret_value() == "postgresql://user:pass@host/db"
        assert cfg.storage.bucket == "test-bucket"
        assert len(cfg.tables) == 1

    def test_global_alias(self):
        data = dict(self._BASE)
        data["global"] = {"max_table_concurrency": 5}
        cfg = AppConfig.model_validate(data)
        assert cfg.global_config.max_table_concurrency == 5

    def test_no_tables_raises(self):
        data = dict(self._BASE, tables=[])
        with pytest.raises(Exception, match="At least one table"):
            AppConfig.model_validate(data)

    def test_dsn_is_secret(self):
        cfg = AppConfig.model_validate(self._BASE)
        # SecretStr should not expose value in repr
        assert "pass" not in repr(cfg.database.dsn)

    def test_access_key_is_secret(self):
        data = dict(self._BASE)
        data["storage"] = {
            "bucket": "b",
            "access_key": "AKIAIOSFODNN7",
            "secret_key": "wJalrXUt",
        }
        cfg = AppConfig.model_validate(data)
        assert "AKIAIOSFODNN7" not in repr(cfg.storage.access_key)
        assert cfg.storage.access_key.get_secret_value() == "AKIAIOSFODNN7"


class TestLoadConfig:
    def test_file_not_found(self, tmp_path):
        with pytest.raises(ConfigError, match="not found"):
            load_config(str(tmp_path / "nonexistent.yaml"))

    def test_invalid_yaml(self, tmp_path):
        f = tmp_path / "bad.yaml"
        f.write_text("{{invalid: yaml: :")
        with pytest.raises(ConfigError, match="Invalid YAML"):
            load_config(str(f))

    def test_valid_minimal(self, tmp_path):
        f = tmp_path / "config.yaml"
        f.write_text("""
database:
  dsn: "postgresql://u:p@h/db"
storage:
  bucket: my-bucket
tables:
  - name: readings
    timestamp_column: ts
""")
        cfg = load_config(str(f))
        assert cfg.tables[0].name == "readings"

    def test_validation_error_wrapped(self, tmp_path):
        f = tmp_path / "config.yaml"
        f.write_text("""
database:
  dsn: "postgresql://u:p@h/db"
storage:
  bucket: my-bucket
tables: []
""")
        with pytest.raises(ConfigError, match="validation failed"):
            load_config(str(f))

    def test_env_var_expansion_with_default_set(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TEST_BUCKET_DEF", "from-env")
        f = tmp_path / "config.yaml"
        f.write_text("""
database:
  dsn: "postgresql://u:p@h/db"
storage:
  bucket: "${TEST_BUCKET_DEF:=fallback-bucket}"
tables:
  - name: readings
    timestamp_column: ts
""")
        cfg = load_config(str(f))
        assert cfg.storage.bucket == "from-env"

    def test_env_var_expansion_with_default_unset(self, tmp_path, monkeypatch):
        monkeypatch.delenv("TEST_MISSING_BUCKET", raising=False)
        f = tmp_path / "config.yaml"
        f.write_text("""
database:
  dsn: "postgresql://u:p@h/db"
storage:
  bucket: "${TEST_MISSING_BUCKET:=fallback-bucket}"
tables:
  - name: readings
    timestamp_column: ts
""")
        cfg = load_config(str(f))
        assert cfg.storage.bucket == "fallback-bucket"

    def test_env_var_expansion(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TEST_PG_DSN", "postgresql://env_user:env_pass@host/db")
        monkeypatch.setenv("TEST_BUCKET", "env-bucket")
        f = tmp_path / "config.yaml"
        f.write_text("""
database:
  dsn: "${TEST_PG_DSN}"
storage:
  bucket: "${TEST_BUCKET}"
tables:
  - name: readings
    timestamp_column: ts
""")
        cfg = load_config(str(f))
        assert cfg.database.dsn.get_secret_value() == "postgresql://env_user:env_pass@host/db"
        assert cfg.storage.bucket == "env-bucket"

    def test_env_var_expansion_dollar_syntax(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TEST_BUCKET2", "bucket-from-env")
        f = tmp_path / "config.yaml"
        f.write_text("""
database:
  dsn: "postgresql://u:p@h/db"
storage:
  bucket: $TEST_BUCKET2
tables:
  - name: readings
    timestamp_column: ts
""")
        cfg = load_config(str(f))
        assert cfg.storage.bucket == "bucket-from-env"
