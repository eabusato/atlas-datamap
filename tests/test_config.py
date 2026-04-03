"""Unit tests for Atlas connection configuration contracts."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pytest

from atlas.config import (
    AtlasConnectionConfig,
    ConfigValidationError,
    DatabaseEngine,
    PrivacyMode,
)


def _write_toml(target_dir: Path, name: str, content: str) -> Path:
    path = target_dir / name
    path.write_text(textwrap.dedent(content).strip() + "\n", encoding="utf-8")
    return path


class TestFromUrl:
    def test_postgresql_full_url(self) -> None:
        config = AtlasConnectionConfig.from_url(
            "postgresql://app_user:s3cr3t@db.example.com:5433/mydb"
        )
        assert config.engine is DatabaseEngine.postgresql
        assert config.host == "db.example.com"
        assert config.port == 5433
        assert config.database == "mydb"
        assert config.user == "app_user"
        assert config.password == "s3cr3t"

    def test_postgresql_default_port(self) -> None:
        config = AtlasConnectionConfig.from_url("postgresql://localhost/mydb")
        assert config.port == 5432

    def test_postgres_alias_maps_to_postgresql(self) -> None:
        config = AtlasConnectionConfig.from_url("postgres://localhost/mydb")
        assert config.engine is DatabaseEngine.postgresql

    def test_sqlite_memory_url(self) -> None:
        config = AtlasConnectionConfig.from_url("sqlite://:memory:")
        assert config.engine is DatabaseEngine.sqlite
        assert config.database == ":memory:"
        assert config.host == ""

    def test_sqlite_windows_drive_url_normalizes_leading_slash(self) -> None:
        config = AtlasConnectionConfig.from_url("sqlite:///D:/atlas/demo.db")
        assert config.engine is DatabaseEngine.sqlite
        assert config.database == "D:/atlas/demo.db"

    def test_url_query_parameters_become_connect_args(self) -> None:
        config = AtlasConnectionConfig.from_url(
            "postgresql://localhost/db?sslmode=require&application_name=atlas"
        )
        assert config.connect_args == {
            "sslmode": "require",
            "application_name": "atlas",
        }

    def test_url_query_parameters_merge_with_override_connect_args(self) -> None:
        config = AtlasConnectionConfig.from_url(
            "mssql://sa:secret@db.example.com:1433/atlas?encrypt=no",
            connect_args={"driver": "/opt/homebrew/lib/libtdsodbc.so"},
        )
        assert config.connect_args == {
            "encrypt": "no",
            "driver": "/opt/homebrew/lib/libtdsodbc.so",
        }

    def test_url_encoded_password_is_decoded(self) -> None:
        config = AtlasConnectionConfig.from_url("postgresql://user:p%40ss%21@host/db")
        assert config.password == "p@ss!"

    def test_unknown_scheme_raises_validation_error(self) -> None:
        with pytest.raises(ConfigValidationError, match="Unknown engine scheme"):
            AtlasConnectionConfig.from_url("oracle://host/db")

    def test_missing_scheme_raises_validation_error(self) -> None:
        with pytest.raises(ConfigValidationError, match="missing a scheme"):
            AtlasConnectionConfig.from_url("localhost/db")


class TestFromFile:
    def test_minimal_toml(self, phase_tmp_dir: Path) -> None:
        path = _write_toml(
            phase_tmp_dir,
            "minimal.toml",
            """
            [connection]
            engine = "postgresql"
            host = "localhost"
            database = "mydb"
            """,
        )
        config = AtlasConnectionConfig.from_file(path)
        assert config.engine is DatabaseEngine.postgresql
        assert config.host == "localhost"
        assert config.database == "mydb"
        assert config.port == 5432

    def test_full_toml(self, phase_tmp_dir: Path) -> None:
        path = _write_toml(
            phase_tmp_dir,
            "full.toml",
            """
            [connection]
            engine = "mysql"
            host = "db.host"
            port = 3307
            database = "shop"
            user = "reader"
            password = "abc"
            ssl_mode = "require"

            [connection.connect_args]
            charset = "utf8mb4"

            [analysis]
            sample_limit = 100
            privacy_mode = "stats_only"
            schema_filter = ["app"]
            schema_exclude = ["sys"]
            timeout_seconds = 60
            """,
        )
        config = AtlasConnectionConfig.from_file(path)
        assert config.engine is DatabaseEngine.mysql
        assert config.port == 3307
        assert config.sample_limit == 100
        assert config.privacy_mode is PrivacyMode.stats_only
        assert config.schema_filter == ["app"]
        assert config.schema_exclude == ["sys"]
        assert config.timeout_seconds == 60
        assert config.connect_args == {"charset": "utf8mb4"}

    def test_missing_file_raises_validation_error(self, phase_tmp_dir: Path) -> None:
        missing = phase_tmp_dir / "does-not-exist.toml"
        with pytest.raises(ConfigValidationError, match="Configuration file not found"):
            AtlasConnectionConfig.from_file(missing)

    def test_missing_engine_raises_validation_error(self, phase_tmp_dir: Path) -> None:
        path = _write_toml(
            phase_tmp_dir,
            "missing_engine.toml",
            """
            [connection]
            host = "localhost"
            database = "mydb"
            """,
        )
        with pytest.raises(ConfigValidationError, match="Missing 'engine'"):
            AtlasConnectionConfig.from_file(path)

    def test_invalid_privacy_mode_raises_validation_error(self, phase_tmp_dir: Path) -> None:
        path = _write_toml(
            phase_tmp_dir,
            "invalid_privacy.toml",
            """
            [connection]
            engine = "postgresql"
            host = "localhost"
            database = "mydb"

            [analysis]
            privacy_mode = "hidden"
            """,
        )
        with pytest.raises(ConfigValidationError, match="Invalid privacy_mode"):
            AtlasConnectionConfig.from_file(path)

    def test_overrides_apply_after_file_loading(self, phase_tmp_dir: Path) -> None:
        path = _write_toml(
            phase_tmp_dir,
            "override.toml",
            """
            [connection]
            engine = "postgresql"
            host = "localhost"
            database = "mydb"
            """,
        )
        config = AtlasConnectionConfig.from_file(path, sample_limit=200)
        assert config.sample_limit == 200

    def test_file_can_resolve_user_and_password_from_local_env_file(
        self, phase_tmp_dir: Path
    ) -> None:
        env_path = phase_tmp_dir / ".env"
        env_path.write_text(
            'ATLAS_DB_USER="reader"\nATLAS_DB_PASSWORD="secret"\n',
            encoding="utf-8",
        )
        path = _write_toml(
            phase_tmp_dir,
            "env-backed.toml",
            """
            [connection]
            engine = "mysql"
            host = "db.host"
            database = "shop"
            env_file = ".env"
            user_env = "ATLAS_DB_USER"
            password_env = "ATLAS_DB_PASSWORD"
            """,
        )

        config = AtlasConnectionConfig.from_file(path)

        assert config.user == "reader"
        assert config.password == "secret"

    def test_file_can_resolve_url_from_local_env_file(self, phase_tmp_dir: Path) -> None:
        env_path = phase_tmp_dir / ".env"
        env_path.write_text(
            'ATLAS_DB_URL="mysql://reader:secret@db.host:3306/shop"\n',
            encoding="utf-8",
        )
        path = _write_toml(
            phase_tmp_dir,
            "env-url.toml",
            """
            [connection]
            engine = "mysql"
            env_file = ".env"
            url_env = "ATLAS_DB_URL"

            [analysis]
            privacy_mode = "masked"
            """,
        )

        config = AtlasConnectionConfig.from_file(path)

        assert config.engine is DatabaseEngine.mysql
        assert config.host == "db.host"
        assert config.user == "reader"
        assert config.password == "secret"
        assert config.privacy_mode is PrivacyMode.masked

    def test_file_falls_back_to_sibling_dotenv_when_env_file_is_omitted(
        self, phase_tmp_dir: Path
    ) -> None:
        (phase_tmp_dir / ".env").write_text(
            'ATLAS_DB_USER="reader"\nATLAS_DB_PASSWORD="secret"\n',
            encoding="utf-8",
        )
        path = _write_toml(
            phase_tmp_dir,
            "legacy-onboarding.toml",
            """
            [connection]
            engine = "mysql"
            host = "db.host"
            database = "shop"
            user_env = "ATLAS_DB_USER"
            password_env = "ATLAS_DB_PASSWORD"
            """,
        )

        config = AtlasConnectionConfig.from_file(path)

        assert config.user == "reader"
        assert config.password == "secret"


class TestFromEnv:
    def test_basic_environment_loading(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ATLAS_ENGINE", "postgresql")
        monkeypatch.setenv("ATLAS_HOST", "localhost")
        monkeypatch.setenv("ATLAS_DATABASE", "mydb")
        config = AtlasConnectionConfig.from_env()
        assert config.engine is DatabaseEngine.postgresql
        assert config.host == "localhost"
        assert config.database == "mydb"

    def test_missing_engine_raises_validation_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("ATLAS_ENGINE", raising=False)
        with pytest.raises(ConfigValidationError, match="ATLAS_ENGINE"):
            AtlasConnectionConfig.from_env()

    def test_schema_filters_support_comma_separated_values(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ATLAS_ENGINE", "postgresql")
        monkeypatch.setenv("ATLAS_HOST", "localhost")
        monkeypatch.setenv("ATLAS_DATABASE", "mydb")
        monkeypatch.setenv("ATLAS_SCHEMA_FILTER", "public, app, billing")
        config = AtlasConnectionConfig.from_env()
        assert config.schema_filter == ["public", "app", "billing"]

    def test_invalid_timeout_raises_validation_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ATLAS_ENGINE", "postgresql")
        monkeypatch.setenv("ATLAS_HOST", "localhost")
        monkeypatch.setenv("ATLAS_DATABASE", "mydb")
        monkeypatch.setenv("ATLAS_TIMEOUT", "invalid")
        with pytest.raises(ConfigValidationError, match="must be an integer"):
            AtlasConnectionConfig.from_env()


class TestValidationAndSerialization:
    def test_missing_host_raises_validation_error(self) -> None:
        with pytest.raises(ConfigValidationError, match="'host' is required"):
            AtlasConnectionConfig(
                engine=DatabaseEngine.postgresql,
                host="",
                database="mydb",
            )

    def test_missing_database_raises_validation_error(self) -> None:
        with pytest.raises(ConfigValidationError, match="'database' is required"):
            AtlasConnectionConfig(
                engine=DatabaseEngine.postgresql,
                host="localhost",
                database="",
            )

    def test_invalid_port_raises_validation_error(self) -> None:
        with pytest.raises(ConfigValidationError, match="'port' must be between 1 and 65535"):
            AtlasConnectionConfig(
                engine=DatabaseEngine.postgresql,
                host="localhost",
                database="mydb",
                port=70000,
            )

    def test_default_schema_exclude_is_applied_per_engine(self) -> None:
        config = AtlasConnectionConfig(
            engine=DatabaseEngine.postgresql,
            host="localhost",
            database="mydb",
        )
        assert "information_schema" in config.schema_exclude

    def test_to_dict_masks_password_by_default(self) -> None:
        config = AtlasConnectionConfig.from_url("postgresql://user:pass@localhost/mydb")
        serialized = config.to_dict()
        assert serialized["password"] == "***"

    def test_dict_roundtrip_restores_fields(self) -> None:
        original = AtlasConnectionConfig.from_url(
            "postgresql://user:pass@localhost/mydb",
            privacy_mode=PrivacyMode.masked,
        )
        restored = AtlasConnectionConfig.from_dict(original.to_dict(include_password=True))
        assert restored.engine is DatabaseEngine.postgresql
        assert restored.user == "user"
        assert restored.password == "pass"
        assert restored.privacy_mode is PrivacyMode.masked

    def test_json_roundtrip_restores_fields(self) -> None:
        original = AtlasConnectionConfig.from_url(
            "postgresql://user:pass@localhost/mydb",
            privacy_mode=PrivacyMode.stats_only,
        )
        restored = AtlasConnectionConfig.from_json(original.to_json(include_password=True))
        assert restored.to_dict(include_password=True) == original.to_dict(include_password=True)

    def test_connection_string_safe_omits_password(self) -> None:
        config = AtlasConnectionConfig.from_url("postgresql://user:pass@localhost:5432/mydb")
        assert config.connection_string_safe == "postgresql://user@localhost:5432/mydb"

    def test_is_column_sensitive_matches_known_patterns(self) -> None:
        config = AtlasConnectionConfig(
            engine=DatabaseEngine.postgresql,
            host="localhost",
            database="mydb",
        )
        assert config.is_column_sensitive("customer_email")
        assert config.is_column_sensitive("api_secret")
        assert not config.is_column_sensitive("display_name")

    def test_json_output_can_be_parsed(self) -> None:
        config = AtlasConnectionConfig.from_url("mysql://root@localhost/shop")
        payload = json.loads(config.to_json())
        assert payload["engine"] == "mysql"
