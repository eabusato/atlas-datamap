"""Connection configuration contracts and privacy policies for Atlas."""

from __future__ import annotations

import json
import os
import tomllib
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any
from urllib.parse import ParseResult, parse_qsl, unquote, urlparse

_SENSITIVE_COLUMN_PATTERNS: tuple[str, ...] = (
    "email",
    "cpf",
    "cnpj",
    "ssn",
    "password",
    "passwd",
    "senha",
    "token",
    "secret",
    "key",
    "chave",
    "credit_card",
    "card_number",
    "cvv",
    "cvc",
    "pin",
)


def _parse_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        value = raw_value.strip()
        if not key:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return values


class ConfigValidationError(ValueError):
    """Raised when Atlas connection configuration is invalid."""


class DatabaseEngine(StrEnum):
    """Supported database engines."""

    postgresql = "postgresql"
    mysql = "mysql"
    mssql = "mssql"
    sqlite = "sqlite"
    generic = "generic"

    @classmethod
    def from_scheme(cls, scheme: str) -> DatabaseEngine:
        normalized = scheme.lower().split("+")[0]
        mapping = {
            "postgresql": cls.postgresql,
            "postgres": cls.postgresql,
            "mysql": cls.mysql,
            "mariadb": cls.mysql,
            "mssql": cls.mssql,
            "sqlserver": cls.mssql,
            "sqlite": cls.sqlite,
            "generic": cls.generic,
        }
        try:
            return mapping[normalized]
        except KeyError as exc:
            supported = ", ".join(mapping)
            raise ConfigValidationError(
                f"Unknown engine scheme {scheme!r}. Supported schemes: {supported}."
            ) from exc

    @property
    def default_port(self) -> int | None:
        ports: dict[DatabaseEngine, int | None] = {
            DatabaseEngine.postgresql: 5432,
            DatabaseEngine.mysql: 3306,
            DatabaseEngine.mssql: 1433,
            DatabaseEngine.sqlite: None,
            DatabaseEngine.generic: None,
        }
        return ports[self]

    @property
    def default_schema_exclude(self) -> list[str]:
        excludes: dict[DatabaseEngine, list[str]] = {
            DatabaseEngine.postgresql: [
                "information_schema",
                "pg_catalog",
                "pg_toast",
                "pg_temp_1",
                "pg_toast_temp_1",
            ],
            DatabaseEngine.mysql: [
                "information_schema",
                "performance_schema",
                "mysql",
                "sys",
            ],
            DatabaseEngine.mssql: [
                "sys",
                "INFORMATION_SCHEMA",
                "guest",
                "db_owner",
            ],
            DatabaseEngine.sqlite: [],
            DatabaseEngine.generic: [],
        }
        return excludes[self]


class PrivacyMode(StrEnum):
    """Privacy policy for any operation that may access live data."""

    normal = "normal"
    masked = "masked"
    stats_only = "stats_only"
    no_samples = "no_samples"

    @property
    def allows_samples(self) -> bool:
        return self in {PrivacyMode.normal, PrivacyMode.masked}

    @property
    def allows_raw_values(self) -> bool:
        return self is PrivacyMode.normal


@dataclass(slots=True)
class AtlasConnectionConfig:
    """Validated Atlas connection configuration."""

    engine: DatabaseEngine
    host: str
    database: str
    port: int | None = None
    user: str | None = None
    password: str | None = None
    ssl_mode: str = "disable"
    timeout_seconds: int = 30
    schema_filter: list[str] = field(default_factory=list)
    schema_exclude: list[str] = field(default_factory=list)
    sample_limit: int = 50
    privacy_mode: PrivacyMode = PrivacyMode.normal
    connect_args: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._validate()
        self._apply_defaults()

    def _validate(self) -> None:
        errors: list[str] = []

        if self.engine not in {DatabaseEngine.sqlite, DatabaseEngine.generic} and not self.host.strip():
            errors.append("'host' is required for non-SQLite engines.")

        if not self.database.strip():
            errors.append("'database' is required.")

        if self.timeout_seconds < 1:
            errors.append(f"'timeout_seconds' must be >= 1, got {self.timeout_seconds}.")

        if not 1 <= self.sample_limit <= 10_000:
            errors.append(f"'sample_limit' must be between 1 and 10000, got {self.sample_limit}.")

        if self.port is not None and not 1 <= self.port <= 65535:
            errors.append(f"'port' must be between 1 and 65535, got {self.port}.")

        valid_ssl = {"disable", "require", "verify-ca", "verify-full", "preferred"}
        if self.ssl_mode not in valid_ssl:
            valid_display = ", ".join(sorted(valid_ssl))
            errors.append(f"'ssl_mode' must be one of {valid_display}, got {self.ssl_mode!r}.")

        if errors:
            raise ConfigValidationError("Invalid AtlasConnectionConfig:\n- " + "\n- ".join(errors))

    def _apply_defaults(self) -> None:
        if self.port is None:
            object.__setattr__(self, "port", self.engine.default_port)
        if not self.schema_exclude:
            object.__setattr__(self, "schema_exclude", list(self.engine.default_schema_exclude))

    @classmethod
    def from_url(cls, url: str, **overrides: Any) -> AtlasConnectionConfig:
        parsed: ParseResult = urlparse(url)
        if not parsed.scheme:
            raise ConfigValidationError(
                f"Connection URL {url!r} is missing a scheme. Expected "
                "'engine://user:password@host:port/database'."
            )

        engine = DatabaseEngine.from_scheme(parsed.scheme)
        if engine is DatabaseEngine.sqlite:
            database = parsed.path or parsed.netloc
            if not database:
                raise ConfigValidationError(
                    f"SQLite URL {url!r} is invalid. Use sqlite:///path/to/file.db "
                    "or sqlite://:memory:."
                )
            return cls(engine=engine, host="", database=database, **overrides)
        if engine is DatabaseEngine.generic:
            generic_prefix = "generic+"
            sqlalchemy_url = (
                url[len(generic_prefix) :]
                if parsed.scheme.lower().startswith(generic_prefix)
                else url
            )
            sqlalchemy_parsed: ParseResult = urlparse(sqlalchemy_url)
            connect_args = dict(parse_qsl(sqlalchemy_parsed.query, keep_blank_values=True))
            override_connect_args = dict(overrides.pop("connect_args", {}))
            connect_args.update(override_connect_args)
            connect_args["sqlalchemy_url"] = sqlalchemy_url
            if sqlalchemy_parsed.scheme == "sqlite":
                database = sqlalchemy_parsed.path or sqlalchemy_parsed.netloc
            else:
                database = sqlalchemy_parsed.path.lstrip("/") or sqlalchemy_parsed.netloc
            return cls(
                engine=engine,
                host=sqlalchemy_parsed.hostname or "",
                port=sqlalchemy_parsed.port,
                database=database or sqlalchemy_url,
                user=unquote(sqlalchemy_parsed.username) if sqlalchemy_parsed.username else None,
                password=unquote(sqlalchemy_parsed.password) if sqlalchemy_parsed.password else None,
                connect_args=connect_args,
                **overrides,
            )

        connect_args = dict(parse_qsl(parsed.query, keep_blank_values=True))
        override_connect_args = dict(overrides.pop("connect_args", {}))
        connect_args.update(override_connect_args)
        return cls(
            engine=engine,
            host=parsed.hostname or "",
            port=parsed.port,
            database=parsed.path.lstrip("/"),
            user=unquote(parsed.username) if parsed.username else None,
            password=unquote(parsed.password) if parsed.password else None,
            connect_args=connect_args,
            **overrides,
        )

    @classmethod
    def from_file(cls, path: str | os.PathLike[str], **overrides: Any) -> AtlasConnectionConfig:
        config_path = Path(path)
        try:
            with config_path.open("rb") as handle:
                data = tomllib.load(handle)
        except FileNotFoundError as exc:
            raise ConfigValidationError(f"Configuration file not found: {config_path}.") from exc
        except Exception as exc:
            raise ConfigValidationError(f"Failed to parse TOML from {config_path}: {exc}") from exc

        connection = data.get("connection", {})
        analysis = data.get("analysis", {})
        env_file_raw = str(connection.get("env_file", "")).strip()
        has_env_refs = any(
            str(connection.get(key_name, "")).strip()
            for key_name in ("url_env", "user_env", "password_env")
        )
        env_values: dict[str, str] = {}
        if env_file_raw:
            env_path = Path(env_file_raw)
            if not env_path.is_absolute():
                env_path = config_path.parent / env_path
            env_values.update(_parse_env_file(env_path))
        elif has_env_refs:
            env_values.update(_parse_env_file(config_path.parent / ".env"))
        env_values.update(os.environ)

        raw_engine = connection.get("engine")
        if not raw_engine:
            raise ConfigValidationError(
                f"Missing 'engine' in [connection] section of {config_path}."
            )

        try:
            engine = DatabaseEngine(str(raw_engine).lower())
        except ValueError as exc:
            valid = ", ".join(engine.value for engine in DatabaseEngine)
            raise ConfigValidationError(
                f"Unknown engine {raw_engine!r} in {config_path}. Supported values: {valid}."
            ) from exc

        raw_privacy = analysis.get("privacy_mode", PrivacyMode.normal.value)
        try:
            privacy_mode = PrivacyMode(str(raw_privacy).lower())
        except ValueError as exc:
            valid = ", ".join(mode.value for mode in PrivacyMode)
            raise ConfigValidationError(
                f"Invalid privacy_mode {raw_privacy!r} in {config_path}. Supported values: {valid}."
            ) from exc

        def _env_lookup(key_name: str) -> str | None:
            raw_name = str(connection.get(key_name, "")).strip()
            if not raw_name:
                return None
            value = env_values.get(raw_name, "").strip()
            return value or None

        url = _env_lookup("url_env")
        analysis_payload: dict[str, Any] = {
            "ssl_mode": connection.get("ssl_mode", "disable"),
            "timeout_seconds": analysis.get("timeout_seconds", 30),
            "schema_filter": list(analysis.get("schema_filter", [])),
            "schema_exclude": list(analysis.get("schema_exclude", [])),
            "sample_limit": analysis.get("sample_limit", 50),
            "privacy_mode": privacy_mode,
            "connect_args": dict(connection.get("connect_args", {})),
        }
        if url is not None:
            analysis_payload.update(overrides)
            return cls.from_url(url, **analysis_payload)
        payload: dict[str, Any] = {
            "engine": engine,
            "host": connection.get("host", ""),
            "database": connection.get("database", ""),
            "port": connection.get("port"),
            "user": connection.get("user") or _env_lookup("user_env"),
            "password": connection.get("password") or _env_lookup("password_env"),
            **analysis_payload,
        }
        payload.update(overrides)
        return cls(**payload)

    @classmethod
    def from_env(cls, prefix: str = "ATLAS", **overrides: Any) -> AtlasConnectionConfig:
        def env(name: str) -> str | None:
            return os.environ.get(f"{prefix}_{name}")

        def env_int(name: str, default: int) -> int:
            raw = env(name)
            if raw is None:
                return default
            try:
                return int(raw)
            except ValueError as exc:
                raise ConfigValidationError(
                    f"Environment variable {prefix}_{name} must be an integer, got {raw!r}."
                ) from exc

        def env_list(name: str) -> list[str]:
            raw = env(name)
            if not raw:
                return []
            return [item.strip() for item in raw.split(",") if item.strip()]

        raw_engine = env("ENGINE")
        if not raw_engine:
            raise ConfigValidationError(f"Environment variable {prefix}_ENGINE is required.")
        try:
            engine = DatabaseEngine(raw_engine.lower())
        except ValueError as exc:
            valid = ", ".join(engine.value for engine in DatabaseEngine)
            raise ConfigValidationError(
                f"Environment variable {prefix}_ENGINE={raw_engine!r} is invalid. "
                f"Supported values: {valid}."
            ) from exc

        raw_privacy = env("PRIVACY_MODE") or PrivacyMode.normal.value
        try:
            privacy_mode = PrivacyMode(raw_privacy.lower())
        except ValueError as exc:
            valid = ", ".join(mode.value for mode in PrivacyMode)
            raise ConfigValidationError(
                f"Environment variable {prefix}_PRIVACY_MODE={raw_privacy!r} is invalid. "
                f"Supported values: {valid}."
            ) from exc

        payload: dict[str, Any] = {
            "engine": engine,
            "host": env("HOST") or "",
            "database": env("DATABASE") or "",
            "user": env("USER"),
            "password": env("PASSWORD"),
            "ssl_mode": env("SSL_MODE") or "disable",
            "timeout_seconds": env_int("TIMEOUT", 30),
            "schema_filter": env_list("SCHEMA_FILTER"),
            "schema_exclude": env_list("SCHEMA_EXCLUDE"),
            "sample_limit": env_int("SAMPLE_LIMIT", 50),
            "privacy_mode": privacy_mode,
        }
        raw_port = env("PORT")
        if raw_port is not None:
            payload["port"] = env_int("PORT", 0)
        payload.update(overrides)
        return cls(**payload)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AtlasConnectionConfig:
        raw_engine = data.get("engine")
        raw_privacy = data.get("privacy_mode", PrivacyMode.normal.value)
        engine = (
            raw_engine
            if isinstance(raw_engine, DatabaseEngine)
            else DatabaseEngine(str(raw_engine))
        )
        privacy_mode = (
            raw_privacy if isinstance(raw_privacy, PrivacyMode) else PrivacyMode(str(raw_privacy))
        )
        password = data.get("password")
        if password == "***":
            password = None
        return cls(
            engine=engine,
            host=str(data.get("host", "")),
            port=data.get("port"),
            database=str(data.get("database", "")),
            user=data.get("user"),
            password=password,
            ssl_mode=str(data.get("ssl_mode", "disable")),
            timeout_seconds=int(data.get("timeout_seconds", 30)),
            schema_filter=list(data.get("schema_filter", [])),
            schema_exclude=list(data.get("schema_exclude", [])),
            sample_limit=int(data.get("sample_limit", 50)),
            privacy_mode=privacy_mode,
            connect_args=dict(data.get("connect_args", {})),
        )

    def to_dict(self, include_password: bool = False) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "engine": self.engine.value,
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "password": self.password if include_password else ("***" if self.password else None),
            "ssl_mode": self.ssl_mode,
            "timeout_seconds": self.timeout_seconds,
            "schema_filter": list(self.schema_filter),
            "schema_exclude": list(self.schema_exclude),
            "sample_limit": self.sample_limit,
            "privacy_mode": self.privacy_mode.value,
            "connect_args": dict(self.connect_args),
        }
        return payload

    def to_json(self, include_password: bool = False, indent: int = 2) -> str:
        return json.dumps(self.to_dict(include_password=include_password), indent=indent)

    @classmethod
    def from_json(cls, payload: str) -> AtlasConnectionConfig:
        return cls.from_dict(json.loads(payload))

    def is_column_sensitive(self, column_name: str) -> bool:
        lowered = column_name.lower()
        return any(pattern in lowered for pattern in _SENSITIVE_COLUMN_PATTERNS)

    @property
    def connection_string_safe(self) -> str:
        if self.engine is DatabaseEngine.sqlite:
            return f"sqlite:///{self.database}"
        if self.engine is DatabaseEngine.generic:
            raw_url = str(self.connect_args.get("sqlalchemy_url", "")).strip()
            if raw_url:
                parsed = urlparse(raw_url)
                scheme = parsed.scheme
                user_part = f"{unquote(parsed.username)}@" if parsed.username else ""
                port_part = f":{parsed.port}" if parsed.port else ""
                netloc = f"{user_part}{parsed.hostname or ''}{port_part}"
                if parsed.scheme == "sqlite":
                    return f"generic+sqlite://{parsed.path}"
                if netloc:
                    return f"generic+{scheme}://{netloc}{parsed.path}"
                return f"generic+{scheme}://{parsed.path}"
            return f"generic:///{self.database}"
        user_part = f"{self.user}@" if self.user else ""
        port_part = f":{self.port}" if self.port else ""
        return f"{self.engine.value}://{user_part}{self.host}{port_part}/{self.database}"

    def __str__(self) -> str:
        return self.connection_string_safe

    def __repr__(self) -> str:
        return (
            "AtlasConnectionConfig("
            f"engine={self.engine.value!r}, "
            f"host={self.host!r}, "
            f"database={self.database!r}, "
            f"privacy_mode={self.privacy_mode.value!r})"
        )
