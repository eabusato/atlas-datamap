"""Shared helpers for Phase 13 integration tests."""

from __future__ import annotations

import sqlite3
from pathlib import Path


def build_phase13_sqlite_fixture(db_path: Path) -> None:
    if db_path.exists():
        db_path.unlink()

    connection = sqlite3.connect(db_path)
    try:
        connection.executescript(
            """
            PRAGMA foreign_keys = ON;

            CREATE TABLE authors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                display_name VARCHAR(120) NOT NULL,
                active BOOL NOT NULL DEFAULT 1,
                profile_json JSON,
                birth_date DATE,
                created_at DATETIME NOT NULL
            );

            CREATE TABLE books (
                id INTEGER PRIMARY KEY,
                author_id INT NOT NULL,
                title VARCHAR(150) NOT NULL,
                price REAL,
                published_at DATETIME,
                FOREIGN KEY (author_id) REFERENCES authors(id)
            );

            CREATE INDEX idx_books_author_title ON books(author_id, title);
            CREATE INDEX idx_books_price_positive ON books(price) WHERE price > 0;

            CREATE TABLE memberships (
                user_id INTEGER NOT NULL,
                team_id INTEGER NOT NULL,
                role TEXT,
                PRIMARY KEY (user_id, team_id)
            );

            CREATE TABLE membership_audit (
                user_id INTEGER NOT NULL,
                team_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                FOREIGN KEY (user_id, team_id) REFERENCES memberships(user_id, team_id)
            );

            CREATE VIEW active_authors AS
            SELECT id, email, display_name
            FROM authors
            WHERE active = 1;

            INSERT INTO authors (email, display_name, active, profile_json, birth_date, created_at)
            VALUES
                ('alice@example.com', 'Alice', 1, '{"tier":"gold"}', '1990-01-02', '2024-01-01T10:00:00'),
                ('bob@example.com', 'Bob', 0, NULL, '1988-07-09', '2024-01-02T11:30:00');

            INSERT INTO books (id, author_id, title, price, published_at)
            VALUES
                (10, 1, 'Pragmatic Atlas', 42.5, '2024-02-10T09:00:00'),
                (11, 1, 'Sigilo Design', NULL, '2024-02-11T09:00:00'),
                (12, 2, 'SQLite Patterns', 10.0, NULL);

            INSERT INTO memberships (user_id, team_id, role)
            VALUES
                (1, 100, 'owner'),
                (2, 100, 'member');

            INSERT INTO membership_audit (user_id, team_id, action)
            VALUES
                (1, 100, 'created'),
                (2, 100, 'joined');
            """
        )
        connection.commit()
    finally:
        connection.close()
