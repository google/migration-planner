# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Thread-safe SQLite database manager for scan persistence."""

import json
import os
import sqlite3
import threading
import types
from typing import Any


class DatabaseManager:
  """Manages robust SQLite database interactions with thread safety."""

  db_path: str
  _lock: threading.Lock
  _conn: sqlite3.Connection

  def __init__(self, db_path: str) -> None:
    self.db_path = db_path
    self._lock = threading.Lock()
    db_dir = os.path.dirname(self.db_path)
    if db_dir and not self.db_path.startswith(":memory:"):
      os.makedirs(db_dir, exist_ok=True)

    # Permit multi-threaded serialization safely through instance lock
    # and enable substantial timeout for high-load worker retry cycles.
    self._conn = sqlite3.connect(
        self.db_path, check_same_thread=False, timeout=60.0
    )

    # Enable Write-Ahead Logging for safer concurrent multi-threaded operations
    # on durable filesystem installations.
    if not self.db_path.startswith(":memory:"):
      self._conn.execute("PRAGMA journal_mode=WAL;")

    self._create_tables()

  def close(self) -> None:
    """Releases the explicit long-lived persistence connection."""
    with self._lock:
      self._conn.close()

  def __enter__(self) -> "DatabaseManager":
    """Context management access initialization."""
    return self

  def __exit__(
      self,
      _exc_type: type[BaseException] | None,  # pylint: disable=invalid-name
      _exc: BaseException | None,  # pylint: disable=invalid-name
      _tb: types.TracebackType | None,  # pylint: disable=invalid-name
  ) -> None:
    """Guaranteed shutdown resource closure."""
    self.close()

  def _create_tables(self) -> None:
    """Ensures atomic schema creation of vital operation tables."""
    with self._lock:
      with self._conn:
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_chats (
                chat_id TEXT PRIMARY KEY,
                count INTEGER
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_channels (
                team_id TEXT,
                channel_id TEXT,
                root_count INTEGER,
                reply_count INTEGER,
                total_count INTEGER,
                PRIMARY KEY (team_id, channel_id)
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_teams (
                team_id TEXT PRIMARY KEY,
                details TEXT
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_users (
                user_id TEXT PRIMARY KEY,
                chat_ids TEXT
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_channel_memberships (
                team_id TEXT,
                channel_id TEXT,
                count INTEGER,
                PRIMARY KEY (team_id, channel_id)
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS roster_users (
                user_id TEXT PRIMARY KEY,
                user_principal_name TEXT,
                raw_data TEXT
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS roster_teams (
                team_id TEXT PRIMARY KEY,
                raw_data TEXT
            )
        """)

  def get_roster_users(self) -> list[dict[str, Any]] | None:
    """Efficient single-fetch retrieval of user cached records."""
    with self._lock:
      cursor = self._conn.execute("SELECT raw_data FROM roster_users")
      rows = cursor.fetchall()
      if not rows:
        return None
      return [json.loads(row[0]) for row in rows]

  def save_roster_users(self, users: list[dict[str, Any]]) -> None:
    """Atomically records collective roster sets to table storage."""
    data = []
    for user in users:
      user_id = user.get("id") or user.get("userPrincipalName")
      user_principal_name = user.get("userPrincipalName") or ""
      if user_id:
        data.append((user_id, user_principal_name, json.dumps(user)))

    with self._lock:
      with self._conn:
        self._conn.executemany(
            "INSERT OR REPLACE INTO roster_users "
            "(user_id, user_principal_name, raw_data) VALUES (?, ?, ?)",
            data,
        )

  def get_roster_teams(self) -> list[dict[str, Any]] | None:
    """Efficient single-fetch retrieval of cached team rosters."""
    with self._lock:
      cursor = self._conn.execute("SELECT raw_data FROM roster_teams")
      rows = cursor.fetchall()
      if not rows:
        return None
      return [json.loads(row[0]) for row in rows]

  def save_roster_teams(self, teams: list[dict[str, Any]]) -> None:
    """Atomically persists resolved team collection cache."""
    data = []
    for team in teams:
      team_id = team.get("id")
      if team_id:
        data.append((team_id, json.dumps(team)))

    with self._lock:
      with self._conn:
        self._conn.executemany(
            "INSERT OR REPLACE INTO roster_teams (team_id, raw_data) "
            "VALUES (?, ?)",
            data,
        )

  def get_processed_chat(self, chat_id: str) -> int | None:
    """Atomic read acquisition for previous execution state count."""
    with self._lock:
      cursor = self._conn.execute(
          "SELECT count FROM processed_chats WHERE chat_id = ?",
          (chat_id,),
      )
      row = cursor.fetchone()
      return row[0] if row else None

  def save_processed_chat(self, chat_id: str, count: int) -> None:
    """Safely updates persistence barrier state on scan completion."""
    with self._lock:
      with self._conn:
        self._conn.execute(
            "INSERT OR REPLACE INTO processed_chats (chat_id, count) "
            "VALUES (?, ?)",
            (chat_id, count),
        )

  def get_processed_channel(
      self, team_id: str, channel_id: str
  ) -> tuple[int, int, int] | None:
    """Retrieves fully isolated breakdown of past scan totals."""
    with self._lock:
      cursor = self._conn.execute(
          "SELECT root_count, reply_count, total_count "
          "FROM processed_channels "
          "WHERE team_id = ? AND channel_id = ?",
          (team_id, channel_id),
      )
      row = cursor.fetchone()
      return row if row else None

  def save_processed_channel(
      self,
      team_id: str,
      channel_id: str,
      root_count: int,
      reply_count: int,
      total_count: int,
  ) -> None:
    """Safely writes composite records protected by atomic rollback."""
    with self._lock:
      with self._conn:
        self._conn.execute(
            "INSERT OR REPLACE INTO processed_channels "
            "(team_id, channel_id, root_count, reply_count, total_count) "
            "VALUES (?, ?, ?, ?, ?)",
            (team_id, channel_id, root_count, reply_count, total_count),
        )

  def get_processed_team(self, team_id: str) -> dict[str, Any] | None:
    """Fetches specific team structure dictionary map from raw JSON text."""
    with self._lock:
      cursor = self._conn.execute(
          "SELECT details FROM processed_teams WHERE team_id = ?",
          (team_id,),
      )
      row = cursor.fetchone()
      return json.loads(row[0]) if row else None

  def save_processed_team(self, team_id: str, details: dict[str, Any]) -> None:
    """Updates highly complex deserialized nested object graph atomically."""
    with self._lock:
      with self._conn:
        self._conn.execute(
            "INSERT OR REPLACE INTO processed_teams (team_id, details) "
            "VALUES (?, ?)",
            (team_id, json.dumps(details)),
        )

  def get_processed_user(self, user_id: str) -> list[str] | None:
    """Fetches precise array mapping for a unique identifier context."""
    with self._lock:
      cursor = self._conn.execute(
          "SELECT chat_ids FROM processed_users WHERE user_id = ?",
          (user_id,),
      )
      row = cursor.fetchone()
      return json.loads(row[0]) if row else None

  def save_processed_user(self, user_id: str, chat_ids: list[str]) -> None:
    """Guarantees serial update stability to nested string array indexes."""
    with self._lock:
      with self._conn:
        self._conn.execute(
            "INSERT OR REPLACE INTO processed_users (user_id, chat_ids) "
            "VALUES (?, ?)",
            (user_id, json.dumps(chat_ids)),
        )

  def get_processed_channel_membership(
      self, team_id: str, channel_id: str
  ) -> int | None:
    """Pulls secure scalar integer counter value."""
    with self._lock:
      cursor = self._conn.execute(
          "SELECT count FROM processed_channel_memberships "
          "WHERE team_id = ? AND channel_id = ?",
          (team_id, channel_id),
      )
      row = cursor.fetchone()
      return row[0] if row else None

  def save_processed_channel_membership(
      self, team_id: str, channel_id: str, count: int
  ) -> None:
    """Guarantees absolute counter insertion accuracy."""
    with self._lock:
      with self._conn:
        self._conn.execute(
            "INSERT OR REPLACE INTO processed_channel_memberships "
            "(team_id, channel_id, count) VALUES (?, ?, ?)",
            (team_id, channel_id, count),
        )

  def get_discovered_chat_count(self) -> int:
    """Returns the count of unique discovered chats using SQL JSON parsing."""
    with self._lock:
      cursor = self._conn.execute(
          "SELECT COUNT(DISTINCT value) "
          "FROM processed_users, json_each(processed_users.chat_ids)"
      )
      row = cursor.fetchone()
      return row[0] if row else 0

  def get_random_discovered_chat_sample(self, sample_size: int) -> list[str]:
    """Retrieves a randomized unique sample pool of discovered chat IDs."""
    with self._lock:
      cursor = self._conn.execute(
          "SELECT DISTINCT value "
          "FROM processed_users, json_each(processed_users.chat_ids) "
          "ORDER BY random() LIMIT ?",
          (sample_size,),
      )
      rows = cursor.fetchall()
      return [row[0] for row in rows]

