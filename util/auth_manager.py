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

"""Decoupled OAuth2 credential rotation and token validation handlers."""

import base64
import json
import logging
import queue
import threading
import time
import types
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from util.constants import TOKEN_URL_TEMPLATE


logger = logging.getLogger(__name__)


class TokenManager:
  """Manages Microsoft Graph API tokens with rotation and concurrency logic."""

  tenant_id: str
  apps: list[tuple[str, str]]
  concurrency: int
  retries: int
  backoff: int
  token_queue: queue.Queue
  session: requests.Session
  tokens: list[dict[str, Any]]
  _client_secrets: dict[str, str]
  _refresh_lock: threading.Lock

  def __init__(
      self,
      tenant_id: str,
      client_ids: list[str],
      client_secrets: list[str],
      concurrency: int,
      retries: int,
      backoff: int,
  ) -> None:
    self.tenant_id = tenant_id
    self.apps = list(zip(client_ids, client_secrets))
    self._client_secrets = dict(self.apps)
    self.concurrency = concurrency
    self.retries = retries
    self.backoff = backoff
    self.token_queue = queue.Queue()
    self.tokens = []
    self._refresh_lock = threading.Lock()
    self.session = self._create_retry_session()

  def close(self) -> None:
    """Securely release session pool resources."""
    self.session.close()

  def __enter__(self) -> "TokenManager":
    """Safe execution environment initialization."""
    return self

  def __exit__(
      self,
      _exc_type: type[BaseException] | None,  # pylint: disable=invalid-name
      _exception: BaseException | None,  # pylint: disable=invalid-name
      _traceback: types.TracebackType | None,  # pylint: disable=invalid-name
  ) -> None:
    """Automatic resource cleanup handler."""
    self.close()

  def _create_retry_session(self) -> requests.Session:
    """Creates an encapsulated requests session with resilient retry logic."""
    session = requests.Session()
    retries = Retry(
        total=self.retries,
        backoff_factor=self.backoff,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    total_pool_size = len(self.apps) * self.concurrency * 2 + 100
    adapter = HTTPAdapter(
        max_retries=retries,
        pool_connections=total_pool_size,
        pool_maxsize=total_pool_size,
    )
    session.mount("https://", adapter)

    return session

  def authenticate_all(self, required_scopes: list[str] | None = None) -> None:
    """Authenticates all configured applications and validates scopes."""
    logger.info("Authenticating %d applications...", len(self.apps))
    url = TOKEN_URL_TEMPLATE.format(self.tenant_id)
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    for client_id, client_secret in self.apps:
      data = {
          "client_id": client_id,
          "scope": "https://graph.microsoft.com/.default",
          "client_secret": client_secret,
          "grant_type": "client_credentials",
      }
      try:
        # Hardened with explicit timeout.
        response = self.session.post(
            url, headers=headers, data=data, timeout=30.0
        )
        response.raise_for_status()

        token_response = response.json()
        token = token_response["access_token"]
        expires_in = token_response.get("expires_in", 3599)

        token_data = {
            "token": token,
            "expires_at": time.time() + int(expires_in) - 900,
            "client_id": client_id,
            # Critical: "client_secret" stripped to prevent exposure.
        }

        if required_scopes:
          try:
            payload_part = token.split(".")[1]
            payload_part += "=" * (-len(payload_part) % 4)
            decoded_bytes = base64.urlsafe_b64decode(payload_part)
            payload = json.loads(decoded_bytes)

            granted_roles = set(payload.get("roles", []))
            missing = [
                scope for scope in required_scopes if scope not in granted_roles
            ]
            if missing:
              raise ValueError(
                  f"Missing required permissions for App {client_id[:5]}: "
                  f"{', '.join(missing)}"
              )
          except Exception as error:
            logger.error("Token verification failed: %s", error)
            raise

        self.tokens.append(token_data)
        for _ in range(self.concurrency):
          self.token_queue.put(token_data)
        logger.info("App %s... authenticated and verified.", client_id[:5])

      except requests.exceptions.RequestException as error:
        error_text = ""
        if error.response is not None:
          error_text = f": {error.response.text}"
        logger.error(
            "Auth failed for app %s...: %s%s", client_id[:5], error, error_text
        )
        raise ConnectionError(
            f"Authentication failed. Verify credentials. Details: {error_text}"
        ) from error

  def refresh_token_data(self, token_data: dict[str, Any]) -> bool:
    """Attempts explicit inline re-acquisition using isolated private secrets."""
    client_id = token_data["client_id"]
    secret = self._client_secrets.get(client_id)
    if not secret:
      logger.error("Attempted refresh without internal stored secret.")
      return False

    url = TOKEN_URL_TEMPLATE.format(self.tenant_id)
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "client_id": client_id,
        "scope": "https://graph.microsoft.com/.default",
        "client_secret": secret,
        "grant_type": "client_credentials",
    }
    try:
      # Hardened with explicit timeout.
      response = self.session.post(
          url, headers=headers, data=data, timeout=30.0
      )
      response.raise_for_status()
      token_response = response.json()

      token_data["token"] = token_response["access_token"]
      expires_in = token_response.get("expires_in", 3599)
      token_data["expires_at"] = time.time() + int(expires_in) - 900
      return True
    except Exception as error:
      logger.error("Failed to refresh token: %s", error)
      return False

  def get_valid_token_slot(self) -> dict[str, Any]:
    """Yields ready-to-use token, preventing herd stampedes on concurrent expiry."""
    token_data = self.token_queue.get()

    # Critical check for herd mitigation using double-check serialization.
    if time.time() > token_data["expires_at"]:
      with self._refresh_lock:
        # Re-evaluate inside lock to detect if another thread just refreshed it.
        if time.time() > token_data["expires_at"]:
          logger.warning(
              "Token expiring for App %s... triggering serial refresh.",
              token_data["client_id"][:5],
          )
          if self.refresh_token_data(token_data):
            logger.info(
                "Successfully refreshed token for App %s...",
                token_data["client_id"][:5],
            )
    return token_data

  def return_token_slot(self, token_data: dict[str, Any]) -> None:
    """Releases checkout lease, restoring pool availability."""
    self.token_queue.put(token_data)

  def get_session(self) -> requests.Session:
    """Access method for utilizing unified connection pool."""
    return self.session
