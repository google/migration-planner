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

"""SharePoint REST connector for Shallow Scan StorageMetrics and ItemCount queries."""

import random
import threading
import time
from typing import Any, Callable, Dict, Optional, Tuple
from urllib.parse import unquote, urlparse

from util.files_shallow.cert_token_manager import CertTokenManager


class SpRestConnector:
  """Invokes SharePoint REST APIs with certificate auth and throttling retry."""

  def __init__(
      self,
      cert_token_manager: CertTokenManager,
      max_retries: int = 5,
      backoff: int = 2,
  ) -> None:
    self.cert_token_manager = cert_token_manager
    self.max_retries = min(5, max(1, max_retries))
    self.backoff = backoff
    self.throttle_until = 0.0
    self.lock = threading.Lock()

  @staticmethod
  def _parse_int(val: Any) -> int:
    """Safely parses an integer from OData int or string representation."""
    try:
      if val is None or val == "":
        return 0
      return int(val)
    except (ValueError, TypeError):
      return 0

  def _extract_domain(self, web_url: str) -> str:
    parsed = urlparse(web_url)
    if not parsed.netloc:
      raise ValueError(f"Invalid SharePoint URL: {web_url}")
    return parsed.netloc

  def _split_library_endpoint(
      self, web_base_url: str, library_url: str
  ) -> Tuple[str, str, str]:
    """Resolves REST addressing components for a single document library.

    The `_api` endpoint must be rooted at the web that owns the library, while
    the library itself is addressed by its decoded server-relative path. This
    holds for OneDrive (`/personal/<upn>/Documents`) as well as SharePoint
    libraries on site collections and subsites
    (`/sites/<site>/<subsite>/Shared Documents`).

    Args:
      web_base_url: Absolute URL of the web owning the library.
      library_url: Absolute URL of the document library.

    Returns:
      Tuple of (base_url, escaped_library_rel_path, domain).
    """
    base_url = web_base_url.rstrip("/")
    domain = self._extract_domain(base_url)
    library_rel_path = unquote(urlparse(library_url.rstrip("/")).path)
    if not library_rel_path:
      raise ValueError(f"Invalid document library URL: {library_url}")
    return base_url, library_rel_path.replace("'", "''"), domain

  def _execute_get(
      self,
      endpoint: str,
      base_url: str,
      domain: str,
      logger: Callable[[str], None],
      stop_event: Optional[threading.Event] = None,
  ) -> Dict[str, Any]:
    """Executes an authenticated SharePoint REST GET request with retry/backoff."""
    token_data = self.cert_token_manager.get_valid_token_slot(domain, logger)
    session = self.cert_token_manager.get_session()

    try:
      current_try = 0
      while current_try < self.max_retries:
        if stop_event and stop_event.is_set():
          break

        current_try += 1

        with self.lock:
          now = time.time()
          if now < self.throttle_until:
            sleep_sec = self.throttle_until - now
            logger(
                f"SharePoint REST throttling active. Waiting {sleep_sec:.1f}s..."
            )
            if stop_event and stop_event.wait(timeout=sleep_sec):
              break

        headers = {
            "Authorization": f"Bearer {token_data['token']}",
            "Accept": "application/json;odata=nometadata",
        }

        try:
          resp = session.get(endpoint, headers=headers, timeout=30.0)
        except Exception as req_err:
          if current_try >= self.max_retries:
            raise
          wait_sec = min(float(self.backoff ** (current_try - 1)), 8.0)
          logger(
              f"Transient network error querying {base_url} ({req_err}). "
              f"Retry {current_try}/{self.max_retries} in {wait_sec:.1f}s..."
          )
          if stop_event and stop_event.wait(timeout=wait_sec):
            break
          continue

        if resp.status_code == 200:
          return resp.json()

        elif resp.status_code == 404:
          return {}

        elif resp.status_code == 403:
          raise PermissionError(
              f"Access to site {base_url} failed with HTTP 403 Forbidden: {resp.text}"
          )

        elif resp.status_code == 429:
          if current_try >= self.max_retries:
            raise RuntimeError(
                f"SharePoint REST 429 throttled on {base_url} after {self.max_retries} attempts: {resp.text}"
            )
          try:
            wait_sec = int(float(resp.headers.get("Retry-After", 5)))
          except (ValueError, TypeError):
            wait_sec = 5 * current_try
          wait_sec = min(float(wait_sec), 30.0) + random.uniform(0.1, 1.0)
          with self.lock:
            self.throttle_until = max(
                self.throttle_until, time.time() + wait_sec
            )
          logger(
              f"SharePoint REST 429 on {base_url}. Retrying in {wait_sec:.1f}s..."
          )
          if stop_event and stop_event.wait(timeout=wait_sec):
            break
          continue

        elif resp.status_code == 401:
          if current_try >= self.max_retries:
            raise PermissionError(
                f"SharePoint REST 401 Unauthorized on {base_url} after {self.max_retries} attempts: {resp.text}"
            )
          logger(f"SharePoint REST 401 on {base_url}. Refreshing cert token...")
          self.cert_token_manager.refresh_token_data(token_data, logger)
          time.sleep(1.0)
          continue

        elif resp.status_code in [500, 502, 503, 504]:
          if current_try >= self.max_retries:
            raise RuntimeError(
                f"SharePoint REST server error HTTP {resp.status_code} on {base_url} "
                f"after {self.max_retries} attempts: {resp.text}"
            )
          wait_sec = min(float(self.backoff ** (current_try - 1)), 8.0)
          logger(
              f"Transient server error (HTTP {resp.status_code}) on {base_url}. "
              f"Retry {current_try}/{self.max_retries} in {wait_sec:.1f}s..."
          )
          if stop_event and stop_event.wait(timeout=wait_sec):
            break
          continue

        else:
          raise RuntimeError(
              f"SharePoint REST request failed for {base_url} with HTTP "
              f"{resp.status_code}: {resp.text}"
          )
    finally:
      self.cert_token_manager.return_token_slot(domain, token_data)

    return {}

  def get_item_counts(
      self,
      web_base_url: str,
      library_url: str,
      logger: Optional[Callable[[str], None]] = None,
      stop_event: Optional[threading.Event] = None,
  ) -> int:
    """Fetches the recursive item count (files + folders) for a document library.

    Endpoint:
      GET {base_url}/_api/web/GetList('{library_rel_path}')?$select=ItemCount
    """
    if logger is None:
      logger = lambda x: None

    base_url, library_rel_path, domain = self._split_library_endpoint(
        web_base_url, library_url
    )
    endpoint = (
        f"{base_url}/_api/web/GetList('{library_rel_path}')?$select=ItemCount"
    )
    data = self._execute_get(endpoint, base_url, domain, logger, stop_event)
    return self._parse_int(data.get("ItemCount"))

  def get_storage_metrics(
      self,
      web_base_url: str,
      library_url: str,
      logger: Optional[Callable[[str], None]] = None,
      stop_event: Optional[threading.Event] = None,
  ) -> Dict[str, int]:
    """Fetches recursive StorageMetrics from a document library's root folder.

    Endpoint:
      GET {base_url}/_api/web/GetFolderByServerRelativeUrl('{library_rel_path}')?$select=StorageMetrics&$expand=StorageMetrics

    Returns:
      Dict with keys:
        - file_count: TotalFileCount (recursive files count in the library)
        - active_size_bytes: TotalFileStreamSize (current versions only)
        - total_size_bytes: TotalSize (including version history and metadata)
    """
    if logger is None:
      logger = lambda x: None

    base_url, library_rel_path, domain = self._split_library_endpoint(
        web_base_url, library_url
    )
    endpoint = (
        f"{base_url}/_api/web/GetFolderByServerRelativeUrl('{library_rel_path}')"
        "?$select=StorageMetrics&$expand=StorageMetrics"
    )
    data = self._execute_get(endpoint, base_url, domain, logger, stop_event)
    storage_metrics = data.get("StorageMetrics")
    if not isinstance(storage_metrics, dict):
      storage_metrics = data if isinstance(data, dict) else {}

    return {
        "file_count": self._parse_int(storage_metrics.get("TotalFileCount")),
        "active_size_bytes": self._parse_int(
            storage_metrics.get("TotalFileStreamSize")
        ),
        "total_size_bytes": self._parse_int(storage_metrics.get("TotalSize")),
    }

  def get_library_metrics(
      self,
      web_base_url: str,
      library_url: str,
      logger: Optional[Callable[[str], None]] = None,
      stop_event: Optional[threading.Event] = None,
  ) -> Dict[str, int]:
    """Fetches recursive ItemCount and StorageMetrics for one document library.

    Executes sequentially per library over the persistent HTTP Keep-Alive
    session:
      1. GET .../_api/web/GetList('{library_rel_path}')?$select=ItemCount
      2. GET .../_api/web/GetFolderByServerRelativeUrl('{library_rel_path}')?$select=StorageMetrics&$expand=StorageMetrics

    Both calls must succeed; any failure propagates so the caller can record the
    library as failed rather than reporting partial counts.

    Returns:
      Dict containing:
        - item_count: total recursive items (files + folders) in the library
        - file_count: total recursive files in the library
        - folder_count: max(0, item_count - file_count)
        - active_size_bytes: active file stream size in bytes (TotalFileStreamSize)
        - total_size_bytes: total storage size including versions (TotalSize)
    """
    if logger is None:
      logger = lambda x: None

    item_count = self.get_item_counts(
        web_base_url, library_url, logger, stop_event
    )
    storage = self.get_storage_metrics(
        web_base_url, library_url, logger, stop_event
    )

    file_count = storage["file_count"]
    return {
        "item_count": item_count,
        "file_count": file_count,
        "folder_count": max(0, item_count - file_count),
        "active_size_bytes": storage["active_size_bytes"],
        "total_size_bytes": storage["total_size_bytes"],
    }
