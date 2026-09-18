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
from typing import Any, Callable, Dict, List, Optional, Tuple
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

  def _get_escaped_documents_path(self, web_url: str) -> Tuple[str, str]:
    """Returns (base_url, escaped_documents_rel_path) for SharePoint REST calls."""
    base_url = web_url.rstrip("/")
    server_rel_path = unquote(urlparse(base_url).path.rstrip("/"))
    escaped_rel_path = server_rel_path.replace("'", "''")
    return base_url, f"{escaped_rel_path}/Documents"

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
      web_url: str,
      logger: Optional[Callable[[str], None]] = None,
      stop_event: Optional[threading.Event] = None,
  ) -> int:
    """Fetches recursive Documents library item count (files + folders) for a OneDrive site.

    Endpoint:
      GET {base_url}/_api/web/GetList('{escaped_rel_path}/Documents')?$select=ItemCount
    """
    if logger is None:
      logger = lambda x: None

    domain = self._extract_domain(web_url)
    base_url, documents_rel_path = self._get_escaped_documents_path(web_url)
    endpoint = (
        f"{base_url}/_api/web/GetList('{documents_rel_path}')"
        "?$select=ItemCount"
    )
    data = self._execute_get(endpoint, base_url, domain, logger, stop_event)
    return self._parse_int(data.get("ItemCount"))

  def get_storage_metrics(
      self,
      web_url: str,
      logger: Optional[Callable[[str], None]] = None,
      stop_event: Optional[threading.Event] = None,
  ) -> Dict[str, int]:
    """Fetches recursive StorageMetrics from the OneDrive Documents root folder.

    Endpoint:
      GET {base_url}/_api/web/GetFolderByServerRelativeUrl('{escaped_rel_path}/Documents')?$select=StorageMetrics&$expand=StorageMetrics

    Returns:
      Dict with keys:
        - file_count: TotalFileCount (recursive files count in /Documents)
        - active_size_bytes: TotalFileStreamSize (current versions only)
        - total_size_bytes: TotalSize (including version history and metadata)
    """
    if logger is None:
      logger = lambda x: None

    domain = self._extract_domain(web_url)
    base_url, documents_rel_path = self._get_escaped_documents_path(web_url)
    endpoint = (
        f"{base_url}/_api/web/GetFolderByServerRelativeUrl('{documents_rel_path}')"
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

  def get_onedrive_metrics(
      self,
      web_url: str,
      logger: Optional[Callable[[str], None]] = None,
      stop_event: Optional[threading.Event] = None,
  ) -> Dict[str, int]:
    """Fetches recursive ItemCount and StorageMetrics for a OneDrive site using two direct GET calls.

    Executes sequentially per OneDrive over the persistent HTTP Keep-Alive session:
      1. GET .../_api/web/GetList('.../Documents')?$select=ItemCount
      2. GET .../_api/web/GetFolderByServerRelativeUrl('.../Documents')?$select=StorageMetrics&$expand=StorageMetrics

    Returns:
      Dict containing:
        - item_count: total recursive items (files + folders) in library
        - file_count: total recursive files in site
        - folder_count: max(0, item_count - file_count)
        - active_size_bytes: active file stream size in bytes (TotalFileStreamSize)
        - total_size_bytes: total storage size including versions (TotalSize)
    """
    if logger is None:
      logger = lambda x: None

    item_count = self.get_item_counts(web_url, logger, stop_event)
    storage = self.get_storage_metrics(web_url, logger, stop_event)

    file_count = storage["file_count"]
    active_size_bytes = storage["active_size_bytes"]
    total_size_bytes = storage["total_size_bytes"]
    folder_count = max(0, item_count - file_count)

    return {
        "item_count": item_count,
        "file_count": file_count,
        "folder_count": folder_count,
        "active_size_bytes": active_size_bytes,
        "total_size_bytes": total_size_bytes,
    }

  def _execute_post(
      self,
      endpoint: str,
      payload: Dict[str, Any],
      base_url: str,
      domain: str,
      logger: Callable[[str], None],
      stop_event: Optional[threading.Event] = None,
  ) -> Dict[str, Any]:
    """Executes an authenticated SharePoint REST POST request with retry/backoff."""
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
            "Content-Type": "application/json;odata=verbose",
        }

        try:
          resp = session.post(
              endpoint, json=payload, headers=headers, timeout=60.0
          )
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
              f"SharePoint REST POST request failed for {base_url} with HTTP "
              f"{resp.status_code}: {resp.text}"
          )
    finally:
      self.cert_token_manager.return_token_slot(domain, token_data)

    return {}

  @staticmethod
  def _extract_postquery_rows(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extracts search result rows as key-value dicts from a postquery response."""
    if not isinstance(data, dict):
      return []

    postquery = data.get("d", {}).get("postquery", data)
    if not isinstance(postquery, dict):
      return []

    primary = postquery.get("PrimaryQueryResult", {})
    if not isinstance(primary, dict):
      return []

    relevant = primary.get("RelevantResults", {})
    if not isinstance(relevant, dict):
      return []

    table = relevant.get("Table", {})
    if not isinstance(table, dict):
      return []

    raw_rows = table.get("Rows", [])
    if isinstance(raw_rows, dict):
      raw_rows = raw_rows.get("results", [])
    if not isinstance(raw_rows, list):
      return []

    parsed_rows: List[Dict[str, Any]] = []
    for row in raw_rows:
      if not isinstance(row, dict):
        continue
      raw_cells = row.get("Cells", [])
      if isinstance(raw_cells, dict):
        raw_cells = raw_cells.get("results", [])
      if not isinstance(raw_cells, list):
        continue
      row_dict: Dict[str, Any] = {}
      for cell in raw_cells:
        if isinstance(cell, dict) and "Key" in cell:
          row_dict[str(cell["Key"])] = cell.get("Value")
      if row_dict:
        parsed_rows.append(row_dict)

    return parsed_rows

  def search_encrypted_files_by_labels(
      self,
      base_url: str,
      encrypted_label_ids: List[str],
      path_prefixes: Optional[List[str]] = None,
      logger: Optional[Callable[[str], None]] = None,
      stop_event: Optional[threading.Event] = None,
  ) -> List[Dict[str, Any]]:
    """Queries SharePoint REST `postquery` API for encrypted files using `IndexDocId` cursor pagination.

    Uses `POST {base_url}/_api/search/postquery` sorted by `[DocId]:ascending`
    with `RowLimit=500` and `IndexDocId > {Last_DocId}` cursor boundaries to
    retrieve matching labeled files without hitting standard `StartRow` offset
    limits or Microsoft Graph Search's 1,000-item ceiling.
    """
    if logger is None:
      logger = lambda x: None

    if not encrypted_label_ids:
      return []

    domain = self._extract_domain(base_url)
    clean_base_url = base_url.rstrip("/")
    endpoint = f"{clean_base_url}/_api/search/postquery"

    label_clause = " OR ".join(
        f'InformationProtectionLabelId:"{lid}"'
        for lid in sorted(encrypted_label_ids)
    )
    if path_prefixes:
      cleaned_prefixes = [
          p.rstrip("/") for p in path_prefixes if p and p.startswith("http")
      ]
      if cleaned_prefixes:
        path_clause = " OR ".join(f'Path:"{p}*"' for p in cleaned_prefixes)
        base_kql = f"({label_clause}) AND ({path_clause}) AND IsDocument:true"
      else:
        base_kql = f"({label_clause}) AND IsDocument:true"
    else:
      base_kql = f"({label_clause}) AND IsDocument:true"

    select_props = [
        "DocId",
        "Size",
        "Path",
        "OriginalPath",
        "ParentLink",
        "SPWebUrl",
        "SiteId",
        "WebId",
        "ListId",
        "UniqueId",
        "InformationProtectionLabelId",
    ]

    all_rows: List[Dict[str, Any]] = []
    last_doc_id: Optional[int] = None
    start_row = 0
    row_limit = 500

    while not (stop_event and stop_event.is_set()):
      if last_doc_id is not None:
        query_text = f"{base_kql} AND IndexDocId>{last_doc_id}"
        current_start_row = 0
      else:
        query_text = base_kql
        current_start_row = start_row

      payload = {
          "request": {
              "__metadata": {
                  "type": "Microsoft.Office.Server.Search.REST.SearchRequest"
              },
              "Querytext": query_text,
              "RowLimit": row_limit,
              "StartRow": current_start_row,
              "TrimDuplicates": False,
              "SelectProperties": {"results": select_props},
              "SortList": {
                  "results": [{"Property": "DocId", "Direction": "0"}]
              },
          }
      }

      resp_data = self._execute_post(
          endpoint, payload, clean_base_url, domain, logger, stop_event
      )
      rows = self._extract_postquery_rows(resp_data)
      if not rows:
        break

      all_rows.extend(rows)

      max_batch_doc_id: Optional[int] = None
      for row in rows:
        doc_id_val = self._parse_int(row.get("DocId"))
        if doc_id_val > 0 and (
            max_batch_doc_id is None or doc_id_val > max_batch_doc_id
        ):
          max_batch_doc_id = doc_id_val

      if len(rows) < row_limit:
        break

      if max_batch_doc_id is not None and (
          last_doc_id is None or max_batch_doc_id > last_doc_id
      ):
        last_doc_id = max_batch_doc_id
      else:
        start_row += len(rows)
        if start_row >= 50000:
          break

    return all_rows

