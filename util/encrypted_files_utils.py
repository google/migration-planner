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

"""Utilities for scanning sensitivity-labeled and encrypted files in Deep Scan.

Queries Microsoft Graph for sensitivity label definitions and uses SharePoint REST
Search (`POST /_api/search/postquery`) scoped to each Document Library / Subsite
with `Size` ascending (`Direction: 0`) + `DocId` ascending (`Direction: 0`) ordering.
Metrics are aggregated immediately as each page arrives and page rows are deleted
from memory without retaining `all_rows` or `seen_files`.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import random
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import unquote, urlparse

from util.constants import GRAPH_BASE_URL
from util.files_shallow.cert_token_manager import CertTokenManager

GRAPH_BETA_BASE_URL = "https://graph.microsoft.com/beta"
OLE2_RMS_MAGIC_HEADER = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"
ENCRYPTED_FILES_CONCURRENCY = 3
SEARCH_ROW_LIMIT = 500


def parse_int(val: Any) -> int:
  """Safely parses an integer from OData int or string representation."""
  try:
    if val is None or val == "":
      return 0
    return int(val)
  except (ValueError, TypeError):
    return 0


def extract_domain(web_url: str) -> str:
  """Extracts the network location (domain) from a SharePoint URL."""
  parsed = urlparse(web_url)
  if not parsed.netloc:
    raise ValueError(f"Invalid SharePoint URL: {web_url}")
  return parsed.netloc


def extract_postquery_rows(data: Dict[str, Any]) -> List[Dict[str, Any]]:
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


def fetch_tenant_sensitivity_labels(
    url_invoker: Any,
    logger: Callable[[str], None],
    stop_event: Optional[threading.Event] = None,
) -> Tuple[Set[str], Set[str]]:
  """Fetches tenant sensitivity labels and returns (all_label_ids, encrypted_label_ids)."""
  all_label_ids: Set[str] = set()
  encrypted_label_ids: Set[str] = set()

  def _collect(labels_list: List[Dict[str, Any]]) -> None:
    for label in labels_list:
      if not isinstance(label, dict):
        continue
      label_id = str(label.get("id") or "").strip().lower()
      if label_id:
        all_label_ids.add(label_id)
        if (
            label.get("hasProtection") is True
            or label.get("isEncrypted") is True
            or label.get("encryptionEnabled") is True
        ):
          encrypted_label_ids.add(label_id)
      sublabels = label.get("sublabels")
      if isinstance(sublabels, list) and sublabels:
        _collect(sublabels)

  endpoints = [
      f"{GRAPH_BASE_URL}/security/dataSecurityAndGovernance/sensitivityLabels?$select=id,name,hasProtection,sublabels",
      f"{GRAPH_BETA_BASE_URL}/security/informationProtection/sensitivityLabels",
  ]

  for endpoint in endpoints:
    if stop_event and stop_event.is_set():
      break
    next_url: Optional[str] = endpoint
    success = False
    while next_url and not (stop_event and stop_event.is_set()):
      status = 0
      body: Any = None
      if hasattr(url_invoker, "invoke_url") and callable(url_invoker.invoke_url):
        status, body = url_invoker.invoke_url(
            url=next_url,
            logger=logger,
            stop_event=stop_event,
            context="Fetch Sensitivity Labels",
        )
      elif hasattr(url_invoker, "token_manager") and url_invoker.token_manager is not None:
        token_data = url_invoker.token_manager.get_valid_token_slot(logger)
        session = url_invoker.token_manager.get_session()
        try:
          resp = session.get(
              next_url,
              headers={
                  "Authorization": f"Bearer {token_data['token']}",
                  "Content-Type": "application/json",
              },
              timeout=60,
          )
          status = resp.status_code
          if status == 200:
            body = resp.json()
        except Exception:
          status = 0
        finally:
          url_invoker.token_manager.return_token_slot(token_data)
      if status != 200 or not body:
        break
      try:
        data = json.loads(body) if isinstance(body, str) else body
        values = data.get("value", [])
        if isinstance(values, list):
          _collect(values)
          success = True
        next_url = data.get("@odata.nextLink")
      except Exception:
        break
    if success and all_label_ids:
      break

  return all_label_ids, encrypted_label_ids


class EncryptedFilesSearchRunner:
  """Executes per-Document-Library SharePoint REST Search queries with streaming aggregation."""

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
    self.label_protection_lock = threading.Lock()
    self.label_protection_cache: Dict[str, bool] = {}

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
            if stop_event and stop_event.wait(timeout=sleep_sec):
              break

        headers = {
            "Authorization": f"Bearer {token_data['token']}",
            "Accept": "application/json;odata=nometadata",
            "Content-Type": "application/json;odata=verbose",
        }

        try:
          resp = session.post(endpoint, headers=headers, json=payload, timeout=30.0)
        except Exception as req_err:
          if current_try >= self.max_retries:
            raise RuntimeError(
                f"SharePoint Search POST network failure for {base_url}: {req_err}"
            ) from req_err
          wait_time = (self.backoff ** current_try) + random.uniform(0, 1)
          if stop_event and stop_event.wait(timeout=wait_time):
            break
          continue

        if resp.status_code == 200:
          try:
            return resp.json()
          except Exception:
            return {}

        if resp.status_code == 401 and current_try < self.max_retries:
          self.cert_token_manager.refresh_token_slot(domain, token_data, logger)
          continue

        if resp.status_code in (429, 503):
          retry_after = resp.headers.get("Retry-After")
          wait_time = (
              float(retry_after)
              if retry_after and str(retry_after).isdigit()
              else (self.backoff ** current_try) + random.uniform(0, 1)
          )
          with self.lock:
            self.throttle_until = max(self.throttle_until, time.time() + wait_time)
          if current_try >= self.max_retries:
            raise RuntimeError(
                f"SharePoint Search throttled ({resp.status_code}) for {base_url}"
            )
          if stop_event and stop_event.wait(timeout=wait_time):
            break
          continue

        if resp.status_code >= 500:
          if current_try >= self.max_retries:
            raise RuntimeError(
                f"SharePoint Search server error ({resp.status_code}) for {base_url}"
            )
          wait_time = (self.backoff ** current_try) + random.uniform(0, 1)
          if stop_event and stop_event.wait(timeout=wait_time):
            break
          continue

        return {}
    finally:
      self.cert_token_manager.return_token_slot(domain, token_data)

    return {}

  def _is_file_ole2_encrypted(
      self,
      web_base_url: str,
      domain: str,
      file_url: str,
      logger: Callable[[str], None],
      stop_event: Optional[threading.Event] = None,
  ) -> bool:
    """Checks the first 8 bytes of a file via SharePoint REST to detect OLE2/RMS encryption."""
    if stop_event and stop_event.is_set():
      return False
    parsed = urlparse(file_url)
    rel_path = unquote(parsed.path)
    if not rel_path:
      return False
    escaped_rel = rel_path.replace("'", "''")
    endpoint = (
        f"{web_base_url.rstrip('/')}/_api/web/"
        f"GetFileByServerRelativePath(decodedurl='{escaped_rel}')/$value"
    )
    token_data = self.cert_token_manager.get_valid_token_slot(domain, logger)
    session = self.cert_token_manager.get_session()
    try:
      headers = {
          "Authorization": f"Bearer {token_data['token']}",
          "Range": "bytes=0-7",
      }
      resp = session.get(endpoint, headers=headers, timeout=20.0, stream=True)
      if resp.status_code in (200, 206):
        header_bytes = resp.raw.read(8)
        resp.close()
        return header_bytes == OLE2_RMS_MAGIC_HEADER
      resp.close()
    except Exception:
      pass
    finally:
      self.cert_token_manager.return_token_slot(domain, token_data)
    return False

  def _check_label_encrypted(
      self,
      label_id: str,
      has_graph_labels: bool,
      known_encrypted_label_ids: Set[str],
      known_unencrypted_label_ids: Set[str],
      web_base_url: str,
      domain: str,
      sample_file_url: str,
      logger: Callable[[str], None],
      stop_event: Optional[threading.Event] = None,
  ) -> bool:
    """Determines if a sensitivity label GUID has encryption enabled."""
    if label_id in known_encrypted_label_ids:
      return True
    if has_graph_labels and label_id in known_unencrypted_label_ids:
      return False

    with self.label_protection_lock:
      if label_id in self.label_protection_cache:
        return self.label_protection_cache[label_id]

    is_enc = False
    if sample_file_url:
      is_enc = self._is_file_ole2_encrypted(
          web_base_url, domain, sample_file_url, logger, stop_event
      )

    with self.label_protection_lock:
      self.label_protection_cache[label_id] = is_enc
    return is_enc

  def scan_single_library(
      self,
      drive_id: str,
      web_base_url: str,
      library_url: str,
      has_graph_labels: bool,
      known_encrypted_label_ids: Set[str],
      known_unencrypted_label_ids: Set[str],
      on_page_metrics: Callable[[str, int, int, int, Set[str], Set[str]], None],
      logger: Callable[[str], None],
      stop_event: Optional[threading.Event] = None,
  ) -> None:
    """Scans a single Document Library for labeled/encrypted files in ascending Size order.

    Aggregates metrics as soon as each page arrives via `on_page_metrics` and
    immediately deletes the page rows from memory (`O(1)` memory, no `seen_files`).
    """
    if not library_url or not web_base_url:
      return

    clean_lib_url = library_url.rstrip("/")
    clean_web_url = web_base_url.rstrip("/")
    domain = extract_domain(clean_web_url)
    endpoint = f"{clean_web_url}/_api/search/postquery"

    hex_clause = " OR ".join(
        f"InformationProtectionLabelId:{c}*" for c in "0123456789abcdef"
    )
    base_kql = f'({hex_clause}) AND Path:"{clean_lib_url}" AND IsDocument:true'
    select_props = [
        "DocId",
        "Size",
        "Path",
        "OriginalPath",
        "InformationProtectionLabelId",
    ]

    last_size: Optional[int] = None
    last_doc_id: Optional[int] = None

    while not (stop_event and stop_event.is_set()):
      if last_size is not None and last_doc_id is not None:
        query_text = (
            f"{base_kql} AND (Size>{last_size} OR "
            f"(Size={last_size} AND IndexDocId>{last_doc_id}))"
        )
      else:
        query_text = base_kql

      payload = {
          "request": {
              "__metadata": {
                  "type": "Microsoft.Office.Server.Search.REST.SearchRequest"
              },
              "Querytext": query_text,
              "RowLimit": SEARCH_ROW_LIMIT,
              "StartRow": 0,
              "TrimDuplicates": False,
              "SelectProperties": {"results": select_props},
              "SortList": {
                  "results": [
                      {"Property": "Size", "Direction": 0},
                      {"Property": "DocId", "Direction": 0},
                  ]
              },
          }
      }

      resp_data = self._execute_post(
          endpoint, payload, clean_web_url, domain, logger, stop_event
      )
      rows = extract_postquery_rows(resp_data)
      del resp_data

      if not rows:
        break

      page_row_count = len(rows)
      page_labeled_count = 0
      page_encrypted_count = 0
      page_encrypted_size = 0
      page_all_labels: Set[str] = set()
      page_encrypted_labels: Set[str] = set()

      for row in rows:
        file_size = parse_int(row.get("Size"))
        doc_id_val = parse_int(row.get("DocId"))
        last_size = file_size
        if doc_id_val > 0:
          last_doc_id = doc_id_val

        raw_lid = str(row.get("InformationProtectionLabelId") or "").strip().lower()
        if not raw_lid:
          continue

        page_labeled_count += 1
        page_all_labels.add(raw_lid)

        sample_path = str(row.get("OriginalPath") or row.get("Path") or "")
        is_enc = self._check_label_encrypted(
            label_id=raw_lid,
            has_graph_labels=has_graph_labels,
            known_encrypted_label_ids=known_encrypted_label_ids,
            known_unencrypted_label_ids=known_unencrypted_label_ids,
            web_base_url=clean_web_url,
            domain=domain,
            sample_file_url=sample_path,
            logger=logger,
            stop_event=stop_event,
        )
        if is_enc:
          page_encrypted_count += 1
          page_encrypted_size += file_size
          page_encrypted_labels.add(raw_lid)

      # Immediately update metrics for this page and delete rows from memory
      on_page_metrics(
          drive_id,
          page_labeled_count,
          page_encrypted_count,
          page_encrypted_size,
          page_all_labels,
          page_encrypted_labels,
      )
      del rows

      if page_row_count < SEARCH_ROW_LIMIT or last_doc_id is None:
        break


def scan_libraries_for_encrypted_files(
    cert_token_manager: CertTokenManager,
    url_invoker: Any,
    library_targets: List[Tuple[str, str, str]],
    on_page_metrics: Callable[[str, int, int, int, Set[str], Set[str]], None],
    on_tenant_labels_discovered: Callable[[Set[str], Set[str]], None],
    logger: Callable[[str], None],
    stop_event: Optional[threading.Event] = None,
    max_retries: int = 5,
    backoff: int = 2,
    concurrency: int = ENCRYPTED_FILES_CONCURRENCY,
) -> None:
  """Runs the post-Deep-Scan encrypted & sensitivity-labeled files phase with concurrency=3.

  Args:
    cert_token_manager: Certificate token manager for SharePoint REST Search.
    url_invoker: Graph URL invoker for fetching sensitivity label definitions.
    library_targets: List of `(drive_id, web_base_url, library_url)` tuples.
    on_page_metrics: Callback invoked immediately as each page arrives.
    on_tenant_labels_discovered: Callback invoked with `(all_label_ids, encrypted_label_ids)`.
    logger: Logging callback.
    stop_event: Threading stop event.
    max_retries: Max retry count per SharePoint REST call.
    backoff: Exponential backoff base.
    concurrency: Number of concurrent Document Library workers (default 3).
  """
  if not library_targets or (stop_event and stop_event.is_set()):
    return

  all_label_ids, encrypted_label_ids = fetch_tenant_sensitivity_labels(
      url_invoker, logger, stop_event
  )
  has_graph_labels = bool(all_label_ids)
  known_unencrypted_ids = all_label_ids - encrypted_label_ids
  on_tenant_labels_discovered(all_label_ids, encrypted_label_ids)

  if cert_token_manager is None:
    return

  runner = EncryptedFilesSearchRunner(
      cert_token_manager=cert_token_manager,
      max_retries=max_retries,
      backoff=backoff,
  )

  worker_count = max(1, min(concurrency, len(library_targets)))
  executor = ThreadPoolExecutor(max_workers=worker_count)
  try:
    futures = [
        executor.submit(
            runner.scan_single_library,
            drive_id,
            web_base_url,
            library_url,
            has_graph_labels,
            encrypted_label_ids,
            known_unencrypted_ids,
            on_page_metrics,
            logger,
            stop_event,
        )
        for drive_id, web_base_url, library_url in library_targets
    ]
    for fut in as_completed(futures):
      if stop_event and stop_event.is_set():
        break
      try:
        fut.result()
      except Exception as exc:
        if logger:
          logger(f"Warning: encrypted files library scan error: {exc}")
  finally:
    executor.shutdown(wait=True)
