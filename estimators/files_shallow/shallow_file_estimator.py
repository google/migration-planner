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

"""Shallow Scan estimator for OneDrive and SharePoint files."""

from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from estimators.file_estimator import FileEstimator
from util.connectors import UrlInvoker
from util.enums import FailureType, ResourceType
from util.files_shallow.cert_token_manager import CertTokenManager
from util.files_shallow.sp_rest_connector import SpRestConnector
from util.utils import ScanConfig


class ShallowFileEstimator(FileEstimator):
  """Estimates files corpus metrics using SharePoint REST StorageMetrics & ItemCount APIs."""

  def __init__(
      self,
      config: ScanConfig,
      url_invoker: UrlInvoker,
      cert_token_manager: CertTokenManager,
      logger: Optional[Callable[[str], None]] = None,
      stop_event: Optional[threading.Event] = None,
      progress_update_callback: Optional[Callable[..., None]] = None,
  ) -> None:
    if logger is None:
      logger = lambda *args, **kwargs: None
    if progress_update_callback is None:
      progress_update_callback = lambda *args, **kwargs: None
    super().__init__(
        config=config,
        url_invoker=url_invoker,
        logger=logger,
        stop_event=stop_event,
        progress_update_callback=progress_update_callback,
    )
    self.cert_token_manager = cert_token_manager

  def _initialize_metrics_structure(self) -> Dict[str, Any]:
    """Creates the initial metrics dictionary structure for Shallow Scan."""
    return {
        "isShallowScan": True,
        "driveMetrics": {},
        "siteMetrics": {},
        "personalSiteCount": 0,
        "teamSiteCount": 0,
        "maxEffectiveDepth": "N/A",
        "maxFolderDepth": "N/A",
        "maxSubsiteDepth": 0,
        "subsiteCount": 0,
        "shortcutCount": "N/A",
        "folderCount": 0,
        "fileCount": 0,
        "folderCountExceedingDepthLimit": "N/A",
        "fileCountExceedingDepthLimit": "N/A",
        "listCount": 0,
        "licenseMetrics": {},
        "driveCounts": {
            "documentLibrary": 0,
            "personal": 0,
            "business": 0,
        },
        "personalSiteDLCount": 0,
        "teamSiteDLCount": 0,
        "tenantLevelFileSizeDistribution": {"buckets": []},
        "tenantLevelLargeResources": [],
        "tenantLevelLargeResourceCount": 0,
        "tenantLevelWarningResources": [],
        "tenantLevelWarningResourceCount": 0,
    }

  def _prepare_root_sites(self, metrics: Dict[str, Any]) -> None:
    """Filters siteMetrics to root site collections and sets default fields."""
    metrics["siteMetrics"] = {
        site_id: s_data
        for site_id, s_data in metrics["siteMetrics"].items()
        if s_data.get("siteLevel", 0) == 0
    }

    for s_data in metrics["siteMetrics"].values():
      s_data.setdefault("subsiteCount", 0)
      s_data.setdefault("dlCount", 0)
      s_data.setdefault("listCount", 0)
      s_data.setdefault("folderCount", 0)
      s_data.setdefault("fileCount", 0)
      s_data["shortcutCount"] = "N/A"
      s_data["folderCountExceedingDepthLimit"] = "N/A"
      s_data["fileCountExceedingDepthLimit"] = "N/A"
      s_data.setdefault("largeResourceCount", 0)
      s_data.setdefault("warningResourceCount", 0)
      s_data.setdefault("totalSize", 0)
      s_data.setdefault("resourceCount", 0)

  def _pre_authenticate_domains(
      self, site_ids: List[str], sp_connector: SpRestConnector
  ) -> None:
    """Pre-authenticates unique SharePoint domains before launching workers."""
    domains = set()
    for site_id in site_ids:
      web_url = self.id_to_display.get(site_id, "")
      if web_url and web_url.startswith("http"):
        parsed = urlparse(web_url)
        if parsed.netloc:
          domains.add(parsed.netloc)

    for domain in domains:
      if self.is_hard_stop_requested():
        break
      sp_connector.cert_token_manager.ensure_domain_authenticated(
          domain, self.logger
      )

  def _evaluate_thresholds_for_site(
      self,
      target_site_id: str,
      web_url: str,
      item_count: int,
      s_meta: Dict[str, Any],
      metrics: Dict[str, Any],
  ) -> None:
    """Evaluates >500k and >200k item count limits for DL and Site Collection."""
    dl_id = f"{web_url.rstrip('/')}/Documents"
    self.id_to_display[dl_id] = dl_id

    if item_count > self.config.large_resource_count_limit:
      s_meta["largeResourceCount"] += 1
      metrics["tenantLevelLargeResources"].append({
          "type": ResourceType.DL.value,
          "id": dl_id,
          "subTreeCount": item_count,
          "parent": target_site_id,
          "Limit": self.config.large_resource_count_limit,
      })
    if item_count > self.config.warning_resource_count_limit:
      s_meta["warningResourceCount"] += 1
      metrics["tenantLevelWarningResources"].append({
          "type": ResourceType.DL.value,
          "id": dl_id,
          "subTreeCount": item_count,
          "parent": target_site_id,
          "Limit": self.config.warning_resource_count_limit,
      })

    site_total_items = s_meta.get("resourceCount", item_count)
    if site_total_items > self.config.large_resource_count_limit:
      s_meta["largeResourceCount"] += 1
      metrics["tenantLevelLargeResources"].append({
          "type": ResourceType.SITE.value,
          "id": target_site_id,
          "subTreeCount": site_total_items,
          "parent": "N/A (Top level site)",
          "Limit": self.config.large_resource_count_limit,
      })
    if site_total_items > self.config.warning_resource_count_limit:
      s_meta["warningResourceCount"] += 1
      metrics["tenantLevelWarningResources"].append({
          "type": ResourceType.SITE.value,
          "id": target_site_id,
          "subTreeCount": site_total_items,
          "parent": "N/A (Top level site)",
          "Limit": self.config.warning_resource_count_limit,
      })

  def _scan_active_onedrive_dls(
      self,
      metrics: Dict[str, Any],
      failures: List[Dict[str, str]],
  ) -> Tuple[int, int]:
    """Queries SharePoint REST StorageMetrics & ItemCounts for active OneDrives."""
    active_site_ids = [
        s_id
        for s_id, s_data in metrics["siteMetrics"].items()
        if s_data.get("dlCount", 0) > 0
    ]
    total_sites = len(active_site_ids)
    worker_count = max(1, self.config.concurrency // 10)

    self.logger(
        f"[Phase 2] Querying SharePoint REST StorageMetrics & ItemCounts for "
        f"{total_sites} active OneDrives with concurrency = {worker_count}..."
    )

    self.progress_update_callback(
        "drive_discovery",
        status="Scanning...",
        count=0,
        failed=0,
        folderCount=0,
        fileCount=0,
        progress=0.0,
    )

    if total_sites == 0:
      return 0, 0

    sp_connector = SpRestConnector(
        self.cert_token_manager,
        max_retries=5,
        backoff=self.config.backoff,
    )
    self._pre_authenticate_domains(active_site_ids, sp_connector)

    processed_count = 0
    failed_count = 0
    running_folder_total = 0
    running_file_total = 0
    lock = threading.Lock()

    def _process_single_site(target_site_id: str) -> None:
      nonlocal processed_count, failed_count, running_folder_total, running_file_total
      if self.is_hard_stop_requested():
        return

      s_meta = metrics["siteMetrics"][target_site_id]
      web_url = self.id_to_display.get(target_site_id, "")
      if not web_url or not web_url.startswith("http"):
        with lock:
          processed_count += 1
          failed_count += 1
          self.progress_update_callback(
              "drive_discovery",
              status="Scanning...",
              count=processed_count,
              failed=failed_count,
              folderCount=running_folder_total,
              fileCount=running_file_total,
              progress=processed_count / max(1, total_sites),
          )
        return

      try:
        od_metrics = sp_connector.get_onedrive_metrics(
            web_url, self.logger, self.stop_event
        )
        item_cnt = od_metrics["item_count"]
        file_cnt = od_metrics["file_count"]
        folder_cnt = od_metrics["folder_count"]
        active_size_bytes = od_metrics["active_size_bytes"]

        with lock:
          s_meta["fileCount"] = file_cnt
          s_meta["folderCount"] = folder_cnt
          s_meta["totalSize"] = active_size_bytes
          s_meta["resourceCount"] = max(item_cnt, folder_cnt + file_cnt)

          self._evaluate_thresholds_for_site(
              target_site_id, web_url, item_cnt, s_meta, metrics
          )

          processed_count += 1
          running_folder_total += folder_cnt
          running_file_total += file_cnt

          self.progress_update_callback(
              "drive_discovery",
              status="Scanning...",
              count=processed_count,
              failed=failed_count,
              folderCount=running_folder_total,
              fileCount=running_file_total,
              progress=processed_count / max(1, total_sites),
          )

      except PermissionError as perm_err:
        with lock:
          processed_count += 1
          failed_count += 1
          self.logger(f"Skipping site {web_url}: {perm_err}")
          failures.append({
              "type": FailureType.FAILURE_STATUS_CODE_ERROR.name,
              "statusCode": 403,
              "message": str(perm_err),
          })
          self.progress_update_callback(
              "drive_discovery",
              status="Scanning...",
              count=processed_count,
              failed=failed_count,
              folderCount=running_folder_total,
              fileCount=running_file_total,
              progress=processed_count / max(1, total_sites),
          )

      except Exception as err:
        with lock:
          processed_count += 1
          failed_count += 1
          err_msg = f"Failed StorageMetrics/ItemCounts REST call for {web_url}: {err}"
          self.logger(err_msg)
          failures.append({
              "type": FailureType.UNKNOWN_ERROR.name,
              "statusCode": 500,
              "message": err_msg,
          })
          self.progress_update_callback(
              "drive_discovery",
              status="Scanning...",
              count=processed_count,
              failed=failed_count,
              folderCount=running_folder_total,
              fileCount=running_file_total,
              progress=processed_count / max(1, total_sites),
          )

    with ThreadPoolExecutor(max_workers=worker_count) as shallow_executor:
      futures = [
          shallow_executor.submit(_process_single_site, s_id)
          for s_id in active_site_ids
      ]
      for future in as_completed(futures):
        if self.is_hard_stop_requested():
          break
        try:
          future.result()
        except Exception as exc:
          self.logger(f"Unexpected worker exception in Shallow Scan: {exc}")

    return processed_count, failed_count

  def _finalize_tenant_totals(
      self,
      metrics: Dict[str, Any],
      processed_count: int,
      failed_count: int,
  ) -> None:
    """Aggregates tenant-level totals and fires completion UI events."""
    metrics["folderCount"] = sum(
        int(s.get("folderCount", 0) or 0)
        for s in metrics["siteMetrics"].values()
    )
    metrics["fileCount"] = sum(
        int(s.get("fileCount", 0) or 0)
        for s in metrics["siteMetrics"].values()
    )
    metrics["tenantLevelLargeResourceCount"] = len(
        metrics["tenantLevelLargeResources"]
    )
    metrics["tenantLevelWarningResourceCount"] = len(
        metrics["tenantLevelWarningResources"]
    )
    metrics["siteClassification"] = {
        site_id: "personal" if self._is_subsite_personal(site_id) else "teams"
        for site_id in metrics["siteMetrics"].keys()
    }

    self.progress_update_callback(
        "drive_discovery",
        status="Done",
        count=processed_count,
        failed=failed_count,
        folderCount=metrics["folderCount"],
        fileCount=metrics["fileCount"],
        progress=1.0,
    )
    self.progress_update_callback(
        "phase_status", source="plan_generation", status="complete"
    )
    self.logger(
        f"[Phase 2] Shallow Drive Discovery complete. Active DLs scanned: "
        f"{processed_count} | Total Files: {metrics['fileCount']:,} | Total "
        f"Folders: {metrics['folderCount']:,} | Failed: {failed_count}"
    )

  def calculate_resource_metrics(
      self, data: Dict[str, Any], failures: List[Dict[str, str]]
  ) -> Dict[str, Any]:
    """Orchestrates Shallow Scan site discovery and StorageMetrics calculation."""
    try:
      if failures is None:
        failures = []
      if self.logger is None:
        self.logger = lambda x: None

      drives: List[Dict[str, Any]] = []
      subsite_to_drives: Dict[str, List[Any]] = {}
      subsite_to_top_level_site: Dict[str, str] = {}
      metrics = self._initialize_metrics_structure()

      # ==========================================
      # PHASE 1: SITE DISCOVERY (Reusing Deep Scan)
      # ==========================================
      self._discover_sites(
          data,
          metrics,
          drives,
          subsite_to_drives,
          subsite_to_top_level_site,
          failures,
      )
      self._aggregate_site_structural_counts(
          metrics, subsite_to_drives, subsite_to_top_level_site
      )
      self._prepare_root_sites(metrics)

      if self.is_hard_stop_requested():
        return metrics

      # ==========================================
      # PHASE 2: SHALLOW DRIVE DISCOVERY
      # ==========================================
      processed_count, failed_count = self._scan_active_onedrive_dls(
          metrics, failures
      )

      if self.is_hard_stop_requested():
        return metrics

      self._finalize_tenant_totals(metrics, processed_count, failed_count)
      return metrics

    except Exception as e:
      if self.logger:
        self.logger(
            f"Error in ShallowFileEstimator.calculate_resource_metrics: {e}"
        )
      failures.append({
          "type": FailureType.UNKNOWN_ERROR.name,
          "statusCode": 500,
          "message": f"Exception in calculate_resource_metrics: {str(e)}",
      })
      return {}
