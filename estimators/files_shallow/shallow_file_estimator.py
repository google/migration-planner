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
from dataclasses import dataclass
from datetime import timedelta
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from estimators.file_estimator import FileEstimator
from util.connectors import UrlInvoker
from util.enums import FailureType, ResourceType
from util.files_shallow.cert_token_manager import CertTokenManager
from util.files_shallow.sp_rest_connector import SpRestConnector
from util.utils import ScanConfig


@dataclass(frozen=True)
class LibraryTarget:
  """A single document library to query, resolved during Site Discovery.

  Attributes:
    drive_id: Graph drive id, used as the report-facing resource id.
    library_url: Absolute URL of the document library.
    web_base_url: Absolute URL of the web owning the library. SharePoint's
      `_api` endpoints are web-scoped, so subsite libraries must be addressed
      through their own web rather than the site collection root.
    owning_site_id: Site (or subsite) that directly owns the library.
    top_level_site_id: Root site collection the metrics roll up into.
  """

  drive_id: str
  library_url: str
  web_base_url: str
  owning_site_id: str
  top_level_site_id: str


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

  def _build_library_targets(
      self,
      metrics: Dict[str, Any],
      subsite_to_drives: Dict[str, List[Any]],
      subsite_to_top_level_site: Dict[str, str],
      failures: List[Dict[str, str]],
  ) -> List[LibraryTarget]:
    """Expands Site Discovery output into the flat list of libraries to query.

    Every document library found during discovery is included without further
    filtering, so Shallow and Deep scans operate on an identical library set.
    """
    targets: List[LibraryTarget] = []

    for owning_site_id, drive_ids in subsite_to_drives.items():
      top_level_site_id = subsite_to_top_level_site.get(
          owning_site_id, owning_site_id
      )
      if top_level_site_id not in metrics["siteMetrics"]:
        continue

      web_base_url = self.id_to_display.get(owning_site_id, "")
      if not web_base_url.startswith("http"):
        failures.append({
            "type": FailureType.NOT_FOUND.name,
            "statusCode": None,
            "message": (
                f"Skipping site {owning_site_id}: no resolvable web URL for "
                "SharePoint REST addressing."
            ),
        })
        continue

      for drive_id in drive_ids:
        library_url = self.id_to_display.get(drive_id, "")
        if not library_url.startswith("http"):
          failures.append({
              "type": FailureType.NOT_FOUND.name,
              "statusCode": None,
              "message": (
                  f"Skipping document library {drive_id}: no resolvable URL."
              ),
          })
          continue

        targets.append(
            LibraryTarget(
                drive_id=drive_id,
                library_url=library_url,
                web_base_url=web_base_url,
                owning_site_id=owning_site_id,
                top_level_site_id=top_level_site_id,
            )
        )

    return targets

  def _pre_authenticate_domains(
      self, targets: List[LibraryTarget], sp_connector: SpRestConnector
  ) -> None:
    """Pre-authenticates unique SharePoint domains before launching workers."""
    domains = set()
    for target in targets:
      parsed = urlparse(target.web_base_url)
      if parsed.netloc:
        domains.add(parsed.netloc)

    for domain in sorted(domains):
      if self.is_hard_stop_requested():
        break
      sp_connector.cert_token_manager.ensure_domain_authenticated(
          domain, self.logger
      )

  def _flag_threshold_breaches(
      self,
      metrics: Dict[str, Any],
      s_meta: Dict[str, Any],
      resource_type: str,
      resource_id: str,
      item_count: int,
      parent_id: str,
  ) -> None:
    """Records Large (>500k) and Warning (>200k) breaches for one resource.

    Counters are always incremented on the owning root site collection so the
    site report aggregates identically to Deep Scan.
    """
    if item_count > self.config.large_resource_count_limit:
      s_meta["largeResourceCount"] += 1
      metrics["tenantLevelLargeResources"].append({
          "type": resource_type,
          "id": resource_id,
          "subTreeCount": item_count,
          "parent": parent_id,
          "Limit": self.config.large_resource_count_limit,
      })

    if item_count > self.config.warning_resource_count_limit:
      s_meta["warningResourceCount"] += 1
      metrics["tenantLevelWarningResources"].append({
          "type": resource_type,
          "id": resource_id,
          "subTreeCount": item_count,
          "parent": parent_id,
          "Limit": self.config.warning_resource_count_limit,
      })

  def _evaluate_container_thresholds(
      self,
      metrics: Dict[str, Any],
      subsite_item_totals: Dict[str, int],
      subsite_to_top_level_site: Dict[str, str],
  ) -> None:
    """Applies Subsite and Site Collection thresholds once all DLs are scanned.

    Mirrors Deep Scan, which evaluates Folder, DL, Subsite and Site levels.
    Folder level is unavailable to Shallow Scan as no folder tree is built.
    """
    for owning_site_id in sorted(subsite_item_totals):
      top_level_site_id = subsite_to_top_level_site.get(
          owning_site_id, owning_site_id
      )
      if owning_site_id == top_level_site_id:
        continue
      s_meta = metrics["siteMetrics"].get(top_level_site_id)
      if s_meta is None:
        continue
      self._flag_threshold_breaches(
          metrics,
          s_meta,
          ResourceType.SUBSITE.value,
          owning_site_id,
          subsite_item_totals[owning_site_id],
          top_level_site_id,
      )

    for site_id in sorted(metrics["siteMetrics"]):
      s_meta = metrics["siteMetrics"][site_id]
      self._flag_threshold_breaches(
          metrics,
          s_meta,
          ResourceType.SITE.value,
          site_id,
          s_meta.get("resourceCount", 0),
          "N/A (Top level site)",
      )

  def _scan_document_libraries(
      self,
      metrics: Dict[str, Any],
      targets: List[LibraryTarget],
      subsite_to_top_level_site: Dict[str, str],
      failures: List[Dict[str, str]],
  ) -> Tuple[int, int]:
    """Queries StorageMetrics & ItemCount for every discovered document library.

    Each library is scanned all-or-nothing: if either REST call fails the
    library contributes no metrics and is counted as failed.
    """
    total_libraries = len(targets)
    worker_count = max(1, self.config.concurrency // 10)

    self.logger(
        f"[Phase 2] Querying SharePoint REST StorageMetrics & ItemCounts for "
        f"{total_libraries} document libraries with concurrency = "
        f"{worker_count}..."
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

    if total_libraries == 0:
      return 0, 0

    sp_connector = SpRestConnector(
        self.cert_token_manager,
        max_retries=5,
        backoff=self.config.backoff,
    )
    self._pre_authenticate_domains(targets, sp_connector)

    processed_count = 0
    failed_count = 0
    running_folder_total = 0
    running_file_total = 0
    subsite_item_totals: Dict[str, int] = {}
    lock = threading.Lock()

    def _publish_progress() -> None:
      """Emits a drive_discovery update. Must be called while holding the lock."""
      self.progress_update_callback(
          "drive_discovery",
          status="Scanning...",
          count=processed_count,
          failed=failed_count,
          folderCount=running_folder_total,
          fileCount=running_file_total,
          progress=processed_count / max(1, total_libraries),
      )

    def _record_failure(
        message: str, failure_type: str, status_code: Optional[int]
    ) -> None:
      """Marks the current library as failed and logs the reason."""
      nonlocal processed_count, failed_count
      with lock:
        processed_count += 1
        failed_count += 1
        self.logger(message)
        failures.append({
            "type": failure_type,
            "statusCode": status_code,
            "message": message,
        })
        _publish_progress()

    def _process_single_library(target: LibraryTarget) -> None:
      nonlocal processed_count, running_folder_total, running_file_total
      if self.is_hard_stop_requested():
        return

      try:
        library_metrics = sp_connector.get_library_metrics(
            target.web_base_url, target.library_url, self.logger, self.stop_event
        )
      except PermissionError as perm_err:
        _record_failure(
            f"Skipping document library {target.library_url}: {perm_err}",
            FailureType.FAILURE_STATUS_CODE_ERROR.name,
            403,
        )
        return
      except Exception as err:
        _record_failure(
            "Failed StorageMetrics/ItemCounts REST call for "
            f"{target.library_url}: {err}",
            FailureType.UNKNOWN_ERROR.name,
            500,
        )
        return

      item_cnt = library_metrics["item_count"]
      file_cnt = library_metrics["file_count"]
      folder_cnt = library_metrics["folder_count"]
      active_size_bytes = library_metrics["active_size_bytes"]
      resource_cnt = max(item_cnt, folder_cnt + file_cnt)

      with lock:
        s_meta = metrics["siteMetrics"][target.top_level_site_id]
        s_meta["fileCount"] += file_cnt
        s_meta["folderCount"] += folder_cnt
        s_meta["totalSize"] += active_size_bytes
        s_meta["resourceCount"] += resource_cnt

        subsite_item_totals[target.owning_site_id] = (
            subsite_item_totals.get(target.owning_site_id, 0) + resource_cnt
        )

        metrics["driveMetrics"][target.drive_id] = {
            "fileCount": file_cnt,
            "folderCount": folder_cnt,
            "resourceCount": resource_cnt,
            "totalSize": active_size_bytes,
        }

        self._flag_threshold_breaches(
            metrics,
            s_meta,
            ResourceType.DL.value,
            target.drive_id,
            item_cnt,
            target.owning_site_id,
        )

        processed_count += 1
        running_folder_total += folder_cnt
        running_file_total += file_cnt
        _publish_progress()

    with ThreadPoolExecutor(max_workers=worker_count) as shallow_executor:
      futures = [
          shallow_executor.submit(_process_single_library, target)
          for target in targets
      ]
      for future in as_completed(futures):
        if self.is_hard_stop_requested():
          break
        try:
          future.result()
        except Exception as exc:
          self.logger(f"Unexpected worker exception in Shallow Scan: {exc}")

    self._evaluate_container_thresholds(
        metrics, subsite_item_totals, subsite_to_top_level_site
    )

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
        f"[Phase 2] Shallow Drive Discovery complete. Document libraries "
        f"scanned: {processed_count} | Total Files: {metrics['fileCount']:,} | "
        f"Total Folders: {metrics['folderCount']:,} | Failed: {failed_count}"
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
      t_site_discovery_start = time.time()
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
      t_site_discovery_end = time.time()
      site_discovery_duration = t_site_discovery_end - t_site_discovery_start
      self.logger(
          f"[Phase 1: Site Discovery] Completed in {site_discovery_duration:.2f}s"
          f" ({timedelta(seconds=int(round(site_discovery_duration)))})"
      )

      if self.is_hard_stop_requested():
        metrics["phase_runtimes"] = {
            "site_discovery_seconds": site_discovery_duration,
            "drive_discovery_seconds": 0.0,
        }
        return metrics

      # ==========================================
      # PHASE 2: SHALLOW DRIVE DISCOVERY
      # ==========================================
      t_drive_discovery_start = time.time()
      targets = self._build_library_targets(
          metrics, subsite_to_drives, subsite_to_top_level_site, failures
      )
      processed_count, failed_count = self._scan_document_libraries(
          metrics, targets, subsite_to_top_level_site, failures
      )

      if self.is_hard_stop_requested():
        drive_discovery_duration = time.time() - t_drive_discovery_start
        metrics["phase_runtimes"] = {
            "site_discovery_seconds": site_discovery_duration,
            "drive_discovery_seconds": drive_discovery_duration,
        }
        return metrics

      self._finalize_tenant_totals(metrics, processed_count, failed_count)
      t_drive_discovery_end = time.time()
      drive_discovery_duration = t_drive_discovery_end - t_drive_discovery_start
      self.logger(
          f"[Phase 2: Drive Discovery] Completed in {drive_discovery_duration:.2f}s"
          f" ({timedelta(seconds=int(round(drive_discovery_duration)))})"
      )

      metrics["phase_runtimes"] = {
          "site_discovery_seconds": site_discovery_duration,
          "drive_discovery_seconds": drive_discovery_duration,
      }
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
