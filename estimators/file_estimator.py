from concurrent.futures import Future, ThreadPoolExecutor
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from estimators.estimator import Estimator
from util.connectors import UrlInvoker
from util.utils import ScanConfig, Bucket, FileSizeDistribution, LargeResource, create_batches, create_request_to_response_map, get_batch_responses_map, get_relative_url, process_pagination_responses
from util.enums import FailureType, ResourceType
from util.thread_safe_ds import ThreadSafeMap, ThreadSafeSortedSet, AtomicInt

import traceback
import json

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"

class FileEstimator(Estimator):
    def __init__(self,
        config: ScanConfig, 
        url_invoker: UrlInvoker, 
        logger: Optional[Callable[[str], None]] = None, 
        stop_event: Optional[threading.Event] = None,
        progress_update_callback: Optional[Callable[[int], None]] = None
    ):
        super().__init__()
        self.config = config
        self.url_invoker = url_invoker
        self.logger = logger
        self.stop_event = stop_event
        self.executor = ThreadPoolExecutor(max_workers=self.config.concurrency)
        self.tree_executor = ThreadPoolExecutor(max_workers=self.config.concurrency)
        self.progress_update_callback = progress_update_callback
        self.condition = threading.Condition()

    def get_resource_type(self) -> str:
        return "FILES"

    def get_migration_type(self) -> str:
        return "SHAREPOINT_ONLINE"

    def is_hard_stop_requested(self):
        if self.stop_event is None:
            return False
        
        return self.stop_event.is_set()

    def calculate_resource_count(self, data: Dict[str, Any], failures: List[Dict[str, str]]) -> Dict[str, int]:
        raise NotImplementedError("calculate_resource_count is not required for SharePointEstimator")

    def calculate_migration_eta(self, data: Dict[str, Any]) -> float:
        """Calculates duration in HOURS based on batching throughput constraints."""
        items = data.get("items", {})
        batch_corpus_size = sum(item["size"] for item in items)
        batch_resource_count = sum(item["files"] for item in items)

        average_batch_file_size = batch_corpus_size / batch_resource_count if batch_resource_count > 0 else 0

        max_qps_from_file_size = data.get("FILES_GLOBAL_CORPUS_SIZE_LIMIT") / average_batch_file_size if average_batch_file_size > 0 else data.get("FILES_GLOBAL_CORPUS_SIZE_LIMIT")
        max_qps_from_license_counts = data.get("FILES_GLOBAL_COUNT_LIMIT")
        
        qps = min(max_qps_from_license_counts, max_qps_from_file_size)
        
        time_in_seconds =  batch_resource_count / qps
        return time_in_seconds / 3600

    def calculate_resource_metrics(
        self, 
        data: Dict[str, Any], 
        failures: List[Dict[str, str]]
    ) -> Dict[str, Any]:
        try:
            if failures is None:
                failures = []
            
            if self.logger is None:
                self.logger = lambda x: None
            self.global_folder_count = 0
            self.global_file_count = 0
            self.global_shortcut_count = 0
            self.global_max_depth = 0
            self.global_folder_exceeding_depth_limit = 0
            self.global_file_exceeding_depth_limit = 0
            self.global_skipped_folders_count = 0
            
            drives = []
            subsite_to_drives = {}          # used to calculate effective max Depth
            subsite_to_top_level_site = {}
            metrics = { 
                "driveMetrics": {},
                "siteMetrics": {},
                "personalSiteCount": 0,
                "teamSiteCount": 0,
                "maxEffectiveDepth": 0,
                "maxFolderDepth": 0,        # only includes depth from folders in drives
                "maxSubsiteDepth": 0,       # only includes depth of subsites
                "subsiteCount": 0,
                "shortcutCount": 0,
                "folderCount": 0,
                "fileCount": 0,
                "folderCountExceedingDepthLimit": 0,
                "fileCountExceedingDepthLimit": 0,
                "listCount": 0,
                "licenseMetrics": {},
                "driveCounts": {
                    "documentLibrary": 0,
                    "personal": 0,
                    "business": 0,
                },
                "personalSiteDLCount": 0,
                "teamSiteDLCount": 0,
                "tenantLevelFileSizeDistribution": {
                    "buckets": []
                },
                "tenantLevelLargeResources": [],
                "tenantLevelLargeResourceCount": 0
            }

            if "drives" in data and len(data["drives"]) > 0:
                drives = data["drives"]
            else:
                self.site_to_metadata = {}
                site_discovery_progress_metrics = {
                    "siteCount": 0,
                    "personalSiteCount": 0,
                    "teamSiteCount": 0,
                    "listCount": 0,
                    "licenseCount": 0,
                    "driveCount": 0,
                }

                self.progress_update_callback("site_discovery", status="Fetching...", count=0)
                metrics["licenseMetrics"] = self._get_license_metrics(site_discovery_progress_metrics, failures)

                self._configure_executor_from_license_counts(metrics["licenseMetrics"])

                has_emails = "emailIds" in data and len(data["emailIds"]) > 0
                has_urls = "siteUrls" in data and len(data["siteUrls"]) > 0

                if not has_emails and not has_urls:
                    top_level_sites = self._get_top_level_sites(metrics, site_discovery_progress_metrics, failures)
                    subsite_to_top_level_site = {}
                else:
                    top_level_sites = []
                    subsite_to_top_level_site = {}
                    
                    if has_emails:
                        mail_to_top_level_site = self._get_sites_for_users(data["emailIds"], site_discovery_progress_metrics)
                        for mail, site_id in mail_to_top_level_site.items():
                            top_level_sites.append(site_id)
                            self.site_to_metadata[site_id] = {"isPersonalSite": True}
                            metrics["personalSiteCount"] += 1
                        
                        site_id_to_mail = {site_id: mail for mail, site_id in mail_to_top_level_site.items()}
                        metrics["siteIdToMail"] = site_id_to_mail

                    if has_urls:
                        url_to_site_id = self._get_sites_from_urls(data["siteUrls"], site_discovery_progress_metrics, failures)
                        for url, site_id in url_to_site_id.items():
                            top_level_sites.append(site_id)
                        
                        site_id_to_url = {site_id: url for url, site_id in url_to_site_id.items()}
                        
                    top_level_sites = list(set(top_level_sites))
                    
                metrics["siteCount"] = len(top_level_sites)
                all_sites = [{"siteId": site_id, "siteLevel": 0} for site_id in top_level_sites]
                self._get_subsites_in_site(top_level_sites, all_sites, subsite_to_top_level_site, site_discovery_progress_metrics, failures, 1)
                
                if not has_emails and not has_urls:
                    metrics["personalSiteCount"] = site_discovery_progress_metrics.get("personalSiteCount", 0)
                    metrics["teamSiteCount"] = site_discovery_progress_metrics.get("teamSiteCount", 0)

                for site_detail in all_sites:
                    metrics["siteMetrics"][site_detail["siteId"]] = {
                        "siteLevel": site_detail["siteLevel"]
                    }
                    
                all_site_ids = [site["siteId"] for site in all_sites]
                self._append_tenant_level_metrics(all_site_ids, metrics, drives, subsite_to_drives, site_discovery_progress_metrics, failures)

                self.progress_update_callback(
                    "site_discovery", 
                    status="Done", 
                    count=site_discovery_progress_metrics.get("siteCount", 0), 
                    personalSiteCount=site_discovery_progress_metrics.get("personalSiteCount", 0),
                    teamSiteCount=site_discovery_progress_metrics.get("teamSiteCount", 0),
                    driveCount=site_discovery_progress_metrics.get("driveCount", 0), 
                    listCount=site_discovery_progress_metrics.get("listCount", 0), 
                    licenseCount=site_discovery_progress_metrics.get("licenseCount", 0)
                )

                self.logger("Site Scanning is finished!!!!")

            # get adjacency lists and parent references for each drive
            self.progress_update_callback("drive_discovery", status="Fetching...", count=0)
            drive_discovery_progress_metrics = {
                "folderCount": 0,
                "fileCount": 0,
                "shortcutCount": 0
            }

            drive_id_to_adj_list, parent_references, resource_id_to_details, drive_id_to_total_size = self._create_in_memory_tree([drive["id"] for drive in drives], drive_discovery_progress_metrics, failures)
            self.progress_update_callback("drive_discovery", status="Done", count=len(drives), **drive_discovery_progress_metrics)

            # Calculate metrics for all drives
            drive_metrics = {}
            
            batch_size = max(1, self.config.concurrency // 10)
            total_drives = len(drives)
            processed = 0
            failed = 0
            success = 0
            total_resource_count = 0

            idx = 0
            self.progress_update_callback("phase_status", source="drive_parsing", status="running")
            while idx < total_drives:
                batch = drives[idx: idx + batch_size]
                idx += batch_size
                try:
                    batch_metrics = self._calculate_drive_metrics([drive["id"] for drive in batch], drive_id_to_adj_list, parent_references, resource_id_to_details, failures)
                    drive_metrics.update(batch_metrics)
                    processed += len(batch)
                    success += len(batch)

                    for d_id, d_metric in batch_metrics.items():
                        # print("Inside Batch metrics!!!!!!!!!")
                        self.global_folder_count += d_metric.get("folderCount", 0)
                        self.global_file_count += d_metric.get("fileCount", 0)
                        self.global_shortcut_count += d_metric.get("shortcutCount", 0)
                        self.global_max_depth = max(self.global_max_depth, d_metric.get("maxEffectiveDepth", 0))
                        self.global_folder_exceeding_depth_limit += d_metric.get("folderCountExceedingDepthLimit", 0)
                        self.global_file_exceeding_depth_limit += d_metric.get("fileCountExceedingDepthLimit", 0)

                    self.progress_update_callback(
                        "scan_progress",
                        source="drive_parsing",
                        progress=processed / total_drives if total_drives > 0 else 0,
                        cumulative=self.global_file_count + self.global_folder_count,
                        folderCount=self.global_folder_count,
                        fileCount=self.global_file_count,
                        maxDepth=self.global_max_depth,
                        folderCountExceedingDepthLimit=self.global_folder_exceeding_depth_limit,
                        fileCountExceedingDepthLimit=self.global_file_exceeding_depth_limit,
                        skippedFolderCount=self.global_skipped_folders_count,
                        processed=processed,
                        failed=failed,
                        success=success,
                        entity_type="Drives"
                    )
                    time.sleep(0.2)
                except Exception as e:
                    failed += len(batch)
                    processed += len(batch)
                    prog = processed / total_drives if total_drives > 0 else 0
                    for drive in batch:
                        total_resource_count += len(parent_references[drive["id"]]) + 1
                        
                    # print("Batch Failed!!!!")
                    self.progress_update_callback(
                        "scan_progress",
                        source="drive_parsing",
                        progress=prog,
                        cumulative=total_resource_count,
                        folderCount=self.global_folder_count,
                        fileCount=self.global_file_count,
                        maxDepth=self.global_max_depth,
                        folderCountExceedingDepthLimit=self.global_folder_exceeding_depth_limit,
                        fileCountExceedingDepthLimit=self.global_file_exceeding_depth_limit,
                        processed=processed,
                        failed=failed,
                        success=success,
                        entity_type="Drives"
                    )
                    self._log_and_fail(e, "_calculate_drive_metrics", failures)

            time.sleep(5)
            self.progress_update_callback("phase_status", source="drive_parsing", status="complete")

            self.progress_update_callback("phase_status", source="plan_generation", status="running")
            metrics["driveMetrics"] = drive_metrics
            self._update_tenant_metrics_from_drive_metrics(metrics, subsite_to_drives, subsite_to_top_level_site, drive_id_to_total_size)
            
            # Filter out subsites (siteLevel > 0) to only keep root site collections
            metrics["siteMetrics"] = {
                site_id: s_data 
                for site_id, s_data in metrics["siteMetrics"].items() 
                if s_data.get("siteLevel", 0) == 0
            }
            
            self.progress_update_callback("phase_status", source="plan_generation", status="complete")

            metrics["siteClassification"] = {siteId: "personal" if self._is_subsite_personal(siteId) else "teams" for siteId in metrics["siteMetrics"].keys()}
            return metrics
            
        except Exception as e:
            if self.logger:
                self.logger(f"Error in calculate_resource_metrics: {e}")
            failures.append({
                "type": FailureType.UNKNOWN_ERROR.name,
                "statusCode": 500,
                "message": f"Exception in calculate_resource_metrics: {str(e)}"
            })
            return {}
    

    def _configure_executor_from_license_counts(self, license_metrics: Dict[str, Any]):
        total_license_count = license_metrics.get("totalAllotedUnits", {}).get("User", 0) 
        total_license_count += license_metrics.get("totalAllotedUnits", {}).get("Company", 0)

        # Source: https://learn.microsoft.com/en-us/sharepoint/dev/general-development/how-to-avoid-getting-throttled-or-blocked-in-sharepoint-online
        # TODO Need to optimize this
        if self.executor:
            self.executor.shutdown(wait=False)
        if total_license_count <= 1000:
            self.executor = ThreadPoolExecutor(max_workers=2)
        elif total_license_count <= 5000:
            self.executor = ThreadPoolExecutor(max_workers=3)
        elif total_license_count <= 15000:
            self.executor = ThreadPoolExecutor(max_workers=4)
        else:
            self.executor = ThreadPoolExecutor(max_workers=5)
        
    def _is_subsite_personal(self, site_id: str) -> bool:
        return self.site_to_metadata.get(site_id, {}).get("isPersonalSite", False)

    def _update_tenant_metrics_from_drive_metrics(
        self,
        metrics: Dict[str, Any],
        subsite_to_drives: Dict[str, List[Any]],
        subsite_to_top_level_site: Dict[str, str],
        drive_id_to_total_size: Dict[str, int]
    ):
        self.progress_update_callback(
            "scan_progress",
            source="plan_generation",
            progress=0.33,
            extra_text="Calculating metrics...",
        )

        for drive_metric in metrics["driveMetrics"].values():
            metrics["maxFolderDepth"] = max(metrics["maxFolderDepth"], drive_metric["maxEffectiveDepth"])
            metrics["shortcutCount"] += drive_metric.get("shortcutCount", 0)
            metrics["folderCount"] += drive_metric.get("folderCount", 0)
            metrics["fileCount"] += drive_metric.get("fileCount", 0)
            metrics["folderCountExceedingDepthLimit"] += drive_metric.get("folderCountExceedingDepthLimit", 0)
            metrics["fileCountExceedingDepthLimit"] += drive_metric.get("fileCountExceedingDepthLimit", 0)
        
        # Aggregate listCount for all sites (including those without drives or with failed drive scans)
        for site_id in list(metrics["siteMetrics"].keys()):
            top_level_site = subsite_to_top_level_site.get(site_id, site_id)
            if top_level_site in metrics["siteMetrics"]:
                metrics["siteMetrics"][top_level_site]["listCount"] = metrics["siteMetrics"][top_level_site].get("listCount", 0) + self.site_to_metadata.get(site_id, {}).get("listCount", 0)

        for subsite_id, drive_ids in subsite_to_drives.items():
            metrics["maxSubsiteDepth"] = max(metrics["maxSubsiteDepth"], metrics["siteMetrics"][subsite_id]["siteLevel"])
            top_level_site = subsite_to_top_level_site.get(subsite_id, subsite_id)

            if self.id_to_display.get(subsite_id, "") == "https://smh3v.sharepoint.com/subsiteofrootsite":
                print(f"FOUND the URL: {subsite_to_top_level_site.get(subsite_id, "")}")

            if top_level_site != subsite_id:
                metrics["siteMetrics"][top_level_site]["subsiteCount"] = metrics["siteMetrics"][top_level_site].get("subsiteCount", 0) + 1
            
            for drive_id in drive_ids:
                if drive_id in metrics["driveMetrics"]:
                    drive_metric = metrics["driveMetrics"][drive_id]
                    
                    metrics["siteMetrics"][top_level_site]["largeResourceCount"] = metrics["siteMetrics"].get(top_level_site, {}).get("largeResourceCount", 0) + len(drive_metric.get("largeResources", []))
                    metrics["siteMetrics"][top_level_site]["folderCount"] =  metrics["siteMetrics"].get(top_level_site, {}).get("folderCount", 0) + drive_metric.get("folderCount", 0)
                    metrics["siteMetrics"][top_level_site]["fileCount"] =  metrics["siteMetrics"].get(top_level_site, {}).get("fileCount", 0) + drive_metric.get("fileCount", 0)
                    metrics["siteMetrics"][top_level_site]["shortcutCount"] =  metrics["siteMetrics"].get(top_level_site, {}).get("shortcutCount", 0) + drive_metric.get("shortcutCount", 0)
                    metrics["siteMetrics"][top_level_site]["totalSize"] =  metrics["siteMetrics"].get(top_level_site, {}).get("totalSize", 0) + drive_id_to_total_size.get(drive_id, 0)
                    metrics["siteMetrics"][top_level_site]["folderCountExceedingDepthLimit"] =  metrics["siteMetrics"].get(top_level_site, {}).get("folderCountExceedingDepthLimit", 0) + drive_metric.get("folderCountExceedingDepthLimit", 0)
                    metrics["siteMetrics"][top_level_site]["fileCountExceedingDepthLimit"] =  metrics["siteMetrics"].get(top_level_site, {}).get("fileCountExceedingDepthLimit", 0) + drive_metric.get("fileCountExceedingDepthLimit", 0)
                
            
            metrics["siteMetrics"][top_level_site]["dlCount"] = metrics["siteMetrics"].get(top_level_site, {}).get("dlCount", 0) + len(drive_ids)
            
            if self._is_subsite_personal(subsite_id):
                metrics["personalSiteDLCount"] += len(drive_ids)
            else:
                metrics["teamSiteDLCount"] += len(drive_ids)
            
            if top_level_site != subsite_id:
                metrics["subsiteCount"] += 1
                    
        for siteId in subsite_to_drives.keys():
            top_level_site = subsite_to_top_level_site.get(siteId, siteId)
            if top_level_site != siteId:
                continue

            if top_level_site in metrics["siteMetrics"]:
                metrics["siteMetrics"][top_level_site]["resourceCount"] = metrics["siteMetrics"][top_level_site].get("folderCount", 0) + metrics["siteMetrics"][top_level_site].get("fileCount", 0) + metrics["siteMetrics"][top_level_site].get("shortcutCount", 0)

        for size_range in self.config.bucket_ranges:
            metrics["tenantLevelFileSizeDistribution"]["buckets"].append({
                "sizeRange": size_range,
                "count": 0
            })
        
        self.progress_update_callback(
            "scan_progress",
            source="plan_generation",
            progress=0.66,
            extra_text="Calculating metrics...",
        )

        for tenant_bucket in metrics["tenantLevelFileSizeDistribution"]["buckets"]:
            for metric in metrics["driveMetrics"].values():
                if "fileSizeDistribution" in metric:
                    for bucket in metric["fileSizeDistribution"]["buckets"]:
                        if bucket["sizeRange"] == tenant_bucket["sizeRange"]:
                            tenant_bucket["count"] += bucket["count"]
                            break

        for drive_id, metric in metrics["driveMetrics"].items():
            for large_resource in metric["largeResources"]:
                curr_dict = large_resource
                curr_dict["drive"] = drive_id
                metrics["tenantLevelLargeResources"].append(curr_dict)
        
        metrics["tenantLevelLargeResourceCount"] = len(metrics["tenantLevelLargeResources"])
        
        self.progress_update_callback(
            "scan_progress",
            source="plan_generation",
            progress=1,
            extra_text="Calculated metrics for all drives...",
        )

    def _get_subsites_in_site(
        self,
        site_ids: List[str],
        all_sites: Dict[str, int],
        subsite_to_top_level_site: Dict[str, str],
        site_discovery_progress_metrics: Dict[str, Any],
        failures: List[Dict[str, str]],
        level: int = 1
    ):
        try:
            site_url = "/sites/{siteId}/sites?$select=id,weburl,isPersonalSite&$top=999"
            batches = create_batches(site_url, [{"siteId": site_id} for site_id in site_ids], self.config.parallel_batches, True)

            futures_map: Dict[int, Future[List[Dict[str, Any]]]] = {}
            batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]] = {}
            idx = 0
            for batch in batches:
                futures_map[idx] = self.executor.submit(self.url_invoker.invoke, GRAPH_BASE_URL, batch, self.logger, self.stop_event, self.get_resource_type())
                batch_id_to_batch_map[idx] = batch
                idx += 1

            from concurrent.futures import as_completed
            future_to_batch_id = {future: bid for bid, future in futures_map.items()}

            site_to_resp_map: Dict[str, Dict[str, Any]] = {}
            pending_next_items = []

            def local_progress_callback(responses: List, has_next=False):
                if self.config.includeTeamSites:
                    site_discovery_progress_metrics["teamSiteCount"] += len([site for site in responses if not site.get("isPersonalSite", False)])
                if self.config.includePersonalSites:
                    site_discovery_progress_metrics["personalSiteCount"] += len([site for site in responses if site.get("isPersonalSite", False)])

                site_discovery_progress_metrics["siteCount"] = (
                    site_discovery_progress_metrics.get("personalSiteCount", 0)
                    + site_discovery_progress_metrics.get("teamSiteCount", 0)
                )
                self.progress_update_callback(
                    "site_discovery",
                    count=site_discovery_progress_metrics.get("siteCount", 0),
                    personalSiteCount=site_discovery_progress_metrics.get("personalSiteCount", 0),
                    teamSiteCount=site_discovery_progress_metrics.get("teamSiteCount", 0),
                )

            for future in as_completed(futures_map.values()):
                batch_id = future_to_batch_id[future]
                responses = future.result()
                batch = batch_id_to_batch_map[batch_id]
                batch_responses_map = get_batch_responses_map(responses, self.logger)
                for req in batch:
                    req_id = req["id"]
                    if req_id in batch_responses_map:
                        resp = batch_responses_map[req_id]
                        site_id = req["headers"]["siteId"]
                        site_to_resp_map[site_id] = resp

                        if "body" in resp and "value" in resp["body"]:
                            local_progress_callback(resp["body"]["value"])

                        if "body" in resp and "@odata.nextLink" in resp["body"]:
                            next_url = resp["body"]["@odata.nextLink"]
                            relative_url = get_relative_url(next_url, GRAPH_BASE_URL)
                            pending_next_items.append({
                                "siteId": site_id,
                                "url": relative_url
                            })
                        elif "body" in resp and "error" in resp["body"]:
                            failures.append({
                                "type": FailureType.FAILURE_STATUS_CODE_ERROR,
                                "statusCode": resp["status"],
                                "message": f"Error in fetching subsites for site {site_id}: {resp['body']['error']['message']}"
                            })
                    else:
                        failures.append({
                            "type": FailureType.NOT_FOUND,
                            "statusCode": None,
                            "message": f"No response found for subsites API for site {req['headers']['siteId']}."
                        })

            while pending_next_items and not self.is_hard_stop_requested():
                batches = create_batches("{url}", pending_next_items, self.config.parallel_batches, True)
                
                next_futures_map: Dict[int, Future[List[Dict[str, Any]]]] = {}
                next_batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]] = {}
                idx = 0
                for batch in batches:
                    next_futures_map[idx] = self.executor.submit(self.url_invoker.invoke, GRAPH_BASE_URL, batch, self.logger, self.stop_event, self.get_resource_type())
                    next_batch_id_to_batch_map[idx] = batch
                    idx += 1
                    
                future_to_batch_id = {future: bid for bid, future in next_futures_map.items()}
                new_pending_next_items = []
                
                for future in as_completed(next_futures_map.values()):
                    batch_id = future_to_batch_id[future]
                    responses = future.result()
                    batch = next_batch_id_to_batch_map[batch_id]
                    new_pending_next_items.extend(process_pagination_responses(batch, responses, site_to_resp_map, "siteId", GRAPH_BASE_URL, failures, False, progress_callback=local_progress_callback))
                    
                pending_next_items = new_pending_next_items

            new_sub_site_ids = []
            for site_id, resp in site_to_resp_map.items():
                if "body" in resp and "value" in resp["body"]:
                    for site in resp["body"]["value"]:
                        all_sites.append({"siteId": site["id"], "siteLevel": level})
                        new_sub_site_ids.append(site["id"])
                        subsite_to_top_level_site[site["id"]] = site_id if site_id not in subsite_to_top_level_site else subsite_to_top_level_site[site_id]
                        self.site_to_metadata[site["id"]] = {
                            "isPersonalSite": site.get("isPersonalSite", False)
                        }

            if new_sub_site_ids:
                self._get_subsites_in_site(new_sub_site_ids, all_sites, subsite_to_top_level_site, site_discovery_progress_metrics, failures, level + 1)

        except Exception as e:
            self._log_and_fail("Error in _get_subsites_in_site", e, failures)

    def _get_top_level_sites(
        self,
        tenant_metrics: Dict[str, Any],
        site_discovery_progress_metrics: Dict[str, Any],
        failures: List[Dict[str, str]]
    ):
        try:
            sites = []
            url = f"{GRAPH_BASE_URL}/sites?$select=id,webUrl,isPersonalSite,parentReference&$top=999"
            token_data = self.url_invoker.token_manager.get_valid_token_slot(self.logger)
            token = token_data["token"]
            session = self.url_invoker.token_manager.get_session()
            headers = {"Authorization": f"Bearer {token}"}
            try:
                while url and not self.is_hard_stop_requested():
                    # Check mid-loop for extremely long tenant scans
                    if time.time() > token_data["expires_at"]:
                        self.url_invoker.token_manager.return_token_slot(token_data)
                        token_data = self.url_invoker.token_manager.get_valid_token_slot(self.logger)
                        token = token_data["token"]
                        headers = {"Authorization": f"Bearer {token}"}

                    attempts = 0
                    max_attempts = self.config.retries + 1
                    while attempts < max_attempts and not self.is_hard_stop_requested():
                        try:
                            r = session.get(url, headers=headers, timeout=180)
                            if r.status_code != 200:
                                raise Exception(f"Error in fetching site : {r.status_code}")
                            d = r.json()
                            break
                        except Exception as e:
                            attempts += 1
                            if attempts == max_attempts:
                                self._log_and_fail("Error in fetching site", e, failures)
                                break
                            elif self.logger is not None:
                                wait_time = min(10, max(2, self.config.backoff) ** (attempts - 1))
                                self.logger(f"Error in fetching site. Attempt count: {attempts} | Retrying in {wait_time} seconds...")
                                time.sleep(wait_time)

                    local_all_sites = d.get("value", [])
                    personal_sites = [site for site in local_all_sites if site["isPersonalSite"]]
                    team_sites = [site for site in local_all_sites if not site["isPersonalSite"]]

                    if self.config.includePersonalSites:
                        site_discovery_progress_metrics["personalSiteCount"] += len(personal_sites)
                        tenant_metrics["personalSiteCount"] += len(personal_sites)
                    if self.config.includeTeamSites:
                        site_discovery_progress_metrics["teamSiteCount"] += len(team_sites)
                        tenant_metrics["teamSiteCount"] += len(team_sites)

                    site_discovery_progress_metrics["siteCount"] = (
                        site_discovery_progress_metrics.get("personalSiteCount", 0)
                        + site_discovery_progress_metrics.get("teamSiteCount", 0)
                    )

                    self.progress_update_callback(
                        "site_discovery",
                        count=site_discovery_progress_metrics.get("siteCount", 0),
                        personalSiteCount=site_discovery_progress_metrics.get("personalSiteCount", 0),
                        teamSiteCount=site_discovery_progress_metrics.get("teamSiteCount", 0),
                    )

                    if self.config.includePersonalSites:
                        sites.extend(personal_sites)
                    if self.config.includeTeamSites:
                        sites.extend(team_sites)

                    url = d.get("@odata.nextLink")
                
            except Exception as e:
                self._log_and_fail("Error in _get_top_level_sites", e, failures)
            finally:
                self.url_invoker.token_manager.return_token_slot(token_data)
            
            for site in sites:
                self.site_to_metadata[site["id"]] = {
                    "isPersonalSite": site.get("isPersonalSite", False)
                }
                self.id_to_display[site["id"]] = site["webUrl"]

        except Exception as e:
            self._log_and_fail("Error in _calculate_site_metrics", e, failures)
        
        return [site["id"] for site in sites]

    def _get_site_id_to_level(
        self, 
        sites: List[Dict[str, Any]], 
        subsite_to_top_level_site: Dict[str, str], 
        failures: List[Dict[str, str]]
    ):
        site_id_to_level = {}
        parent_map = {}
        
        for site in sites:
            site_id = site["id"]
            parent_ref = site.get("parentReference")
            if parent_ref and "siteId" in parent_ref:
                parent_map[site_id] = parent_ref["siteId"]
                
        def get_level_and_parent(site_id):
            if site_id in site_id_to_level:
                return site_id_to_level[site_id], subsite_to_top_level_site.get(site_id, site_id)
            
            parent_id = parent_map.get(site_id)
            if not parent_id:
                level = 0
                top_level_parent = site_id
            else:
                # If parent is not in our list, assume parent is level 0
                level, top_level_parent = get_level_and_parent(parent_id)
                level += 1
                
            site_id_to_level[site_id] = level
            subsite_to_top_level_site[site_id] = top_level_parent
            return level, top_level_parent
            
        for site in sites:
            level, top_level_par = get_level_and_parent(site["id"])
            
        return site_id_to_level
    
    def _append_tenant_level_metrics(
        self,
        site_ids: List[str],
        tenant_metrics: Dict[str, Any],
        drives: List[Any],
        subsite_to_drives: Dict[str, List[Any]],
        site_discovery_progress_metrics: Dict[str, int],
        failures: List[Dict[str, str]]
    ):
        try:
            tenant_metrics["listCount"] = self._get_list_count(site_ids, tenant_metrics, site_discovery_progress_metrics, failures)
            drive_type_to_count = self._get_drives(site_ids, drives, subsite_to_drives, site_discovery_progress_metrics, failures)
            for key, value in drive_type_to_count.items():
                if key not in tenant_metrics["driveCounts"]:
                    tenant_metrics["driveCounts"][key] = 0
                tenant_metrics["driveCounts"][key] += value
                
        except Exception as e:
            self._log_and_fail("Error in _append_tenant_level_metrics", e, failures)

    def _get_list_count(
        self,
        site_ids: List[str],
        tenant_metrics: Dict[str, Any],
        site_discovery_progress_metrics: Dict[str, int],
        failures: List[Dict[str, str]]
    ) -> int:
        try:
            list_url = "/sites/{siteId}/lists?$select=id&$top=999"
            batches = create_batches(list_url, [{"siteId": site_id} for site_id in site_ids], self.config.parallel_batches, True)

            futures_map: Dict[int, Future[List[Dict[str, Any]]]] = {}
            batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]] = {}
            idx = 0
            for batch in batches:
                futures_map[idx] = self.executor.submit(self.url_invoker.invoke, GRAPH_BASE_URL, batch, self.logger, self.stop_event, self.get_resource_type())
                batch_id_to_batch_map[idx] = batch
                idx += 1

            from concurrent.futures import as_completed
            future_to_batch_id = {future: bid for bid, future in futures_map.items()}

            site_to_resp_map: Dict[str, Dict[str, Any]] = {}
            pending_next_items = []

            def local_progress_callback(responses: List, has_next=False):
                site_discovery_progress_metrics["listCount"] += len(responses)
                self.progress_update_callback(
                    "site_discovery", 
                    count=site_discovery_progress_metrics.get("siteCount", 0), 
                    personalSiteCount=site_discovery_progress_metrics.get("personalSiteCount", 0),
                    teamSiteCount=site_discovery_progress_metrics.get("teamSiteCount", 0),
                    driveCount=site_discovery_progress_metrics.get("driveCount", 0), 
                    listCount=site_discovery_progress_metrics.get("listCount", 0), 
                    licenseCount=site_discovery_progress_metrics.get("licenseCount", 0)
                )

            for future in as_completed(futures_map.values()):
                batch_id = future_to_batch_id[future]
                responses = future.result()
                batch = batch_id_to_batch_map[batch_id]
                batch_responses_map = get_batch_responses_map(responses, self.logger)
                for req in batch:
                    req_id = req["id"]
                    if req_id in batch_responses_map:
                        resp = batch_responses_map[req_id]
                        site_id = req["headers"]["siteId"]
                        site_to_resp_map[site_id] = resp

                        if "body" in resp and "value" in resp["body"]:
                            local_progress_callback(resp["body"]["value"])

                        if "body" in resp and "@odata.nextLink" in resp["body"]:
                            next_url = resp["body"]["@odata.nextLink"]
                            relative_url = get_relative_url(next_url, GRAPH_BASE_URL)
                            pending_next_items.append({
                                "siteId": site_id,
                                "url": relative_url
                            })
                        elif "body" in resp and "error" in resp["body"]:
                            failures.append({
                                "type": FailureType.FAILURE_STATUS_CODE_ERROR.name,
                                "statusCode": resp["status"],
                                "message": f"Error in fetching lists for site {site_id}: {resp['body']['error']['message']}"
                            })
                    else:
                        failures.append({
                            "type": FailureType.NOT_FOUND.name,
                            "statusCode": None,
                            "message": f"No response found for lists API for site {req['headers']['siteId']}."
                        })

            while pending_next_items and not self.is_hard_stop_requested():
                batches = create_batches("{url}", pending_next_items, self.config.parallel_batches, True)
                
                next_futures_map: Dict[int, Future[List[Dict[str, Any]]]] = {}
                next_batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]] = {}
                idx = 0
                for batch in batches:
                    next_futures_map[idx] = self.executor.submit(self.url_invoker.invoke, GRAPH_BASE_URL, batch, self.logger, self.stop_event, self.get_resource_type())
                    next_batch_id_to_batch_map[idx] = batch
                    idx += 1
                    
                from concurrent.futures import as_completed
                
                future_to_batch_id = {future: bid for bid, future in next_futures_map.items()}
                new_pending_next_items = []
                
                for future in as_completed(next_futures_map.values()):
                    batch_id = future_to_batch_id[future]
                    responses = future.result()
                    batch = next_batch_id_to_batch_map[batch_id]
                    new_pending_next_items.extend(process_pagination_responses(batch, responses, site_to_resp_map, "siteId", GRAPH_BASE_URL, failures, False, local_progress_callback))
                    
                pending_next_items = new_pending_next_items

            total_lists = 0
            for site_id, resp in site_to_resp_map.items():
                if "body" in resp and "value" in resp["body"]:
                    total_lists += len(resp["body"]["value"])
                    self.site_to_metadata[site_id]["listCount"] = len(resp["body"]["value"])

            return total_lists
        except Exception as e:
            self._log_and_fail("Error in _get_list_count", e, failures)
            return 0

    def _get_license_metrics(
        self,
        site_discovery_progress_metrics: Dict[str, int],
        failures: List[Dict[str, str]]
    ) -> Dict[str, Any]:
        licenses = []
        url = "https://graph.microsoft.com/v1.0/subscribedSkus?$select=prepaidUnits,consumedUnits,servicePlans,appliesTo"
        token_data = self.url_invoker.token_manager.get_valid_token_slot(self.logger)
        token = token_data["token"]
        session = self.url_invoker.token_manager.get_session()
        headers = {"Authorization": f"Bearer {token}"}
        try:
            while url and not self.is_hard_stop_requested():
                # Check mid-loop for extremely long tenant scans
                if time.time() > token_data["expires_at"]:
                    self.url_invoker.token_manager.return_token_slot(token_data)
                    token_data = self.url_invoker.token_manager.get_valid_token_slot(self.logger)
                    token = token_data["token"]
                    headers = {"Authorization": f"Bearer {token}"}

                attempts = 0
                max_attempts = self.config.retries + 1
                while attempts < max_attempts and not self.is_hard_stop_requested():
                    try:
                        r = session.get(url, headers=headers, timeout=180)
                        if r.status_code != 200:
                            break
                        d = r.json()
                        break
                    except Exception as e:
                        attempts += 1
                        if attempts == max_attempts:
                            self._log_and_fail("Error in fetching licenses", e, failures)
                            break
                        elif self.logger is not None:
                            wait_time = min(10, max(2, self.config.backoff) ** (attempts - 1))
                            self.logger(f"Error in fetching licenses. Attempt count: {attempts} | Retrying in {wait_time} seconds...")
                            time.sleep(wait_time)

                all_licenses = d.get("value", [])
                sharepoint_licenses = [
                    l for l in all_licenses 
                    if any(sp.get("servicePlanType", "").lower() == "sharepoint" for sp in l.get("servicePlans", []))
                ]
                # print("Sharepoint Licenses: " + str(len(sharepoint_licenses)))

                site_discovery_progress_metrics["licenseCount"] += sum([l.get("prepaidUnits", {}).get("enabled", 0) for l in sharepoint_licenses])

                licenses.extend(sharepoint_licenses)
                url = d.get("@odata.nextLink")

        except Exception as e:
            self._log_and_fail("Error in _get_license_metrics", e, failures)
        finally:
            self.url_invoker.token_manager.return_token_slot(token_data)
        
        license_metrics = {
            "totalLicenseCount": {
                "User": 0,
                "Company": 0
            },
            "totalAllotedUnits": {
                "User": 0,
                "Company": 0
            },
            "consumedUnits": {
                "User": 0,
                "Company": 0
            }
        }
        for license in licenses:
            applies_to = license.get("appliesTo", "")
            if applies_to == "User":
                license_metrics["totalLicenseCount"]["User"] += 1
                license_metrics["consumedUnits"]["User"] += license.get("consumedUnits", 0)
                license_metrics["totalAllotedUnits"]["User"] += license.get("prepaidUnits", {}).get("enabled", 0)
            elif applies_to == "Company":
                license_metrics["totalLicenseCount"]["Company"] += 1
                license_metrics["consumedUnits"]["Company"] += license.get("consumedUnits", 0)
                license_metrics["totalAllotedUnits"]["Company"] += license.get("prepaidUnits", {}).get("enabled", 0)

        return license_metrics

    def _get_drives(
        self,
        site_ids: List[str],
        drives: List[Any],
        subsite_to_drives: Dict[str, List[Any]],
        site_discovery_progress_metrics: Dict[str, int],
        failures: List[Dict[str, str]]
    ) -> Dict[str, int]:
        try:
            def filter_personal_cache_library(batch_responses):
                if not batch_responses:
                    return
                for resp in batch_responses:
                    if "body" in resp and "value" in resp["body"] and isinstance(resp["body"]["value"], list):
                        resp["body"]["value"] = [
                            d for d in resp["body"]["value"]
                            if d.get("name") != "PersonalCacheLibrary"
                        ]

            drive_url = "/sites/{siteId}/drives?$select=id,name,driveType,webUrl&$top=999"
            batches = create_batches(drive_url, [{"siteId": site_id} for site_id in site_ids], self.config.parallel_batches, True)

            futures_map: Dict[int, Future[List[Dict[str, Any]]]] = {}
            batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]] = {}
            idx = 0
            for batch in batches:
                futures_map[idx] = self.executor.submit(self.url_invoker.invoke, GRAPH_BASE_URL, batch, self.logger, self.stop_event, self.get_resource_type())
                batch_id_to_batch_map[idx] = batch
                idx += 1

            from concurrent.futures import as_completed
            future_to_batch_id = {future: bid for bid, future in futures_map.items()}

            site_to_resp_map: Dict[str, Dict[str, Any]] = {}
            pending_next_items = []

            def local_progress_callback(responses: List, has_next=False):
                site_discovery_progress_metrics["driveCount"] += len(responses)
                self.progress_update_callback(
                    "site_discovery", 
                    count=site_discovery_progress_metrics.get("siteCount", 0), 
                    personalSiteCount=site_discovery_progress_metrics.get("personalSiteCount", 0),
                    teamSiteCount=site_discovery_progress_metrics.get("teamSiteCount", 0),
                    driveCount=site_discovery_progress_metrics.get("driveCount", 0), 
                    listCount=site_discovery_progress_metrics.get("listCount", 0), 
                    licenseCount=site_discovery_progress_metrics.get("licenseCount", 0)
                )

            for future in as_completed(futures_map.values()):
                batch_id = future_to_batch_id[future]
                responses = future.result()
                filter_personal_cache_library(responses)
                batch = batch_id_to_batch_map[batch_id]
                batch_responses_map = get_batch_responses_map(responses, self.logger)
                for req in batch:
                    req_id = req["id"]
                    if req_id in batch_responses_map:
                        resp = batch_responses_map[req_id]
                        site_id = req["headers"]["siteId"]
                        site_to_resp_map[site_id] = resp

                        if "body" in resp and "value" in resp["body"]:
                            local_progress_callback(resp["body"]["value"])

                        if "body" in resp and "@odata.nextLink" in resp["body"]:
                            next_url = resp["body"]["@odata.nextLink"]
                            relative_url = get_relative_url(next_url, GRAPH_BASE_URL)
                            pending_next_items.append({
                                "siteId": site_id,
                                "url": relative_url
                            })
                        elif "body" in resp and "error" in resp["body"]:
                            failures.append({
                                "type": FailureType.FAILURE_STATUS_CODE_ERROR.name,
                                "statusCode": resp["status"],
                                "message": f"Error in fetching drives for site {site_id}: {resp['body']['error']['message']}"
                            })
                    else:
                        failures.append({
                            "type": FailureType.NOT_FOUND.name,
                            "statusCode": None,
                            "message": f"No response found for drives API for site {req['headers']['siteId']}."
                        })

            while pending_next_items and not self.is_hard_stop_requested():
                batches = create_batches("{url}", pending_next_items, self.config.parallel_batches, True)
                
                next_futures_map: Dict[int, Future[List[Dict[str, Any]]]] = {}
                next_batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]] = {}
                idx = 0
                for batch in batches:
                    next_futures_map[idx] = self.executor.submit(self.url_invoker.invoke, GRAPH_BASE_URL, batch, self.logger, self.stop_event, self.get_resource_type())
                    next_batch_id_to_batch_map[idx] = batch
                    idx += 1
                    
                from concurrent.futures import as_completed
                future_to_batch_id = {future: bid for bid, future in next_futures_map.items()}
                    
                new_pending_next_items = []
                
                for future in as_completed(next_futures_map.values()):
                    batch_id = future_to_batch_id[future]
                    responses = future.result()
                    filter_personal_cache_library(responses)
                    batch = next_batch_id_to_batch_map[batch_id]
                    new_pending_next_items.extend(process_pagination_responses(batch, responses, site_to_resp_map, "siteId", GRAPH_BASE_URL, failures, False, local_progress_callback))
                    
                pending_next_items = new_pending_next_items

            drive_type_to_count = { "documentLibrary": 0, "personal": 0, "business": 0, "unknown": 0 }
            for site_id, resp in site_to_resp_map.items():
                if "body" in resp and "value" in resp["body"]:
                    for entry in resp["body"]["value"]:
                        self.id_to_display[entry["id"]] = entry["webUrl"]
                        if "driveType" in entry:
                            if entry["driveType"] not in drive_type_to_count:
                                drive_type_to_count[entry["driveType"]] = 0
                            drive_type_to_count[entry["driveType"]] += 1
                        else:
                            drive_type_to_count["unknown"] += 1 
                    drives.extend(resp["body"]["value"])
                    if site_id not in subsite_to_drives:
                        subsite_to_drives[site_id] = []
                    subsite_to_drives[site_id].extend([drive["id"] for drive in resp["body"]["value"]])

            return drive_type_to_count
        except Exception as e:
            self._log_and_fail("Error in _get_drives", e, failures)
            return 0, 0

    def _create_in_memory_tree(
        self, 
        drive_ids: List[str], 
        drive_discovery_progress_metrics: Dict[str, int],
        failures: List[Dict[str, str]]
    ):
        completed_drives = 0
        total_drives = len(drive_ids)
        adj_list = {}
        parent_references: Dict[str, Dict[str, str]] = {}
        resource_id_to_details: Dict[str, Dict[str, Any]] = {}
        
        for drive_id in drive_ids:
            adj_list[drive_id] = {}
            parent_references[drive_id] = {}
        try:
            # use delta api to fetch the folders
            delta_api = "/drives/{driveId}/root/delta?$select=id,parentReference,name,folder,file,remoteItem,size"
            batches = create_batches(delta_api, [{"driveId": drive_id} for drive_id in drive_ids], self.config.parallel_batches, True)

            futures_map: Dict[int, Future[List[Dict[str, Any]]]] = {}
            batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]] = {}
            idx = 0
            for batch in batches:
                futures_map[idx] = self.executor.submit(self.url_invoker.invoke, GRAPH_BASE_URL, batch, self.logger, self.stop_event, self.get_resource_type())
                batch_id_to_batch_map[idx] = batch
                idx += 1

            from concurrent.futures import as_completed
            future_to_batch_id = {future: bid for bid, future in futures_map.items()}

            drive_to_resp_map: Dict[str, Dict[str, Any]] = {}
            pending_next_items = []

            seen_ids = set()
            drive_id_to_total_size = {}

            def _is_root(resource):
                return "id" not in resource["parentReference"] and resource["name"] == "root"

            def local_progress_callback(responses: List, has_next=False):
                nonlocal completed_drives
                if not has_next:
                    completed_drives += 1
                for curr_response in responses:
                    if curr_response["id"] in seen_ids or _is_root(curr_response):
                        continue
                    seen_ids.add(curr_response["id"])
                    
                    if "folder" in curr_response:
                        drive_discovery_progress_metrics["folderCount"] += 1
                    elif "file" in curr_response:
                        drive_discovery_progress_metrics["fileCount"] += 1
                        drive_id = curr_response["parentReference"]["driveId"]
                        drive_id_to_total_size[drive_id] = drive_id_to_total_size.get(drive_id, 0) + curr_response.get("size", 0)
                    elif "remoteItem" in curr_response:
                        drive_discovery_progress_metrics["shortcutCount"] += 1
                

                self.progress_update_callback(
                    "drive_discovery",
                    count=completed_drives,
                    total_drives=total_drives,
                    fileCount=drive_discovery_progress_metrics.get("fileCount", 0),
                    folderCount=drive_discovery_progress_metrics.get("folderCount", 0),
                    shortcutCount=drive_discovery_progress_metrics.get("shortcutCount", 0)
                )

            for future in as_completed(futures_map.values()):
                batch_id = future_to_batch_id[future]
                responses = future.result()
                batch = batch_id_to_batch_map[batch_id]
                batch_responses_map = get_batch_responses_map(responses, self.logger)
                for req in batch:
                    req_id = req["id"]
                    if req_id in batch_responses_map:
                        resp = batch_responses_map[req_id]
                        drive_id = req["headers"]["driveId"]
                        drive_to_resp_map[drive_id] = resp

                        if "body" in resp and "value" in resp["body"]:
                            has_next = "@odata.nextLink" in resp["body"]
                            local_progress_callback(resp["body"]["value"], has_next)

                        if "body" in resp and "@odata.nextLink" in resp["body"]:
                            next_url = resp["body"]["@odata.nextLink"]
                            relative_url = get_relative_url(next_url, GRAPH_BASE_URL)
                            pending_next_items.append({
                                "driveId": drive_id,
                                "url": relative_url
                            })
                        elif "body" in resp and "error" in resp["body"]:
                            failures.append({
                                "type": FailureType.FAILURE_STATUS_CODE_ERROR.name,
                                "statusCode": resp["status"],
                                "message": f"Error in fetching delta for drive {drive_id}: {resp['body']['error']['message']}"
                            })
                    else:
                        failures.append({
                            "type": FailureType.NOT_FOUND.name,
                            "statusCode": None,
                            "message": f"No response found for delta API for drive {req['headers']['driveId']}."
                        })

            while pending_next_items and not self.is_hard_stop_requested():
                batches = create_batches("{url}", pending_next_items, self.config.parallel_batches, True)
                
                next_futures_map: Dict[int, Future[List[Dict[str, Any]]]] = {}
                next_batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]] = {}
                idx = 0
                for batch in batches:
                    next_futures_map[idx] = self.executor.submit(self.url_invoker.invoke, GRAPH_BASE_URL, batch, self.logger, self.stop_event, self.get_resource_type())
                    next_batch_id_to_batch_map[idx] = batch
                    idx += 1
                    
                from concurrent.futures import as_completed
                future_to_batch_id = {future: bid for bid, future in next_futures_map.items()}
                    
                new_pending_next_items = []
                
                for future in as_completed(next_futures_map.values()):
                    batch_id = future_to_batch_id[future]
                    responses = future.result()
                    batch = next_batch_id_to_batch_map[batch_id]
                    new_pending_next_items.extend(process_pagination_responses(batch, responses, drive_to_resp_map, "driveId", GRAPH_BASE_URL, failures, False, local_progress_callback))
                    
                pending_next_items = new_pending_next_items

            # 1. First populate resource details and parent references
            for drive_id, resp in drive_to_resp_map.items():
                if "body" in resp and "value" in resp["body"]:
                    for file in resp["body"]["value"]:
                        resource_id_to_details[file["id"]] = file
                        
                        if "parentReference" in file and "id" in file["parentReference"]:
                            parent_references[drive_id][file["id"]] = file["parentReference"]["id"]
                            
            # 2. Now build adj_list from parent_references to guarantee a true tree!
            for drive_id in drive_ids:
                for child_id, parent_id in parent_references[drive_id].items():
                    if parent_id not in adj_list[drive_id]:
                        adj_list[drive_id][parent_id] = []
                    adj_list[drive_id][parent_id].append(child_id)

            return adj_list, parent_references, resource_id_to_details, drive_id_to_total_size
        except Exception as e:
            self._log_and_fail(f"Error in _create_in_memory_tree", e, failures)
            return {}, {}, {}

    def _calculate_drive_metrics(
        self, 
        drive_ids: List[str], 
        drive_id_to_adj_list: Dict[str, List[str]], 
        parent_references: Dict[str, Dict[str, str]], 
        resource_id_to_details: Dict[str, Dict[str, Any]],
        failures: List[Dict[str, str]]
    ) -> Dict[str, Any]:

        drive_metrics = self._calculate_metrics_using_upside_down_parsing(
            drive_ids,
            drive_id_to_adj_list,
            parent_references,
            resource_id_to_details,
            failures
        )

        additional_metrics = self._calculate_metrics_using_regular_parsing(
            drive_ids,
            drive_id_to_adj_list,
            parent_references,
            resource_id_to_details,
            failures
        )

        for drive_id, metrics in additional_metrics.items():
            if drive_id not in drive_metrics:
                drive_metrics[drive_id] = metrics
            else:
                drive_metrics[drive_id].update(metrics)
        
        return drive_metrics

    def _calculate_metrics_using_regular_parsing(
        self, 
        drive_ids: List[str], 
        drive_id_to_adj_list: Dict[str, List[str]], 
        parent_references: Dict[str, Dict[str, str]], 
        resource_id_to_details: Dict[str, Dict[str, Any]],
        failures: List[Dict[str, str]]
    ) -> Dict[str, Any]:
        roots = {}

        def _is_root(drive_id, node):
            if node not in parent_references[drive_id].keys():
                return False                # We want to avoid implicit roots due to duplicate key issues
            
            parent = parent_references[drive_id][node]
            parent_resource = resource_id_to_details.get(parent, {})
            return "id" not in parent_resource.get("parentReference", {})

        for drive_id in drive_ids:
            for node1, node2 in parent_references[drive_id].items():
                if _is_root(drive_id, node1):
                    if drive_id not in roots:
                        roots[drive_id] = []

                    roots[drive_id].append(node1)

        active_thread_count = AtomicInt(0)
        additional_metrics = {}
        for drive_id in drive_ids:
            additional_metrics[drive_id] = {
                "folderCountExceedingDepthLimit": AtomicInt(0),
                "fileCountExceedingDepthLimit": AtomicInt(0)
            }
        
        def _dfs(drive_id, node, current_depth = 1):
            try:
                if current_depth > self.config.max_allowed_depth:
                    resource_details = resource_id_to_details[node]
                    if "folder" in resource_details:
                        additional_metrics[drive_id]["folderCountExceedingDepthLimit"].increment()
                    else:
                        additional_metrics[drive_id]["fileCountExceedingDepthLimit"].increment()
                        
                children = set(drive_id_to_adj_list[drive_id].get(node, []))
                for child in children:
                    active_thread_count.increment()
                    self.tree_executor.submit(_dfs, drive_id, child, current_depth + 1)
            finally:
                active_thread_count.decrement()
                with self.condition:
                    self.condition.notify_all()
        
        for drive_id in drive_ids:
            for root in roots.get(drive_id, []):
                active_thread_count.increment()
                self.tree_executor.submit(_dfs, drive_id, root)

        
        while active_thread_count.get_value() > 0:
            with self.condition:
                self.condition.wait()

        for drive_id in additional_metrics:
            additional_metrics[drive_id]["folderCountExceedingDepthLimit"] = additional_metrics[drive_id]["folderCountExceedingDepthLimit"].get_value()
            additional_metrics[drive_id]["fileCountExceedingDepthLimit"] = additional_metrics[drive_id]["fileCountExceedingDepthLimit"].get_value()

        return additional_metrics

    def _calculate_metrics_using_upside_down_parsing(
        self, 
        drive_ids: List[str], 
        drive_id_to_adj_list: Dict[str, List[str]], 
        parent_references: Dict[str, Dict[str, str]], 
        resource_id_to_details: Dict[str, Dict[str, Any]],
        failures: List[Dict[str, str]]
    ) -> Dict[str, Any]:

        # print("Inside _calculate_metrics_using_upside_down_parsing")
        drive_metrics = {}
        for drive_id in drive_ids:
            drive_metrics[drive_id] = {
                "maxEffectiveDepth": 0,
                "folderCount": 0,
                "fileCount": 0,
                "shortcutCount": 0,
                "fileSizeDistribution": {"buckets": []},
                "largeResources": []
            }

            for size_range in self.config.bucket_ranges:
                drive_metrics[drive_id]["fileSizeDistribution"]["buckets"].append({
                    "sizeRange": size_range,
                    "count": 0
                })
        
        resource_metrics = ThreadSafeMap()

        try:
            dependency_set = ThreadSafeSortedSet()
            resource_to_dependency_count = ThreadSafeMap()

            for drive_id in drive_ids:
                if drive_id in parent_references:
                    edges = parent_references[drive_id]
                    for resource_id, parent_id in edges.items():
                        curr_value = resource_to_dependency_count.get(parent_id, 0)
                        resource_to_dependency_count.update(parent_id, curr_value + 1)
                        if not resource_to_dependency_count.contains(resource_id):
                            resource_to_dependency_count.update(resource_id, 0)             # To ensure the map accounts for all the nodes in the tree
            active_thread_count = AtomicInt(0)

            leaves = []
            for resource_id, count in resource_to_dependency_count.get_all().items():
                if count == 0:
                    leaves.append(resource_id)
                else:
                    dependency_set.add((count, resource_id))

            # print(f"Leaf Size: {len(leaves)}")
            for leaf_id in leaves:
                try:
                    active_thread_count.increment()
                    self.tree_executor.submit(self._extract_metrics_from_subtrees, leaf_id, drive_id_to_adj_list, parent_references, resource_id_to_details, dependency_set, resource_to_dependency_count, resource_metrics, drive_metrics, active_thread_count)
                except Exception as e:
                    active_thread_count.decrement()
                    self._log_and_fail(f"Error while submitting to executor in _calculate_drive_metrics", e, failures)
            
            with self.condition:
                while active_thread_count.get_value() > 0:
                    self.condition.wait()
            
            return drive_metrics
        except Exception as e:
            self._log_and_fail(f"Error in _calculate_drive_metrics for drive {drive_id}", e, failures)
            
        return drive_metrics
    
    def _extract_metrics_from_subtrees(
        self, 
        resource_id: str,
        drive_id_to_adj_list: Dict[str, Dict[str, List[str]]],
        parent_references: Dict[str, Dict[str, str]],
        resource_id_to_details: Dict[str, Dict[str, Any]],
        dependency_set: ThreadSafeSortedSet,
        resource_to_dependency_count: ThreadSafeMap,
        resource_metrics: Dict[str, Any],
        drive_metrics: Dict[str, Dict[str, Any]],
        active_thread_count: AtomicInt
    ):       
        try:
            resource = resource_id_to_details[resource_id]

            # Root folder. Skipping it as it is an implicit folder added by default with common ID across multiple drives.
            if "id" not in resource["parentReference"]:
                return

            drive_id = resource["parentReference"]["driveId"]
            is_resource_folder = "folder" in resource

            subtree_count = 0
            max_depth = 0

            if is_resource_folder and resource["id"] in drive_id_to_adj_list[drive_id]:             # Check for empty folders
                for child_id in drive_id_to_adj_list[drive_id][resource["id"]]:
                    child_metrics = resource_metrics.get(child_id, None)
                    if child_metrics:
                        subtree_count += child_metrics["subTreeCount"]
                        max_depth = max(max_depth, child_metrics["maxDepth"] + 1)

            subtree_count += 1

            resource_metrics.update(resource["id"], {
                "subTreeCount": subtree_count,
                "maxDepth": max_depth
            })

            self._update_drive_metrics_from_resource(resource, resource_metrics.get(resource_id, {}), drive_metrics[drive_id])

            parent_resource_id = parent_references[drive_id].get(resource_id)

            if not parent_resource_id:
                return

            with self.condition:
                dependency_count_of_par = resource_to_dependency_count.get(parent_resource_id, 0)
                dependency_set.remove((dependency_count_of_par, parent_resource_id))

                dependency_count_of_par -= 1
                resource_to_dependency_count.update(parent_resource_id, dependency_count_of_par)

                if dependency_count_of_par > 0:
                    dependency_set.add((dependency_count_of_par, parent_resource_id))
                else:
                    self.tree_executor.submit(self._extract_metrics_from_subtrees, parent_resource_id, drive_id_to_adj_list, parent_references, resource_id_to_details, dependency_set, resource_to_dependency_count, resource_metrics, drive_metrics, active_thread_count)
                    active_thread_count.increment()

        except Exception as e:
            self._log_and_fail(f"Error while extracting metrics from subtrees for resource {resource_id}", e, failures)
        finally:
            active_thread_count.decrement()
            with self.condition:
                self.condition.notify_all()

    def _update_drive_metrics_from_resource(
        self,
        resource: Dict[str, Any],
        resource_metric: Dict[str, Any],
        drive_metric: Dict[str, Any]
    ):
        with self.condition:
            # Update max depth
            drive_metric["maxEffectiveDepth"] = max(drive_metric["maxEffectiveDepth"], resource_metric["maxDepth"])
            
            # Update shortcut count
            if "remoteItem" in resource:
                drive_metric["shortcutCount"] += 1
            elif "folder" in resource:
                drive_metric["folderCount"] += 1
            elif "file" in resource:
                drive_metric["fileCount"] += 1
            
            # Update file size distribution if it's a file
            # TODO Check if we need folders here as well
            if "folder" not in resource:
                size_in_kb = resource.get("size", 0) / 1024 # assuming size in bytes
                for bucket in drive_metric["fileSizeDistribution"]["buckets"]:
                    low, high = bucket["sizeRange"]
                    if low <= size_in_kb and size_in_kb <= high:
                        bucket["count"] += 1
                        break
                        
            # Update large resources
            if resource_metric["subTreeCount"] >= self.config.large_resource_count_limit:
                drive_metric["largeResources"].append({
                    "type": ResourceType.FOLDER.value if "folder" in resource else ResourceType.FILE.value,
                    "id": resource["name"],
                    "subTreeCount": resource_metric["subTreeCount"],
                    "Limit": self.config.large_resource_count_limit
                })

    def _log_and_fail(self, message: str, e: Exception, failures: List[Dict[str, str]]):
        if self.logger:
            self.logger(f"{message}: {e}")
        failures.append({
            "type": FailureType.UNKNOWN_ERROR.name,
            "statusCode": 500,
            "message": f"{message}: {str(e)}"
        })

    def shutdown(self):
        self.executor.shutdown(wait=False)
        self.tree_executor.shutdown(wait=False)

    def _get_sites_for_users(self, email_ids: List[str], site_discovery_progress_metrics: Dict[str, Any]) -> Dict[str, str]:
        from concurrent.futures import as_completed
        
        mail_to_top_level_site = {}
        token_data = self.url_invoker.token_manager.get_valid_token_slot(self.logger)
        token = token_data["token"]
        session = self.url_invoker.token_manager.get_session()
        headers = {"Authorization": f"Bearer {token}"}
        
        def fetch_drives_ids(email):
            try:
                url = f"{GRAPH_BASE_URL}/users/{email}/drives?$select=sharePointIds"
                
                attempts = 0
                max_attempts = self.config.retries + 1
                while attempts < max_attempts and not self.is_hard_stop_requested():
                    try:
                        r = session.get(url, headers=headers, timeout=60)
                        if r.status_code == 200:
                            return email, r.json()
                        elif r.status_code == 404:
                            return email, None
                        else:
                            raise Exception(f"Graph API returned status {r.status_code}")
                    except Exception as e:
                        attempts += 1
                        if attempts == max_attempts:
                            break
                        wait_time = min(10, max(2, self.config.backoff) ** (attempts - 1))
                        time.sleep(wait_time)
            except Exception as e:
                pass
            return email, None

        try:
            future_to_email = {self.executor.submit(fetch_drives_ids, email): email for email in email_ids}
            
            for future in as_completed(future_to_email):
                email = future_to_email[future]
                upn, response_data = future.result()
                
                if response_data and "value" in response_data:
                    site_discovery_progress_metrics["siteCount"] += 1
                    site_discovery_progress_metrics["personalSiteCount"] += 1
                    drives_list = response_data["value"]
                    for drive in drives_list:
                        sp_ids = drive.get("sharePointIds")
                        if sp_ids and "siteId" in sp_ids and "siteUrl" in sp_ids:
                            site_id = sp_ids["siteId"]
                            site_url = sp_ids["siteUrl"]
                            
                            self.id_to_display[site_id] = site_url
                            mail_to_top_level_site[upn] = site_id
                            break
                            
                    self.progress_update_callback(
                        "site_discovery",
                        count=site_discovery_progress_metrics["siteCount"],
                        personalSiteCount=site_discovery_progress_metrics["personalSiteCount"],
                        teamSiteCount=site_discovery_progress_metrics["teamSiteCount"]
                    )
        finally:
            self.url_invoker.token_manager.return_token_slot(token_data)
            
        return mail_to_top_level_site

    def _get_sites_from_urls(self, site_urls: List[str], site_discovery_progress_metrics: Dict[str, Any], failures: List[Dict[str, str]]) -> Dict[str, str]:
        from concurrent.futures import as_completed
        import urllib.parse
        
        url_to_site_id = {}
        token_data = self.url_invoker.token_manager.get_valid_token_slot(self.logger)
        token = token_data["token"]
        session = self.url_invoker.token_manager.get_session()
        headers = {"Authorization": f"Bearer {token}"}
        
        def fetch_site_id(site_url):
            try:
                parsed = urllib.parse.urlparse(site_url)
                hostname = parsed.netloc
                path = parsed.path
                if path.endswith("/"):
                    path = path[:-1]
                
                url = f"{GRAPH_BASE_URL}/sites/{hostname}:{path}?$select=id,isPersonalSite"
                
                attempts = 0
                max_attempts = self.config.retries + 1
                while attempts < max_attempts and not self.is_hard_stop_requested():
                    try:
                        r = session.get(url, headers=headers, timeout=60)
                        if r.status_code == 200:
                            return site_url, r.json()
                        elif r.status_code == 404:
                            return site_url, None
                        else:
                            raise Exception(f"Graph API returned status {r.status_code}")
                    except Exception as e:
                        attempts += 1
                        if attempts == max_attempts:
                            break
                        wait_time = min(10, max(2, self.config.backoff) ** (attempts - 1))
                        time.sleep(wait_time)
            except Exception as e:
                pass
            return site_url, None

        try:
            future_to_url = {self.executor.submit(fetch_site_id, u): u for u in site_urls}
            
            for future in as_completed(future_to_url):
                site_url = future_to_url[future]
                url, response_data = future.result()
                
                if response_data and "id" in response_data:
                    site_id = response_data["id"]
                    is_personal = response_data.get("isPersonalSite", False)
                    url_to_site_id[url] = site_id
                    
                    self.site_to_metadata[site_id] = {"isPersonalSite": is_personal}
                    if is_personal:
                        site_discovery_progress_metrics["personalSiteCount"] += 1
                    else:
                        site_discovery_progress_metrics["teamSiteCount"] += 1
                    site_discovery_progress_metrics["siteCount"] += 1
                    
                    self.id_to_display[site_id] = site_url
                else:
                    failures.append({
                        "siteUrl": site_url,
                        "type": FailureType.NOT_FOUND.name,
                        "message": "Site collection not found."
                    })
                    
                self.progress_update_callback(
                    "site_discovery",
                    count=site_discovery_progress_metrics["siteCount"],
                    personalSiteCount=site_discovery_progress_metrics["personalSiteCount"],
                    teamSiteCount=site_discovery_progress_metrics["teamSiteCount"]
                )
        finally:
            self.url_invoker.token_manager.return_token_slot(token_data)
            
        return url_to_site_id
