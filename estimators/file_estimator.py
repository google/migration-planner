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
            
            drives = []
            subsite_to_drives = {}          # used to calculate effective max Depth
            metrics = { 
                "driveMetrics": {},
                "siteMetrics": {},
                "maxEffectiveDepth": 0,
                "maxFolderDepth": 0,        # only includes depth from folders in drives
                "maxSubsiteDepth": 0,       # only includes depth of subsites
                "subsiteCount": 0,
                "shortcutCount": 0,
                "listCount": 0,
                "licenseMetrics": {},
                "driveCounts": {
                    "documentLibrary": 0,
                    "personal": 0,
                    "business": 0,
                },
                "tenantLevelFileSizeDistribution": {
                    "buckets": []
                },
                "tenantLevelLargeResources": []
            }
            if "drives" in data and len(data["drives"]) > 0:
                # One Drive Flow
                drives = data["drives"]
            else:
                # Sharepoint Flow
                site_discovery_progress_metrics = {
                    "listCount": 0,
                    "licenseCount": 0,
                    "driveCount": 0,
                }
                self.progress_update_callback("site_discovery", status="Fetching...", count=0)
                subsite_count = self._get_subsite_metrics_and_drives(metrics, drives, subsite_to_drives, site_discovery_progress_metrics, failures)
                self.logger("Site Scanning is finished!!!!")
                self.progress_update_callback("site_discovery", status="Done", count=subsite_count, **site_discovery_progress_metrics)

            # get adjacency lists and parent references for each drive
            self.progress_update_callback("drive_discovery", status="Fetching...", count=0)
            drive_discovery_progress_metrics = {
                "folderCount": 0,
                "fileCount": 0,
                "shortcutCount": 0
            }
            drive_id_to_adj_list, parent_references, resource_id_to_details = self._create_in_memory_tree([drive["id"] for drive in drives], drive_discovery_progress_metrics, failures)
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

                    for drive in batch:
                        total_resource_count += len(parent_references[drive["id"]]) + 1
                    prog = processed / total_drives if total_drives > 0 else 0
                    self.progress_update_callback(
                        "scan_progress",
                        source="drive_parsing",
                        progress=prog,
                        cumulative=total_resource_count,
                        processed=processed,
                        failed=failed,
                        success=success,
                        entity_type="Drives"
                    )
                    time.sleep(0.2)                     # TODO Just for testing if progress proceeds smoothly. Remove before merging
                except Exception as e:
                    failed += len(batch)
                    processed += len(batch)
                    prog = processed / total_drives if total_drives > 0 else 0
                    for drive in batch:
                        total_resource_count += len(parent_references[drive["id"]]) + 1
                        
                    self.progress_update_callback(
                        "scan_progress",
                        source="drive_parsing",
                        progress=prog,
                        cumulative=total_resource_count,
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
            self._update_tenant_metrics_from_drive_metrics(metrics, subsite_to_drives)
            self.progress_update_callback("phase_status", source="plan_generation", status="complete")

            return metrics
            
        except Exception as e:
            if self.logger:
                self.logger(f"Error in calculate_resource_metrics: {e}")
            failures.append({
                "type": FailureType.UNKNOWN_ERROR,
                "statusCode": 500,
                "message": f"Exception in calculate_resource_metrics: {str(e)}"
            })
            return {}

    def _update_tenant_metrics_from_drive_metrics(
        self,
        metrics: Dict[str, Any],
        subsite_to_drives: Dict[str, List[Any]]
    ):
        self.progress_update_callback(
            "scan_progress",
            source="plan_generation",
            progress=0.33,
            extra_text="Calculating metrics...",
        )
        for drive_metric in metrics["driveMetrics"].values():
            metrics["maxEffectiveDepth"] = max(metrics["maxEffectiveDepth"], drive_metric["maxEffectiveDepth"])
            metrics["maxFolderDepth"] = max(metrics["maxFolderDepth"], drive_metric["maxEffectiveDepth"])
            metrics["shortcutCount"] += drive_metric.get("shortcutCount", 0)
        
        for subsite_id, drive_ids in subsite_to_drives.items():
            metrics["maxSubsiteDepth"] = max(metrics["maxSubsiteDepth"], metrics["siteMetrics"][subsite_id]["siteLevel"])
            for drive_id in drive_ids:
                metrics["maxEffectiveDepth"] = max(metrics["maxEffectiveDepth"], metrics["siteMetrics"][subsite_id]["siteLevel"] + metrics["driveMetrics"][drive_id]["maxEffectiveDepth"])  

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
        
        self.progress_update_callback(
            "scan_progress",
            source="plan_generation",
            progress=1,
            extra_text="Calculated metrics for all drives...",
        )

    def _get_subsite_metrics_and_drives(
        self,
        tenant_metrics: Dict[str, Any],
        drives: List[Any],
        subsite_to_drives: Dict[str, List[Any]],
        site_discovery_progress_metrics: Dict[str, int],
        failures: List[Dict[str, str]]
    ) -> int:
        try:
            # Fetch the root first without batching
            manager = self.url_invoker.token_manager

            url = f"{GRAPH_BASE_URL}/sites/root"
            token_data = manager.get_valid_token_slot(self.logger)
            token = token_data["token"]
            session = manager.get_session()
            headers = {
                "Authorization": f"Bearer {token}"
            }

            attempts = 0
            max_attempts = self.config.retries + 1
            while attempts < max_attempts:
                try:
                    r = session.get(url, headers=headers)
                    if r.status_code != 200:
                        raise Exception(f"Error in fetching root site : {r.status_code}")
                    root_site = r.json()
                    break
                except Exception as e:
                    attempts += 1
                    if attempts == max_attempts:
                        self._log_and_fail("Error in fetching root site", e, failures)
                        return
                    elif self.logger is not None:
                        wait_time = min(10, max(2, self.config.backoff) ** (attempts - 1))
                        self.logger(f"Error in fetching root site. Attempt count: {attempts} | Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)

            root_id = root_site["id"]
            all_sites = [{"siteId": root_id, "siteLevel": 0}]
            
            # Crawl all the subsites and collect them
            self._get_subsites_in_site([root_id], all_sites, failures)

            all_site_ids = [site["siteId"] for site in all_sites]
            for site_detail in all_sites:
                tenant_metrics["siteMetrics"][site_detail["siteId"]] = {
                    "siteLevel": site_detail["siteLevel"]
                }
            tenant_metrics["subsite_count"] = len(all_site_ids)
            site_discovery_progress_metrics["siteCount"] = len(all_site_ids)
            self._append_tenant_level_metrics(all_site_ids, tenant_metrics, drives, subsite_to_drives, site_discovery_progress_metrics, failures)

            return len(all_sites)

        except Exception as e:
            self._log_and_fail("Error in _calculate_site_metrics", e, failures)

    
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
            tenant_metrics["listCount"] = self._get_list_count(site_ids, site_discovery_progress_metrics, failures)
            tenant_metrics["licenseMetrics"] = self._get_license_metrics(site_discovery_progress_metrics, failures)
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
        site_discovery_progress_metrics: Dict[str, int],
        failures: List[Dict[str, str]]
    ) -> int:
        try:
            list_url = "/sites/{siteId}/lists"
            batches = create_batches(list_url, [{"siteId": site_id} for site_id in site_ids], self.config.parallel_batches, True)

            futures_map: Dict[int, Future[List[Dict[str, Any]]]] = {}
            batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]] = {}
            idx = 0
            for batch in batches:
                futures_map[idx] = self.executor.submit(self.url_invoker.invoke, GRAPH_BASE_URL, batch, self.logger, self.stop_event, self.get_resource_type())
                batch_id_to_batch_map[idx] = batch
                idx += 1

            response_map: Dict[int, List[Dict[str, Any]]] = {}
            for batch_id, future in futures_map.items():
                response_map[batch_id] = future.result()

            site_to_resp_map: Dict[str, Dict[str, Any]] = {}
            pending_next_items = []

            def local_progress_callback(responses: List):
                site_discovery_progress_metrics["listCount"] += len(responses)
                self.progress_update_callback(
                    "site_discovery",
                    count=site_discovery_progress_metrics.get("siteCount", 0),
                    listCount=site_discovery_progress_metrics.get("listCount", 0),
                    driveCount=site_discovery_progress_metrics.get("driveCount", 0),
                    licenseCount=site_discovery_progress_metrics.get("licenseCount", 0)
                )

            for batch_id, responses in response_map.items():
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
                                "message": f"Error in fetching lists for site {site_id}: {resp['body']['error']['message']}"
                            })
                    else:
                        failures.append({
                            "type": FailureType.NOT_FOUND,
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
                    
                next_response_map: Dict[int, List[Dict[str, Any]]] = {}
                for batch_id, future in next_futures_map.items():
                    next_response_map[batch_id] = future.result()
                    
                new_pending_next_items = []
                
                for batch_id, responses in next_response_map.items():
                    batch = next_batch_id_to_batch_map[batch_id]
                    new_pending_next_items.extend(process_pagination_responses(batch, responses, site_to_resp_map, "siteId", GRAPH_BASE_URL, failures, False, local_progress_callback))
                    
                pending_next_items = new_pending_next_items

            total_lists = 0
            for site_id, resp in site_to_resp_map.items():
                if "body" in resp and "value" in resp["body"]:
                    total_lists += len(resp["body"]["value"])

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
        url = "https://graph.microsoft.com/v1.0/subscribedSkus?$select=consumedUnits,appliesTo"
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

                r = session.get(url, headers=headers)
                if r.status_code != 200:
                    break
                d = r.json()

                site_discovery_progress_metrics["licenseCount"] += len(d.get("value", []))
                self.progress_update_callback(
                    "site_discovery",
                    count=site_discovery_progress_metrics.get("siteCount", 0),
                    listCount=site_discovery_progress_metrics.get("listCount", 0),
                    driveCount=site_discovery_progress_metrics.get("driveCount", 0),
                    licenseCount=site_discovery_progress_metrics.get("licenseCount", 0)
                )

                licenses.extend(d.get("value", []))
                url = d.get("@odata.nextLink")

        except Exception as e:
            self._log_and_fail("Error in _get_license_metrics", e, failures)
            return {}
        finally:
            self.url_invoker.token_manager.return_token_slot(token_data)
        
        # TODO Check what all other metrics are required

        license_metrics = {
            "totalLicenseCount": {
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
            elif applies_to == "Company":
                license_metrics["totalLicenseCount"]["Company"] += 1
                license_metrics["consumedUnits"]["Company"] += license.get("consumedUnits", 0)

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
            drive_url = "/sites/{siteId}/drives?$select=id,driveType"
            batches = create_batches(drive_url, [{"siteId": site_id} for site_id in site_ids], self.config.parallel_batches, True)

            futures_map: Dict[int, Future[List[Dict[str, Any]]]] = {}
            batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]] = {}
            idx = 0
            for batch in batches:
                futures_map[idx] = self.executor.submit(self.url_invoker.invoke, GRAPH_BASE_URL, batch, self.logger, self.stop_event, self.get_resource_type())
                batch_id_to_batch_map[idx] = batch
                idx += 1

            response_map: Dict[int, List[Dict[str, Any]]] = {}
            for batch_id, future in futures_map.items():
                response_map[batch_id] = future.result()

            site_to_resp_map: Dict[str, Dict[str, Any]] = {}
            pending_next_items = []

            def local_progress_callback(responses: List):
                site_discovery_progress_metrics["driveCount"] += len(responses)
                self.progress_update_callback(
                    "site_discovery",
                    count=site_discovery_progress_metrics.get("siteCount", 0),
                    listCount=site_discovery_progress_metrics.get("listCount", 0),
                    driveCount=site_discovery_progress_metrics.get("driveCount", 0),
                    licenseCount=site_discovery_progress_metrics.get("licenseCount", 0)
                )

            for batch_id, responses in response_map.items():
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
                                "message": f"Error in fetching drives for site {site_id}: {resp['body']['error']['message']}"
                            })
                    else:
                        failures.append({
                            "type": FailureType.NOT_FOUND,
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
                    
                next_response_map: Dict[int, List[Dict[str, Any]]] = {}
                for batch_id, future in next_futures_map.items():
                    next_response_map[batch_id] = future.result()
                    
                new_pending_next_items = []
                
                for batch_id, responses in next_response_map.items():
                    batch = next_batch_id_to_batch_map[batch_id]
                    new_pending_next_items.extend(process_pagination_responses(batch, responses, site_to_resp_map, "siteId", GRAPH_BASE_URL, failures, False, local_progress_callback))
                    
                pending_next_items = new_pending_next_items

            drive_type_to_count = { "documentLibrary": 0, "personal": 0, "business": 0, "unknown": 0 }
            for site_id, resp in site_to_resp_map.items():
                if "body" in resp and "value" in resp["body"]:
                    for entry in resp["body"]["value"]:
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

    def _get_subsites_in_site(
        self,
        site_ids: List[str],
        all_sites: List[Dict[str, Any]],
        failures: List[Dict[str, str]],
        level: int = 1
    ):
        try:
            site_url = "/sites/{siteId}/sites"
            batches = create_batches(site_url, [{"siteId": site_id} for site_id in site_ids], self.config.parallel_batches, True)

            futures_map: Dict[int, Future[List[Dict[str, Any]]]] = {}
            batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]] = {}
            idx = 0
            for batch in batches:
                futures_map[idx] = self.executor.submit(self.url_invoker.invoke, GRAPH_BASE_URL, batch, self.logger, self.stop_event, self.get_resource_type())
                batch_id_to_batch_map[idx] = batch
                idx += 1

            response_map: Dict[int, List[Dict[str, Any]]] = {}
            for batch_id, future in futures_map.items():
                response_map[batch_id] = future.result()

            site_to_resp_map: Dict[str, Dict[str, Any]] = {}
            pending_next_items = []

            for batch_id, responses in response_map.items():
                batch = batch_id_to_batch_map[batch_id]
                batch_responses_map = get_batch_responses_map(responses, self.logger)
                for req in batch:
                    req_id = req["id"]
                    if req_id in batch_responses_map:
                        resp = batch_responses_map[req_id]
                        site_id = req["headers"]["siteId"]
                        site_to_resp_map[site_id] = resp

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
                    
                next_response_map: Dict[int, List[Dict[str, Any]]] = {}
                for batch_id, future in next_futures_map.items():
                    next_response_map[batch_id] = future.result()
                    
                new_pending_next_items = []
                
                for batch_id, responses in next_response_map.items():
                    batch = next_batch_id_to_batch_map[batch_id]
                    new_pending_next_items.extend(process_pagination_responses(batch, responses, site_to_resp_map, "siteId", GRAPH_BASE_URL, failures, False))
                    
                pending_next_items = new_pending_next_items

            new_sub_site_ids = []
            for site_id, resp in site_to_resp_map.items():
                if "body" in resp and "value" in resp["body"]:
                    for site in resp["body"]["value"]:
                        all_sites.append({"siteId": site["id"], "siteLevel": level})
                        new_sub_site_ids.append(site["id"])
                        self.progress_update_callback("site_discovery", status="Scanning Subsites...", count=len(all_sites))

            if new_sub_site_ids:
                self._get_subsites_in_site(new_sub_site_ids, all_sites, failures, level + 1)

        except Exception as e:
            self._log_and_fail("Error in _get_subsites_in_site", e, failures)

    def _create_in_memory_tree(
        self, 
        drive_ids: List[str], 
        drive_discovery_progress_metrics: Dict[str, int],
        failures: List[Dict[str, str]]
    ):
        unique_drives = set()
        folder_count = 0
        adj_list = {}
        parent_references: Dict[str, Dict[str, str]] = {}
        resource_id_to_details: Dict[str, Dict[str, Any]] = {}
        try:
            # use delta api to fetch the folders
            delta_api = "drives/{driveId}/root/delta"
            batches = create_batches(delta_api, [{"driveId": drive_id} for drive_id in drive_ids], self.config.parallel_batches, True)

            futures_map: Dict[int, Future[List[Dict[str, Any]]]] = {}
            batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]] = {}
            idx = 0
            for batch in batches:
                futures_map[idx] = self.executor.submit(self.url_invoker.invoke, GRAPH_BASE_URL, batch, self.logger, self.stop_event, self.get_resource_type())
                batch_id_to_batch_map[idx] = batch
                idx += 1

            response_map: Dict[int, List[Dict[str, Any]]] = {}
            for batch_id, future in futures_map.items():
                response_map[batch_id] = future.result()

            drive_to_resp_map: Dict[str, Dict[str, Any]] = {}
            pending_next_items = []

            def local_progress_callback(responses):
                for curr_response in responses:
                    if "folder" in curr_response:
                        drive_discovery_progress_metrics["folderCount"] += 1
                    elif "file" in curr_response:
                        drive_discovery_progress_metrics["fileCount"] += 1
                    elif "remoteItem" in curr_response:
                        drive_discovery_progress_metrics["shortcutCount"] += 1
                

                self.progress_update_callback(
                    "drive_discovery",
                    count=len(unique_drives),
                    fileCount=drive_discovery_progress_metrics.get("fileCount", 0),
                    folderCount=drive_discovery_progress_metrics.get("folderCount", 0),
                    shortcutCount=drive_discovery_progress_metrics.get("shortcutCount", 0)
                )

            for batch_id, responses in response_map.items():
                batch = batch_id_to_batch_map[batch_id]
                batch_responses_map = get_batch_responses_map(responses, self.logger)
                for req in batch:
                    req_id = req["id"]
                    if req_id in batch_responses_map:
                        resp = batch_responses_map[req_id]
                        drive_id = req["headers"]["driveId"]
                        drive_to_resp_map[drive_id] = resp

                        if "body" in resp and "value" in resp["body"]:
                            unique_drives.add(drive_id)
                            local_progress_callback(resp["body"]["value"])

                        if "body" in resp and "@odata.nextLink" in resp["body"]:
                            next_url = resp["body"]["@odata.nextLink"]
                            relative_url = get_relative_url(next_url, GRAPH_BASE_URL)
                            pending_next_items.append({
                                "driveId": drive_id,
                                "url": relative_url
                            })
                            folder_count += len(resp["body"].get("value", []))
                        elif "body" in resp and "error" in resp["body"]:
                            failures.append({
                                "type": FailureType.FAILURE_STATUS_CODE_ERROR,
                                "statusCode": resp["status"],
                                "message": f"Error in fetching delta for drive {drive_id}: {resp['body']['error']['message']}"
                            })
                    else:
                        failures.append({
                            "type": FailureType.NOT_FOUND,
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
                    
                next_response_map: Dict[int, List[Dict[str, Any]]] = {}
                for batch_id, future in next_futures_map.items():
                    next_response_map[batch_id] = future.result()
                    
                new_pending_next_items = []
                
                for batch_id, responses in next_response_map.items():
                    batch = next_batch_id_to_batch_map[batch_id]
                    new_pending_next_items.extend(process_pagination_responses(batch, responses, drive_to_resp_map, "driveId", GRAPH_BASE_URL, failures, False, local_progress_callback))
                    
                pending_next_items = new_pending_next_items

            # Now process all merged responses to build the tree
            for drive_id, resp in drive_to_resp_map.items():
                adj_list[drive_id] = {}
                parent_references[drive_id] = {}
                
                if "body" in resp and "value" in resp["body"]:
                    for file in resp["body"]["value"]:
                        resource_id_to_details[file["id"]] = file
                        
                        if "parentReference" in file and "id" in file["parentReference"]:
                            parent_id = file["parentReference"]["id"]
                            if parent_id in adj_list[drive_id]:
                                adj_list[drive_id][parent_id].append(file["id"])
                            else:
                                adj_list[drive_id][parent_id] = [file["id"]]
                        
                        if "parentReference" in file and "path" in file["parentReference"]:
                            parent_id = file["parentReference"]["id"]
                            parent_references[drive_id][file["id"]] = parent_id

            return adj_list, parent_references, resource_id_to_details
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

        drive_metrics = {}
        
        for drive_id in drive_ids:
            drive_metrics[drive_id] = {
                "maxEffectiveDepth": 0,
                "shortcutCount": 0,
                "fileSizeDistribution": {"buckets": []},
                "largeResources": []
            }

            for size_range in self.config.bucket_ranges:
                drive_metrics[drive_id]["fileSizeDistribution"]["buckets"].append({
                    "sizeRange": size_range,
                    "count": 0
                })
        
        resource_metrics = {}

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

            for leaf_id in leaves:
                try:
                    active_thread_count.increment()
                    self.executor.submit(self._extract_metrics_from_subtrees, leaf_id, drive_id_to_adj_list, parent_references, resource_id_to_details, dependency_set, resource_to_dependency_count, resource_metrics, drive_metrics, active_thread_count)
                except Exception as e:
                    active_thread_count.decrement()
                    self._log_and_fail(f"Error while submitting to executor in _calculate_drive_metrics", e, failures)
            
            while active_thread_count.get_value() > 0:
                with self.condition:
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
            if "id" not in resource["parentReference"]:
                # Root folder. Skipping it as it is an implicit folder added by default with common ID across multiple drives.
                return

            drive_id = resource["parentReference"]["driveId"]
            is_resource_folder = "folder" in resource

            subtree_size = 0
            subtree_count = 0
            max_depth = 0

            if is_resource_folder and resource["id"] in drive_id_to_adj_list[drive_id]:             # Check for empty folders
                for child_id in drive_id_to_adj_list[drive_id][resource["id"]]:
                    subtree_count += resource_metrics[child_id]["subTreeCount"]
                    subtree_size += resource_metrics[child_id]["subTreeSize"]
                    max_depth = max(max_depth, resource_metrics[child_id]["maxDepth"] + 1)

            subtree_count += 1
            subtree_size += resource["size"]

            resource_metrics[resource["id"]] = {
                "subTreeCount": subtree_count,
                "subTreeSize": subtree_size,
                "maxDepth": max_depth
            }

            self._update_drive_metrics_from_resource(resource, resource_metrics[resource_id], drive_metrics[drive_id])

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
                    self.executor.submit(self._extract_metrics_from_subtrees, parent_resource_id, drive_id_to_adj_list, parent_references, resource_id_to_details, dependency_set, resource_to_dependency_count, resource_metrics, drive_metrics, active_thread_count)
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
            
            # Update file size distribution if it's a file
            if "folder" not in resource:
                size_in_mb = resource.get("size", 0) / (1024 * 1024) # assuming size in bytes
                for bucket in drive_metric["fileSizeDistribution"]["buckets"]:
                    low, high = bucket["sizeRange"]
                    if low <= size_in_mb and size_in_mb <= high:
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
            "type": FailureType.UNKNOWN_ERROR,
            "statusCode": 500,
            "message": f"{message}: {str(e)}"
        })

    def shutdown(self):
        self.executor.shutdown(wait=False)
        for level, exec in self.level_to_executor.items():
            exec.shutdown(wait=False)
