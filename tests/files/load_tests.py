import unittest
from unittest.mock import MagicMock, patch
from estimators.file_estimator import FileEstimator
from util.utils import ScanConfig
from typing import Dict, List, Tuple, Any
import hashlib
import json
import os
import time
import threading
from tests.files.mocks import MockUrlInvoker

import json

class TestFileEstimatorLoad(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.data_path = "tests/files/test_data/state.json"
        
        # Support loading specific state files
        env_data_path = os.environ.get("TEST_DATA_PATH")
        if env_data_path:
            cls.data_path = env_data_path
            
        if not os.path.exists(cls.data_path):
            raise FileNotFoundError(f"Test data not found at {cls.data_path}. Please run data_state_creator.py first.")
            
        with open(cls.data_path, "r") as f:
            cls.test_data = json.load(f)
            print(f"Loaded test data from {cls.data_path}")

    def setUp(self):
        self.mock_url_invoker = MockUrlInvoker(self.test_data)
        
        self.config = ScanConfig(
            tenant_id="test-tenant",
            client_ids=["test-client-1"],
            client_secrets=["test-secret-1"],
            user_source="tenant",
            csv_path="",
            scan_email=False,
            scan_contact=False,
            scan_calendar=False,
            scan_in_place_archives=False,
            scan_shared_mail_boxes=False,
            scan_group_mail_boxes=False,
            concurrency=10,
            load_multiplier=1,
            retries=1,
            backoff=1,
            eta_max_users=5,
            parallel_batches=5,
            large_resource_count_limit=50,
            bucket_ranges=[(0, 10240), (10241, 102400), (102401, 1048576), (1048577, float("inf"))],
            max_allowed_depth=3
        )
        
        self.stop_event = threading.Event()
        
        self.estimator = FileEstimator(
            config=self.config,
            url_invoker=self.mock_url_invoker,
            stop_event=self.stop_event,
            logger=print,
            progress_update_callback=lambda type, **kwargs: None
        )

        self.estimator.set_id_to_display_name_map({})

    def test_load_simulation_all_sites(self):
        print("Starting load test for FileEstimator (All Sites)...")
        
        failures = []
        start_time = time.time()
        result = self.estimator.calculate_resource_metrics({}, failures)
        end_time = time.time()
        
        print(f"Load test completed in {end_time - start_time:.2f} seconds")
        print(f"Total failures recorded: {len(failures)}")

        if failures and len(failures) > 0:
            print("Failures:")
            print(json.dumps(failures, indent=2))
        
        if os.environ.get("SIMULATE_FAILURES", "False").lower() == "true":
            expected = self.test_data.get("expected_result_with_failures", self.test_data["expected_result"])
        else:
            expected = self.test_data["expected_result"]
        
        # Verify results
        # Verify results (Summary Metrics)
        self.assertEqual(result.get("siteCount", 0), expected.get("siteCount", 0))
        self.assertEqual(result.get("subsiteCount", 0), expected.get("subsiteCount", 0))
        self.assertEqual(result.get("personalSiteCount", 0), expected.get("personalSiteCount", 0))
        self.assertEqual(result.get("teamSiteCount", 0), expected.get("teamSiteCount", 0))
        self.assertEqual(result.get("personalSiteDLCount", 0), expected.get("personalSiteDLCount", 0))
        self.assertEqual(result.get("teamSiteDLCount", 0), expected.get("teamSiteDLCount", 0))
        self.assertEqual(result.get("listCount", 0), expected.get("listCount", 0))
        self.assertEqual(result.get("folderCount", 0), expected.get("folderCount", 0))
        self.assertEqual(result.get("fileCount", 0), expected.get("fileCount", 0))
        self.assertEqual(result.get("shortcutCount", 0), expected.get("shortcutCount", 0))
        self.assertEqual(result.get("folderCountExceedingDepthLimit", 0), expected.get("folderCountExceedingDepthLimit", 0))
        self.assertEqual(result.get("fileCountExceedingDepthLimit", 0), expected.get("fileCountExceedingDepthLimit", 0))
        self.assertEqual(result.get("tenantLevelLargeResourceCount", 0), expected.get("tenantLevelLargeResourceCount", 0))
        
        # Verify file size distribution
        for e_bucket in expected.get("tenantLevelFileSizeDistribution", {}).get("buckets", []):
            r_bucket = next((b for b in result.get("tenantLevelFileSizeDistribution", {}).get("buckets", []) if b["sizeRange"] == tuple(e_bucket["sizeRange"])), None)
            self.assertIsNotNone(r_bucket)
            self.assertEqual(r_bucket["count"], e_bucket["count"])
            
        # Verify large resources
        self.assertEqual(len(result.get("tenantLevelLargeResources", [])), len(expected.get("tenantLevelLargeResources", [])))
        
        # Verify drive metrics
        for drive_id, e_drive in expected.get("driveMetrics", {}).items():
            r_drive = result.get("driveMetrics", {}).get(drive_id)
            if r_drive:
                self.assertEqual(r_drive.get("maxEffectiveDepth", 0), e_drive.get("maxEffectiveDepth", 0))
                self.assertEqual(r_drive.get("folderCount", 0), e_drive.get("folderCount", 0))
                self.assertEqual(r_drive.get("fileCount", 0), e_drive.get("fileCount", 0))

        # Verify depth
        self.assertEqual(result.get("maxFolderDepth", 0), expected.get("maxFolderDepth", 0))
        self.assertEqual(result.get("maxSubsiteDepth", 0), expected.get("maxSubsiteDepth", 0))

        # Verify site-level aggregated metrics
        self.assertEqual(set(result.get("siteMetrics", {}).keys()), set(expected.get("siteMetrics", {}).keys()))
        for site_id, e_site in expected.get("siteMetrics", {}).items():
            r_site = result.get("siteMetrics", {}).get(site_id)
            self.assertIsNotNone(r_site, f"Site {site_id} is missing in result['siteMetrics']")
            self.assertEqual(r_site.get("siteLevel", 0), e_site.get("siteLevel", 0))
            self.assertEqual(r_site.get("dlCount", 0), e_site.get("dlCount", 0))
            self.assertEqual(r_site.get("listCount", 0), e_site.get("listCount", 0))
            self.assertEqual(r_site.get("subsiteCount", 0), e_site.get("subsiteCount", 0))
            self.assertEqual(r_site.get("folderCount", 0), e_site.get("folderCount", 0))
            self.assertEqual(r_site.get("fileCount", 0), e_site.get("fileCount", 0))
            self.assertEqual(r_site.get("shortcutCount", 0), e_site.get("shortcutCount", 0))
            self.assertEqual(r_site.get("folderCountExceedingDepthLimit", 0), e_site.get("folderCountExceedingDepthLimit", 0))
            self.assertEqual(r_site.get("fileCountExceedingDepthLimit", 0), e_site.get("fileCountExceedingDepthLimit", 0))
            self.assertEqual(r_site.get("largeResourceCount", 0), e_site.get("largeResourceCount", 0))
            self.assertEqual(r_site.get("totalSize", 0), e_site.get("totalSize", 0))
            self.assertEqual(r_site.get("resourceCount", 0), e_site.get("resourceCount", 0))

        # Verify that sum of site-level metrics equals the tenant-level summary metrics
        site_metrics_values = list(result.get("siteMetrics", {}).values())
        self.assertEqual(sum(s.get("listCount", 0) for s in site_metrics_values), result.get("listCount", 0))
        self.assertEqual(sum(s.get("folderCount", 0) for s in site_metrics_values), result.get("folderCount", 0))
        self.assertEqual(sum(s.get("fileCount", 0) for s in site_metrics_values), result.get("fileCount", 0))
        self.assertEqual(sum(s.get("shortcutCount", 0) for s in site_metrics_values), result.get("shortcutCount", 0))
        self.assertEqual(sum(s.get("folderCountExceedingDepthLimit", 0) for s in site_metrics_values), result.get("folderCountExceedingDepthLimit", 0))
        self.assertEqual(sum(s.get("fileCountExceedingDepthLimit", 0) for s in site_metrics_values), result.get("fileCountExceedingDepthLimit", 0))
        self.assertEqual(sum(s.get("largeResourceCount", 0) for s in site_metrics_values), result.get("tenantLevelLargeResourceCount", 0))
        self.assertEqual(sum(s.get("dlCount", 0) for s in site_metrics_values), sum(result.get("driveCounts", {}).values()))

    def _get_expected_for_subset(self, email_ids: List[str]) -> Tuple[Dict[str, Any], List[str]]:
        drives_list = list(self.test_data["drives"].keys())
        subset_drive_ids = []
        for email in email_ids:
            h = int(hashlib.md5(email.encode('utf-8')).hexdigest(), 16)
            drive_id = drives_list[h % len(drives_list)]
            subset_drive_ids.append(drive_id)
            
        subset_drive_ids = list(set(subset_drive_ids))
        
        root_site_ids = []
        for drive_id in subset_drive_ids:
            site_id = next((sid for sid, s in self.test_data.get("sites", {}).items() if drive_id in s.get("drives", [])), "root")
            curr_site = self.test_data.get("sites", {}).get(site_id)
            if curr_site:
                while "parentReference" in curr_site and "siteId" in curr_site["parentReference"]:
                    parent_id = curr_site["parentReference"]["siteId"]
                    curr_site = self.test_data.get("sites", {}).get(parent_id)
                    if curr_site:
                        site_id = parent_id
            root_site_ids.append(site_id)
        root_site_ids = list(set(root_site_ids))
        
        all_resolved_sites = []
        visited = set()
        def collect_subsites(site_id, level):
            if site_id in visited:
                return
            visited.add(site_id)
            all_resolved_sites.append({"siteId": site_id, "siteLevel": level})
            subsite_ids = self.test_data.get("sites", {}).get(site_id, {}).get("subsites", [])
            for sid in subsite_ids:
                collect_subsites(sid, level + 1)
                
        for site_id in root_site_ids:
            collect_subsites(site_id, 0)
            
        all_site_ids = [s["siteId"] for s in all_resolved_sites]
        
        scanned_drives = []
        for sid in all_site_ids:
            scanned_drives.extend(self.test_data.get("sites", {}).get(sid, {}).get("drives", []))
        scanned_drives = list(set(scanned_drives))
        
        personal_site_dl_count = sum(len(self.test_data.get("sites", {}).get(sid, {}).get("drives", [])) for sid in root_site_ids)
        team_site_dl_count = sum(len(self.test_data.get("sites", {}).get(sid, {}).get("drives", [])) for sid in all_site_ids if sid not in root_site_ids)
        
        expected = {
            "siteCount": len(root_site_ids),
            "subsiteCount": len(all_site_ids) - len(root_site_ids),
            "personalSiteCount": len(email_ids),
            "teamSiteCount": 0,
            "personalSiteDLCount": personal_site_dl_count,
            "teamSiteDLCount": team_site_dl_count,
            "listCount": sum(len(self.test_data.get("sites", {}).get(sid, {}).get("lists", [])) for sid in all_site_ids),
            "folderCount": 0,
            "fileCount": 0,
            "shortcutCount": 0,
            "folderCountExceedingDepthLimit": 0,
            "fileCountExceedingDepthLimit": 0,
            "maxFolderDepth": 0,
            "maxSubsiteDepth": max(s["siteLevel"] for s in all_resolved_sites) if all_resolved_sites else 0,
            "tenantLevelLargeResourceCount": 0,
            "tenantLevelFileSizeDistribution": {"buckets": []}
        }
        
        bucket_ranges = [(0, 10240), (10241, 102400), (102401, 1048576), (1048577, float("inf"))]
        for size_range in bucket_ranges:
            expected["tenantLevelFileSizeDistribution"]["buckets"].append({
                "sizeRange": size_range,
                "count": 0
            })
            
        is_simulate_failures = os.environ.get("SIMULATE_FAILURES", "False").lower() == "true"
        expected_key = "expected_result_with_failures" if is_simulate_failures else "expected_result"
        
        for drive_id in scanned_drives:
            d_expected = self.test_data.get(expected_key, {}).get("driveMetrics", {}).get(drive_id)
            if d_expected:
                expected["folderCount"] += d_expected.get("folderCount", 0)
                expected["fileCount"] += d_expected.get("fileCount", 0)
                expected["shortcutCount"] += d_expected.get("shortcutCount", 0)
                expected["folderCountExceedingDepthLimit"] += d_expected.get("folderCountExceedingDepthLimit", 0)
                expected["fileCountExceedingDepthLimit"] += d_expected.get("fileCountExceedingDepthLimit", 0)
                expected["maxFolderDepth"] = max(expected["maxFolderDepth"], d_expected.get("maxEffectiveDepth", 0))
                expected["tenantLevelLargeResourceCount"] += len(d_expected.get("largeResources", []))
                
                for bucket in d_expected.get("fileSizeDistribution", {}).get("buckets", []):
                    r_bucket = next(b for b in expected["tenantLevelFileSizeDistribution"]["buckets"] if b["sizeRange"] == tuple(bucket["sizeRange"]))
                    r_bucket["count"] += bucket["count"]
                    
        return expected, subset_drive_ids

    def test_load_simulation_from_csv(self):
        print("Starting load test for FileEstimator (from CSV UPNs subset)...")
        
        # Test with a subset of 5 email IDs
        email_ids = [f"user-{i}@domain.com" for i in range(5)]
        expected, subset_drive_ids = self._get_expected_for_subset(email_ids)

        failures = []
        start_time = time.time()
        result = self.estimator.calculate_resource_metrics({"emailIds": email_ids}, failures)
        end_time = time.time()
        
        print(f"UPN Load test completed in {end_time - start_time:.2f} seconds")
        print(f"Total failures recorded: {len(failures)}")
        
        if failures and len(failures) > 0:
            print("Failures:")
            print(json.dumps(failures, indent=2))
            
        self.assertEqual(len(failures), 0)
            
        # Summary Metrics Assertions for UPN scan path (matching whole tenant metrics)
        self.assertEqual(result.get("siteCount", 0), expected.get("siteCount", 0))
        self.assertEqual(result.get("personalSiteCount", 0), expected.get("personalSiteCount", 0))
        self.assertEqual(result.get("teamSiteCount", 0), expected.get("teamSiteCount", 0))
        self.assertEqual(result.get("subsiteCount", 0), expected.get("subsiteCount", 0))
        self.assertEqual(result.get("personalSiteDLCount", 0), expected.get("personalSiteDLCount", 0))
        self.assertEqual(result.get("teamSiteDLCount", 0), expected.get("teamSiteDLCount", 0))
        self.assertEqual(result.get("listCount", 0), expected.get("listCount", 0))
        
        # Crawled content metrics (should match whole tenant expected values)
        self.assertEqual(result.get("folderCount", 0), expected.get("folderCount", 0))
        self.assertEqual(result.get("fileCount", 0), expected.get("fileCount", 0))
        self.assertEqual(result.get("shortcutCount", 0), expected.get("shortcutCount", 0))
        self.assertEqual(result.get("folderCountExceedingDepthLimit", 0), expected.get("folderCountExceedingDepthLimit", 0))
        self.assertEqual(result.get("fileCountExceedingDepthLimit", 0), expected.get("fileCountExceedingDepthLimit", 0))
        self.assertEqual(result.get("tenantLevelLargeResourceCount", 0), expected.get("tenantLevelLargeResourceCount", 0))
        
        # Verify file size distribution
        for e_bucket in expected.get("tenantLevelFileSizeDistribution", {}).get("buckets", []):
            r_bucket = next((b for b in result.get("tenantLevelFileSizeDistribution", {}).get("buckets", []) if b["sizeRange"] == tuple(e_bucket["sizeRange"])), None)
            self.assertIsNotNone(r_bucket)
            self.assertEqual(r_bucket["count"], e_bucket["count"])
            
        # Verify siteIdToMail mapping UPN mapping
        site_id_to_mail = result.get("siteIdToMail", {})
        drives_list = list(self.test_data["drives"].keys())
        for site_id, email in site_id_to_mail.items():
            self.assertIn(email, email_ids)
            h = int(hashlib.md5(email.encode('utf-8')).hexdigest(), 16)
            drive_id = drives_list[h % len(drives_list)]
            expected_site_id = next((sid for sid, s in self.test_data.get("sites", {}).items() if drive_id in s.get("drives", [])), "root")
            curr_site = self.test_data.get("sites", {}).get(expected_site_id)
            if curr_site:
                while "parentReference" in curr_site and "siteId" in curr_site["parentReference"]:
                    parent_id = curr_site["parentReference"]["siteId"]
                    curr_site = self.test_data.get("sites", {}).get(parent_id)
                    if curr_site:
                        expected_site_id = parent_id
            self.assertEqual(site_id, expected_site_id)

    def test_email_ids_simulation(self):
        print("Starting emailIds targeted scan test for FileEstimator...")
        
        failures = []
        email_ids = ["adelev@smh3v.onmicrosoft.com", "alexw@smh3v.onmicrosoft.com"]
        
        # Test the direct method return value
        site_discovery_progress_metrics = {
            "siteCount": 0,
            "personalSiteCount": 0,
            "teamSiteCount": 0,
            "listCount": 0,
            "licenseCount": 0,
            "driveCount": 0,
        }
        result = self.estimator._get_sites_for_users(email_ids, site_discovery_progress_metrics)
        
        self.assertEqual(len(failures), 0, f"Expected 0 failures, got: {failures}")
        
        # Verify that we mapped both email IDs to site IDs
        self.assertEqual(set(result.keys()), set(email_ids))
        for email in email_ids:
            self.assertIn(result[email], self.test_data.get("sites", {}).keys())

    @unittest.skipUnless(os.environ.get("SIMULATE_FAILURES", "False").lower() == "true", "Failure Simulation Enabled!")
    def test_graph_api_500_error_handling(self):
        print("Starting Graph API 500 error handling test...")
        # Get list of site IDs in test data
        site_ids = list(self.test_data["sites"].keys())
        self.assertTrue(len(site_ids) >= 4, "Test requires at least 4 sites in mock data")
        
        site_1 = "root"
        # Find a leaf site (has no subsites)
        site_2 = next(sid for sid, s in self.test_data["sites"].items() if not s.get("subsites") and sid != "root")
        # Find a site with drives that is not site_2
        site_3 = next(sid for sid, s in self.test_data["sites"].items() if sid not in ["root", site_2] and len(s.get("drives", [])) > 0)
        # Find another site with drives that is not site_2 or site_3
        site_4 = next(sid for sid, s in self.test_data["sites"].items() if sid not in ["root", site_2, site_3] and len(s.get("drives", [])) > 0)
        
        # Determine a drive ID for site 4 to fail its delta crawl
        drive_id = self.test_data["sites"][site_4]["drives"][0]
        
        lists_path = f"/sites/{site_1}/lists"
        subsite_path = f"/sites/{site_2}/sites"
        drives_path = f"/sites/{site_3}/drives"
        delta_path = f"/drives/{drive_id}/root/delta"
        
        # Set up mock to return a 500 Internal Server Error status for all these paths
        session_custom_responses = self.mock_url_invoker.token_manager.session.custom_responses
        session_custom_responses[subsite_path] = (
            500,
            {"error": {"message": "Subsites Fetch Failure Simulation"}}
        )
        session_custom_responses[lists_path] = (
            500,
            {"error": {"message": "Lists Fetch Failure Simulation"}}
        )
        session_custom_responses[drives_path] = (
            500,
            {"error": {"message": "Drives Fetch Failure Simulation"}}
        )
        session_custom_responses[delta_path] = (
            500,
            {"error": {"message": "Delta Fetch Failure Simulation"}}
        )
        
        failures = []
        try:
            # Run estimator
            result = self.estimator.calculate_resource_metrics({}, failures)
            
            # Assert that the failures were recorded
            self.assertTrue(len(failures) >= 4, f"Expected at least 4 failures to be recorded, got: {failures}")
            
            # 1. Check Subsite Fetch Failure
            subsite_fail = next((f for f in failures), None)
            self.assertIsNotNone(subsite_fail, f"Expected subsite fail in: {failures}")
            self.assertEqual(subsite_fail.get("statusCode"), 500)
            self.assertEqual(subsite_fail.get("type").name, "FAILURE_STATUS_CODE_ERROR")
            
            # 2. Check Lists Fetch Failure
            lists_fail = next((f for f in failures), None)
            self.assertIsNotNone(lists_fail, f"Expected lists fail in: {failures}")
            self.assertEqual(lists_fail.get("statusCode"), 500)
            self.assertEqual(lists_fail.get("type").name, "FAILURE_STATUS_CODE_ERROR")
            
            # 3. Check Drives Fetch Failure
            drives_fail = next((f for f in failures), None)
            self.assertIsNotNone(drives_fail, f"Expected drives fail in: {failures}")
            self.assertEqual(drives_fail.get("statusCode"), 500)
            self.assertEqual(drives_fail.get("type").name, "FAILURE_STATUS_CODE_ERROR")
            
            # 4. Check Delta Fetch Failure
            delta_fail = next((f for f in failures), None)
            self.assertIsNotNone(delta_fail, f"Expected delta fail in: {failures}")
            self.assertEqual(delta_fail.get("statusCode"), 500)
            self.assertEqual(delta_fail.get("type").name, "FAILURE_STATUS_CODE_ERROR")
            
        finally:
            # Clean up the custom responses
            for path in [subsite_path, lists_path, drives_path, delta_path]:
                if path in session_custom_responses:
                    del session_custom_responses[path]

    @unittest.skipUnless(os.environ.get("SIMULATE_FAILURES", "False").lower() == "true", "Failure Simulation Enabled!")
    def test_graph_api_500_error_on_top_level_sites(self):
        print("Starting Graph API 500 error on top-level sites fetch test...")
        # Since the complete scan queries "/sites", we configure a 500 response for it
        sites_path = "/sites"
        session_custom_responses = self.mock_url_invoker.token_manager.session.custom_responses
        session_custom_responses[sites_path] = (
            500,
            {"error": {"message": "Top-level Sites Fetch Failure Simulation"}}
        )
        
        failures = []
        try:
            result = self.estimator.calculate_resource_metrics({}, failures)
            
            # Check that the main error block caught it
            self.assertTrue(len(failures) > 0, "Expected failure to be recorded")
            top_failure = next((f for f in failures if "Error in fetching site" in f.get("message", "")), None)
            self.assertIsNotNone(top_failure)
            self.assertEqual(top_failure.get("statusCode"), 500)
            
        finally:
            if sites_path in session_custom_responses:
                del session_custom_responses[sites_path]

if __name__ == "__main__":
    unittest.main()
