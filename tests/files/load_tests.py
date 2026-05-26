import unittest
from unittest.mock import MagicMock, patch
from estimators.file_estimator import FileEstimator
from util.utils import ScanConfig
from util.enums import FailureType
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

    def test_load_simulation(self):
        print("Starting load test for FileEstimator...")
        
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

    def test_email_ids_simulation(self):
        print("Starting emailIds targeted scan test for FileEstimator...")
        
        failures = []
        email_ids = ["adelev@smh3v.onmicrosoft.com", "alexw@smh3v.onmicrosoft.com"]
        
        # Test the direct method return value
        result = self.estimator._get_sites_for_users(email_ids)
        
        self.assertEqual(len(failures), 0, f"Expected 0 failures, got: {failures}")
        
        # Verify that we mapped both email IDs to site IDs
        self.assertEqual(set(result.keys()), set(email_ids))
        for email in email_ids:
            self.assertTrue(result[email].startswith("site-mock-"))

if __name__ == "__main__":
    unittest.main()
