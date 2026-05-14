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
            bucket_ranges=[(0, 1000), (1001, 10000), (10001, 100000)]
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
        self.assertEqual(result.get("subsite_count", result.get("subsiteCount", 0)), expected.get("subsiteCount", 0))
        self.assertEqual(result.get("listCount", 0), expected.get("listCount", 0))
        self.assertEqual(result.get("folderCount", 0), expected.get("folderCount", 0))
        self.assertEqual(result.get("fileCount", 0), expected.get("fileCount", 0))
        
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
        self.assertEqual(result.get("maxEffectiveDepth", 0), expected.get("maxEffectiveDepth", 0))
        self.assertEqual(result.get("maxFolderDepth", 0), expected.get("maxFolderDepth", 0))
        self.assertEqual(result.get("maxSubsiteDepth", 0), expected.get("maxSubsiteDepth", 0))

if __name__ == "__main__":
    unittest.main()
