import unittest
from unittest.mock import MagicMock, patch
from estimators.eo_in_place_archive_estimator import EOInPlaceArchiveEstimator
from util.connectors import TokenManager, UrlInvoker
from util.utils import ScanConfig
from util.enums import FailureType
import json
import os
import random
import time
import threading

class TestEOInPlaceArchiveLoad(unittest.TestCase):
    
    # Flag to enable/disable simulated failures (defaults to True)
    simulate_failures = os.environ.get("SIMULATE_FAILURES", "True").lower() == "true"
    
    # Flag to enable/disable quota tracking (defaults to False)
    track_quotas = os.environ.get("TRACK_QUOTAS", "False").lower() == "true"

    @classmethod
    def setUpClass(cls):
        cls.data_path = "tests/eo_in_place_archives/test_data/state.json"
        
        # Support loading specific state files (e.g., generated with a seed)
        env_data_path = os.environ.get("TEST_DATA_PATH")
        if env_data_path:
            cls.data_path = env_data_path
            
        if not os.path.exists(cls.data_path):
            raise FileNotFoundError(f"Test data not found at {cls.data_path}. Please run data_state_creator.py first.")
            
        with open(cls.data_path, "r") as f:
            cls.test_data = json.load(f)
            print(f"Loaded test data from {cls.data_path}")

    def setUp(self):
        self.mock_token_manager = MagicMock(spec=TokenManager)
        self.mock_url_invoker = MagicMock(spec=UrlInvoker)
        
        self.config = ScanConfig(
            tenant_id="test-tenant",
            client_ids=["test-client"],
            client_secrets=["test-secret"],
            user_source="tenant",
            csv_path="",
            scan_email=False,
            scan_contact=False,
            scan_calendar=False,
            scan_in_place_archives=True,
            scan_group_mail_boxes=False,
            concurrency=10,
            parallel_batches=20,
            hierarchial_crawl_batch_limit=4,
            load_multiplier=1,
            retries=1,
            backoff=1,
            eta_max_users=5
        )
        
        self.estimator = EOInPlaceArchiveEstimator(
            manager=self.mock_token_manager,
            config=self.config,
            url_invoker=self.mock_url_invoker
        )
        
        # Quota tracking
        self.max_concurrent_per_mailbox = 0
        self.max_batch_size = 0
        self.active_requests_lock = threading.Lock()
        self.active_requests = {} # mailboxId -> count

    def _run_simulation(self):
        data = self.test_data
        
        def mock_invoke(base_url, batch, logger, stop_event, resource_type):
            mailbox_ids = []
            
            if self.track_quotas:
                # Assert batch size
                with self.active_requests_lock:
                    self.max_batch_size = max(self.max_batch_size, len(batch))
                    
                # Identify mailbox IDs in this batch
                for req in batch:
                    if "headers" in req:
                        if "mailboxId" in req["headers"]:
                            mailbox_ids.append(req["headers"]["mailboxId"])
                        elif "userId" in req["headers"]:
                            user_id = req["headers"]["userId"]
                            if user_id in data["users"]:
                                mailbox_ids.append(data["users"][user_id])
                # Increment active counts
                with self.active_requests_lock:
                    for mid in mailbox_ids:
                        self.active_requests[mid] = self.active_requests.get(mid, 0) + 1
                        self.max_concurrent_per_mailbox = max(self.max_concurrent_per_mailbox, self.active_requests[mid])
                    
            # Simulate network delay
            time.sleep(random.uniform(0.01, 0.1))
            
            responses = []
            for req in batch:
                req_id = req.get("id")
                
                if "headers" in req and "userId" in req["headers"]:
                    user_id = req["headers"]["userId"]
                    if user_id in data["users"]:
                        mailbox_id = data["users"][user_id]
                        responses.append({
                            "id": req_id,
                            "status": 200,
                            "body": {
                                "inPlaceArchiveMailboxId": mailbox_id
                            }
                        })
                    else:
                        responses.append({"id": req_id, "status": 404, "body": {"error": {"message": "User not found"}}})
                        
                elif "headers" in req and "folderId" in req["headers"]:
                    folder_id = req["headers"]["folderId"]
                    if folder_id in data["folders"]:
                        f_data = data["folders"][folder_id]
                        
                        if self.simulate_failures and f_data.get("fail", False):
                            responses.append({
                                "id": req_id,
                                "status": 500,
                                "body": {
                                    "error": {
                                        "message": "Simulated folder failure"
                                    }
                                }
                            })
                        else:
                            child_ids = f_data["childFolders"]
                            child_list = []
                            for cid in child_ids:
                                c_data = data["folders"][cid]
                                child_list.append({
                                    "id": c_data["id"],
                                    "totalItemCount": c_data["totalItemCount"],
                                    "childFolderCount": c_data["childFolderCount"]
                                })
                            responses.append({
                                "id": req_id,
                                "status": 200,
                                "body": {
                                    "value": child_list
                                }
                            })
                    else:
                        responses.append({"id": req_id, "status": 404, "body": {"error": {"message": "Folder not found"}}})
                        
                elif "headers" in req and "mailboxId" in req["headers"]:
                    mailbox_id = req["headers"]["mailboxId"]
                    if mailbox_id in data["mailboxes"]:
                        folder_ids = data["mailboxes"][mailbox_id]
                        folder_list = []
                        for fid in folder_ids:
                            f_data = data["folders"][fid]
                            folder_list.append({
                                "id": f_data["id"],
                                "totalItemCount": f_data["totalItemCount"],
                                "childFolderCount": f_data["childFolderCount"]
                            })
                        responses.append({
                            "id": req_id,
                            "status": 200,
                            "body": {
                                "value": folder_list
                            }
                        })
                    else:
                        responses.append({"id": req_id, "status": 404, "body": {"error": {"message": "Mailbox not found"}}})
                else:
                    responses.append({"id": req_id, "status": 400, "body": {"error": {"message": "Bad Request"}}})
                    
            if self.track_quotas:
                with self.active_requests_lock:
                    for mid in mailbox_ids:
                        self.active_requests[mid] -= 1
                        
            return responses

        self.mock_url_invoker.invoke.side_effect = mock_invoke

        user_ids = list(data["users"].keys())
        input_data = {"user_ids": user_ids}
        failures = []
        
        result = self.estimator.calculate_resource_count(input_data, failures)
        return result, failures

    def test_load_simulation(self):
        user_ids = list(self.test_data["users"].keys())
        print(f"Starting load test for {len(user_ids)} users (Simulate Failures: {self.simulate_failures}, Track Quotas: {self.track_quotas})...")
        
        start_time = time.time()
        result, failures = self._run_simulation()
        end_time = time.time()
        
        print(f"Load test completed in {end_time - start_time:.2f} seconds")
        print(f"Total failures recorded: {len(failures)}")
        
        if self.track_quotas:
            print(f"Max batch size observed: {self.max_batch_size}")
            print(f"Max concurrent requests per mailbox observed: {self.max_concurrent_per_mailbox}")
        
        self.assertEqual(len(result), len(user_ids))
        for user_id in user_ids:
            if self.simulate_failures is False:
                self.assertEqual(result[user_id], self.test_data["expected_result"][user_id], f"Result mismatch for user {user_id}")
            else:
                self.assertEqual(result[user_id], self.test_data["expected_result_with_failures"][user_id], f"Result mismatch for user {user_id}")
        
        if self.track_quotas:
            self.assertLessEqual(self.max_batch_size, 20, "Max batch size exceeded")
            self.assertLessEqual(self.max_concurrent_per_mailbox, 4, "Max concurrent requests per mailbox exceeded")

    def test_flakiness(self):
        n_runs = int(os.environ.get("FLAKINESS_RUNS", "3"))
        print(f"\nStarting flakiness test ({n_runs} runs, Track Quotas: {self.track_quotas})...")
        
        first_result = None
        first_failures = None
        
        for i in range(n_runs):
            # Reset tracking for each run
            self.max_concurrent_per_mailbox = 0
            self.max_batch_size = 0
            self.active_requests = {}
            
            start_time = time.time()
            result, failures = self._run_simulation()
            end_time = time.time()
            
            print(f"Run {i+1} completed in {end_time - start_time:.2f} seconds")
            
            if self.track_quotas:
                print(f"  Max batch size: {self.max_batch_size}")
                print(f"  Max concurrent per mailbox: {self.max_concurrent_per_mailbox}")
                
                self.assertLessEqual(self.max_batch_size, 20, f"Max batch size exceeded in run {i+1}")
                self.assertLessEqual(self.max_concurrent_per_mailbox, 4, f"Max concurrent requests per mailbox exceeded in run {i+1}")
            
            if i == 0:
                first_result = result
                first_failures = failures
            else:
                self.assertEqual(result, first_result, f"Results differed in run {i+1}")
                self.assertEqual(failures, first_failures, f"Failures differed in run {i+1}")
                
        print("Flakiness test passed: All runs yielded identical results.")

if __name__ == "__main__":
    unittest.main()
