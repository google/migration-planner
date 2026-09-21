"""Load tests for the Exchange Online in-place archive scan.

Prerequisite: generate the synthetic tenant first, it is not checked in.

    python3 tests/eo_in_place_archives/data_state_creator.py
    python3 -m unittest tests.eo_in_place_archives.load_tests

setUp patches TokenManager.authenticate_all because the estimator
authenticates inside __init__. Without that patch the constructor sets the
stop event and every metric silently comes back as zero.
See tests/README.md for the full list of suites and common failures.
"""

import unittest
from unittest.mock import MagicMock, patch
from estimators.eo_in_place_archive_estimator import EOInPlaceArchiveEstimator
from util.auth_manager import TokenManager
from util.connectors import UrlInvoker
from util.utils import ScanConfig
from util.enums import FailureType
import json
import os
import random
import time
import threading
import urllib.parse

class TestEOInPlaceArchiveLoad(unittest.TestCase):
    
    # Flag to enable/disable simulated failures (defaults to True)
    simulate_failures = os.environ.get("SIMULATE_FAILURES", "True").lower() == "true"
    
    # Flag to enable/disable quota tracking (defaults to False)
    track_quotas = os.environ.get("TRACK_QUOTAS", "False").lower() == "true"

    # Flag to enable/disable delta APIs (defaults to False)
    use_delta_api = os.environ.get("USE_DELTA_API", "False").lower() == "true"

    # Safe parsing of PAGE_SIZE from environment variable
    try:
        PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "5"))
        if PAGE_SIZE <= 0:
            print("Warning: PAGE_SIZE must be positive. Falling back to default (5).")
            PAGE_SIZE = 5
    except ValueError:
        print("Warning: Invalid PAGE_SIZE provided. Falling back to default (5).")
        PAGE_SIZE = 5

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
        self.mock_url_invoker = MagicMock(spec=UrlInvoker)
        self.mock_child_folder_url_invoker = MagicMock(spec=UrlInvoker)
        
        self.config = ScanConfig(
            tenant_id="test-tenant",
            client_ids=["test-client-1", "test-client-2"],
            client_secrets=["test-secret-1", "test-secret-2"],
            user_source="tenant",
            csv_path="",
            scan_email=False,
            scan_contact=False,
            scan_calendar=False,
            scan_in_place_archives=True,
            scan_group_mail_boxes=False,
            scan_shared_mail_boxes=False,
            concurrency=10,
            parallel_batches=20,
            hierarchial_crawl_batch_limit=4,
            load_multiplier=1,
            retries=1,
            backoff=1,
            eta_max_users=5
        )
        
        self.stop_event = threading.Event()

        # EOInPlaceArchiveEstimator authenticates its auxiliary TokenManager inside
        # __init__. Left unpatched that call reaches the real Microsoft login
        # endpoint, fails for the fake tenant and sets the stop event, which makes
        # every scan short-circuit to zero. Stub it out to keep the test offline.
        auth_patcher = patch.object(TokenManager, "authenticate_all", return_value=None)
        self.mock_authenticate_all = auth_patcher.start()
        self.addCleanup(auth_patcher.stop)

        self.estimator = EOInPlaceArchiveEstimator(
            config=self.config,
            url_invoker=self.mock_url_invoker,
            child_folder_url_invoker=self.mock_child_folder_url_invoker,
            stop_event=self.stop_event,
            use_delta_api=self.use_delta_api
        )
        
        # Quota tracking
        self.max_concurrent_per_mailbox = 0
        self.max_batch_size = 0
        self.active_requests_lock = threading.Lock()
        self.active_requests = {} # mailboxId -> count

    def _run_simulation(self):
        data = self.test_data
        
        # Quota tracking per app
        self.max_concurrent_app1 = 0
        self.max_concurrent_app2 = 0
        self.active_requests_app1 = {}
        self.active_requests_app2 = {}
        
        # Token pools
        num_app1_tokens = self.config.concurrency * len(self.config.client_ids)
        num_app2_tokens = len(self.config.client_ids)
        
        app1_token_pool = threading.Semaphore(num_app1_tokens)
        app2_token_pool = threading.Semaphore(num_app2_tokens)
        
        def make_mock_invoke(app_id, us_delta_apis=False):
            token_pool = app1_token_pool if app_id == 1 else app2_token_pool
            
            def mock_invoke(base_url, batch, logger, stop_event, resource_type):
                token_pool.acquire()
                try:
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
                            active_reqs = self.active_requests_app1 if app_id == 1 else self.active_requests_app2
                            for mid in mailbox_ids:
                                active_reqs[mid] = active_reqs.get(mid, 0) + 1
                                if app_id == 1:
                                    self.max_concurrent_app1 = max(self.max_concurrent_app1, active_reqs[mid])
                                else:
                                    self.max_concurrent_app2 = max(self.max_concurrent_app2, active_reqs[mid])
                            
                    # Simulate network delay
                    time.sleep(random.uniform(0.01, 0.1))
                    
                    responses = []
                    for req in batch:
                        req_id = req.get("id")
                        url = req.get("url")
                        
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
                            if self.use_delta_api:
                                assert False, "Delta APIs should not use child folder mocks"

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
                                    # Pagination support
                                    parsed_url = urllib.parse.urlparse(url)
                                    query_params = urllib.parse.parse_qs(parsed_url.query)
                                    skip = int(query_params.get("$skip", [0])[0])
                                    
                                    page_size = TestEOInPlaceArchiveLoad.PAGE_SIZE

                                    sliced_child_ids = f_data["childFolders"][skip : skip + page_size]
                                    child_list = []
                                    for cid in sliced_child_ids:
                                        c_data = data["folders"][cid]
                                        child_list.append({
                                            "id": c_data["id"],
                                            "totalItemCount": c_data["totalItemCount"],
                                            "childFolderCount": c_data["childFolderCount"]
                                        })
                                    
                                    body = {"value": child_list}
                                    
                                    if skip + page_size < len(f_data["childFolders"]):
                                        new_query = query_params.copy()
                                        new_query["$skip"] = [str(skip + page_size)]
                                        next_link = parsed_url.path + "?" + urllib.parse.urlencode(new_query, doseq=True)
                                        body["@odata.nextLink"] = next_link

                                    responses.append({
                                        "id": req_id,
                                        "status": 200,
                                        "body": body
                                    })
                            else:
                                responses.append({"id": req_id, "status": 404, "body": {"error": {"message": "Folder not found"}}})
                                
                        elif "headers" in req and "mailboxId" in req["headers"]:
                            mailbox_id = req["headers"]["mailboxId"]
                            mailbox_key = "mailboxes" if self.use_delta_api is False else "flattened_mailboxes"

                            if mailbox_id in data[mailbox_key]:                                
                                # Pagination support
                                parsed_url = urllib.parse.urlparse(url)
                                query_params = urllib.parse.parse_qs(parsed_url.query)
                                skip = int(query_params.get("$skip", [0])[0])
                                
                                page_size = TestEOInPlaceArchiveLoad.PAGE_SIZE

                                sliced_folder_ids = data[mailbox_key][mailbox_id][skip : skip + page_size]
                                folder_list = []
                                for fid in sliced_folder_ids:
                                    f_data = data["folders"][fid]
                                    folder_list.append({
                                        "id": f_data["id"],
                                        "totalItemCount": f_data["totalItemCount"],
                                        "childFolderCount": f_data["childFolderCount"]
                                    })
    
                                body = {"value": folder_list}
                                
                                if skip + page_size < len(data[mailbox_key][mailbox_id]):
                                    new_query = query_params.copy()
                                    new_query["$skip"] = [str(skip + page_size)]
                                    next_link = parsed_url.path + "?" + urllib.parse.urlencode(new_query, doseq=True)
                                    body["@odata.nextLink"] = next_link
                                    
                                responses.append({
                                    "id": req_id,
                                    "status": 200,
                                    "body": body
                                })
                            else:
                                responses.append({"id": req_id, "status": 404, "body": {"error": {"message": "Mailbox not found"}}})
                        else:
                            responses.append({"id": req_id, "status": 400, "body": {"error": {"message": "Bad Request"}}})
                            
                    if self.track_quotas:
                        with self.active_requests_lock:
                            active_reqs = self.active_requests_app1 if app_id == 1 else self.active_requests_app2
                            for mid in mailbox_ids:
                                active_reqs[mid] -= 1
                                
                    return responses
                finally:
                    token_pool.release()
            return mock_invoke

        self.mock_url_invoker.invoke.side_effect = make_mock_invoke(1, self.use_delta_api)
        self.mock_child_folder_url_invoker.invoke.side_effect = make_mock_invoke(2, self.use_delta_api)

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
            print(f"Max concurrent requests per mailbox observed (App 1): {self.max_concurrent_app1}")
            print(f"Max concurrent requests per mailbox observed (App 2): {self.max_concurrent_app2}")
        
        self.assertEqual(len(result), len(user_ids))
        for user_id in user_ids:
            if self.simulate_failures is False:
                self.assertEqual(result[user_id], self.test_data["expected_result"][user_id], f"Result mismatch for user {user_id}")
            else:
                self.assertEqual(result[user_id], self.test_data["expected_result_with_failures"][user_id], f"Result mismatch for user {user_id}")
        
        if self.track_quotas:
            self.assertLessEqual(self.max_batch_size, 20, "Max batch size exceeded")
            self.assertLessEqual(self.max_concurrent_app1, 4, "Max concurrent requests per mailbox exceeded for App 1")
            self.assertLessEqual(self.max_concurrent_app2, 4, "Max concurrent requests per mailbox exceeded for App 2")

    def test_flakiness(self):
        n_runs = int(os.environ.get("FLAKINESS_RUNS", "3"))
        print(f"\nStarting flakiness test ({n_runs} runs, Track Quotas: {self.track_quotas})...")
        
        first_result = None
        first_failures = None
        
        for i in range(n_runs):
            # Reset tracking for each run
            self.max_concurrent_app1 = 0
            self.max_concurrent_app2 = 0
            self.max_batch_size = 0
            self.active_requests_app1 = {}
            self.active_requests_app2 = {}
            
            start_time = time.time()
            result, failures = self._run_simulation()
            end_time = time.time()
            
            print(f"Run {i+1} completed in {end_time - start_time:.2f} seconds")
            
            if self.track_quotas:
                print(f"  Max batch size: {self.max_batch_size}")
                print(f"  Max concurrent per mailbox (App 1): {self.max_concurrent_app1}")
                print(f"  Max concurrent per mailbox (App 2): {self.max_concurrent_app2}")
                
                self.assertLessEqual(self.max_batch_size, 20, f"Max batch size exceeded in run {i+1}")
                self.assertLessEqual(self.max_concurrent_app1, 4, f"Max concurrent requests per mailbox exceeded for App 1 in run {i+1}")
                self.assertLessEqual(self.max_concurrent_app2, 4, f"Max concurrent requests per mailbox exceeded for App 2 in run {i+1}")
            
            if i == 0:
                first_result = result
                first_failures = failures
            else:
                self.assertEqual(result, first_result, f"Results differed in run {i+1}")
                self.assertEqual(failures, first_failures, f"Failures differed in run {i+1}")
                
        print("Flakiness test passed: All runs yielded identical results.")

if __name__ == "__main__":
    unittest.main()
