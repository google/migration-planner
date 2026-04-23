import unittest
from unittest.mock import MagicMock, patch
from estimators.eo_group_mailbox_estimator import EOGroupMailBoxEstimator
from util.connectors import TokenManager, UrlInvoker
from util.utils import ScanConfig
from util.enums import FailureType
import threading

class TestEOGroupMailBoxEstimator(unittest.TestCase):

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
            scan_in_place_archives=False,
            scan_group_mail_boxes=True,
            concurrency=2,
            parallel_batches=2,
            hierarchial_crawl_batch_limit=2,
            load_multiplier=1,
            retries=1,
            backoff=1,
            eta_max_users=5
        )
        
        self.stop_event = threading.Event()
        self.estimator = EOGroupMailBoxEstimator(
            manager=self.mock_token_manager,
            config=self.config,
            url_invoker=self.mock_url_invoker,
            stop_event=self.stop_event
        )

    def test_calculate_resource_count_success(self):
        def mock_invoke(base_url, batch, logger, stop_event, resource_type):
            responses = []
            for req in batch:
                req_id = req.get("id")
                url = req.get("url")
                if "headers" in req and "userId" in req["headers"]:
                    user_id = req["headers"]["userId"]
                    if "mailboxSettings" in url:
                        responses.append({
                            "id": req_id,
                            "status": 200,
                            "body": {
                                "value": "shared" if "user1" in user_id else "user"
                            }
                        })
                    elif "messages" in url:
                        responses.append({
                            "id": req_id,
                            "status": 200,
                            "body": {
                                "@odata.count": 5
                            }
                        })
                else:
                    responses.append({"id": req_id, "status": 200, "body": {"value": "user"}})
            return responses

        self.mock_url_invoker.invoke.side_effect = mock_invoke

        data = {"user_ids": ["user1", "user2"]}
        failures = []
        
        result = self.estimator.calculate_resource_count(data, failures)
        
        self.assertEqual(result, {"user1": 5, "user2": 0})
        self.assertEqual(len(failures), 0)

    def test_calculate_resource_count_api_error(self):
        def mock_invoke(base_url, batch, logger, stop_event, resource_type):
            responses = []
            for req in batch:
                req_id = req.get("id")
                if "headers" in req and "userId" in req["headers"]:
                    responses.append({
                        "id": req_id,
                        "status": 400,
                        "body": {
                            "error": {
                                "message": "Bad Request"
                            }
                        }
                    })
            return responses

        self.mock_url_invoker.invoke.side_effect = mock_invoke

        data = {"user_ids": ["user1"]}
        failures = []
        
        result = self.estimator.calculate_resource_count(data, failures)
        
        self.assertEqual(result, {"user1": 0})
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]["type"], FailureType.FAILURE_STATUS_CODE_ERROR)
        self.assertEqual(failures[0]["statusCode"], 400)
        self.assertIn("Bad Request", failures[0]["message"])

    def test_calculate_resource_count_invalid_payload_missing_body_in_purpose(self):
        def mock_invoke(base_url, batch, logger, stop_event, resource_type):
            responses = []
            for req in batch:
                req_id = req.get("id")
                responses.append({
                    "id": req_id,
                    "status": 200       # No body present
                })
            return responses

        self.mock_url_invoker.invoke.side_effect = mock_invoke

        data = {"user_ids": ["user1"]}
        failures = []
        
        result = self.estimator.calculate_resource_count(data, failures)
        
        self.assertEqual(result, {"user1": 0})          # By default the user Purpose should be assumed to be user.
        self.assertEqual(len(failures), 1)              # But a failure should be reported for transparency
        self.assertEqual(failures[0]["type"], FailureType.INVALID_DATA)
        self.assertEqual(failures[0]["statusCode"], 200)
        self.assertIn("Invalid data - userPurpose not present", failures[0]["message"])

    def test_calculate_resource_count_invalid_payload_missing_body_in_count(self):
        def mock_invoke(base_url, batch, logger, stop_event, resource_type):
            responses = []
            for req in batch:
                req_id = req.get("id")
                url = req.get("url")

                if "messages" in url:
                    responses.append({
                        "id": req_id,
                        "status": 200       # No body present
                    })
                elif "mailboxSettings" in url:
                    responses.append({
                        "id": req_id,
                        "status": 200,
                        "body": {
                            "value": "shared"
                        }
                    })
            return responses

        self.mock_url_invoker.invoke.side_effect = mock_invoke

        data = {"user_ids": ["user1"]}
        failures = []
        
        result = self.estimator.calculate_resource_count(data, failures)
        
        self.assertEqual(result, {"user1": 0})          # By default the count should be estimated to be 0 in case of missing count
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]["type"], FailureType.INVALID_DATA)
        self.assertEqual(failures[0]["statusCode"], 200)
        self.assertIn("Invalid data - No count present", failures[0]["message"])

    def test_calculate_resource_count_invalid_id_type(self):
        def mock_invoke(base_url, batch, logger, stop_event, resource_type):
            return [{"id": "abc", "status": 200, "body": {}}]

        self.mock_url_invoker.invoke.side_effect = mock_invoke

        data = {"user_ids": ["user1"]}
        failures = []
        
        result = self.estimator.calculate_resource_count(data, failures)
        
        self.assertEqual(result, {"user1": 0})
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]["type"], FailureType.INVALID_DATA)
        self.assertEqual(failures[0]["statusCode"], 200)
        self.assertIn("Invalid data - Unable to convert id to integer:", failures[0]["message"])

    def test_calculate_resource_count_null_users(self):
        data = {"user_ids": [None]}
        failures = []
        with self.assertRaises(Exception) as context:
            self.estimator.calculate_resource_count(data, failures)
        self.assertIn("Invalid user ids provided", str(context.exception))

    def test_calculate_resource_count_stop_event(self):
        self.stop_event.set()
        data = {"user_ids": ["user1"]}
        failures = []
        result = self.estimator.calculate_resource_count(data, failures)
        self.assertEqual(result, {"user1": 0})

    def test_calculate_resource_count_invalid_total_item_count(self):
        def mock_invoke(base_url, batch, logger, stop_event, resource_type):
            responses = []
            for req in batch:
                req_id = req.get("id")
                url = req.get("url")
                if "mailboxSettings" in url:
                    responses.append({"id": req_id, "status": 200, "body": {"value": "shared"}})
                elif "messages" in url:
                    responses.append({
                        "id": req_id,
                        "status": 200,
                        "body": {
                            "@odata.count": "abc"
                        }
                    })
            return responses

        self.mock_url_invoker.invoke.side_effect = mock_invoke
        data = {"user_ids": ["user1"]}
        failures = []
        result = self.estimator.calculate_resource_count(data, failures)
        self.assertEqual(result, {"user1": 0})
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]["type"], FailureType.INVALID_DATA)
        self.assertIn("Invalid data - Unable to convert count to integer", failures[0]["message"])

if __name__ == "__main__":
    unittest.main()
