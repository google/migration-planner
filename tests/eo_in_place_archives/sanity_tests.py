import unittest
from unittest.mock import MagicMock, patch
from estimators.eo_in_place_archive_estimator import EOInPlaceArchiveEstimator
from util.connectors import TokenManager, UrlInvoker
from util.utils import ScanConfig
from util.enums import FailureType
import threading

class TestEOInPlaceArchiveEstimator(unittest.TestCase):

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
            scan_group_mail_boxes=False,
            concurrency=2,
            parallel_batches=2,
            hierarchial_crawl_batch_limit=2,
            load_multiplier=1,
            retries=1,
            backoff=1,
            eta_max_users=5
        )
        
        self.stop_event = threading.Event()
        self.estimator = EOInPlaceArchiveEstimator(
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
                if "headers" in req and "userId" in req["headers"]:
                    user_id = req["headers"]["userId"]
                    responses.append({
                        "id": req_id,
                        "status": 200,
                        "body": {
                            "inPlaceArchiveMailboxId": f"archive-{user_id}"
                        }
                    })
                elif "headers" in req and "mailboxId" in req["headers"]:
                    mailbox_id = req["headers"]["mailboxId"]
                    count = 10 if "user1" in mailbox_id else 20
                    responses.append({
                        "id": req_id,
                        "status": 200,
                        "body": {
                            "value": [
                                {
                                    "id": f"folder-1-{mailbox_id}",
                                    "totalItemCount": count,
                                    "childFolderCount": 0
                                }
                            ]
                        }
                    })
                else:
                    responses.append({"id": req_id, "status": 200, "body": {"value": []}})
            return responses

        self.mock_url_invoker.invoke.side_effect = mock_invoke

        data = {"user_ids": ["user1", "user2"]}
        failures = []
        
        result = self.estimator.calculate_resource_count(data, failures)
        
        self.assertEqual(result, {"user1": 10, "user2": 20})
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
        self.assertEqual(failures[0]["message"], "Bad Request")

    def test_calculate_resource_count_invalid_payload_missing_body(self):
        def mock_invoke(base_url, batch, logger, stop_event, resource_type):
            responses = []
            for req in batch:
                req_id = req.get("id")
                responses.append({
                    "id": req_id,
                    "status": 200
                })
            return responses

        self.mock_url_invoker.invoke.side_effect = mock_invoke

        data = {"user_ids": ["user1"]}
        failures = []
        
        result = self.estimator.calculate_resource_count(data, failures)
        
        self.assertEqual(result, {"user1": 0})
        self.assertEqual(len(failures), 0)

    def test_calculate_resource_count_invalid_payload_missing_id(self):
        def mock_invoke(base_url, batch, logger, stop_event, resource_type):
            return [{"status": 200, "body": {}}]

        self.mock_url_invoker.invoke.side_effect = mock_invoke

        data = {"user_ids": ["user1"]}
        failures = []
        
        result = self.estimator.calculate_resource_count(data, failures)
        
        self.assertEqual(result, {"user1": 0})
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]["type"], FailureType.NOT_FOUND)
        self.assertEqual(failures[0]["message"], "In-place archive mailbox not found for the user.")

    def test_calculate_resource_count_invalid_id_type(self):
        def mock_invoke(base_url, batch, logger, stop_event, resource_type):
            return [{"id": "abc", "status": 200, "body": {}}]

        self.mock_url_invoker.invoke.side_effect = mock_invoke

        data = {"user_ids": ["user1"]}
        failures = []
        
        result = self.estimator.calculate_resource_count(data, failures)
        
        self.assertEqual(result, {"user1": 0})
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]["type"], FailureType.NOT_FOUND)
        self.assertEqual(failures[0]["message"], "In-place archive mailbox not found for the user.")

    def test_calculate_resource_count_empty_users(self):
        data = {"user_ids": []}
        failures = []
        with self.assertRaises(Exception) as context:
            self.estimator.calculate_resource_count(data, failures)
        self.assertIn("Invalid user ids provided", str(context.exception))

    def test_calculate_resource_count_null_users(self):
        data = {"user_ids": [None]}
        failures = []
        with self.assertRaises(Exception) as context:
            self.estimator.calculate_resource_count(data, failures)
        self.assertIn("Invalid user ids provided", str(context.exception))

    def test_calculate_resource_count_child_folder_error(self):
        def mock_invoke(base_url, batch, logger, stop_event, resource_type):
            responses = []
            for req in batch:
                req_id = req.get("id")
                if "headers" in req and "userId" in req["headers"]:
                    responses.append({"id": req_id, "status": 200, "body": {"inPlaceArchiveMailboxId": "archive-1"}})
                elif "headers" in req and "mailboxId" in req["headers"]:
                    responses.append({"id": req_id, "status": 200, "body": {"value": [{"id": "folder-1", "totalItemCount": 10, "childFolderCount": 1}]}})
                elif "headers" in req and "folderId" in req["headers"]:
                    responses.append({
                        "id": req_id,
                        "status": 500,
                        "body": {
                            "error": {
                                "message": "Internal Server Error"
                            }
                        }
                    })
            return responses

        self.mock_url_invoker.invoke.side_effect = mock_invoke
        data = {"user_ids": ["user1"]}
        failures = []
        result = self.estimator.calculate_resource_count(data, failures)
        self.assertEqual(result, {"user1": 10})
        self.assertEqual(len(failures), 1)
        self.assertTrue(failures[0]["isPartial"])

    def test_calculate_resource_count_pagination(self):
        call_count = 0
        def mock_invoke(base_url, batch, logger, stop_event, resource_type):
            nonlocal call_count
            responses = []
            for req in batch:
                req_id = req.get("id")
                if "headers" in req and "userId" in req["headers"]:
                    responses.append({"id": req_id, "status": 200, "body": {"inPlaceArchiveMailboxId": "archive-1"}})
                elif "headers" in req and "mailboxId" in req["headers"]:
                    if call_count == 0:
                        responses.append({
                            "id": req_id,
                            "status": 200,
                            "body": {
                                "value": [{"id": "folder-1", "totalItemCount": 10, "childFolderCount": 0}],
                                "@odata.nextLink": "https://graph.microsoft.com/beta/next-page"
                            }
                        })
                        call_count += 1
                    else:
                        responses.append({
                            "id": req_id,
                            "status": 200,
                            "body": {
                                "value": [{"id": "folder-2", "totalItemCount": 5, "childFolderCount": 0}]
                            }
                        })
                else:
                    responses.append({"id": req_id, "status": 200, "body": {"value": []}})
            return responses

        self.mock_url_invoker.invoke.side_effect = mock_invoke
        data = {"user_ids": ["user1"]}
        failures = []
        result = self.estimator.calculate_resource_count(data, failures)
        self.assertEqual(result, {"user1": 15})

    def test_calculate_resource_count_stop_event(self):
        self.stop_event.set()
        data = {"user_ids": ["user1"]}
        failures = []
        result = self.estimator.calculate_resource_count(data, failures)
        self.assertEqual(result, {"user1": 0})

    def test_calculate_resource_count_multithreading(self):
        def mock_invoke(base_url, batch, logger, stop_event, resource_type):
            responses = []
            for req in batch:
                req_id = req.get("id")
                if "headers" in req and "userId" in req["headers"]:
                    responses.append({"id": req_id, "status": 200, "body": {"inPlaceArchiveMailboxId": "archive-1"}})
                elif "headers" in req and "mailboxId" in req["headers"]:
                    responses.append({
                        "id": req_id,
                        "status": 200,
                        "body": {
                            "value": [
                                {"id": "folder-1", "totalItemCount": 10, "childFolderCount": 1},
                                {"id": "folder-2", "totalItemCount": 20, "childFolderCount": 1}
                            ]
                        }
                    })
                elif "headers" in req and "folderId" in req["headers"]:
                    responses.append({
                        "id": req_id,
                        "status": 200,
                        "body": {
                            "value": [
                                {"id": f"sub-{req['headers']['folderId']}", "totalItemCount": 5, "childFolderCount": 0}
                            ]
                        }
                    })
            return responses

        self.mock_url_invoker.invoke.side_effect = mock_invoke
        data = {"user_ids": ["user1"]}
        failures = []
        result = self.estimator.calculate_resource_count(data, failures)
        self.assertEqual(result, {"user1": 40})

    def test_calculate_resource_count_missing_user_ids_key(self):
        data = {}
        failures = []
        with self.assertRaises(KeyError):
            self.estimator.calculate_resource_count(data, failures)

    def test_calculate_resource_count_invalid_total_item_count(self):
        def mock_invoke(base_url, batch, logger, stop_event, resource_type):
            responses = []
            for req in batch:
                req_id = req.get("id")
                if "headers" in req and "userId" in req["headers"]:
                    responses.append({"id": req_id, "status": 200, "body": {"inPlaceArchiveMailboxId": "archive-1"}})
                elif "headers" in req and "mailboxId" in req["headers"]:
                    responses.append({
                        "id": req_id,
                        "status": 200,
                        "body": {
                            "value": [
                                {"id": "folder-1", "totalItemCount": "abc", "childFolderCount": 0}
                            ]
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
        self.assertIn("Invalid totalItemCount", failures[0]["message"])

    def test_calculate_resource_count_invalid_child_folder_count(self):
        def mock_invoke(base_url, batch, logger, stop_event, resource_type):
            responses = []
            for req in batch:
                req_id = req.get("id")
                if "headers" in req and "userId" in req["headers"]:
                    responses.append({"id": req_id, "status": 200, "body": {"inPlaceArchiveMailboxId": "archive-1"}})
                elif "headers" in req and "mailboxId" in req["headers"]:
                    responses.append({
                        "id": req_id,
                        "status": 200,
                        "body": {
                            "value": [
                                {"id": "folder-1", "totalItemCount": 10, "childFolderCount": "xyz"}
                            ]
                        }
                    })
            return responses

        self.mock_url_invoker.invoke.side_effect = mock_invoke
        data = {"user_ids": ["user1"]}
        failures = []
        result = self.estimator.calculate_resource_count(data, failures)
        self.assertEqual(result, {"user1": 10})
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]["type"], FailureType.INVALID_DATA)
        self.assertIn("Invalid childFolderCount", failures[0]["message"])

if __name__ == "__main__":
    unittest.main()
