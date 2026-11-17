import unittest
from unittest.mock import MagicMock, patch
from estimators.eo_group_mailbox_estimator import EOGroupMailBoxEstimator
from util.connectors import UrlInvoker
from util.utils import ScanConfig
from util.enums import FailureType
import threading

class TestEOGroupMailBoxEstimator(unittest.TestCase):

    def setUp(self):
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
            scan_shared_mail_boxes=False,
            concurrency=2,
            graph_batch_size=2,
            hierarchial_crawl_batch_limit=2,
            load_multiplier=1,
            retries=1,
            backoff=1,
            eta_max_users=5
        )
        
        self.stop_event = threading.Event()
        self.estimator = EOGroupMailBoxEstimator(
            config=self.config,
            url_invoker=self.mock_url_invoker,
            stop_event=self.stop_event
        )

    def test_calculate_resource_count_success(self):
        def mock_invoke(base_url, batch, logger, stop_event, resource_type):
            responses = []
            for req in batch:
                req_id = req.get("id")
                headers = req.get("headers", {})
                
                if "tenant_query" in headers:
                    responses.append({
                        "id": req_id,
                        "status": 200,
                        "body": {
                            "value": [
                                {"id": "group1", "mail": "group1@test.com"},
                                {"id": "group2", "mail": "group2@test.com"}
                            ]
                        }
                    })
                elif "group_id" in headers and "thread_id" not in headers:
                    group_id = headers["group_id"]
                    responses.append({
                        "id": req_id,
                        "status": 200,
                        "body": {
                            "value": [
                                {"id": f"thread1-{group_id}"},
                                {"id": f"thread2-{group_id}"}
                            ]
                        }
                    })
                elif "thread_id" in headers:
                    responses.append({
                        "id": req_id,
                        "status": 200,
                        "body": {
                            "@odata.count": 5
                        }
                    })
                else:
                    responses.append({"id": req_id, "status": 200, "body": {"value": []}})
            return responses

        self.mock_url_invoker.invoke.side_effect = mock_invoke

        data = {}
        failures = []
        
        result, thread_ids_count = self.estimator.calculate_resource_count(data, failures)
        
        self.assertEqual(result, {"group1": 10, "group2": 10})
        self.assertEqual(thread_ids_count, {"group1": 2, "group2": 2})
        self.assertEqual(len(failures), 0)

    def test_calculate_resource_count_success_with_provided_groups(self):
        def mock_invoke(base_url, batch, logger, stop_event, resource_type):
            responses = []
            for req in batch:
                req_id = req.get("id")
                headers = req.get("headers", {})
                
                if "group_id" in headers and "thread_id" not in headers:
                    group_id = headers["group_id"]
                    responses.append({
                        "id": req_id,
                        "status": 200,
                        "body": {
                            "value": [
                                {"id": f"thread1-{group_id}"}
                            ]
                        }
                    })
                elif "thread_id" in headers:
                    responses.append({
                        "id": req_id,
                        "status": 200,
                        "body": {
                            "@odata.count": 3
                        }
                    })
                else:
                    responses.append({"id": req_id, "status": 200, "body": {"value": []}})
            return responses

        self.mock_url_invoker.invoke.side_effect = mock_invoke

        data = {"group_ids": ["group1"]}
        failures = []
        
        result, thread_ids_count = self.estimator.calculate_resource_count(data, failures)
        
        self.assertEqual(result, {"group1": 3})
        self.assertEqual(thread_ids_count, {"group1": 1})
        self.assertEqual(len(failures), 0)

    def test_calculate_resource_count_api_error_groups(self):
        def mock_invoke(base_url, batch, logger, stop_event, resource_type):
            responses = []
            for req in batch:
                req_id = req.get("id")
                headers = req.get("headers", {})
                
                if "tenant_query" in headers:
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

        data = {}
        failures = []
        
        result, thread_ids_count = self.estimator.calculate_resource_count(data, failures)
        
        self.assertEqual(result, {})
        self.assertEqual(thread_ids_count, {})
        self.assertEqual(len(failures), 0)


    def test_calculate_resource_count_api_error_threads(self):
        def mock_invoke(base_url, batch, logger, stop_event, resource_type):
            responses = []
            for req in batch:
                req_id = req.get("id")
                headers = req.get("headers", {})
                
                if "tenant_query" in headers:
                    responses.append({
                        "id": req_id,
                        "status": 200,
                        "body": {
                            "value": [{"id": "group1", "mail": "group1@test.com"}]
                        }
                    })
                elif "group_id" in headers:
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

        data = {}
        failures = []
        
        result, thread_ids_count = self.estimator.calculate_resource_count(data, failures)
        
        self.assertEqual(result, {"group1": 0})
        self.assertEqual(thread_ids_count, {"group1": 0})
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]["groupId"], "group1")
        self.assertEqual(failures[0]["isPartial"], False)
        self.assertEqual(failures[0]["type"], FailureType.FAILURE_STATUS_CODE_ERROR)
        self.assertEqual(failures[0]["statusCode"], 500)

    def test_calculate_resource_count_api_error_posts(self):
        def mock_invoke(base_url, batch, logger, stop_event, resource_type):
            responses = []
            for req in batch:
                req_id = req.get("id")
                headers = req.get("headers", {})
                
                if "tenant_query" in headers:
                    responses.append({
                        "id": req_id,
                        "status": 200,
                        "body": {
                            "value": [{"id": "group1", "mail": "group1@test.com"}]
                        }
                    })
                elif "group_id" in headers and "thread_id" not in headers:
                    responses.append({
                        "id": req_id,
                        "status": 200,
                        "body": {
                            "value": [{"id": "thread1"}]
                        }
                    })
                elif "thread_id" in headers:
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

        data = {}
        failures = []
        
        result, thread_ids_count = self.estimator.calculate_resource_count(data, failures)
        
        self.assertEqual(result, {"group1": 0})
        self.assertEqual(thread_ids_count, {"group1": 1})
        # _get_post_count_for_threads does take failures list and appends to it via create_request_to_response_map
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]["type"], FailureType.FAILURE_STATUS_CODE_ERROR)

    def test_calculate_resource_count_pagination_groups(self):
        call_count = 0
        def mock_invoke(base_url, batch, logger, stop_event, resource_type):
            nonlocal call_count
            responses = []
            for req in batch:
                req_id = req.get("id")
                headers = req.get("headers", {})
                
                if "tenant_query" in headers:
                    if call_count == 0:
                        responses.append({
                            "id": req_id,
                            "status": 200,
                            "body": {
                                "value": [{"id": "group1", "mail": "group1@test.com"}],
                                "@odata.nextLink": "https://graph.microsoft.com/v1.0/groups?$select=id,mail&$top=999&$skip=1"
                            }
                        })
                        call_count += 1
                    else:
                        responses.append({
                            "id": req_id,
                            "status": 200,
                            "body": {
                                "value": [{"id": "group2", "mail": "group2@test.com"}]
                            }
                        })
                elif "group_id" in headers:
                    responses.append({
                        "id": req_id,
                        "status": 200,
                        "body": {
                            "value": []
                        }
                    })
            return responses

        self.mock_url_invoker.invoke.side_effect = mock_invoke

        data = {}
        failures = []
        
        result, thread_ids_count = self.estimator.calculate_resource_count(data, failures)
        
        self.assertEqual(result, {"group1": 0, "group2": 0})
        self.assertEqual(thread_ids_count, {"group1": 0, "group2": 0})

    def test_calculate_resource_count_stop_event(self):
        self.stop_event.set()
        data = {"group_ids": ["group1"]}
        failures = []
        
        result, thread_ids_count = self.estimator.calculate_resource_count(data, failures)
        
        self.assertEqual(result, {"group1": 0})
        self.assertEqual(thread_ids_count, {"group1": 0})

    def test_calculate_resource_count_pagination_threads(self):
        call_count = 0
        def mock_invoke(base_url, batch, logger, stop_event, resource_type):
            nonlocal call_count
            responses = []
            for req in batch:
                req_id = req.get("id")
                headers = req.get("headers", {})
                
                if "tenant_query" in headers:
                    responses.append({
                        "id": req_id,
                        "status": 200,
                        "body": {
                            "value": [{"id": "group1", "mail": "group1@test.com"}]
                        }
                    })
                elif "group_id" in headers and "thread_id" not in headers:
                    if call_count == 0:
                        responses.append({
                            "id": req_id,
                            "status": 200,
                            "body": {
                                "value": [{"id": "thread1"}],
                                "@odata.nextLink": "https://graph.microsoft.com/v1.0/groups/group1/threads?$select=id&$top=999&$skip=1"
                            }
                        })
                        call_count += 1
                    else:
                        responses.append({
                            "id": req_id,
                            "status": 200,
                            "body": {
                                "value": [{"id": "thread2"}]
                            }
                        })
                elif "thread_id" in headers:
                    responses.append({
                        "id": req_id,
                        "status": 200,
                        "body": {
                            "@odata.count": 5
                        }
                    })
            return responses

        self.mock_url_invoker.invoke.side_effect = mock_invoke

        data = {}
        failures = []
        
        result, thread_ids_count = self.estimator.calculate_resource_count(data, failures)
        
        self.assertEqual(result, {"group1": 10})
        self.assertEqual(thread_ids_count, {"group1": 2})

if __name__ == "__main__":
    unittest.main()

