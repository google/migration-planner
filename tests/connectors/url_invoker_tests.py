"""Tests for UrlInvoker $batch retries and failure logging.

Run with:
  python3 -m unittest tests.connectors.url_invoker_tests -v
"""

import unittest
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

from util.connectors import UrlInvoker
from util.utils import create_batches


def batch_http_response(responses: List[Dict[str, Any]]) -> MagicMock:
    http_response = MagicMock()
    http_response.status_code = 200
    http_response.json.return_value = {"responses": responses}
    return http_response


class TestUrlInvoker(unittest.TestCase):

    def setUp(self):
        self.session = MagicMock()
        self.token_manager = MagicMock()
        self.token_manager.retries = 3
        self.token_manager.get_valid_token_slot.return_value = {"token": "token", "client_id": "client"}
        self.token_manager.get_session.return_value = self.session
        self.invoker = UrlInvoker(self.token_manager, 2, 2, 1, 0.5)
        self.logs: List[str] = []
        self.sleep_patch = patch("util.connectors.time.sleep")
        self.sleep_patch.start()

    def tearDown(self):
        self.sleep_patch.stop()

    def _batch(self, count: int = 2) -> List[Dict[str, Any]]:
        # create_batches builds int request ids; Graph answers with string ids.
        return create_batches("users/{userId}/messages", [{"userId": f"u{i}"} for i in range(count)], 20, True)[0]

    def _sent_ids(self, call_index: int) -> List[Any]:
        return [request["id"] for request in self.session.post.call_args_list[call_index].kwargs["json"]["requests"]]

    def test_retryable_sub_response_is_retried_for_int_request_ids(self):
        self.session.post.side_effect = [
            batch_http_response([
                {"id": "0", "status": 503, "body": {"error": {"message": "Service unavailable"}}},
                {"id": "1", "status": 200, "body": {"@odata.count": 5}},
            ]),
            batch_http_response([{"id": "0", "status": 200, "body": {"@odata.count": 7}}]),
        ]

        responses = self.invoker.invoke("https://graph.microsoft.com/v1.0", self._batch(), self.logs.append)

        self.assertEqual(self.session.post.call_count, 2)
        self.assertEqual(self._sent_ids(1), [0])
        self.assertEqual(sorted(r["body"]["@odata.count"] for r in responses), [5, 7])

    def test_throttled_sub_response_is_retried(self):
        self.session.post.side_effect = [
            batch_http_response([
                {"id": "0", "status": 429, "headers": {"Retry-After": "1"}, "body": {"error": {"message": "Too many requests"}}},
                {"id": "1", "status": 200, "body": {"@odata.count": 1}},
            ]),
            batch_http_response([{"id": "0", "status": 200, "body": {"@odata.count": 2}}]),
        ]

        responses = self.invoker.invoke("https://graph.microsoft.com/v1.0", self._batch(), self.logs.append)

        self.assertEqual(len(responses), 2)
        self.assertFalse(any("could not be matched" in line for line in self.logs))

    def test_dropped_after_max_retries_is_logged_with_urls(self):
        failing = {"id": "0", "status": 503, "body": {"error": {"message": "Service unavailable"}}}
        self.session.post.side_effect = [batch_http_response([dict(failing)]) for _ in range(10)]

        self.invoker.invoke("https://graph.microsoft.com/v1.0", self._batch(1), self.logs.append)

        dropped = [line for line in self.logs if "items dropped permanently" in line]
        self.assertTrue(dropped)
        self.assertIn("users/u0/messages", dropped[0])

    def test_error_responses_are_logged_with_status_and_url(self):
        self.session.post.side_effect = [batch_http_response([
            {"id": "0", "status": 404, "body": {"error": {"code": "ErrorItemNotFound", "message": "Not found"}}},
            {"id": "1", "status": 400, "body": {"code": "BadRequest", "message": "Non-standard error body"}},
        ])]

        self.invoker.invoke("https://graph.microsoft.com/v1.0", self._batch(), self.logs.append)

        errors = [line for line in self.logs if line.startswith("Error encountered in batch response")]
        self.assertEqual(len(errors), 2)
        self.assertTrue(any("[404]" in line and "URL: users/u0/messages" in line for line in errors))
        self.assertTrue(any("[400]" in line and "Non-standard error body" in line and "URL: users/u1/messages" in line for line in errors))

    def test_missing_response_item_is_logged(self):
        self.session.post.side_effect = [batch_http_response([{"id": "1", "status": 200, "body": {"@odata.count": 1}}])]

        self.invoker.invoke("https://graph.microsoft.com/v1.0", self._batch(), self.logs.append)

        self.assertTrue(any("No response item for request 0" in line and "users/u0/messages" in line for line in self.logs))

    def test_success_is_not_logged_as_failure(self):
        self.session.post.side_effect = [batch_http_response([
            {"id": "0", "status": 200, "body": {"@odata.count": 1}},
            {"id": "1", "status": 200, "body": {"@odata.count": 2}},
        ])]

        responses = self.invoker.invoke("https://graph.microsoft.com/v1.0", self._batch(), self.logs.append)

        self.assertEqual(len(responses), 2)
        self.assertEqual(self.logs, [])


if __name__ == "__main__":
    unittest.main()
