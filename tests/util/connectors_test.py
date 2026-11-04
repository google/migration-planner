# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for UrlInvoker batch failure logging."""

from unittest import mock
import unittest

from util.auth_manager import TokenManager
from util.connectors import UrlInvoker


class TestUrlInvoker(unittest.TestCase):

  def _invoke_with_failed_responses(self, failed_responses):
    url_invoker = UrlInvoker(
        token_manager=mock.Mock(spec=TokenManager),
        batch_retry_count=1,
        batch_backoff=2,
        initial_delay=0,
        jitter=0,
    )

    with mock.patch.object(
        UrlInvoker, "execute_batch_request", return_value=failed_responses
    ) as execute_batch_request:
      logs = []
      result = url_invoker.invoke(
          url="https://graph.microsoft.com/v1.0/$batch",
          batch=[
              {
                  "id": 0,
                  "method": "GET",
                  "url": "https://graph.microsoft.com/v1.0/users",
              }
          ],
          logger=logs.append,
          context="test-context",
      )

    return logs, result, execute_batch_request

  def test_invoke_logs_failed_response_error_body(self):
    """Failed batch responses must be logged safely, including dict error bodies."""
    failed_responses = {
        0: {
            "id": 0,
            "status": 500,
            "body": {"error": {"code": "InternalServerError", "message": "Graph API exploded"}},
        }
    }

    logs, result, execute_batch_request = self._invoke_with_failed_responses(
        failed_responses
    )

    self.assertEqual(result, list(failed_responses.values()))
    self.assertEqual(execute_batch_request.call_count, 1)
    self.assertTrue(
        any("Consistent failures observed" in log for log in logs),
        f"Expected a consistent-failures log, got: {logs}",
    )
    self.assertTrue(
        any("Graph API exploded" in log for log in logs),
        f"Expected the Graph API error message in the log, got: {logs}",
    )
    self.assertFalse(
        any("Error in test-context" in log for log in logs),
        f"Expected no swallowed error logs, got: {logs}",
    )
