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

"""Tests for ChatEstimator batching and ETA calculations."""

from unittest import mock
import unittest
from estimators.chat_estimator import ChatEstimator
from util.utils import ScanConfig


class TestChatEstimator(unittest.TestCase):

  def setUp(self):
    self.config = mock.Mock(spec=ScanConfig)
    self.config.mode = "sampling"
    self.config.sample_percentage = 100.0
    self.config.parallel_batches = 2
    self.config.concurrency = 2

    self.url_invoker = mock.Mock()
    self.estimator = ChatEstimator(
        config=self.config,
        url_invoker=self.url_invoker,
    )

  def test_calculate_migration_eta_with_combined_workload(self):
    """Verify that estimation handles both teams and users, placing them in the same waves."""
    # Build mock data
    data = {
        "private_channels": 2,
        "total_teams": 2,
        "total_users": 2,
        "t_map": {
            "team_1": {"messages": 1000, "channels": 5, "memberships": 20},
            "team_2": {"messages": 2000, "channels": 10, "memberships": 40},
        },
        "u_map": {
            "user1@test.com": {"chats": 3, "messages": 300, "memberships": 6},
            "user2@test.com": {"chats": 5, "messages": 500, "memberships": 10},
        },
    }

    eta = self.estimator.calculate_migration_eta(data)

    # 1. Verify that the ETA is a positive float
    self.assertGreater(eta, 0.0)

    # 2. Verify that batches were generated and persisted
    self.assertTrue(hasattr(self.estimator, "last_batches"))
    batches = self.estimator.last_batches
    self.assertGreater(len(batches), 0)

    # 3. Verify that the first batch contains both users and teams co-located
    combined_has_both = False
    for batch in batches:
      if batch.get("users", 0) > 0 and batch.get("total_teams", 0) > 0:
        combined_has_both = True
      
      # Verify list contents
      self.assertIn("team_ids", batch)
      self.assertIn("user_ids", batch)
      self.assertEqual(len(batch["team_ids"]), batch["total_teams"])
      self.assertEqual(len(batch["user_ids"]), batch["users"])

    self.assertTrue(
        combined_has_both,
        "Expected at least one wave to contain both users and teams co-located.",
    )


if __name__ == "__main__":
  unittest.main()
