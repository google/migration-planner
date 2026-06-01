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

"""Tests for MigrationScanner in v2."""

import asyncio
from unittest import mock
import unittest
from chat.scanner import MigrationScanner
from util.auth_manager import TokenManager
from util.batch_client import GraphBatchClient
from util.db_manager import DatabaseManager
from util.state_registry import MetricsRegistry


class TestMigrationScanner(unittest.TestCase):

  def setUp(self):
    self.db = mock.Mock(spec=DatabaseManager)
    self.db.get_processed_channel.return_value = None
    self.client = mock.Mock(spec=GraphBatchClient)
    self.metrics = mock.Mock(spec=MetricsRegistry)
    self.token_manager = mock.Mock(spec=TokenManager)

    self.scanner = MigrationScanner(
        db=self.db, client=self.client, metrics=self.metrics
    )

    self.token_data = {"token": "fake_token"}
    self.token_manager.get_valid_token_slot.return_value = self.token_data
    self.session = mock.Mock()
    self.token_manager.get_session.return_value = self.session

  def test_count_channel_messages_async_density_fail_cap(self):
    """Verify that count is capped at 10k when density estimation fails."""

    async def _run():
      call_count = 0

      async def mock_get(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count < 20:
          return {"value": [{"id": "1"}], "@odata.nextLink": "http://dummy?$top=50"}, 200
        return {"value": [{"id": "1"}]}, 200

      self.client.get_with_retry_async.side_effect = mock_get
      self.scanner._jump_scan = mock.AsyncMock(return_value=(10000, None))

      root_count, reply_count, total = (
          await self.scanner.count_channel_messages_async(
              self.token_manager,
              self.session,
              "team_id",
              "channel_id",
              self.token_data,
          )
      )

      self.assertEqual(root_count, 10000)
      self.assertEqual(reply_count, 5000)
      self.assertEqual(total, 15000)
      self.assertTrue(self.db.save_processed_channel.called)

    asyncio.run(_run())

  def test_count_channel_messages_async_density_success(self):
    """Verify that count is estimated when density estimation succeeds."""

    async def _run():
      self.scanner._jump_scan = mock.AsyncMock(return_value=(10000, None))

      call_count = 0

      async def mock_get(*args, **kwargs):
        nonlocal call_count
        url = args[1]
        if "$top=50" in url:
          call_count += 1
          if call_count < 20:
            return {"value": [{"id": "1"}], "@odata.nextLink": "http://dummy?$top=50"}, 200
          return {"value": [{"id": "1"}]}, 200
        elif "messages?$top=1" in url and "$skip=" not in url:
          return {"value": [{"createdDateTime": "2026-05-07T08:00:00Z"}]}, 200
        elif "$skip=10000" in url:
          return {"value": [{"createdDateTime": "2026-05-07T07:00:00Z"}]}, 200
        elif url.endswith("/channels/channel_id"):
          return {"createdDateTime": "2026-05-07T06:00:00Z"}, 200
        return {"value": []}, 200

      self.client.get_with_retry_async.side_effect = mock_get

      root_count, reply_count, total = (
          await self.scanner.count_channel_messages_async(
              self.token_manager,
              self.session,
              "team_id",
              "channel_id",
              self.token_data,
          )
      )

      self.assertEqual(root_count, 20000)
      self.assertEqual(reply_count, 10000)
      self.assertEqual(total, 30000)
      self.assertTrue(self.db.save_processed_channel.called)

    asyncio.run(_run())

  def test_count_channel_messages_async_binary_search(self):
    """Verify that binary search is used when count < 10k."""

    async def _run():
      self.scanner._jump_scan = mock.AsyncMock(return_value=(4000, 8000))
      self.scanner._binary_search = mock.AsyncMock(return_value=5000)

      call_count = 0

      async def mock_get(*args, **kwargs):
        nonlocal call_count
        url = args[1]
        if "$top=50" in url:
          call_count += 1
          if call_count < 20:
            return {"value": [{"id": "1"}], "@odata.nextLink": "http://dummy?$top=50"}, 200
          return {"value": [{"id": "1"}]}, 200
        return {"value": []}, 200

      self.client.get_with_retry_async.side_effect = mock_get

      root_count, reply_count, total = (
          await self.scanner.count_channel_messages_async(
              self.token_manager,
              self.session,
              "team_id",
              "channel_id",
              self.token_data,
          )
      )

      self.assertEqual(root_count, 5000)
      self.assertEqual(reply_count, 2500)
      self.assertEqual(total, 7500)
      self.assertTrue(self.db.save_processed_channel.called)

    asyncio.run(_run())


from chat.chat_service import ChatScannerService
import pandas as pd

class TestChatScannerService(unittest.TestCase):

  def setUp(self):
    self.db = mock.Mock(spec=DatabaseManager)
    self.client = mock.Mock(spec=GraphBatchClient)
    self.metrics = mock.Mock(spec=MetricsRegistry)
    self.audit_logger = mock.Mock()
    self.stop_event = mock.Mock()
    self.stop_event.is_set.return_value = False
    self.log_func = mock.Mock()
    self.ui_callback = mock.Mock()

    self.service = ChatScannerService(
        db=self.db,
        client=self.client,
        metrics=self.metrics,
        audit_logger=self.audit_logger,
        stop_event=self.stop_event,
        log_func=self.log_func,
        ui_callback=self.ui_callback,
    )

    self.token_manager = mock.Mock(spec=TokenManager)
    self.token_data = {"token": "fake_token"}
    self.token_manager.get_valid_token_slot.return_value = self.token_data
    self.session = mock.Mock()
    self.token_manager.get_session.return_value = self.session

  @mock.patch('pandas.read_csv')
  @mock.patch('os.path.exists')
  def test_execute_scan_heuristics_with_csv(self, mock_exists, mock_read_csv):
    """Verify heuristics mode uses CSV baseline."""
    mock_exists.return_value = True
    
    real_df = pd.DataFrame({
        "Email or Team ID": ["user1@test.com", "user2@test.com", "team1", "team2"],
        "Type": ["User", "User", "Team", "Team"]
    })
    mock_read_csv.return_value = real_df

    config = mock.Mock()
    config.mode = "heuristics"
    config.user_source = "csv"
    config.csv_path = "fake.csv"
    config.tenant_id = "tenant_id"
    config.percent = 10.0
    config.sample_percentage = 10.0

    scanner_mock = mock.Mock()
    scanner_mock.fetch_report_user_detail.return_value = {"total_users": 100, "total_chats": 1000}
    scanner_mock.fetch_report_team_activity.return_value = {"teams": 10, "channels": 50, "messages": 5000}
    
    with mock.patch('chat.chat_service.MigrationScanner') as mock_scanner_class:
        mock_scanner_class.return_value = scanner_mock
        
        result = self.service.execute_scan(config, self.token_manager, 10)
        
        self.assertEqual(result["total_users"], 2)
        self.assertEqual(result["total_teams"], 2)
        self.assertEqual(result["private_chats"], 100) # 2 * 150 / 3
        self.assertEqual(result["channels"], 7) # 2 * 3.5

  @mock.patch('pandas.read_csv')
  @mock.patch('os.path.exists')
  def test_execute_scan_sampling_with_csv(self, mock_exists, mock_read_csv):
    """Verify sampling mode uses CSV entities."""
    mock_exists.return_value = True
    
    real_df = pd.DataFrame({
        "Email or Team ID": ["user1@test.com", "user2@test.com", "team1", "team2"],
        "Type": ["User", "User", "Team", "Team"]
    })
    mock_read_csv.return_value = real_df

    config = mock.Mock()
    config.mode = "sampling"
    config.user_source = "csv"
    config.csv_path = "fake.csv"
    config.tenant_id = "tenant_id"
    config.percent = 100.0
    config.sample_percentage = 100.0

    scanner_mock = mock.Mock()
    scanner_mock.fetch_user_chat_counts_batch.return_value = {"user1@test.com": ["chat1"], "user2@test.com": ["chat2"]}
    scanner_mock.fetch_all_channels_for_teams_batch.return_value = {"team1": {"channels": 2, "all_channel_ids": ["ch1", "ch2"]}, "team2": {"channels": 1, "all_channel_ids": ["ch3"]}}
    scanner_mock.fetch_team_details_batch.return_value = {"team1": {"members": 5}, "team2": {"members": 3}}

    with mock.patch('chat.chat_service.MigrationScanner') as mock_scanner_class:
        mock_scanner_class.return_value = scanner_mock
        
        self.service._scan_sampled_chats = mock.Mock(return_value=(10, 20, 2))
        
        async def mock_async_scan(*args, **kwargs):
            return 100, 2
            
        self.service._scan_sampled_channels_async = mock_async_scan
        
        result = self.service.execute_scan(config, self.token_manager, 10)
        
        self.assertEqual(result["total_users"], 2)
        self.assertEqual(result["total_teams"], 2)
        self.assertEqual(result["channels"], 3)

  @mock.patch('pandas.read_csv')
  @mock.patch('os.path.exists')
  def test_execute_scan_sampling_with_only_teams_csv(self, mock_exists, mock_read_csv):
    """Verify sampling mode works with only teams in CSV."""
    mock_exists.return_value = True
    
    real_df = pd.DataFrame({
        "Email or Team ID": ["team1", "team2"],
        "Type": ["Team", "Team"]
    })
    mock_read_csv.return_value = real_df

    config = mock.Mock()
    config.mode = "sampling"
    config.user_source = "csv"
    config.csv_path = "fake.csv"
    config.tenant_id = "tenant_id"
    config.percent = 100.0
    config.sample_percentage = 100.0

    scanner_mock = mock.Mock()
    scanner_mock.fetch_all_channels_for_teams_batch.return_value = {"team1": {"channels": 2, "all_channel_ids": ["ch1", "ch2"]}, "team2": {"channels": 1, "all_channel_ids": ["ch3"]}}
    scanner_mock.fetch_team_details_batch.return_value = {"team1": {"members": 5}, "team2": {"members": 3}}

    with mock.patch('chat.chat_service.ChatScannerService._scan_sampled_channels_async') as mock_async_scan:
        mock_async_scan.return_value = (100, 2)
        
        with mock.patch('chat.chat_service.MigrationScanner') as mock_scanner_class:
            mock_scanner_class.return_value = scanner_mock
            
            result = self.service.execute_scan(config, self.token_manager, 10)
            
            self.assertEqual(result["total_users"], 0)
            self.assertEqual(result["total_teams"], 2)
            self.assertEqual(result["channels"], 3)
            self.assertEqual(result["private_chats"], 0)

if __name__ == "__main__":
  unittest.main()
