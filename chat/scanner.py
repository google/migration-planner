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

"""Algorithmic conducting engine handling pagination, estimation, and traversal."""

import asyncio
import concurrent.futures
import csv
import datetime
import io
import logging
import threading
import typing

import aiohttp
from util.auth_manager import TokenManager
from util.batch_client import GraphBatchClient
from util.constants import GRAPH_BASE_URL
from util.db_manager import DatabaseManager
from util.rate_limiter import HybridLimiter
from util.state_registry import AuditLogger
from util.state_registry import MetricsRegistry

logger = logging.getLogger(__name__)


class MigrationScanner:
  """Conducts highly complex data collection scans including extrapolation."""

  db: DatabaseManager
  client: GraphBatchClient
  metrics: MetricsRegistry
  audit_logger: AuditLogger | None
  default_limiter: HybridLimiter
  chat_limiter: HybridLimiter
  chat_batch_limiter: HybridLimiter
  channel_batch_limiter: HybridLimiter
  stop_event: threading.Event
  log_func: typing.Callable[[str], None] | None

  def __init__(
      self,
      db: DatabaseManager,
      client: GraphBatchClient,
      metrics: MetricsRegistry,
      audit_logger: AuditLogger | None = None,
      default_limiter: HybridLimiter | None = None,
      chat_limiter: HybridLimiter | None = None,
      chat_batch_limiter: HybridLimiter | None = None,
      channel_batch_limiter: HybridLimiter | None = None,
      stop_event: threading.Event | None = None,
      log_func: typing.Callable[[str], None] | None = None,
  ) -> None:
    self.db = db
    self.client = client
    self.metrics = metrics
    self.audit_logger = audit_logger
    self.log_func = log_func
    self.default_limiter = default_limiter or HybridLimiter(
        requests_per_second=20.0
    )
    self.chat_limiter = chat_limiter or HybridLimiter(requests_per_second=30.0)
    self.chat_batch_limiter = chat_batch_limiter or HybridLimiter(
        requests_per_second=1.5
    )
    self.channel_batch_limiter = channel_batch_limiter or HybridLimiter(
        requests_per_second=3.0
    )
    self.stop_event = stop_event or threading.Event()

  def _sanitize_and_parse_csv(self, content: str) -> list[dict[str, str]]:
    """Universal UTF-8 BOM stripping and safe dictionary generation."""
    if content.startswith("\ufeff"):
      content = content.lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(content))
    return list(reader)

  def _get_case_insensitive(self, row: dict[str, str], key: str) -> str:
    """Protects against NoneType headers and yields case agnostic values."""
    for header_key, header_value in row.items():
      if header_key and header_key.lower() == key.lower():
        return header_value or "0"
    return "0"

  def count_chat_messages(
      self, token_manager: TokenManager, chat_id: str
  ) -> int:
    """Acquires total message count using hybrid linear and binary searching."""
    stop = self.stop_event
    if "19:meeting_" in chat_id:
      # Currently meeting-based chats are not part of the migration.
      return 0

    cached_count = self.db.get_processed_chat(chat_id)
    if cached_count is not None:
      logger.info("Chat cache hit %s. Count: %d", chat_id, cached_count)
      return cached_count

    count = 0
    base_url = f"{GRAPH_BASE_URL}/chats/{chat_id}/messages?$top=50"
    token_data = token_manager.get_valid_token_slot()
    token = token_data["token"]
    session = token_manager.get_session()
    headers = {"Authorization": f"Bearer {token}"}

    pages = 0
    next_link = base_url
    hit_limit = False

    try:
      while next_link and not stop.is_set():
        if pages >= 15:
          hit_limit = True
          break

        response = self.client.get_with_retry(
            session,
            next_link,
            headers,
            token_manager,
            token_data,
            self.metrics,
            self.chat_limiter,
            self.audit_logger,
        )
        response_data = response.json()
        value = response_data.get("value", [])
        count += len(value)
        next_link = response_data.get("@odata.nextLink")
        pages += 1

      if hit_limit:
        logger.info(
            "Extrapolating heavy chat %s. Launching jump scan.", chat_id
        )

        async def _chat_probe(skip_value: int) -> bool:
          def _sync_probe():
            test_url = f"{GRAPH_BASE_URL}/chats/{chat_id}/messages?$top=1&$skip={skip_value}"
            response = self.client.get_with_retry(
                session,
                test_url,
                headers,
                token_manager,
                token_data,
                self.metrics,
                self.default_limiter,
            )
            return bool(response.json().get("value"))

          return await asyncio.to_thread(_sync_probe)

        async def _run_extrapolation():
          lower_bound, upper_bound = await self._jump_scan(
              _chat_probe, count, 10000
          )
          if upper_bound is None:
            upper_bound = 10000
          return await self._binary_search(
              _chat_probe, lower_bound, upper_bound
          )

        count = asyncio.run(_run_extrapolation())

    except Exception:
      # Fixed Critical: Prevents recording partial states on terminal crash.
      logger.exception("Critical chat scan interruption on %s.", chat_id)
      raise
    finally:
      token_manager.return_token_slot(token_data)

    # Success confirmation only executes on clean exhaust cycle completion.
    self.db.save_processed_chat(chat_id, count)
    return count

  async def count_channel_messages_async(
      self,
      token_manager: TokenManager,
      session: aiohttp.ClientSession,
      team_id: str,
      channel_id: str,
      token_data: dict[str, typing.Any],
  ) -> tuple[int, int, int]:
    """Asynchronous heavy scanning enforcing density estimation math."""
    stop = self.stop_event
    cached = await asyncio.to_thread(
        self.db.get_processed_channel, team_id, channel_id
    )
    if cached is not None:
      return cached

    root_count = 0
    pages = 0
    next_link = (
        f"{GRAPH_BASE_URL}/teams/{team_id}/channels/"
        f"{channel_id}/messages?$top=50"
    )
    hit_limit = False
    headers = {"Authorization": f"Bearer {token_data['token']}"}

    def convert_timestamp(timestamp_str: str) -> datetime.datetime:
      return datetime.datetime.fromisoformat(
          timestamp_str.split(".")[0].replace("Z", "") + "+00:00"
      )

    try:
      while next_link and not stop.is_set():
        if pages >= 15:
          hit_limit = True
          break

        response_data, status = await self.client.get_with_retry_async(
            session,
            next_link,
            headers,
            token_manager,
            token_data,
            self.metrics,
            self.default_limiter,
        )
        if status in [403, 404]:
          logger.warning(
              "Channel %s in team %s is inaccessible or deleted (status %d)."
              " Skipping.",
              channel_id,
              team_id,
              status,
          )
          return None
        elif status != 200 or not response_data:
          raise ConnectionError(
              f"Unexpected async status {status} for channel."
          )

        value = response_data.get("value", [])
        root_count += len(value)
        next_link = response_data.get("@odata.nextLink")
        pages += 1

      if hit_limit:
        logger.info("Heavy channel limit hit %s. Entering jumping.", channel_id)

        async def _chan_probe(skip_value: int) -> bool:
          test_url = (
              f"{GRAPH_BASE_URL}/teams/{team_id}/channels/{channel_id}"
              f"/messages?$top=1&$skip={skip_value}"
          )
          probe_data, probe_status = await self.client.get_with_retry_async(
              session,
              test_url,
              headers,
              token_manager,
              token_data,
              self.metrics,
              self.default_limiter,
          )
          return bool(
              probe_status == 200 and probe_data and probe_data.get("value")
          )

        lower_bound, upper_bound = await self._jump_scan(
            _chan_probe, root_count, 10000
        )
        if lower_bound == 10000 and upper_bound is None:
          # Case 1: Count is >= 10,000. Probing beyond is not supported by $skip.
          logger.info(
              "Heavy channel limit hit %s. Attempting density estimation.",
              channel_id,
          )
          root_count = 10000  # Fallback if density estimation fails

          try:
            latest_url = f"{GRAPH_BASE_URL}/teams/{team_id}/channels/{channel_id}/messages?$top=1"
            latest_data, _ = await self.client.get_with_retry_async(
                session,
                latest_url,
                headers,
                token_manager,
                token_data,
                self.metrics,
                self.default_limiter,
            )
            latest_value = latest_data.get("value", []) if latest_data else []
            latest_time_str = (
                latest_value[0].get("createdDateTime") if latest_value else None
            )

            url_10k = f"{GRAPH_BASE_URL}/teams/{team_id}/channels/{channel_id}/messages?$top=1&$skip=10000"
            data_10k, _ = await self.client.get_with_retry_async(
                session,
                url_10k,
                headers,
                token_manager,
                token_data,
                self.metrics,
                self.default_limiter,
            )
            value_10k = data_10k.get("value", []) if data_10k else []
            time_10k_str = (
                value_10k[0].get("createdDateTime") if value_10k else None
            )

            chan_url = f"{GRAPH_BASE_URL}/teams/{team_id}/channels/{channel_id}"
            channel_data, _ = await self.client.get_with_retry_async(
                session,
                chan_url,
                headers,
                token_manager,
                token_data,
                self.metrics,
                self.default_limiter,
            )
            channel_creation_time_str = (
                channel_data.get("createdDateTime") if channel_data else None
            )

            if latest_time_str and time_10k_str and channel_creation_time_str:
              total_duration_seconds = (
                  convert_timestamp(latest_time_str)
                  - convert_timestamp(channel_creation_time_str)
              ).total_seconds()
              duration_10k_delta = (
                  convert_timestamp(latest_time_str)
                  - convert_timestamp(time_10k_str)
              ).total_seconds()

              if total_duration_seconds > 0 and duration_10k_delta > 0:
                root_count = max(
                    lower_bound,
                    int(10000 * (total_duration_seconds / duration_10k_delta)),
                )
                logger.info("Density estimation successful: %d", root_count)
          except Exception as error:
            logger.warning("Density extrapolation failed: %s.", error)

          if root_count == 10000:
            logger.warning(
                "Density estimation failed or not applicable. Capping count at"
                " 10k."
            )
        else:
          # Case 2: Count is < 10,000. We can safely binary search.
          root_count = await self._binary_search(
              _chan_probe, lower_bound, upper_bound
          )

    except Exception:
      logger.exception("Critical async scan interrupt. Aborting DB write.")
      raise

    reply_count = int(root_count * 0.5)
    total = root_count + reply_count

    await asyncio.to_thread(
        self.db.save_processed_channel,
        team_id,
        channel_id,
        root_count,
        reply_count,
        total,
    )
    return root_count, reply_count, total

  def fetch_report_user_detail(
      self, token_manager: TokenManager
  ) -> dict[str, int] | None:
    """Pulls D180 interval report dataset for global counting."""
    url = f"{GRAPH_BASE_URL}/reports/getTeamsUserActivityUserDetail(period='D180')"
    token_data = token_manager.get_valid_token_slot()
    headers = {"Authorization": f"Bearer {token_data['token']}"}
    try:
      response = self.client.get_with_retry(
          token_manager.get_session(),
          url,
          headers,
          token_manager,
          token_data,
          self.metrics,
          self.default_limiter,
      )
      if response.status_code != 200:
        return None
      rows = self._sanitize_and_parse_csv(response.text)
      total_chats, users = 0, 0
      for row in rows:
        users += 1
        chat_count_str = self._get_case_insensitive(
            row, "Private Chat Message Count"
        )
        try:
          total_chats += int(chat_count_str or "0")
        except (ValueError, TypeError):
          pass
      return {"total_users": users, "total_chats": total_chats}
    finally:
      token_manager.return_token_slot(token_data)

  def fetch_report_team_activity(
      self, token_manager: TokenManager
  ) -> dict[str, typing.Any] | None:
    """Pulls activity data and triggers serialized field parsing."""
    url = f"{GRAPH_BASE_URL}/reports/getTeamsTeamActivityDetail(period='D180')"
    token_data = token_manager.get_valid_token_slot()
    headers = {"Authorization": f"Bearer {token_data['token']}"}
    try:
      response = self.client.get_with_retry(
          token_manager.get_session(),
          url,
          headers,
          token_manager,
          token_data,
          self.metrics,
          self.default_limiter,
      )
      if response.status_code != 200:
        return None
      # Apply universal parser logic.
      rows = self._sanitize_and_parse_csv(response.text)
      teams_count, channels_count, messages_count, ids = 0, 0, 0, []

      for row in rows:
        team_id = self._get_case_insensitive(row, "Team Id")
        if not team_id or team_id == "0":
          continue
        teams_count += 1
        ids.append(team_id)
        try:
          channels_value = self._get_case_insensitive(row, "Active Channels")
          messages_value = self._get_case_insensitive(row, "Channel Messages")
          channels_count += int(channels_value)
          messages_count += int(messages_value)
        except (ValueError, TypeError):
          pass
      return {
          "teams": teams_count,
          "channels": channels_count,
          "messages": messages_count,
          "ids": ids,
      }
    finally:
      token_manager.return_token_slot(token_data)

  def fetch_user_chat_counts_batch(
      self,
      token_manager: TokenManager,
      users: list[dict[str, typing.Any]],
      ui_callback: typing.Callable[..., None] | None = None,
  ) -> dict[str, list[str]]:
    """Constructs concurrency bound workers for aggregated batch extraction."""
    counts: dict[str, list[str]] = {}
    to_do = []
    for user in users:
      user_id = user.get("id") or user.get("userPrincipalName")
      cached = self.db.get_processed_user(user_id) if user_id else None
      if cached is not None:
        counts[user_id] = cached
      elif user_id:
        to_do.append(user)

    if not to_do:
      return counts

    chunks = [to_do[i : i + 20] for i in range(0, len(to_do), 20)]

    def _worker(chunk: list[dict[str, typing.Any]]) -> dict[str, list[str]]:
      if self.stop_event.is_set():
        return {}
      token_data = token_manager.get_valid_token_slot()
      batch_requests = []
      request_map = {}
      for index, user in enumerate(chunk):
        user_id = user.get("id") or user.get("userPrincipalName")
        request_map[str(index)] = user_id
        batch_requests.append({
            "id": str(index),
            "method": "GET",
            "url": f"/users/{user_id}/chats",
            "headers": {"ConsistencyLevel": "eventual"},
        })
      try:
        responses = self.client.execute_batch_request(
            token_manager.get_session(),
            f"{GRAPH_BASE_URL}/$batch",
            token_manager,
            token_data,
            batch_requests,
            self.metrics,
            self.chat_batch_limiter,
            self.stop_event,
            "user_chats",
        )
        local_counts = {}
        for request_id, response in responses.items():
          user_id = request_map.get(request_id)
          if not user_id:
            continue
          if response.get("status") == 200:
            body = response.get("body", {})
            chat_id_list = [
                chat.get("id")
                for chat in body.get("value", [])
                if chat.get("id")
            ]
            local_counts[user_id] = chat_id_list
            self.db.save_processed_user(user_id, chat_id_list)
          else:
            status_val = response.get("status")
            body = response.get("body", {})
            error_msg = body.get("error", {}).get("message", "Unknown error")
            if self.log_func:
              self.log_func(
                  f"WARNING: Batch request for user {user_id} chats returned status {status_val}: {error_msg}"
              )
        return local_counts
      except Exception:
        logger.exception("Async batch failure trapped inside worker.")
        return {}
      finally:
        token_manager.return_token_slot(token_data)

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
      futures = [executor.submit(_worker, chunk) for chunk in chunks]
      for future in concurrent.futures.as_completed(futures):
        result = future.result()
        counts.update(result)
        if ui_callback:
          progress_val = 0.2 * (len(counts) / max(1, len(users)))
          ui_callback(
              "scan_progress",
              source="chats",
              entity_type="Chats",
              progress=progress_val,
              extra_text=(
                  f"Enumerating Chats ({len(counts)}/{len(users)} users)"
              ),
              processed=len(counts),
              failed=0,
              cumulative=0,
          )
    return counts

  def fetch_users_joined_teams_batch(
      self,
      token_manager: TokenManager,
      users: list[dict[str, typing.Any]],
      ui_callback: typing.Callable[..., None] | None = None,
  ) -> dict[str, str]:
    """Fetch unique teams that the given users are members of.

    Returns:
      A dictionary mapping team_id to team_displayName.
    """
    unique_teams: dict[str, str] = {}
    to_do = []
    for user in users:
      user_id = user.get("id") or user.get("userPrincipalName")
      if user_id:
        to_do.append(user)

    if not to_do:
      return {}

    chunks = [to_do[i : i + 20] for i in range(0, len(to_do), 20)]

    def _worker(chunk: list[dict[str, typing.Any]]) -> dict[str, str]:
      if self.stop_event.is_set():
        return {}
      token_data = token_manager.get_valid_token_slot()
      batch_requests = []
      request_map = {}
      for index, user in enumerate(chunk):
        user_id = user.get("id") or user.get("userPrincipalName")
        request_map[str(index)] = user_id
        batch_requests.append({
            "id": str(index),
            "method": "GET",
            "url": f"/users/{user_id}/joinedTeams?$select=id,displayName",
        })
      try:
        responses = self.client.execute_batch_request(
            token_manager.get_session(),
            f"{GRAPH_BASE_URL}/$batch",
            token_manager,
            token_data,
            batch_requests,
            self.metrics,
            self.chat_batch_limiter,
            self.stop_event,
            "user_joined_teams",
        )
        local_teams = {}
        for request_id, response in responses.items():
          user_id = request_map.get(request_id)
          if not user_id:
            continue
          if response.get("status") == 200:
            body = response.get("body", {})
            for team in body.get("value", []):
              t_id = team.get("id")
              t_name = team.get("displayName")
              if t_id:
                local_teams[t_id] = t_name or t_id
          else:
            status_val = response.get("status")
            body = response.get("body", {})
            error_msg = body.get("error", {}).get("message", "Unknown error")
            if self.log_func:
              self.log_func(
                  f"WARNING: Batch request for user {user_id} joinedTeams returned status {status_val}: {error_msg}"
              )
        return local_teams
      except Exception:
        logger.exception("Async batch failure trapped inside joinedTeams worker.")
        return {}
      finally:
        token_manager.return_token_slot(token_data)

    processed_users = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
      futures = {executor.submit(_worker, chunk): chunk for chunk in chunks}
      for future in concurrent.futures.as_completed(futures):
        chunk = futures[future]
        result = future.result()
        unique_teams.update(result)
        processed_users += len(chunk)
        if ui_callback:
          progress_val = 0.1 * (processed_users / max(1, len(to_do)))
          ui_callback(
              "scan_progress",
              source="channels",
              entity_type="Teams",
              progress=progress_val,
              extra_text=(
                  f"Resolving user teams ({processed_users}/{len(to_do)} users)"
              ),
              processed=processed_users,
              failed=0,
              cumulative=len(unique_teams),
          )
    return unique_teams


  def fetch_all_channels_for_teams_batch(
      self,
      token_manager: TokenManager,
      teams: list[str],
      ui_callback: typing.Callable[..., None] | None = None,
  ) -> dict[str, dict[str, typing.Any]]:
    """Orchestrates threaded batch fetching of team channel sub-structures."""
    details: dict[str, dict[str, typing.Any]] = {}
    to_do = []
    for team_id in teams:
      cached = self.db.get_processed_team(team_id)
      if cached and "all_channel_ids" in cached:
        details[team_id] = cached
      else:
        to_do.append(team_id)

    if not to_do:
      return details

    chunks = [to_do[i : i + 20] for i in range(0, len(to_do), 20)]

    def _worker(chunk: list[str]) -> dict[str, dict[str, typing.Any]]:
      if self.stop_event.is_set():
        return {}
      token_data = token_manager.get_valid_token_slot()
      batch_requests = []
      request_map = {}
      for index, team_id in enumerate(chunk):
        request_id = f"{index}_ch"
        request_map[request_id] = team_id
        batch_requests.append({
            "id": request_id,
            "method": "GET",
            "url": f"/teams/{team_id}/channels",
        })
      try:
        responses = self.client.execute_batch_request(
            token_manager.get_session(),
            f"{GRAPH_BASE_URL}/$batch",
            token_manager,
            token_data,
            batch_requests,
            self.metrics,
            self.channel_batch_limiter,
            self.stop_event,
            "channels_batch",
        )
        local_details = {}
        for request_id, response in responses.items():
          team_id = request_map.get(request_id)
          if not team_id:
            continue
          if response.get("status") == 200:
            channels_list = response.get("body", {}).get("value", [])
            all_channels = [
                channel.get("id")
                for channel in channels_list
                if channel.get("id")
            ]
            private_channels = [
                channel.get("id")
                for channel in channels_list
                if channel.get("id")
                and channel.get("membershipType") in ("private", "shared")
            ]
            team_entry = {
                "channels": len(all_channels),
                "all_channel_ids": all_channels,
                "private_channels": len(private_channels),
            }
            local_details[team_id] = team_entry
            self.db.save_processed_team(team_id, team_entry)
          else:
            status_val = response.get("status")
            body = response.get("body", {})
            error_msg = body.get("error", {}).get("message", "Unknown error")
            if self.log_func:
              self.log_func(
                  f"WARNING: Batch request for team {team_id} channels returned status {status_val}: {error_msg}"
              )
        return local_details
      except Exception:
        logger.exception("Failed to fetch channels for teams batch.")
        return {}
      finally:
        token_manager.return_token_slot(token_data)

    processed_count = len(details)
    total_to_do = len(to_do)

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
      futures = [executor.submit(_worker, chunk) for chunk in chunks]
      for future in concurrent.futures.as_completed(futures):
        result = future.result()
        details.update(result)
        if ui_callback and total_to_do > 0:
          processed_count += len(result)
          progress_val = 0.4 * (processed_count / max(1, len(teams)))
          ui_callback(
              "scan_progress",
              source="channels",
              entity_type="Channels",
              progress=progress_val,
              extra_text=(
                  f"Enumerating Channels ({processed_count}/{len(teams)} teams)"
              ),
              processed=processed_count,
              failed=0,
              cumulative=0,
          )
    return details

  def fetch_all_users_graph(
      self, token_manager: TokenManager
  ) -> list[dict[str, typing.Any]]:
    """Retrieves absolute unified roster of users across target directory."""
    users = []
    url = f"{GRAPH_BASE_URL}/users?$select=id,userPrincipalName&$top=999"
    token_data = token_manager.get_valid_token_slot()
    headers = {"Authorization": f"Bearer {token_data['token']}"}
    try:
      while url and not self.stop_event.is_set():
        response = self.client.get_with_retry(
            token_manager.get_session(),
            url,
            headers,
            token_manager,
            token_data,
            self.metrics,
            self.default_limiter,
        )
        if response.status_code != 200:
          break
        response_data = response.json()
        users.extend(response_data.get("value", []))
        url = response_data.get("@odata.nextLink")
      return users
    finally:
      token_manager.return_token_slot(token_data)

  def fetch_all_teams_graph(
      self, token_manager: TokenManager
  ) -> list[dict[str, typing.Any]]:
    """Recovers fully indexed set of active graph team identities."""
    cached = self.db.get_roster_teams()
    if cached is not None:
      return cached

    teams = []
    url = (
        f"{GRAPH_BASE_URL}/groups?$filter=resourceProvisioningOptions/any(x:x"
        " eq 'Team')&$select=id,displayName&$top=999"
    )
    token_data = token_manager.get_valid_token_slot()
    headers = {"Authorization": f"Bearer {token_data['token']}"}
    try:
      while url and not self.stop_event.is_set():
        response = self.client.get_with_retry(
            token_manager.get_session(),
            url,
            headers,
            token_manager,
            token_data,
            self.metrics,
            self.default_limiter,
        )
        if response.status_code != 200:
          break
        response_data = response.json()
        teams.extend(response_data.get("value", []))
        url = response_data.get("@odata.nextLink")
    finally:
      token_manager.return_token_slot(token_data)

    if teams and self.stop_event and not self.stop_event.is_set():
      self.db.save_roster_teams(teams)
    return teams

  def fetch_team_details_batch(
      self, token_manager: TokenManager, teams: list[str]
  ) -> dict[str, dict[str, typing.Any]]:
    """Simultaneously gathers comprehensive channel and membership counts."""
    details: dict[str, dict[str, typing.Any]] = {}
    to_do = []
    for team_id in teams:
      cached = self.db.get_processed_team(team_id)
      if cached and "members" in cached:
        details[team_id] = cached
      else:
        to_do.append(team_id)

    if not to_do:
      return details

    # Explicit 10-batch segmentation for high bandwidth safety.
    chunks = [to_do[i : i + 10] for i in range(0, len(to_do), 10)]

    def _worker(chunk: list[str]) -> dict[str, dict[str, typing.Any]]:
      stop = self.stop_event
      if stop.is_set():
        return {}
      token_data = token_manager.get_valid_token_slot()
      batch_requests = []
      request_map = {}
      for index, team_id in enumerate(chunk):
        request_id_channels, request_id_members = f"{index}_ch", f"{index}_mem"
        request_map[request_id_channels], request_map[request_id_members] = (
            team_id,
            "channels",
        ), (
            team_id,
            "members",
        )
        batch_requests.extend([
            {
                "id": request_id_channels,
                "method": "GET",
                "url": f"/teams/{team_id}/channels",
            },
            {
                "id": request_id_members,
                "method": "GET",
                "url": f"/teams/{team_id}/members",
            },
        ])
      try:
        responses = self.client.execute_batch_request(
            token_manager.get_session(),
            f"{GRAPH_BASE_URL}/$batch",
            token_manager,
            token_data,
            batch_requests,
            self.metrics,
            self.default_limiter,
            self.stop_event,
            "teams_details",
        )
        local_details: dict[str, dict[str, typing.Any]] = {}
        for request_id, response in responses.items():
          request_info = request_map.get(request_id)
          if not request_info:
            continue
          team_id, detail_type = request_info
          if team_id not in local_details:
            existing_team = self.db.get_processed_team(team_id)
            if existing_team is not None:
              local_details[team_id] = existing_team
            else:
              local_details[team_id] = {
                  "channels": 0,
                  "members": 0,
              }

          if response.get("status") == 200:
            items_list = response.get("body", {}).get("value", [])
            # Pagination capture for heavy collections.
            next_link = response.get("body", {}).get("@odata.nextLink")
            while next_link and not stop.is_set():  # type: ignore
              headers = {"Authorization": f"Bearer {token_data['token']}"}
              next_response = self.client.get_with_retry(
                  token_manager.get_session(),
                  next_link,
                  headers,
                  token_manager,
                  token_data,
                  self.metrics,
                  self.default_limiter,
              )
              try:
                next_data = next_response.json()
                items_list.extend(next_data.get("value", []))
                next_link = next_data.get("@odata.nextLink")
              except ValueError:
                break
            local_details[team_id][detail_type] = len(items_list)
            if detail_type == "channels":
              local_details[team_id]["all_channel_ids"] = [
                  channel.get("id")
                  for channel in items_list
                  if channel.get("id")
              ]
              local_details[team_id]["private_channel_ids"] = [
                  channel.get("id")
                  for channel in items_list
                  if channel.get("id")
                  and channel.get("membershipType") in ("private", "shared")
              ]
              local_details[team_id]["private_channels"] = len(
                  local_details[team_id]["private_channel_ids"]
              )
            self.db.save_processed_team(team_id, local_details[team_id])
          else:
            status_val = response.get("status")
            body = response.get("body", {})
            error_msg = body.get("error", {}).get("message", "Unknown error")
            if self.log_func:
              self.log_func(
                  f"WARNING: Batch request for team {team_id} {detail_type} returned status {status_val}: {error_msg}"
              )
        return local_details
      except Exception:
        logger.exception("Failed to fetch team details batch.")
        return {}
      finally:
        token_manager.return_token_slot(token_data)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
      futures = [executor.submit(_worker, chunk) for chunk in chunks]
      for future in concurrent.futures.as_completed(futures):
        details.update(future.result())
    return details

  async def _jump_scan(
      self, probe_func, lower_bound: int, max_skip: int
  ) -> tuple[int, int | None]:
    """Executes exponential doubling jump iterations to bound the data set."""
    stop = self.stop_event
    current_skip = max(1000, lower_bound * 2)
    upper_bound = None
    while current_skip <= max_skip and not stop.is_set():
      found = None
      for attempt in range(4):  # 1 initial attempt + 3 retries
        try:
          if attempt > 0:
            logger.warning(
                "Probe failed in jump_scan, retrying... (attempt %d)", attempt
            )
            if stop.is_set():
              raise asyncio.CancelledError("Stop event set during retry")
            await asyncio.sleep(1 * attempt)

          found = await probe_func(current_skip)
          break  # Success, break the retry loop
        except Exception as error:
          if attempt == 3:
            logger.exception(
                "Probe failed permanently in jump_scan after 3 retries."
            )
            raise error
          logger.warning(
              "Probe attempt %d failed in jump_scan: %s", attempt + 1, error
          )

      if found:
        lower_bound = current_skip
        if current_skip >= max_skip:
          break
        current_skip = min(max_skip, current_skip * 2)
      else:
        upper_bound = current_skip
        break
    return lower_bound, upper_bound

  async def _binary_search(
      self, probe_func, lower_bound: int, upper_bound: int
  ) -> int:
    """Drives strict convergence iteration to resolve precise final counts."""
    stop = self.stop_event
    while (
        upper_bound
        and (upper_bound - lower_bound) > 1
        and (upper_bound - lower_bound) >= 0.05 * upper_bound
        and not stop.is_set()
    ):
      mid = (lower_bound + upper_bound) // 2
      found = None

      for attempt in range(4):  # 1 initial attempt + 3 retries
        try:
          if attempt > 0:
            logger.warning("Probe failed, retrying... (attempt %d)", attempt)
            if stop.is_set():
              raise asyncio.CancelledError("Stop event set during retry")
            await asyncio.sleep(1 * attempt)

          found = await probe_func(mid)
          break  # Success, break the retry loop
        except Exception as error:
          if attempt == 3:
            logger.exception("Probe failed permanently after 3 retries.")
            raise error
          logger.warning("Probe attempt %d failed: %s", attempt + 1, error)

      if found:
        lower_bound = mid
      else:
        upper_bound = mid

    return lower_bound
