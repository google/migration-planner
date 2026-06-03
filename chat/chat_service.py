import asyncio
import concurrent.futures
import hashlib
import json
import os
import random
import ssl
import time
import traceback
import typing

import aiohttp
import certifi
from chat.scanner import MigrationScanner
import pandas as pd
import requests
from util.auth_manager import TokenManager
from util.batch_client import GraphBatchClient
from util.constants import GRAPH_BASE_URL
from util.db_manager import DatabaseManager
from util.rate_limiter import HybridLimiter
import util.state_registry


class ChatScannerService:
  """Unified service executing end-to-end scan routines without platform-coupling."""

  def __init__(
      self,
      db: DatabaseManager,
      client: GraphBatchClient,
      metrics: util.state_registry.MetricsRegistry,
      audit_logger: util.state_registry.AuditLogger,
      stop_event: typing.Any,
      log_func: typing.Callable[[str], None],
      ui_callback: typing.Callable[..., None],
  ) -> None:
    self.db = db
    self.client = client
    self.metrics = metrics
    self.audit_logger = audit_logger
    self.stop_event = stop_event
    self.log_func = log_func
    self.ui_callback = ui_callback

    # Instantiate internal limiters
    self.default_limiter = HybridLimiter(requests_per_second=20.0)
    self.chat_limiter = HybridLimiter(requests_per_second=30.0)
    self.chat_batch_limiter = HybridLimiter(requests_per_second=1.5)
    self.channel_batch_limiter = HybridLimiter(requests_per_second=3.0)

  def _process_single_chat(
      self,
      chat_id: str,
      auth: TokenManager,
      scanner: MigrationScanner,
      token_data: typing.Dict[str, typing.Any],
      session: requests.Session,
  ) -> typing.Tuple[bool, int, int]:
    """Helper to scan a single private chat and fetch membership."""
    if self.stop_event.is_set():
      return False, 0, 0
    try:
      start_time = time.monotonic()
      message_count = scanner.count_chat_messages(auth, chat_id)
      wait_time = max(0, 1.0 - (time.monotonic() - start_time))
      if wait_time > 0:
        time.sleep(wait_time)
      members_response = self.client.get_with_retry(
          session,
          f"{GRAPH_BASE_URL}/chats/{chat_id}/members",
          {"Authorization": f"Bearer {token_data['token']}"},
          auth,
          token_data,
          self.metrics,
          self.default_limiter,
      )
      member_count = len(members_response.json().get("value", []))
      return True, message_count, member_count
    except Exception:
      return False, 0, 0

  def _scan_sampled_chats(
      self,
      auth: TokenManager,
      scanner: MigrationScanner,
      chat_id_pool: typing.List[str],
      concurrency_val: int,
  ) -> typing.Tuple[int, int, int]:
    """Executes threaded scan of user private chat pool."""
    messages_aggregate, members_aggregate, successes_aggregate = 0, 0, 0
    token_data = auth.get_valid_token_slot()
    session = auth.get_session()

    try:
      with concurrent.futures.ThreadPoolExecutor(
          max_workers=concurrency_val
      ) as executor_pool:
        futures = [
            executor_pool.submit(
                self._process_single_chat,
                chat_id,
                auth,
                scanner,
                token_data,
                session,
            )
            for chat_id in chat_id_pool
        ]
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
          is_success, message_count, member_count = future.result()
          if is_success:
            messages_aggregate += message_count
            members_aggregate += member_count
            successes_aggregate += 1
          self.ui_callback(
              "scan_progress",
              source="chats",
              entity_type="Chats",
              progress=0.2 + 0.8 * (i / max(1, len(futures))),
              extra_text=f"Scanning Chats ({i}/{len(futures)})",
              processed=i,
              failed=i - successes_aggregate,
              cumulative=messages_aggregate,
          )
    finally:
      auth.return_token_slot(token_data)
    return messages_aggregate, members_aggregate, successes_aggregate

  async def _channel_worker(
      self,
      channel_queue: asyncio.Queue[typing.Tuple[str, str]],
      session: aiohttp.ClientSession,
      token_data: typing.Dict[str, typing.Any],
      auth: TokenManager,
      scanner: MigrationScanner,
      stats: typing.Dict[str, int],
      total_len: int,
  ) -> None:
    """Async task iterating through the channel queue."""
    while not self.stop_event.is_set():
      try:
        team_id, channel_id = channel_queue.get_nowait()
      except asyncio.QueueEmpty:
        break
      try:
        res = await scanner.count_channel_messages_async(
            auth, session, team_id, channel_id, token_data
        )
        if res is not None:
          _, _, message_count = res
          stats["messages"] += message_count
          stats["success_count"] += 1
      except Exception as e:
        self.log_func(
            f"Worker exception on team {team_id}, channel {channel_id}: {e}"
        )
        self.log_func(traceback.format_exc())
      finally:
        stats["processed"] += 1
        current_progress = stats["processed"]
        self.ui_callback(
            "scan_progress",
            source="channels",
            entity_type="Channels",
            progress=0.4 + 0.6 * (current_progress / max(1, total_len)),
            extra_text=f"Scanning Channels ({current_progress}/{total_len})",
            processed=current_progress,
            failed=current_progress - stats["success_count"],
            cumulative=stats["messages"],
        )
        channel_queue.task_done()

  async def _scan_sampled_channels_async(
      self,
      auth: TokenManager,
      scanner: MigrationScanner,
      sampled_channels: typing.List[typing.Tuple[str, str]],
      concurrency_val: int,
  ) -> typing.Tuple[int, int]:
    """Runs concurrent async event loop execution for channel message estimation."""
    channel_queue = asyncio.Queue()
    for team_channel_pair in sampled_channels:
      channel_queue.put_nowait(team_channel_pair)

    stats = {"messages": 0, "success_count": 0, "processed": 0}
    total_len = len(sampled_channels)

    session_token_data = auth.get_valid_token_slot()
    try:
      ssl_context = ssl.create_default_context(cafile=certifi.where())
      connector = aiohttp.TCPConnector(
          limit=100, keepalive_timeout=30, ssl=ssl_context
      )
      async with aiohttp.ClientSession(connector=connector) as async_session:
        tasks = [
            asyncio.create_task(
                self._channel_worker(
                    channel_queue,
                    async_session,
                    session_token_data,
                    auth,
                    scanner,
                    stats,
                    total_len,
                )
            )
            for _ in range(min(concurrency_val, len(sampled_channels) + 1))
        ]
        done_waiter = asyncio.create_task(channel_queue.join())
        while not self.stop_event.is_set() and not done_waiter.done():
          await asyncio.sleep(0.2)

        if not done_waiter.done():
          done_waiter.cancel()
        for task in tasks:
          task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
      auth.return_token_slot(session_token_data)
    return stats["messages"], stats["success_count"]

  def execute_scan(
      self, config: typing.Any, auth: TokenManager, concurrency_val: int
  ) -> typing.Dict[str, typing.Any] | None:
    """Master orchestrator logic ported from legacy monolithic wrappers."""
    self.log_func("--- Orchestrating Unified Parallel Scan (Decoupled) ---")

    # Unified fallback mapping across CLI/GUI config variations
    sample_percentage = float(
        getattr(config, "percent", getattr(config, "sample_percentage", 10.0))
    )
    csv_path = getattr(config, "csv", getattr(config, "csv_path", None))
    user_source = getattr(
        config, "user_source", "csv" if csv_path else "tenant"
    )

    if config.tenant_id:
      seed_val = int(
          hashlib.sha256(config.tenant_id.encode("utf-8")).hexdigest(), 16
      )
      random.seed(seed_val)

    scanner = MigrationScanner(
        db=self.db,
        client=self.client,
        metrics=self.metrics,
        audit_logger=self.audit_logger,
        default_limiter=self.default_limiter,
        chat_limiter=self.chat_limiter,
        chat_batch_limiter=self.chat_batch_limiter,
        channel_batch_limiter=self.channel_batch_limiter,
        stop_event=self.stop_event,
        log_func=self.log_func,
    )

    # Initialize variables that will be set differently in each mode
    private_channels = 0
    team_memberships = 0

    csv_users = []
    csv_teams = []

    if user_source == "csv":
      if not csv_path or not os.path.exists(csv_path):
        msg = "Target manifest CSV file not resolved."
        self.log_func(msg)
        self.ui_callback("error", message=msg)
        return None

      df = pd.read_csv(csv_path)
      df.columns = df.columns.str.strip()

      if "Email or Team ID" in df.columns and "Type" in df.columns:
        csv_users = [
            {
                "userPrincipalName": row["Email or Team ID"],
                "id": None,
            }
            for _, row in df.iterrows()
            if pd.notna(row["Email or Team ID"])
            and str(row.get("Type")).strip().lower() == "user"
        ]
        csv_teams = [
            {"id": row["Email or Team ID"]}
            for _, row in df.iterrows()
            if pd.notna(row["Email or Team ID"])
            and str(row.get("Type")).strip().lower() == "team"
        ]
      else:
        if "userId" in df.columns:
          csv_users = [
              {
                  "userPrincipalName": row["userId"],
                  "id": row.get("User ID"),
              }
              for _, row in df.iterrows()
              if pd.notna(row["userId"])
          ]

        if "teamId" in df.columns:
          csv_teams = [
              {"id": row["teamId"]}
              for _, row in df.iterrows()
              if pd.notna(row["teamId"])
          ]

    if config.mode == "heuristics":
      self.log_func("Applying automated heuristics estimation profiles.")

      user_activity = scanner.fetch_report_user_detail(auth)
      team_activity = scanner.fetch_report_team_activity(auth)

      if not user_activity or not team_activity:
        msg = "Aborted. Null report diagnostics encountered."
        self.log_func(msg)
        self.ui_callback("error", message=msg)
        return None

      total_users = (
          len(csv_users) if csv_users else user_activity["total_users"]
      )
      self.ui_callback("user_discovery", count=total_users, status="Done")
      private_chat_messages = user_activity["total_chats"]

      teams = len(csv_teams) if csv_teams else team_activity["teams"]

      private_chats = int(total_users * 150 / 3)
      private_chat_memberships = int(total_users * 150)
      channels = int(teams * 3.5)
      channel_messages = int(channels * 110)
      self.ui_callback("phase_status", source="chats", status="complete")
      self.ui_callback("phase_status", source="channels", status="complete")
      team_details = {}

    else:
      self.log_func(f"Engaging sampled framework ({sample_percentage}%)...")
      if csv_users:
        all_users = csv_users
      elif user_source == "csv":
        all_users = []
      else:
        all_users = scanner.fetch_all_users_graph(auth)

      if not all_users and not csv_teams:
        msg = (
            "Roster enumeration yielded zero entities (no users and no teams)."
            " Halting."
        )
        self.log_func(msg)
        self.ui_callback("error", message=msg)
        return None

      total_users = len(all_users)
      self.ui_callback("user_discovery", count=total_users, status="Done")

      chat_id_pool = []
      private_chats = 0

      if all_users:
        sample_size = max(1, int(len(all_users) * (sample_percentage / 100.0)))
        all_users.sort(key=lambda x: json.dumps(x, sort_keys=True))
        sampled_users = random.sample(all_users, sample_size)

        self.ui_callback("phase_status", source="chats", status="running")
        chat_counts = scanner.fetch_user_chat_counts_batch(auth, sampled_users, self.ui_callback)

        for chats in chat_counts.values():
          chat_id_pool.extend(chats[:50])

        total_chats_est = sum(len(chats) for chats in chat_counts.values())
        avg_chats = total_chats_est / len(chat_counts) if chat_counts else 0
        private_chats = int(avg_chats * len(all_users))
      else:
        self.log_func("No users to sample, skipping private chat scanning.")

      self.ui_callback("phase_status", source="channels", status="running")
      if csv_teams:
        self.log_func(f"Using {len(csv_teams)} Team IDs from CSV.")
        all_teams = csv_teams
        team_ids = [t["id"] for t in all_teams if t.get("id")]
        teams = len(all_teams)
      else:
        all_teams = scanner.fetch_all_teams_graph(auth)
        team_ids = [t["id"] for t in all_teams if t.get("id")]
        teams = len(all_teams)
      team_details = scanner.fetch_all_channels_for_teams_batch(auth, team_ids, self.ui_callback)

      channels = sum(details["channels"] for details in team_details.values())
      private_channels = sum(
          details.get("private_channels", 0)
          for details in team_details.values()
      )

      all_ch_tuples = []
      for team_id, details in team_details.items():
        for channel_id in details.get("all_channel_ids", []):
          all_ch_tuples.append((team_id, channel_id))

      if all_ch_tuples:
        channel_sample_size = max(
            1, int(len(all_ch_tuples) * (sample_percentage / 100.0))
        )
        channel_sample_size = min(len(all_ch_tuples), channel_sample_size)
        sampled_channels = random.sample(all_ch_tuples, channel_sample_size)
      else:
        sampled_channels = []

      self.log_func(
          f"Firing Parallel Gate: {len(chat_id_pool)} chats &"
          f" {len(sampled_channels)} channels queued."
      )
      with concurrent.futures.ThreadPoolExecutor(max_workers=2) as gate:
        chat_future = gate.submit(
            self._scan_sampled_chats,
            auth,
            scanner,
            chat_id_pool,
            concurrency_val,
        )
        channel_future = gate.submit(
            lambda: asyncio.run(
                self._scan_sampled_channels_async(
                    auth, scanner, sampled_channels, concurrency_val
                )
            )
        )

        chat_messages, chat_members, chat_successes = chat_future.result()
        channel_messages_from_sample, channel_successes = (
            channel_future.result()
        )

        self.ui_callback("phase_status", source="chats", status="complete")
        self.ui_callback("phase_status", source="channels", status="complete")

        if self.stop_event.is_set():
          self.ui_callback("error", message="Scan Terminated by User.")
          return None

      avg_messages_per_chat = (
          chat_messages / chat_successes if chat_successes else 0
      )
      avg_members_per_chat = (
          chat_members / chat_successes if chat_successes else 2
      )
      private_chat_messages = int(avg_messages_per_chat * private_chats)
      private_chat_memberships = int(avg_members_per_chat * private_chats)

      avg_messages_per_channel = (
          channel_messages_from_sample / channel_successes
          if channel_successes
          else 0
      )
      channel_messages = int(avg_messages_per_channel * channels)

      if team_ids:
        member_sample_size = max(
            1, int(len(team_ids) * (sample_percentage / 100.0))
        )
        member_sample_size = min(len(team_ids), member_sample_size)
        sampled_member_teams = random.sample(team_ids, member_sample_size)
      else:
        sampled_member_teams = []
      team_member_details = scanner.fetch_team_details_batch(
          auth, sampled_member_teams
      )
      team_member_estimate = sum(
          details.get("members", 0) for details in team_member_details.values()
      )
      average_members = (
          team_member_estimate / len(team_member_details)
          if team_member_details
          else 0
      )
      team_memberships = int(average_members * len(team_ids))

    # Prepare output mapping for ETA engine
    team_map = {}
    if config.mode == "sampling" and team_details:
      for team_id, details in team_details.items():
        channel_count = details.get("channels", 0)
        effective_messages = (
            int(channel_messages * (channel_count / max(1, channels)))
            if channels > 0
            else 0
        )
        team_map[team_id] = {
            "messages": effective_messages,
            "channels": channel_count,
            "memberships": team_memberships // max(1, len(team_ids) or 1),
        }
    else:
      messages_per_team = channel_messages // max(1, teams)
      channels_per_team = channels // max(1, teams)
      for team_index in range(teams):
        team_map[f"team_{team_index}"] = {
            "messages": messages_per_team,
            "channels": channels_per_team,
            "memberships": team_memberships // max(1, teams),
        }

    return {
        "total_users": total_users,
        "total_teams": teams,
        "channels": channels,
        "private_channels": private_channels,
        "team_memberships": team_memberships,
        "channel_messages": channel_messages,
        "private_chat_messages": private_chat_messages,
        "private_chats": private_chats,
        "private_chat_memberships": private_chat_memberships,
        "t_map": team_map,
    }
