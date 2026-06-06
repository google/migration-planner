import threading
from typing import Any, Callable, Dict, List, Optional

from chat.chat_service import ChatScannerService
from estimators.estimator import Estimator
from util.batch_client import GraphBatchClient
from util.connectors import UrlInvoker
from util.constants import BLENDED_MSG_COST_SEC
from util.constants import CHANNEL_COST_SEC
from util.constants import MAX_TEAMS_PER_BATCH
from util.constants import MEMBERSHIP_COST_SEC
from util.constants import CHANNEL_QPS
from util.constants import MESSAGE_QPS
from util.constants import MEMBERSHIP_QPS
from util.db_manager import DatabaseManager
from util.state_registry import AuditLogger
from util.state_registry import MetricsRegistry
from util.utils import ScanConfig


class ChatEstimator(Estimator):

  def __init__(
      self,
      config: ScanConfig,
      url_invoker: UrlInvoker,
      logger: Optional[Callable[[str], None]] = None,
      stop_event: Optional[threading.Event] = None,
  ):
    self.config = config
    self.url_invoker = url_invoker
    self.logger = logger
    self.stop_event = (
        stop_event if stop_event is not None else threading.Event()
    )

  def calculate_resource_count(
      self, data: Dict[str, Any], failures: List[Dict[str, str]]
  ) -> Dict[str, Any]:
    """Leverages the unified ChatScannerService logic."""

    def _ui_proxy(event_type, **kwargs):
      if event_type == "scan_progress" and self.logger:
        extra = kwargs.get("extra_text")
        if extra:
          self.logger(f"[CHAT] {extra}")
      elif event_type == "error" and self.logger:
        self.logger(f"[CHAT ERROR] {kwargs.get('message')}")

      # Bridge telemetry back to external GUI visual handlers
      ext_cb = data.get("ui_callback")
      if ext_cb:
        ext_cb(event_type, **kwargs)

    try:
      # Establish persistent data store anchor for isolated metrics
      with DatabaseManager("data/chat_migration_v2.db") as db:
        with AuditLogger("outputs") as auditor:
          metrics = MetricsRegistry()
          client = GraphBatchClient(log_func=self.logger)

          service = ChatScannerService(
              db=db,
              client=client,
              metrics=metrics,
              audit_logger=auditor,
              stop_event=self.stop_event,
              log_func=self.logger,
              ui_callback=_ui_proxy,
          )

          token_manager = self.url_invoker.token_manager
          concurrency = getattr(self.config, "concurrency", 10)

          # Execute consolidated parallel orchestration
          result = service.execute_scan(self.config, token_manager, concurrency)

          if result is None:
            return {}

          return result

    except Exception as e:
      if self.logger:
        self.logger(f"Fatal Chat integration exception: {e}")
      failures.append({"type": "CHAT_FAIL", "message": str(e)})
      return {}

  def calculate_migration_eta(self, data: Dict[str, Any]) -> float:
    """Calculates chat-centric migration ETA using robust team batch chunking logic."""
    private_channels = data.get("private_channels", 0)

    private_channel_elapsed_sec = private_channels * CHANNEL_COST_SEC
    private_eta_hours = private_channel_elapsed_sec / 3600.0

    team_metrics_map = data.get("t_map", {})
    if not team_metrics_map:
      total_channel_messages = data.get("channel_messages", 0)
      total_channels = data.get("channels", 0)
      total_team_memberships = data.get("team_memberships", 0)

      total_measured_sec = (
          (total_channel_messages * BLENDED_MSG_COST_SEC)
          + (total_channels * CHANNEL_COST_SEC)
          + (total_team_memberships * MEMBERSHIP_COST_SEC)
      )
      return (total_measured_sec / 3600.0) + private_eta_hours

    mode = getattr(self.config, "mode", "sampling")
    sample_percentage = getattr(
        self.config, "percent", getattr(self.config, "sample_percentage", 10)
    )
    total_teams = data.get("total_teams", 0)

    items_list = [
        {
            "id": team_id,
            "messages": team_metrics.get("messages", 0),
            "channels": team_metrics.get("channels", 0),
            "memberships": team_metrics.get("memberships", 0),
        }
        for team_id, team_metrics in team_metrics_map.items()
    ]

    if mode == "sampling":
      avg_msg = 0
      avg_channels = 0
      avg_mem = 0
      if items_list:
        avg_msg = sum(x["messages"] for x in items_list) / len(items_list)
        avg_channels = sum(x["channels"] for x in items_list) / len(items_list)
        avg_mem = sum(x["memberships"] for x in items_list) / len(items_list)

      effective_sample = sample_percentage if sample_percentage > 0 else 1.0
      total_est = (
          max(len(items_list), total_teams)
          if total_teams > 0
          else int(len(items_list) * (100.0 / effective_sample))
      )
      missing_count = total_est - len(items_list)
      for i in range(missing_count):
        items_list.append({
            "id": f"extrapolated_team_{i}",
            "messages": int(avg_msg),
            "channels": int(avg_channels),
            "memberships": int(avg_mem),
        })

    for x in items_list:
      x["weight"] = (
          (x["messages"] * BLENDED_MSG_COST_SEC)
          + (x["channels"] * CHANNEL_COST_SEC)
          + (x["memberships"] * MEMBERSHIP_COST_SEC)
      )
    items_list.sort(key=lambda x: x["weight"], reverse=True)

    msg_prefix = [0]
    chan_prefix = [0]
    mem_prefix = [0]
    running_msg = 0
    running_chan = 0
    running_mem = 0

    for x in items_list:
      running_msg += x["messages"]
      running_chan += x["channels"]
      running_mem += x["memberships"]
      msg_prefix.append(running_msg)
      chan_prefix.append(running_chan)
      mem_prefix.append(running_mem)

    num_parallel = max(
        1,
        getattr(
            self.config,
            "parallel_batches",
            getattr(self.config, "concurrency", 10),
        ),
    )
    candidate_hours = [3, 6, 12, 24, 48, 72, 120, 168, 240, 360, 480, 720]

    MAX_ALLOWED_BATCHES = 50
    best_total_eta = float("inf")

    def calculate_batch_eta_fast(s_idx, e_idx):
      total_messages = msg_prefix[e_idx] - msg_prefix[s_idx]
      total_channels = chan_prefix[e_idx] - chan_prefix[s_idx]
      total_memberships = mem_prefix[e_idx] - mem_prefix[s_idx]

      effective_channel_qps = CHANNEL_QPS / num_parallel
      effective_message_qps = MESSAGE_QPS / num_parallel
      effective_membership_qps = MEMBERSHIP_QPS / num_parallel

      channel_time = total_channels / effective_channel_qps
      message_time = total_messages / effective_message_qps
      membership_time = total_memberships / effective_membership_qps

      batch_eta_seconds = max(channel_time, message_time) + membership_time
      return batch_eta_seconds / 3600.0

    total_count = len(items_list)
    if total_count == 0:
      return private_eta_hours

    batches = []
    best_batches = []
    for target_hours in candidate_hours:
      start_idx = 0
      batches = []

      while start_idx < total_count:
        remaining = total_count - start_idx
        current_max = min(MAX_TEAMS_PER_BATCH, remaining)
        current_min = min(1, current_max)

        chosen_size = current_min
        low = current_min
        high = current_max

        while low <= high:
          mid = (low + high) // 2
          eta = calculate_batch_eta_fast(start_idx, start_idx + mid)

          if eta <= target_hours:
            chosen_size = mid
            low = mid + 1
          else:
            high = mid - 1

        chosen_size = max(current_min, chosen_size)
        end_idx = start_idx + chosen_size
        b_eta = calculate_batch_eta_fast(start_idx, end_idx)

        batches.append({
            "name": f"Batch {len(batches) + 1}",
            "eta": b_eta,
            "users": 0,
            "total_teams": chosen_size,
            "total_channels": chan_prefix[end_idx] - chan_prefix[start_idx],
            "total_channel_messages": (
                msg_prefix[end_idx] - msg_prefix[start_idx]
            ),
            "total_emails": 0,
            "total_events": 0,
            "total_contacts": 0,
            "total_in_place_archives": 0,
            "total_shared_mails": 0,
            "total_group_mails": 0,
            "team_ids": [x["id"] for x in items_list[start_idx:end_idx]],
        })
        start_idx = end_idx

      if len(batches) <= MAX_ALLOWED_BATCHES:
        buckets = [0.0] * min(num_parallel, max(1, len(batches)))
        for b in batches:
          idx = buckets.index(min(buckets))
          buckets[idx] += b["eta"]

        total_project_eta = max(buckets) if buckets else 0.0
        if total_project_eta < best_total_eta:
          best_total_eta = total_project_eta
          best_batches = list(batches)

    if best_total_eta == float("inf"):
      buckets = [0.0] * min(num_parallel, max(1, len(batches)))
      for b in batches:
        idx = buckets.index(min(buckets))
        buckets[idx] += b["eta"]
      best_total_eta = max(buckets) if buckets else 0.0
      best_batches = list(batches)

    # Persist simulated batches array for the frontend rendering layer
    self.last_batches = best_batches
    return private_eta_hours + best_total_eta

  def get_resource_type(self) -> str:
    return "CHAT"

  def get_migration_type(self) -> str:
    return "TEAMS"
