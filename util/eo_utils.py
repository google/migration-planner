from typing import List, Dict, Any, Callable, Optional
import math
from datetime import datetime, timezone, timedelta
from queue import Queue
import threading
import urllib.parse
from urllib.parse import quote
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from urllib.parse import urljoin
import urllib.request, urllib.error, urllib.parse
import requests
from util.connectors import UrlInvoker
from util.connectors import TokenManager
from util.constants import *

def fetch_user_batch_data(
    user_chunk: List[Dict[str, Any]],
    resource_type: str,
    token_manager: TokenManager,
    logger: Callable[[str], None],
    stop_event: Optional[threading.Event] = None,
) -> Dict[str, int]:
  """Fetches data for a batch of users for a specific resource type."""
  if stop_event and stop_event.is_set():
    return {}
  token_data = token_manager.get_valid_token_slot(logger)

  session = token_manager.get_session()
  batch_url = f"{GRAPH_BASE_URL}/$batch"
  batch_requests = []

  batch_emails_count = 0
  batch_contacts_count = 0
  batch_cals_count = 0
  batch_events_count = 0

  b_failed = 0
  failed_details = []

  for i, user in enumerate(user_chunk):
    user_id = user["User ID / Group ID"]
    req_id = str(i)
    if resource_type == "calendars":
      url = f"/users/{user_id}/calendars?$select=id,name&$top=100"
    else:
      url = f"/users/{user_id}/{resource_type}?$count=true&$top=1&$select=id"
    batch_requests.append({
        "id": req_id,
        "method": "GET",
        "url": url,
        "headers": {"ConsistencyLevel": "eventual"},
    })

  try:
    url_invoker = UrlInvoker(
        token_manager,
        None, None, None, None
    )
    responses = url_invoker.execute_batch_request(
        session,
        batch_url,
        token_manager,
        token_data,
        batch_requests,
        logger,
        stop_event=stop_event,
        context=resource_type,
    )

    b_failed = 0

    for i, user in enumerate(user_chunk):
      if stop_event and stop_event.is_set():
        break
      req_id = str(i)
      r_data = responses.get(req_id)

      if not r_data:
        b_failed += 1
        failed_details.append({
            "user": user["User Principal Name / Group Mail"],
            "cause": "User dropped after max retries.",
        })
        continue
      status = r_data.get("status", 0)
      if status == 200:
        if resource_type == "calendars":
          calendars_list = r_data.get("body", {}).get("value", [])
          c_count = len(calendars_list)
          e_count = 0
          if calendars_list:
            e_count = fetch_calendar_events(
                user["User ID / Group ID"],
                calendars_list,
                session,
                token_manager,
                token_data,
                logger,
                stop_event,
            )
          user["Calendar Count"] = c_count
          user["Event Count"] = e_count
          batch_cals_count += c_count
          batch_events_count += e_count
        else:
          try:
            body = r_data.get("body", {})
            count_val = body.get("@odata.count", 0)
          except:
            count_val = 0
          if resource_type == "messages":
            user["Email Count"] = count_val
            batch_emails_count += count_val
          else:
            user["Contact Count"] = count_val
            batch_contacts_count += count_val
      elif status == 404:
        b_failed += 1
        cause = f"[{status}] Mailbox not found."
        failed_details.append(
            {"user": user["User Principal Name / Group Mail"], "cause": cause}
        )
      else:
        err_msg = (
            r_data.get("body", {}).get("error", {}).get("message", "Unknown")
        )
        logger(
            f"Batch Item Error [{status}] for {user['User Principal Name / Group Mail']}:"
            f" {err_msg}"
        )
        b_failed += 1
        cause = f"[{status}] {err_msg}"
        failed_details.append(
            {"user": user["User Principal Name / Group Mail"], "cause": cause}
        )
  except Exception as e:
    logger(f"Worker Exception: {e}")
    b_failed += len(user_chunk)
    for u in user_chunk:
      failed_details.append({"user": u["User Principal Name / Group Mail"], "cause": str(e)})
  finally:
    token_manager.return_token_slot(token_data)

  return {
      "emails": batch_emails_count,
      "contacts": batch_contacts_count,
      "calendars": batch_cals_count,
      "events": batch_events_count,
      "failed": b_failed,
      "failed_details": failed_details,
  }


def fetch_calendar_events(
    user_id: str,
    calendars: List[Dict[str, Any]],
    session: requests.Session,
    token_manager: TokenManager,
    token_data: Dict[str, Any],
    logger: Callable[[str], None],
    stop_event: Optional[threading.Event] = None,
) -> int:
  """Fetches event counts for a list of calendars."""
  total_events = 0
  batch_url = f"{GRAPH_BASE_URL}/$batch"
  batch_size = 4  # Since these are effectively concurrent calls within a mailbox, maintain at < 4.
  for i in range(0, len(calendars), batch_size):
    if stop_event and stop_event.is_set():
      break
    chunk = calendars[i : i + batch_size]
    sub_requests = []
    for j, cal in enumerate(chunk):
      cal_id_encoded = urllib.parse.quote(cal["id"], safe="")
      sub_requests.append({
          "id": str(j),
          "method": "GET",
          "url": (
              f"/users/{user_id}/calendars/{cal_id_encoded}/events?"
              "$count=true&$top=1&$select=id,organizer"
          ),
          "headers": {"ConsistencyLevel": "eventual"},
      })
    
    url_invoker = UrlInvoker(
        token_manager,
        None, None, None, None
    )
    responses = url_invoker.execute_batch_request(
        session,
        batch_url,
        token_manager,
        token_data,
        sub_requests,
        logger,
        stop_event=stop_event,
        context=f"User {user_id} Events",
    )
    for r_id, r_data in responses.items():
      if r_data.get("status") == 200:
        total_events += r_data.get("body", {}).get("@odata.count", 0)
  return total_events


def calculate_batch_duration(
    item_counts: List[int],
    global_limit: int,
    user_limit: int,
    batch_size: int,
    batch_time: int,
) -> float:
  """Calculates duration in HOURS based on batching throughput constraints."""
  active_counts = [c for c in item_counts if c > 0]
  if not active_counts:
    return 0.0

  batch_counts = [math.ceil(c / batch_size) for c in active_counts]
  batch_counts.sort()

  total_seconds = 0.0
  previous_level = 0
  n = len(batch_counts)

  for i, current_level in enumerate(batch_counts):
    delta = current_level - previous_level
    if delta > 0:
      active_users = n - i
      max_user_capacity = active_users * user_limit
      effective_concurrency = min(global_limit, max_user_capacity)
      current_throughput = effective_concurrency / batch_time
      total_layer_batches = delta * active_users
      seconds_for_layer = total_layer_batches / current_throughput
      total_seconds += seconds_for_layer
    previous_level = current_level

  return total_seconds / 3600.0

