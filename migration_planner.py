# Copyright 2026 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Migration Estimator Tool for Microsoft Graph data estimation.

This application allows users to estimate the time and resources required to
migrate data from Microsoft 365 to Google Workspace. It connects to the
Microsoft Graph API to fetch user, email, contact, and calendar data counts.
"""

import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
import math
import os
import queue
import random
import threading
import time
from tkinter import filedialog, messagebox
from typing import Any, Callable, Dict, List, Optional, Tuple
import urllib.parse
import webbrowser

import customtkinter as ctk
import pandas as pd
import psutil
import requests
from requests.adapters import HTTPAdapter
import urllib3
from urllib3.util.retry import Retry

# =================================================================================
# CONSTANTS
# =================================================================================

# --- API Configuration ---
GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
TOKEN_URL_TEMPLATE = "https://login.microsoftonline.com/{0}/oauth2/v2.0/token"
MAX_RETRIES = 30
BACKOFF = 2
SHOW_LOAD_MULTIPLIER = False

# --- UI Colors (Google Material 3) ---
COLOR_PRIMARY = "#0B57D0"  # Google Blue
COLOR_ON_PRIMARY = "#FFFFFF"  # White text on blue
COLOR_SURFACE = "#FFFFFF"  # Card background
COLOR_BACKGROUND = "#F0F2F5"  # App background (Light Gray/Blue tint)
COLOR_TEXT_MAIN = "#1F1F1F"  # High emphasis text
COLOR_TEXT_SUB = "#444746"  # Medium emphasis text
COLOR_OUTLINE = "#747775"  # Input borders
COLOR_OUTLINE_LIGHT = "#E0E2E0"  # Card borders
COLOR_TONAL_BG = "#D3E3FD"  # Light Blue (Secondary Container)
COLOR_TONAL_TEXT = "#041E49"  # Dark Blue (On Secondary Container)
COLOR_TONAL_HOVER = "#C2D0EA"  # Slightly darker for hover state
COLOR_SUCCESS = "#188038"  # Google Green
COLOR_ERROR = "#B3261E"  # GM3 Error Red
COLOR_ERROR_HOVER = "#8C1D18"  # Darker Red for hover
COLOR_PRIMARY_HOVER = "#0842a0"  # Darker Blue for hover
COLOR_SECONDARY_HOVER = "#F1F3F4"  # Light Gray for hover
COLOR_SURFACE_HOVER = "#EFF6FF"  # Light Blue for surface hover
COLOR_SURFACE_VARIANT = "#F8F9FA"  # Light Gray for advanced settings
COLOR_WAVE_BAR = "#8AB4F8"  # Light Blue for wave bars

# --- Fonts ---
FONT_HEADER_LARGE = ("Roboto", 32, "bold")
FONT_HEADER_MEDIUM = ("Roboto", 24, "bold")
FONT_HEADER_SMALL = ("Roboto", 18, "bold")
FONT_BODY_LARGE = ("Roboto", 14)
FONT_BODY_BOLD = ("Roboto", 14, "bold")
FONT_BODY_MEDIUM = ("Roboto", 12)
FONT_BODY_SMALL = ("Roboto", 11)
FONT_ICON_LARGE = ("Arial", 26)
FONT_ICON_MEDIUM = ("Arial", 24)

# --- ETA Calculation Parameters (Test/Alpha)---
ENABLE_EMAIL_ETA = True
ETA_EMAIL_GLOBAL_LIMIT = 1200
ETA_EMAIL_USER_LIMIT = 6
ETA_EMAIL_BATCH_SIZE = 1
ETA_EMAIL_BATCH_TIME = 6

ENABLE_CALENDAR_ETA = False
ETA_CALENDAR_GLOBAL_LIMIT = 50
ETA_CALENDAR_USER_LIMIT = 1
ETA_CALENDAR_BATCH_SIZE = 8
ETA_CALENDAR_BATCH_TIME = 25

ENABLE_CONTACT_ETA = False
ETA_CONTACT_GLOBAL_LIMIT = 30
ETA_CONTACT_USER_LIMIT = 1
ETA_CONTACT_BATCH_SIZE = 10
ETA_CONTACT_BATCH_TIME = 25

# =================================================================================
# CONFIGURATION
# =================================================================================

@dataclass
class ScanConfig:
  """Holds configuration for the current scan job."""

  tenant_id: str
  client_ids: List[str]
  client_secrets: List[str]
  user_source: str
  csv_path: str
  scan_email: bool
  scan_contact: bool
  scan_calendar: bool
  concurrency: int
  load_multiplier: int
  retries: int
  backoff: int
  eta_max_users: int
  parallel_waves: int


# =================================================================================
# BACKEND HELPERS
# =================================================================================


class ResourceMonitor(threading.Thread):
  """Monitors CPU and RAM usage in a separate thread.

  Attributes:
      interval: Time in seconds between measurements.
      stop_event: Event to signal the thread to stop.
      cpu_readings: List of CPU usage percentages.
      ram_readings: List of RAM usage percentages.
  """

  def __init__(self, interval: float = 1.0):
    super().__init__()
    self.interval = interval
    self.stop_event = threading.Event()
    self.cpu_readings: List[float] = []
    self.ram_readings: List[float] = []
    self.daemon = True

  def run(self) -> None:
    """Continuously records system metrics until stopped."""
    while not self.stop_event.is_set():
      self.cpu_readings.append(psutil.cpu_percent(interval=None))
      self.ram_readings.append(psutil.virtual_memory().percent)
      time.sleep(self.interval)

  def stop(self) -> None:
    """Signals the monitor to stop recording."""
    self.stop_event.set()

  def get_stats(self) -> Tuple[float, float, float, float]:
    """Calculates average and maximum CPU and RAM usage.

    Returns:
        A tuple containing (avg_cpu, max_cpu, avg_ram, max_ram).
    """
    if not self.cpu_readings:
      return 0.0, 0.0, 0.0, 0.0
    avg_cpu = sum(self.cpu_readings) / len(self.cpu_readings)
    max_cpu = max(self.cpu_readings)
    avg_ram = sum(self.ram_readings) / len(self.ram_readings)
    max_ram = max(self.ram_readings)
    return avg_cpu, max_cpu, avg_ram, max_ram


class TokenManager:
  """Manages Microsoft Graph API tokens with rotation and concurrency.

  Handles authentication for multiple client applications to distribute
  load and avoid rate limiting.
  """

  def __init__(
      self,
      tenant_id: str,
      client_ids: List[str],
      client_secrets: List[str],
      concurrency: int,
      retries: int,
      backoff: int,
  ):
    self.tenant_id = tenant_id
    self.apps = list(zip(client_ids, client_secrets))
    self.concurrency = concurrency
    self.retries = retries
    self.backoff = backoff
    self.token_queue: queue.Queue = queue.Queue()
    self.session = self._create_retry_session()
    self.tokens: List[Dict[str, Any]] = []

  def _create_retry_session(self) -> requests.Session:
    """Creates a requests session with retry logic."""
    session = requests.Session()
    session.verify = False
    retries = Retry(
        total=self.retries,
        backoff_factor=self.backoff,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    total_pool_size = len(self.apps) * self.concurrency * 2 + 100
    adapter = HTTPAdapter(
        max_retries=retries,
        pool_connections=total_pool_size,
        pool_maxsize=total_pool_size,
    )
    session.mount("https://", adapter)
    return session

  def authenticate_all(
      self,
      logger: Callable[[str], None],
      required_scopes: Optional[List[str]] = None,
  ) -> None:
    """Authenticates all configured applications and verifies permissions.

    Args:
        logger: Function to log messages.
        required_scopes: List of permission scopes required (e.g.,
          ['User.Read.All']).

    Raises:
        Exception: If authentication fails or required permissions are missing.
    """
    logger(f"Authenticating {len(self.apps)} apps...")
    url = TOKEN_URL_TEMPLATE.format(self.tenant_id)
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    for client_id, client_secret in self.apps:
      data = {
          "client_id": client_id,
          "scope": "https://graph.microsoft.com/.default",
          "client_secret": client_secret,
          "grant_type": "client_credentials",
      }
      try:
        resp = self.session.post(url, headers=headers, data=data)
        resp.raise_for_status()

        token_resp = resp.json()
        token = token_resp["access_token"]
        expires_in = token_resp.get("expires_in", 3599)

        token_data = {
            "token": token,
            "expires_at": time.time() + int(expires_in) - 900,
            "client_id": client_id,
            "client_secret": client_secret,
        }

        if required_scopes:
          try:
            # Decode JWT Payload (No signature verification needed for client-side check)
            payload_part = token.split(".")[1]
            payload_part += "=" * (-len(payload_part) % 4)
            decoded_bytes = base64.urlsafe_b64decode(payload_part)
            payload = json.loads(decoded_bytes)

            granted_roles = set(payload.get("roles", []))
            missing = [s for s in required_scopes if s not in granted_roles]
            if missing:
              raise Exception(
                  f"Missing Required Permissions for App {client_id[:5]}...: "
                  f"{', '.join(missing)}\n"
                  f"Current Assigned Roles: {', '.join(granted_roles)}\n"
                  "Please grant these Application permissions in Azure Portal."
              )
          except Exception as e:
            logger(f"Token Verification Failed: {e}")
            raise

        self.tokens.append(token_data)
        for _ in range(self.concurrency):
          self.token_queue.put(token_data)
        logger(f"App {client_id[:5]}... authenticated & verified.")

      except requests.exceptions.RequestException as e:
        error_text = ""
        if e.response is not None:
          error_text = f": {e.response.text}"
        logger(f"Auth Failed for app {client_id}: {e}{error_text}")
        raise Exception(
            "Authentication Failed. Check Client ID/Secret/Tenant. Details:"
            f" {error_text}"
        )
      except Exception as e:
        logger(f"Error for app {client_id}: {e}")
        raise

  def refresh_token_data(
      self, token_data: Dict[str, Any], logger: Callable[[str], None]
  ) -> bool:
    """Refreshes a specific token dictionary in-place."""
    url = TOKEN_URL_TEMPLATE.format(self.tenant_id)
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "client_id": token_data["client_id"],
        "scope": "https://graph.microsoft.com/.default",
        "client_secret": token_data["client_secret"],
        "grant_type": "client_credentials",
    }
    try:
      resp = self.session.post(url, headers=headers, data=data)
      resp.raise_for_status()
      token_resp = resp.json()

      token_data["token"] = token_resp["access_token"]
      expires_in = token_resp.get("expires_in", 3599)
      token_data["expires_at"] = time.time() + int(expires_in) - 900
      return True
    except Exception as e:
      logger(f"Failed to refresh token: {e}")
      return False

  def get_valid_token_slot(
      self, logger: Callable[[str], None]
  ) -> Dict[str, Any]:
    """Retrieves an available token, refreshing it if nearing expiration."""
    token_data = self.token_queue.get()

    if time.time() > token_data["expires_at"]:
      logger(
          f"Token expiring soon for App {token_data['client_id'][:5]}...,"
          " refreshing..."
      )
      if self.refresh_token_data(token_data, logger):
        logger(
            "Successfully refreshed token for App"
            f" {token_data['client_id'][:5]}..."
        )
    return token_data

  def return_token_slot(self, token_data: Dict[str, Any]) -> None:
    """Returns a token data object to the queue after use."""
    self.token_queue.put(token_data)

  def get_session(self) -> requests.Session:
    """Returns the shared requests session."""
    return self.session


def execute_batch_request(
    session: requests.Session,
    batch_url: str,
    token_manager: TokenManager,
    token_data: Dict[str, Any],
    requests_payload: List[Dict[str, Any]],
    logger: Callable[[str], None],
    stop_event: Optional[threading.Event] = None,
    context: str = "",
) -> Dict[str, Any]:
  """Executes a batch request against MS Graph with retry logic for throttling."""
  successful_responses = {}
  pending_requests = requests_payload
  max_retries = token_manager.retries
  current_try = 0

  while pending_requests and current_try < max_retries:
    if stop_event and stop_event.is_set():
      break
    current_try += 1

    payload = {"requests": pending_requests}

    # Build headers dynamically so they use the refreshed token if updated
    headers = {
        "Authorization": f"Bearer {token_data['token']}",
        "Content-Type": "application/json",
    }

    try:
      resp = session.post(batch_url, headers=headers, json=payload, timeout=180)
      if resp.status_code == 200:
        try:
          batch_responses = resp.json().get("responses", [])
        except ValueError:
          logger(f"Invalid JSON response in batch. Retrying...")
          time.sleep(2)
          continue

        current_batch_map = {r["id"]: r for r in pending_requests}
        next_retry_requests = []
        retry_after_delay = 0
        needs_refresh = False

        for response_item in batch_responses:
          req_id = response_item.get("id")
          status = response_item.get("status")
          if status == 429:
            headers_429 = response_item.get("headers", {})
            try:
              wait_sec = int(float(headers_429.get("Retry-After", 0)))
            except (ValueError, TypeError):
              wait_sec = 2
            retry_after_delay = max(retry_after_delay, wait_sec)
            if req_id in current_batch_map:
              next_retry_requests.append(current_batch_map[req_id])
          elif status == 401:
            # Handle inner 401 (just in case single items fail authorization)
            needs_refresh = True
            if req_id in current_batch_map:
              next_retry_requests.append(current_batch_map[req_id])
              retry_after_delay = max(retry_after_delay, 2)
          elif status in [500, 502, 503, 504]:
            if req_id in current_batch_map:
              next_retry_requests.append(current_batch_map[req_id])
              retry_after_delay = max(retry_after_delay, 2)
          else:
            successful_responses[req_id] = response_item

        if next_retry_requests:
          if stop_event and stop_event.is_set():
            break

          if needs_refresh:
            logger(
                f"Individual items returned 401 in {context}. Refreshing token"
                " inline..."
            )
            token_manager.refresh_token_data(token_data, logger)

          # If we have a specific Retry-After delay (retry_after_delay > 0), use it directly.
          # Otherwise, use exponential backoff.
          if retry_after_delay > 0:
            sleep_time = float(retry_after_delay)
            # Add jitter (0-1000ms)
            sleep_time += random.uniform(0, 1.0)
          else:
            sleep_time = BACKOFF ** (current_try - 1)

          # Increased cap from 30s to 300s to handle severe throttling without dropping data
          sleep_time = min(sleep_time, 300)

          logger(
              f"Batch partial failure: {len(next_retry_requests)} items failed "
              f"(429/401/5xx). Retrying in {sleep_time:.1f}s..."
          )
          if stop_event and stop_event.is_set():
            break

          time.sleep(sleep_time)
          pending_requests = next_retry_requests
        else:
          pending_requests = []
      elif resp.status_code == 429:
        if stop_event and stop_event.is_set():
          break
        try:
          raw_wait = int(float(resp.headers.get("Retry-After", 5)))
        except (ValueError, TypeError):
          # Default backoff if no header
          raw_wait = 5 * (current_try + 1)

        wait = min(float(raw_wait), 300.0)
        # Add jitter
        wait += random.uniform(0, 1.0)

        logger(
            f"Batch 429 Throttled. Waiting {wait:.1f}s (Requested:"
            f" {raw_wait}s)..."
        )
        time.sleep(wait)
        continue
      elif resp.status_code == 401:
        # Handle outer batch 401 (entire batch rejected due to token expiry)
        if stop_event and stop_event.is_set():
          break
        logger(
            f"Batch 401 Unauthorized in {context}. Token expired. Refreshing"
            " inline..."
        )
        if token_manager.refresh_token_data(token_data, logger):
          logger("Successfully refreshed token after 401.")
        else:
          logger("Failed to refresh token after 401. Will retry anyway.")

        # Decrement try counter so the expired token doesn't punish the retry limits
        current_try -= 1
        continue
      else:
        logger(f"Batch failed with {resp.status_code}: {resp.text[:100]}")
        break
    except Exception as e:
      logger(
          "Network Exception in batch (Attempt"
          f" {current_try}/{max_retries}): {e}"
      )
      if current_try < max_retries:
        if stop_event and stop_event.is_set():
          break
        time.sleep(min(2 * current_try, 30))
        continue
      else:
        logger(f"Max retries reached for batch in {context}. Data lost.")
        break

  if pending_requests:
    logger(
        f"WARNING: Max retries ({max_retries}) reached in {context}."
        f" {len(pending_requests)} items dropped permanently."
    )

  return successful_responses


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
    user_id = user["User ID"]
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
    responses = execute_batch_request(
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
            "user": user["User Principal Name"],
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
                user["User ID"],
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
            {"user": user["User Principal Name"], "cause": cause}
        )
      else:
        err_msg = (
            r_data.get("body", {}).get("error", {}).get("message", "Unknown")
        )
        logger(
            f"Batch Item Error [{status}] for {user['User Principal Name']}:"
            f" {err_msg}"
        )
        b_failed += 1
        cause = f"[{status}] {err_msg}"
        failed_details.append(
            {"user": user["User Principal Name"], "cause": cause}
        )
  except Exception as e:
    logger(f"Worker Exception: {e}")
    b_failed += len(user_chunk)
    for u in user_chunk:
      failed_details.append({"user": u["User Principal Name"], "cause": str(e)})
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
    responses = execute_batch_request(
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


def calculate_wave_duration(
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


# =================================================================================
# UI CLASS
# =================================================================================


class MigrationEstimatorTool(ctk.CTk):
  """Main Application Class for Migration Planner."""

  def __init__(self):
    super().__init__()
    # Style Configuration
    ctk.set_appearance_mode("Light")
    ctk.set_default_color_theme("blue")

    self.configure(fg_color=COLOR_BACKGROUND)
    self.title("Migration Planner")
    self.geometry("950x900")

    self.log_queue = queue.Queue()
    self.log_buffer = []
    self.log_lock = threading.Lock()
    self.stop_scan_event = threading.Event()

    self.spinners_active = {}
    self.spinner_chars = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    self.spinner_indices = {}

    self.setup_variables()
    self.create_widgets()
    self.after(100, self.process_log_queue)

  def setup_variables(self):
    """Initializes all Tkinter variables."""
    self.tenant_id = ctk.StringVar()
    self.client_ids = ctk.StringVar()
    self.client_secrets = ctk.StringVar()
    self.user_source = ctk.StringVar(value="tenant")
    self.user_csv_path = ctk.StringVar()
    self.scan_email = ctk.BooleanVar(value=True)
    self.scan_contact = ctk.BooleanVar(value=True)
    self.scan_calendar = ctk.BooleanVar(value=True)
    self.concurrency = ctk.IntVar(value=10)
    self.load_multiplier = ctk.IntVar(value=1)
    self.retries = ctk.IntVar(value=MAX_RETRIES)
    self.backoff = ctk.IntVar(value=BACKOFF)
    self.eta_min_users = ctk.IntVar(value=200)
    self.eta_max_users = ctk.IntVar(value=5000)
    self.eta_max_waves = ctk.IntVar(value=50)
    self.parallel_waves = ctk.IntVar(value=10)
    self.scan_result_csv_path = ctk.StringVar()

  def create_widgets(self):
    """Creates the main UI layout."""
    # --- HEADER ---
    self.header_frame = ctk.CTkFrame(self, fg_color="transparent")
    self.header_frame.pack(fill="x", padx=30, pady=(20, 10))

    # Title
    ctk.CTkLabel(
        self.header_frame,
        text="Migration Planner",
        font=FONT_HEADER_LARGE,
        text_color=COLOR_TEXT_MAIN,
    ).pack(anchor="center")

    ctk.CTkLabel(
        self.header_frame,
        text=(
            "Plan your migration with confidence. Your data remains local and"
            " secure."
        ),
        font=FONT_BODY_LARGE,
        text_color=COLOR_TEXT_SUB,
    ).pack(anchor="center", pady=(5, 0))

    # --- MAIN CARD ---
    self.main_card = ctk.CTkFrame(
        self,
        fg_color=COLOR_SURFACE,
        corner_radius=16,
        border_color=COLOR_OUTLINE_LIGHT,
        border_width=1,
    )
    self.main_card.pack(fill="both", expand=True, padx=30, pady=(10, 10))

    self.main_card.grid_rowconfigure(0, weight=1)
    self.main_card.grid_columnconfigure(0, weight=1)

    # --- VIEW CONTAINER ---
    self.view_container = ctk.CTkFrame(self.main_card, fg_color="transparent")
    self.view_container.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)

    # --- FOOTER ---
    self.footer = ctk.CTkFrame(
        self,
        fg_color=COLOR_SURFACE,
        height=80,
        corner_radius=16,
        border_color=COLOR_OUTLINE_LIGHT,
        border_width=1,
    )
    self.footer.pack(fill="x", padx=30, pady=(0, 30), side="bottom")

    # Primary Button
    self.btn_action_primary = ctk.CTkButton(
        self.footer,
        text="Primary",
        width=180,
        height=40,
        corner_radius=20,
        font=FONT_BODY_BOLD,
        fg_color=COLOR_PRIMARY,
        hover_color=COLOR_PRIMARY_HOVER,
    )

    # Secondary Button
    self.btn_action_secondary = ctk.CTkButton(
        self.footer,
        text="Secondary",
        width=140,
        height=40,
        corner_radius=20,
        font=FONT_BODY_BOLD,
        fg_color="transparent",
        border_width=1,
        border_color=COLOR_OUTLINE,
        text_color=COLOR_PRIMARY,
        hover_color=COLOR_SECONDARY_HOVER,
    )

    # Initialize Views
    self.build_config_view()
    self.build_progress_view()
    self.build_results_view()

    # Show Start Page
    self.show_config_view()

  # ==========================
  # VIEW: CONFIGURATION
  # ==========================
  def build_config_view(self):
    """Builds the Configuration View."""
    self.view_config = ctk.CTkFrame(self.view_container, fg_color="transparent")
    self.view_config.grid_columnconfigure(0, weight=1)
    self.view_config.grid_rowconfigure(2, weight=1)

    # Header
    header_container = ctk.CTkFrame(self.view_config, fg_color="transparent")
    header_container.grid(row=0, column=0, sticky="w", padx=25, pady=(20, 5))
    ctk.CTkLabel(
        header_container,
        text="How would you like to provide data?",
        font=FONT_HEADER_MEDIUM,
        text_color=COLOR_TEXT_MAIN,
    ).pack(anchor="w")

    # Status Line
    status_container = ctk.CTkFrame(self.view_config, fg_color="transparent")
    status_container.grid(row=1, column=0, sticky="w", padx=25, pady=(0, 20))
    ctk.CTkLabel(
        status_container,
        text="✔",
        text_color=COLOR_SUCCESS,
        font=FONT_BODY_MEDIUM,
    ).pack(side="left")
    ctk.CTkLabel(
        status_container,
        text=(
            " Data stays local. We never transmit credentials or data"
            " externally."
        ),
        font=FONT_BODY_MEDIUM,
        text_color=COLOR_TEXT_SUB,
    ).pack(side="left", padx=(5, 0))

    # Main Content
    self.scroll_connect = ctk.CTkScrollableFrame(
        self.view_config,
        fg_color="transparent",
        scrollbar_button_color="white",
        scrollbar_button_hover_color=COLOR_SECONDARY_HOVER,
    )
    self.scroll_connect.grid(row=2, column=0, sticky="nsew", padx=15)

    ctk.CTkLabel(
        self.scroll_connect,
        text="Connect your Microsoft Azure account to fetch the data.",
        font=("Roboto", 13),
        text_color=COLOR_TEXT_SUB,
    ).pack(anchor="w", pady=(0, 15))

    # Input Container
    inputs_frame = ctk.CTkFrame(
        self.scroll_connect,
        fg_color=COLOR_SURFACE,
        border_color=COLOR_OUTLINE_LIGHT,
        border_width=1,
        corner_radius=8,
    )
    inputs_frame.pack(fill="x", pady=5)

    inner_pad = ctk.CTkFrame(inputs_frame, fg_color="transparent")
    inner_pad.pack(fill="x", padx=15, pady=15)

    self.create_entry(inner_pad, "Tenant ID", self.tenant_id)
    self.create_entry(inner_pad, "Client ID", self.client_ids)
    self.create_entry(inner_pad, "Client Secret", self.client_secrets, show="*")

    ctk.CTkLabel(
        self.scroll_connect,
        text="Source:",
        font=FONT_BODY_BOLD,
        text_color=COLOR_TEXT_SUB,
    ).pack(anchor="w", pady=(15, 5))

    source_selection_frame = ctk.CTkFrame(
        self.scroll_connect, fg_color="transparent"
    )
    source_selection_frame.pack(fill="x", anchor="w")
    ctk.CTkRadioButton(
        source_selection_frame,
        text="Scan All Users",
        variable=self.user_source,
        value="tenant",
        border_color=COLOR_TEXT_SUB,
    ).pack(side="left", padx=20)
    ctk.CTkRadioButton(
        source_selection_frame,
        text="Upload CSV",
        variable=self.user_source,
        value="csv",
        border_color=COLOR_TEXT_SUB,
    ).pack(side="left")
    ctk.CTkButton(
        source_selection_frame,
        text="Browse",
        command=self.browse_user_csv,
        width=80,
        fg_color="transparent",
        hover_color=COLOR_SURFACE_HOVER,
        border_width=1,
        text_color=COLOR_PRIMARY,
        corner_radius=16,
    ).pack(side="left", padx=10)
    ctk.CTkLabel(
        source_selection_frame,
        textvariable=self.user_csv_path,
        text_color=COLOR_TEXT_SUB,
    ).pack(side="left")

    # Advanced Settings
    self.adv_frame = ctk.CTkFrame(
        self.scroll_connect, fg_color=COLOR_SURFACE_VARIANT, corner_radius=12
    )
    self.adv_visible = False
    self.btn_adv = ctk.CTkButton(
        self.scroll_connect,
        text="Show Advanced Settings ▼",
        command=self.toggle_adv,
        fg_color="transparent",
        text_color=COLOR_PRIMARY,
        hover=False,
        anchor="w",
    )
    self.btn_adv.pack(anchor="w", pady=(15, 5))

    # Advanced Content
    ctk.CTkLabel(
        self.adv_frame,
        text="Scan Settings",
        font=FONT_BODY_BOLD,
        text_color=COLOR_TEXT_MAIN,
    ).pack(anchor="w", padx=15, pady=(10, 15))
    scan_options_frame = ctk.CTkFrame(self.adv_frame, fg_color="transparent")
    scan_options_frame.pack(fill="x", padx=15)
    ctk.CTkCheckBox(
        scan_options_frame,
        text="Contacts",
        variable=self.scan_contact,
        corner_radius=4,
        fg_color=COLOR_PRIMARY,
        border_color=COLOR_TEXT_SUB,
    ).pack(side="left", padx=10)
    ctk.CTkCheckBox(
        scan_options_frame,
        text="Calendars",
        variable=self.scan_calendar,
        corner_radius=4,
        fg_color=COLOR_PRIMARY,
        border_color=COLOR_TEXT_SUB,
    ).pack(side="left", padx=10)
    ctk.CTkCheckBox(
        scan_options_frame,
        text="Emails",
        variable=self.scan_email,
        corner_radius=4,
        fg_color=COLOR_PRIMARY,
        border_color=COLOR_TEXT_SUB,
        state="disabled",
    ).pack(side="left", padx=10)
    ctk.CTkLabel(
        self.adv_frame,
        text="* Only root contacts are scanned.",
        font=FONT_BODY_SMALL,
        text_color=COLOR_TEXT_SUB,
    ).pack(anchor="w", padx=25, pady=(2, 5))

    concurrency_frame = ctk.CTkFrame(self.adv_frame, fg_color="transparent")
    concurrency_frame.pack(fill="x", padx=15)

    ctk.CTkLabel(
        concurrency_frame, text="Concurrency:", text_color=COLOR_TEXT_SUB
    ).grid(row=0, column=0, sticky="w", padx=5, pady=5)
    slider = ctk.CTkSlider(
        concurrency_frame,
        from_=10,
        to=100,
        number_of_steps=9,
        variable=self.concurrency,
    )
    slider.grid(row=0, column=1, sticky="ew", padx=5, pady=5)
    ctk.CTkLabel(
        concurrency_frame,
        textvariable=self.concurrency,
        text_color=COLOR_TEXT_MAIN,
        width=30,
    ).grid(row=0, column=2, sticky="w", padx=5)
    ctk.CTkLabel(
        self.adv_frame,
        text=(
            "* Reduce concurrency if your CPU is slowing down or you see"
            " throttling errors in your logs."
        ),
        font=FONT_BODY_SMALL,
        text_color=COLOR_TEXT_SUB,
    ).pack(anchor="w", padx=25, pady=(2, 5))

    if SHOW_LOAD_MULTIPLIER:
      self.create_grid_entry(
          concurrency_frame, 1, 0, "Load Multiplier:", self.load_multiplier
      )

    ctk.CTkLabel(
        self.adv_frame,
        text="Migration Plan Options",
        font=FONT_BODY_BOLD,
        text_color=COLOR_TEXT_MAIN,
    ).pack(anchor="w", padx=15, pady=(15, 5))

    eta_settings_frame = ctk.CTkFrame(self.adv_frame, fg_color="transparent")
    eta_settings_frame.pack(fill="x", padx=15)

    ctk.CTkLabel(
        eta_settings_frame, text="Max Waves:", text_color=COLOR_TEXT_SUB
    ).grid(row=1, column=3, sticky="w", padx=5, pady=5)
    slider_max_waves = ctk.CTkSlider(
        eta_settings_frame,
        from_=10,
        to=100,
        number_of_steps=18,
        variable=self.eta_max_waves,
    )
    slider_max_waves.grid(row=1, column=4, sticky="ew", padx=5, pady=5)
    ctk.CTkLabel(
        eta_settings_frame,
        textvariable=self.eta_max_waves,
        text_color=COLOR_TEXT_MAIN,
        width=40,
    ).grid(row=1, column=5, sticky="w", padx=5)

    ctk.CTkLabel(
        self.adv_frame,
        text=(
            "* The migration plan will try to keep the total number of waves"
            " below this number."
        ),
        font=FONT_BODY_SMALL,
        text_color=COLOR_TEXT_SUB,
    ).pack(anchor="w", padx=25, pady=(2, 15))

  # ==========================
  # VIEW: PROGRESS
  # ==========================
  def build_progress_view(self):
    self.view_progress = ctk.CTkScrollableFrame(
        self.view_container,
        fg_color="transparent",
        scrollbar_button_color="white",
        scrollbar_button_hover_color=COLOR_SECONDARY_HOVER,
    )

    ctk.CTkLabel(
        self.view_progress,
        text="Scan in progress...",
        font=FONT_HEADER_MEDIUM,
        text_color=COLOR_TEXT_MAIN,
    ).pack(anchor="w", padx=25, pady=(25, 5))
    ctk.CTkLabel(
        self.view_progress,
        text=(
            "This may take several minutes depending on the corpus of your"
            " tenant."
        ),
        font=FONT_BODY_MEDIUM,
        text_color=COLOR_TEXT_SUB,
    ).pack(anchor="w", padx=25)

    self.scan_container = ctk.CTkFrame(
        self.view_progress, fg_color="transparent"
    )
    self.scan_container.pack(fill="x", padx=25, pady=20)

    self.create_progress_row(
        self.scan_container, "users", "Scanning Users", is_user=True
    )
    self.prog_widgets = {}

  # ==========================
  # VIEW: RESULTS
  # ==========================
  def build_results_view(self):
    self.view_results = ctk.CTkScrollableFrame(
        self.view_container,
        fg_color="transparent",
        scrollbar_button_color="white",
        scrollbar_button_hover_color=COLOR_SECONDARY_HOVER,
    )

  def show_config_view(self):
    self.btn_action_secondary.configure(state="disabled")
    self.after(10, self.perform_view_switch)

  def perform_view_switch(self):
    for w in self.view_results.winfo_children():
      w.destroy()
    self.view_results.pack_forget()
    self.view_progress.pack_forget()

    if hasattr(self, "view_config") and self.view_config.winfo_exists():
      self.view_config.destroy()

    self.build_config_view()
    self.view_config.pack(fill="both", expand=True)

    if hasattr(self, "btn_export_logs"):
      self.btn_export_logs.destroy()

    self.btn_action_secondary.pack_forget()
    self.btn_action_primary.configure(
        text="Get migration estimates",
        command=self.start_scan,
        fg_color=COLOR_PRIMARY,
        hover_color=COLOR_PRIMARY_HOVER,
        state="normal",
    )
    self.btn_action_primary.pack(side="right", padx=25, pady=15)
    self.btn_action_secondary.configure(state="normal")

  def show_progress_view(self):
    self.view_config.pack_forget()
    self.view_results.pack_forget()
    self.view_progress.pack(fill="both", expand=True)

    self.btn_action_secondary.pack_forget()
    self.btn_action_primary.configure(
        text="Stop scan",
        command=self.stop_scan_logic,
        fg_color=COLOR_ERROR,
        hover_color=COLOR_ERROR_HOVER,
        width=180,
    )
    self.btn_action_primary.pack(side="right", padx=25, pady=15)

  def show_results_content(self, data):
    try:
      self.last_scan_data = data
      self.view_config.pack_forget()
      self.view_progress.pack_forget()

      for w in self.view_results.winfo_children():
        w.destroy()

      # Data Corpus Report Header
      ctk.CTkLabel(
          self.view_results,
          text="Data Corpus Report",
          font=FONT_HEADER_SMALL,
          text_color=COLOR_TEXT_MAIN,
      ).pack(anchor="w", padx=10, pady=(10, 0))
      ctk.CTkLabel(
          self.view_results,
          text="Review the analyzed data.",
          font=FONT_BODY_MEDIUM,
          text_color=COLOR_TEXT_SUB,
      ).pack(anchor="w", padx=10, pady=(0, 10))

      # Cards
      card_frame = ctk.CTkFrame(self.view_results, fg_color="transparent")
      card_frame.pack(fill="x", pady=10)

      self.create_stat_card(
          card_frame, "Users", f"{data['total_users']:,}", "👥"
      )
      self.create_stat_card(
          card_frame, "Emails", f"{data['total_emails']:,}", "📩"
      )
      self.create_stat_card(
          card_frame,
          "Calendar Events",
          f"{data['total_events']:,}",
          "📅",
          sub=f"({data['total_calendars']:,} Calendars)",
      )
      self.create_stat_card(
          card_frame, "Contacts", f"{data['total_contacts']:,}", "📞"
      )

      # Timeline
      ctk.CTkLabel(
          self.view_results,
          text="Timeline Estimates",
          font=FONT_HEADER_SMALL,
          text_color=COLOR_TEXT_MAIN,
      ).pack(anchor="w", padx=10, pady=(20, 5))
      ctk.CTkLabel(
          self.view_results,
          text=(
              "Projected migration timeline based on the proposed execution"
              " plan."
          ),
          font=FONT_BODY_MEDIUM,
          text_color=COLOR_TEXT_SUB,
      ).pack(anchor="w", padx=10, pady=(0, 10))

      # Total Footer
      foot = ctk.CTkFrame(self.view_results, fg_color="transparent")
      foot.pack(fill="x", pady=10)
      self.create_summary_box(
          foot, self.format_eta(data["total_eta"]), "Estimated Time"
      )
      self.create_summary_box(foot, f"{data['total_items']:,}", "Total Items")

      # --- NEW: Container for Paginated Content ---
      self.paginated_frame = ctk.CTkFrame(
          self.view_results, fg_color="transparent"
      )
      self.paginated_frame.pack(fill="x", expand=True)

      # Disclaimer
      disclaimer = (
          "* The estimations provided by this tool are calculated projections"
          " intended for preliminary planning only. Actual migration timelines"
          " (ETAs) and wave execution may vary based on real-time network"
          " conditions, source/target throttling policies, migration"
          " configurations, and the volume of delta migrations. These figures"
          " do not constitute a performance guarantee or a binding service"
          " level agreement (SLA)."
      )
      ctk.CTkLabel(
          self.view_results,
          text=disclaimer,
          font=FONT_BODY_SMALL,
          text_color=COLOR_TEXT_SUB,
          wraplength=800,
          justify="left",
      ).pack(anchor="w", padx=10, pady=(10, 20))

      # Resources
      ctk.CTkLabel(
          self.view_results,
          text="RESOURCES",
          font=FONT_BODY_BOLD,
          text_color=COLOR_TEXT_SUB,
      ).pack(anchor="w", padx=10, pady=(10, 10))

      res_frame = ctk.CTkFrame(self.view_results, fg_color="transparent")
      res_frame.pack(fill="x", pady=0)
      res_frame.grid_columnconfigure(0, weight=1)
      res_frame.grid_columnconfigure(1, weight=1)

      self.create_resource_card(
          res_frame,
          0,
          "🚀",
          "Data migration (New)",
          "Our new migration platform for enterprise - totally free.",
          "Learn more",
          "https://support.google.com/a/answer/14012274?hl=en&ref_topic=14012345&sjid=3864823775656113447-NC",
      )
      self.create_resource_card(
          res_frame,
          1,
          "☑️",
          "Best Practices Guide",
          "Essential tips for a smooth transition to Google Workspace.",
          "Read guide",
          "https://support.google.com/a/topic/14012345?hl=en&ref_topic=13002773&sjid=3864823775656113447-NC",
      )

      self.view_results.pack(fill="both", expand=True)

      # Footer Config
      self.btn_action_primary.pack_forget()
      self.btn_action_secondary.pack_forget()
      if hasattr(self, "btn_export_logs"):
        self.btn_export_logs.destroy()

      self.btn_action_primary.configure(
          text="Export full report",
          command=self.export_current_report,
          fg_color=COLOR_PRIMARY,
          hover_color=COLOR_PRIMARY_HOVER,
          width=160,
          state="normal",
      )
      self.btn_action_primary.pack(side="right", padx=25, pady=15)

      self.btn_export_logs = ctk.CTkButton(
          self.footer,
          text="Export logs",
          command=self.export_logs,
          fg_color=COLOR_TONAL_BG,
          text_color=COLOR_TONAL_TEXT,
          hover_color=COLOR_TONAL_HOVER,
          border_width=0,
          font=FONT_BODY_BOLD,
          width=120,
          height=40,
          corner_radius=20,
      )
      self.btn_export_logs.pack(side="right", pady=15)

      self.btn_action_secondary.configure(
          text="Start new search", command=self.show_config_view, width=140
      )
      self.btn_action_secondary.pack(side="left", padx=(25, 0), pady=15)

      self.selected_page_size = "50"
      self.render_paginated_view(0)
    except Exception as e:
      print(f"ERROR in show_results_content: {e}")
      for w in self.view_results.winfo_children():
        w.destroy()
      ctk.CTkLabel(
          self.view_results,
          text=f"Error displaying results: {e}",
          wraplength=700,
      ).pack(padx=20, pady=20)
      self.view_results.pack(fill="both", expand=True)

  def render_paginated_view(self, page):
    """Renders a specific page of the Gantt chart and Wave Details."""
    try:
      # Save the current scroll position before clearing the UI
      try:
        current_scroll = self.view_results._parent_canvas.yview()[0]
      except:
        current_scroll = 0.0

      # Clear previous paginated content
      for w in self.paginated_frame.winfo_children():
        w.destroy()

      data = self.last_scan_data
      waves = data.get("waves", [])
      buckets = data.get("buckets", [])
      max_duration = data.get("total_eta", 0)

      # --- NEW: Calculate Page Size from Dropdown ---
      selected_size = getattr(self, "selected_page_size", "50")
      if selected_size == "All":
        actual_items_per_page = len(waves) if len(waves) > 0 else 50
        view_all = True
      else:
        actual_items_per_page = int(selected_size)
        view_all = False

      # Calculate Page Boundaries
      total_pages = max(1, math.ceil(len(waves) / actual_items_per_page))

      if view_all:
        page = 0

      start_idx = page * actual_items_per_page
      end_idx = min(start_idx + actual_items_per_page, len(waves))
      page_waves = waves[start_idx:end_idx]

      # Fast O(1) lookup to check if a wave belongs on this page
      page_wave_names = {w["name"] for w in page_waves}

      # --- Single Master Container ---
      master_container = ctk.CTkFrame(
          self.paginated_frame,
          fg_color=COLOR_SURFACE,
          corner_radius=12,
          border_color=COLOR_OUTLINE_LIGHT,
          border_width=1,
      )
      master_container.pack(fill="x", padx=10, pady=10)

      # --- Header Row (Title Left, Pagination Right) ---
      header_row = ctk.CTkFrame(master_container, fg_color="transparent")
      header_row.pack(fill="x", padx=20, pady=(15, 5))

      # Title (Left)
      ctk.CTkLabel(
          header_row,
          text="Wave Execution Plan",
          font=FONT_BODY_BOLD,
          text_color=COLOR_TEXT_MAIN,
      ).pack(side="left")

      # Pagination Controls (Right)
      # Always show if total waves > 50 so user can use the dropdown to expand
      if len(waves) > 50 or selected_size != "50":
        ctrl_frame = ctk.CTkFrame(header_row, fg_color="transparent")
        ctrl_frame.pack(side="right")

        btn_state_prev = "normal" if page > 0 and not view_all else "disabled"
        btn_state_next = (
            "normal" if page < total_pages - 1 and not view_all else "disabled"
        )

        # 1. First Page Button ("|<")
        btn_first = ctk.CTkButton(
            ctrl_frame,
            text="|<",
            width=30,
            height=30,
            font=FONT_BODY_BOLD,  # FIX: Set height=30
            command=lambda: self.render_paginated_view(0),
            state=btn_state_prev,
            fg_color="transparent",
            border_width=0,
            text_color=COLOR_TEXT_SUB,
            hover_color=COLOR_SECONDARY_HOVER,
        )
        btn_first.pack(side="left", padx=(0, 5))

        # 2. Page Label
        label_text = (
            "All Pages" if view_all else f"Page {page + 1} of {total_pages}"
        )
        ctk.CTkLabel(
            ctrl_frame,
            text=label_text,
            font=FONT_BODY_BOLD,  # FIX: Matched font size and weight to the buttons
            text_color=COLOR_TEXT_MAIN,
        ).pack(side="left", padx=10)

        # 3. Previous Page Button ("<")
        btn_prev = ctk.CTkButton(
            ctrl_frame,
            text="<",
            width=30,
            height=30,
            font=FONT_BODY_BOLD,  # FIX: Set height=30
            command=lambda: self.render_paginated_view(page - 1),
            state=btn_state_prev,
            fg_color="transparent",
            border_width=0,
            text_color=COLOR_TEXT_SUB,
            hover_color=COLOR_SECONDARY_HOVER,
        )
        btn_prev.pack(side="left", padx=2)

        # 4. Next Page Button (">")
        btn_next = ctk.CTkButton(
            ctrl_frame,
            text=">",
            width=30,
            height=30,
            font=FONT_BODY_BOLD,  # FIX: Set height=30
            command=lambda: self.render_paginated_view(page + 1),
            state=btn_state_next,
            fg_color="transparent",
            border_width=0,
            text_color=COLOR_TEXT_SUB,
            hover_color=COLOR_SECONDARY_HOVER,
        )
        btn_next.pack(
            side="left", padx=(2, 10)
        )  # FIX: Added right padding before dropdown

        # --- Page Size Dropdown ---
        def on_page_size_change(choice):
          self.selected_page_size = choice
          self.render_paginated_view(0)  # Re-render from page 0 on change

        page_size_dropdown = ctk.CTkOptionMenu(
            ctrl_frame,
            values=["50", "100", "200", "All"],
            command=on_page_size_change,
            width=80,
            height=30,
            font=FONT_BODY_BOLD,
            fg_color=COLOR_SURFACE,
            button_color=COLOR_SURFACE,
            button_hover_color=COLOR_SECONDARY_HOVER,
            text_color=COLOR_TEXT_MAIN,  # FIX: Changed text color to match label
            dropdown_font=FONT_BODY_MEDIUM,
        )
        page_size_dropdown.set(selected_size)
        page_size_dropdown.pack(side="left", padx=(5, 0))

      ctk.CTkLabel(
          master_container,
          text=(
              "Each row represents a bucket of waves which can be executed in"
              " parallel to the other buckets."
          ),
          font=FONT_BODY_MEDIUM,
          text_color=COLOR_TEXT_SUB,
      ).pack(anchor="w", padx=20, pady=(0, 15))

      # 3. Parallel Waves Plan (Gantt Chart)
      if buckets:
        # Timeline Scale Header
        tl_row = ctk.CTkFrame(master_container, fg_color="transparent")
        tl_row.pack(fill="x", padx=10, pady=(0, 10))
        tl_scale = ctk.CTkFrame(tl_row, fg_color="transparent", height=20)
        tl_scale.pack(side="left", fill="x", expand=True, padx=(15, 100))

        num_ticks = 6
        for i in range(num_ticks):
          fraction = i / (num_ticks - 1)
          time_val = max_duration * fraction
          if max_duration < 120:
            val = round(time_val)
            label_text = f"{val} Hours"
          else:
            val = round(time_val / 24)
            label_text = f"{val} Days"
          if i == 0:
            label_text = "0"
            anchor_pos = "w"
          elif i == num_ticks - 1:
            anchor_pos = "e"
          else:
            anchor_pos = "center"
          ctk.CTkLabel(
              tl_scale,
              text=label_text,
              font=FONT_BODY_SMALL,
              text_color=COLOR_TEXT_SUB,
          ).place(relx=fraction, rely=0.5, anchor=anchor_pos)

        # Render Buckets
        for b in buckets:
          b_row = ctk.CTkFrame(master_container, fg_color="transparent")
          b_row.pack(fill="x", padx=10, pady=(0, 10))

          track = ctk.CTkFrame(
              b_row, fg_color=COLOR_BACKGROUND, height=24, corner_radius=8
          )
          track.pack(side="left", fill="x", expand=True, padx=10)

          inner_track = ctk.CTkFrame(
              track, fg_color="transparent", height=24, corner_radius=8
          )
          inner_track.pack(fill="both", expand=True, padx=4)

          # Pre-calculate widths to maintain global time scale
          raw_widths = []
          for wave in b["waves"]:
            w = wave["eta"] / max_duration if max_duration > 0 else 0
            raw_widths.append(w)

          min_vis_width = 0.06
          visual_widths = [max(w, min_vis_width) for w in raw_widths]
          total_vis = sum(visual_widths)
          scale_factor = 1.0 if total_vis <= 1.0 else 1.0 / total_vis

          current_relx = 0
          for i, wave in enumerate(b["waves"]):
            final_width = visual_widths[i] * scale_factor

            # ONLY RENDER segment if it belongs to the current page
            if wave["name"] in page_wave_names:
              segment = ctk.CTkFrame(
                  inner_track,
                  fg_color=COLOR_TONAL_BG,
                  corner_radius=12,
                  border_width=1,
                  border_color="white",
              )
              segment.place(
                  relx=current_relx,
                  rely=0.05,
                  relwidth=final_width,
                  relheight=0.9,
              )

              label_text = (
                  wave["name"]
                  if final_width > 0.12
                  else wave["name"].replace("Wave ", "W")
              )
              ctk.CTkLabel(
                  segment,
                  text=label_text,
                  font=("Roboto", 10, "bold"),
                  text_color=COLOR_TONAL_TEXT,
              ).place(relx=0.5, rely=0.5, anchor="center", relheight=0.9)

            # Keep advancing current_relx so the timeline gaps are accurate
            current_relx += final_width

          ctk.CTkLabel(
              b_row,
              text=self.format_eta(b["total"]),
              width=100,
              anchor="e",
              font=FONT_BODY_BOLD,
              text_color=COLOR_TEXT_MAIN,
          ).pack(side="left")

      # Visual Separator between Gantt chart and Wave Details
      ctk.CTkFrame(
          master_container, height=1, fg_color=COLOR_OUTLINE_LIGHT
      ).pack(fill="x", padx=20, pady=15)

      # 4. Wave Details
      if page_waves:
        ctk.CTkLabel(
            master_container,
            text="Wave Details",
            font=FONT_BODY_BOLD,
            text_color=COLOR_TEXT_MAIN,
        ).pack(anchor="w", padx=20, pady=(0, 5))

        ctk.CTkLabel(
            master_container,
            text=(
                "The waves are numbered in the order of their proposed"
                " execution."
            ),
            font=FONT_BODY_MEDIUM,
            text_color=COLOR_TEXT_SUB,
        ).pack(anchor="w", padx=20, pady=(0, 15))

        max_wave_eta = max(w["eta"] for w in waves) if waves else 1
        for w in page_waves:
          self.create_wave_bar(master_container, w, max_wave_eta)

      self.view_results.update_idletasks()
      try:
        self.view_results._parent_canvas.yview_moveto(current_scroll)
      except:
        pass
    except Exception as e:
      print(f"ERROR in render_paginated_view: {e}")
      ctk.CTkLabel(
          self.paginated_frame,
          text=f"Error rendering wave details: {e}",
          wraplength=700,
      ).pack(padx=20, pady=20)

  # --- Widget Generators ---
  def create_entry(self, parent, label, var, show=None):
    f = ctk.CTkFrame(parent, fg_color="transparent")
    f.pack(fill="x", pady=5)
    ctk.CTkLabel(
        f, text=label, width=100, anchor="w", text_color=COLOR_TEXT_SUB
    ).pack(side="left")
    ctk.CTkEntry(
        f,
        textvariable=var,
        show=show,
        height=40,
        corner_radius=4,
        border_width=1,
        border_color=COLOR_OUTLINE,
        fg_color="transparent",
        text_color=COLOR_TEXT_MAIN,
    ).pack(side="left", fill="x", expand=True)

  def create_grid_entry(self, parent, r, c, txt, var):
    ctk.CTkLabel(parent, text=txt, text_color=COLOR_TEXT_SUB).grid(
        row=r, column=c, sticky="w", padx=5, pady=5
    )
    ctk.CTkEntry(
        parent,
        textvariable=var,
        width=80,
        corner_radius=4,
        border_width=1,
        border_color=COLOR_OUTLINE,
        fg_color="transparent",
        text_color=COLOR_TEXT_MAIN,
    ).grid(row=r, column=c + 1, sticky="w", padx=5, pady=5)

  def create_progress_row(self, parent, key, title, is_user=False):
    f = ctk.CTkFrame(parent, fg_color="transparent")
    f.pack(fill="x", pady=10)
    top = ctk.CTkFrame(f, fg_color="transparent")
    top.pack(fill="x")
    lbl_icon = ctk.CTkLabel(
        top,
        text="○",
        font=FONT_HEADER_SMALL,
        width=30,
        text_color=COLOR_TEXT_SUB,
    )
    lbl_icon.pack(side="left", padx=(0, 10))
    ctk.CTkLabel(
        top, text=title, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN
    ).pack(side="left")

    if is_user or key == "users":
      bar = ctk.CTkProgressBar(
          f,
          height=8,
          mode="indeterminate",
          progress_color=COLOR_PRIMARY,
          fg_color=COLOR_OUTLINE_LIGHT,
          corner_radius=4,
      )
    else:
      bar = ctk.CTkProgressBar(
          f,
          height=8,
          progress_color=COLOR_PRIMARY,
          fg_color=COLOR_OUTLINE_LIGHT,
          corner_radius=4,
      )
      bar.set(0)
    bar.pack(fill="x", pady=10)

    lbl_status = ctk.CTkLabel(
        f, text="Waiting...", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB
    )
    lbl_status.pack(anchor="w")

    if not hasattr(self, "prog_widgets"):
      self.prog_widgets = {}
    self.prog_widgets[key] = {
        "bar": bar,
        "lbl": lbl_status,
        "icon": lbl_icon,
    }
    if key == "users":
      self.prog_user = bar
      self.lbl_user_status = lbl_status

  def create_stat_card(self, parent, title, value, icon, sub=None):
    f = ctk.CTkFrame(
        parent,
        fg_color=COLOR_SURFACE,
        width=180,
        height=120,
        corner_radius=12,
        border_color=COLOR_OUTLINE_LIGHT,
        border_width=1,
    )
    f.pack(side="left", padx=10, fill="both", expand=True)
    ctk.CTkLabel(f, text=icon, font=FONT_ICON_LARGE, anchor="center").pack(
        pady=(20, 0)
    )
    ctk.CTkLabel(
        f,
        text=value,
        font=FONT_HEADER_MEDIUM,
        text_color=COLOR_TEXT_MAIN,
        anchor="center",
    ).pack()
    ctk.CTkLabel(
        f,
        text=title,
        font=FONT_BODY_MEDIUM,
        text_color=COLOR_TEXT_SUB,
        anchor="center",
    ).pack(pady=(0, 5))
    if sub:
      ctk.CTkLabel(
          f,
          text=sub,
          font=FONT_BODY_SMALL,
          text_color=COLOR_TEXT_SUB,
          anchor="center",
      ).pack(pady=(0, 5))

  def create_resource_card(
      self, parent, col_idx, icon, title, desc, link_text, link_url
  ):
    f = ctk.CTkFrame(
        parent,
        fg_color=COLOR_SURFACE,
        height=100,
        corner_radius=12,
        border_color=COLOR_OUTLINE_LIGHT,
        border_width=1,
    )
    f.grid(row=0, column=col_idx, sticky="ew", padx=10, pady=5)
    ctk.CTkLabel(f, text=icon, font=FONT_ICON_MEDIUM).pack(
        side="left", padx=20, anchor="n", pady=20
    )
    content = ctk.CTkFrame(f, fg_color="transparent")
    content.pack(side="left", fill="both", expand=True, pady=15, padx=(0, 15))
    ctk.CTkLabel(
        content,
        text=title,
        font=FONT_BODY_BOLD,
        text_color=COLOR_TEXT_MAIN,
        anchor="w",
    ).pack(fill="x")
    ctk.CTkLabel(
        content,
        text=desc,
        font=FONT_BODY_MEDIUM,
        text_color=COLOR_TEXT_SUB,
        anchor="w",
        wraplength=250,
        justify="left",
    ).pack(fill="x", pady=(2, 5))
    link = ctk.CTkLabel(
        content,
        text=f"{link_text} ↗",
        font=FONT_BODY_BOLD,
        text_color=COLOR_PRIMARY,
        anchor="w",
    )
    link.pack(fill="x")
    link.bind("<Button-1>", lambda e: webbrowser.open(link_url))
    link.configure(cursor="hand2")

  def create_wave_bar(self, parent, wave, max_eta):
    f = ctk.CTkFrame(parent, fg_color="transparent")
    f.pack(fill="x", padx=20, pady=8)
    users_str = self.format_metric(wave["users"])
    emails_str = self.format_metric(wave["total_emails"])
    events_str = self.format_metric(wave["total_events"])
    contacts_str = self.format_metric(wave["total_contacts"])
    info = (
        f"{wave['name']} - {users_str} 👥  |  {emails_str} 📩  |  {events_str}"
        f" 📅  |  {contacts_str} 📞"
    )
    ctk.CTkLabel(
        f,
        text=info,
        width=350,
        anchor="w",
        font=FONT_BODY_MEDIUM,
        text_color=COLOR_TEXT_MAIN,
    ).pack(side="left")

    if max_eta > 0:
      pixel_width = int((wave["eta"] / max_eta) * 350)
    else:
      pixel_width = 0

    # Ensure a tiny minimum width (20px) so the bar is always visible
    w_width = max(20, pixel_width)

    bar = ctk.CTkFrame(
        f, width=w_width, height=16, fg_color=COLOR_WAVE_BAR, corner_radius=8
    )
    bar.pack(side="left", padx=10)
    ctk.CTkLabel(
        f,
        text=self.format_eta(wave["eta"]),
        font=FONT_BODY_BOLD,
        text_color=COLOR_TEXT_MAIN,
    ).pack(side="left")

  def format_metric(self, value):
    if value >= 1_000_000:
      return f"{value/1_000_000:.1f}M"
    elif value >= 1_000:
      return f"{value/1_000:.1f}K"
    else:
      return str(int(value))

  def format_eta(self, hours):
    if hours < 24:
      return f"{math.ceil(hours)} Hours"
    else:
      days = int(hours // 24)
      rem = hours % 24
      rem_hours = math.ceil(rem)
      if rem_hours == 24:
        days += 1
        rem_hours = 0
      return f"{days} Days, {rem_hours} Hours"

  def create_summary_box(self, parent, top_text, bot_text):
    f = ctk.CTkFrame(
        parent,
        fg_color=COLOR_SURFACE,
        height=90,
        corner_radius=12,
        border_color=COLOR_OUTLINE_LIGHT,
        border_width=1,
    )
    f.pack(side="left", padx=10, expand=True, fill="x")
    ctk.CTkLabel(
        f, text=top_text, font=FONT_HEADER_MEDIUM, text_color=COLOR_TEXT_MAIN
    ).pack(pady=(20, 0))
    ctk.CTkLabel(
        f, text=bot_text, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB
    ).pack(pady=(0, 20))

  def toggle_adv(self):
    if self.adv_visible:
      self.adv_frame.pack_forget()
      self.btn_adv.configure(text="Show Advanced Settings ▼")
      self.adv_visible = False
    else:
      self.adv_frame.pack(fill="x", pady=10, after=self.btn_adv)
      self.btn_adv.configure(text="Hide Advanced Settings ▲")
      self.adv_visible = True

  def browse_user_csv(self):
    f = filedialog.askopenfilename(filetypes=[("CSV", "*.csv")])
    if f:
      self.user_csv_path.set(f)

  def export_report(self, data):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    f = filedialog.asksaveasfilename(
        initialfile=f"migration_report_{ts}.csv", defaultextension=".csv"
    )
    if f and "df" in data:
      data["df"].to_csv(f, index=False)

  def export_current_report(self):
    if hasattr(self, "last_scan_data"):
      self.export_report(self.last_scan_data)

  def export_logs(self):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    f = filedialog.asksaveasfilename(
        initialfile=f"logs_{ts}.log",
        defaultextension=".log",
        filetypes=[("Log Files", "*.log"), ("All Files", "*.*")],
    )
    if f:
      with self.log_lock:
        content = "\n".join(self.log_buffer)
      with open(f, "w", encoding="utf-8") as file:
        file.write(content)

  # ==========================
  # LOGIC & EXECUTION
  # ==========================
  def animate_spinner(self, source):
    if source not in self.spinners_active or not self.spinners_active[source]:
      return
    idx = self.spinner_indices.get(source, 0)
    icon = self.spinner_chars[idx % len(self.spinner_chars)]
    self.spinner_indices[source] = idx + 1
    if source in self.prog_widgets:
      self.prog_widgets[source]["icon"].configure(
          text=icon, text_color=COLOR_PRIMARY
      )
    self.after(80, lambda: self.animate_spinner(source))

  def update_progress(self, msg):
    if isinstance(msg, str):
      self.log_buffer.append(msg)
    elif isinstance(msg, dict):
      mtype = msg.get("type")
      if mtype == "user_discovery":
        if not self.view_progress.winfo_viewable():
          self.show_progress_view()
        count = msg.get("count", 0)
        status = msg.get("status", "Scanning...")
        if "users" in self.prog_widgets:
          self.prog_widgets["users"]["lbl"].configure(
              text=f"{count} users found"
          )
          if not self.spinners_active.get("users"):
            self.spinners_active["users"] = True
            self.animate_spinner("users")
        if status == "Done":
          self.spinners_active["users"] = False
          if "users" in self.prog_widgets:
            self.prog_widgets["users"]["icon"].configure(
                text="✓", text_color=COLOR_SUCCESS
            )
            self.prog_widgets["users"]["bar"].stop()
            self.prog_widgets["users"]["bar"].configure(mode="determinate")
            self.prog_widgets["users"]["bar"].set(1.0)
      elif mtype == "phase_status":
        source = msg.get("source")
        status = msg.get("status")
        if status == "running":
          self.spinners_active[source] = True
          self.animate_spinner(source)
        elif status == "complete":
          self.spinners_active[source] = False
          if source in self.prog_widgets:
            self.prog_widgets[source]["icon"].configure(
                text="✓", text_color=COLOR_SUCCESS
            )
            if self.prog_widgets[source]["bar"].cget("mode") == "indeterminate":
              self.prog_widgets[source]["bar"].stop()
              self.prog_widgets[source]["bar"].configure(mode="determinate")
            self.prog_widgets[source]["bar"].set(1.0)
            if source == "plan_generation":
              self.prog_widgets[source]["lbl"].configure(
                  text=(
                      "Plan generated, please wait while we prepare the final"
                      " dashboard..."
                  )
              )
      elif mtype == "scan_progress":
        source = msg.get("source")
        val = msg.get("progress", 0.0)
        cumulative = msg.get("cumulative", 0)
        users_proc = msg.get("processed", 0)
        users_fail = msg.get("failed", 0)
        users_tot = msg.get("total", 0)
        if source in self.prog_widgets:
          self.prog_widgets[source]["bar"].set(val)
          if source == "calendars":
            extra = msg.get("extra_text", "")
            self.prog_widgets[source]["lbl"].configure(
                text=(
                    f"Users: {users_proc - users_fail} succeeded , {users_fail}"
                    f" failed | {extra}"
                )
            )
          elif source == "plan_generation":
            self.prog_widgets[source]["lbl"].configure(
                text=msg.get("extra_text", "")
            )
          else:
            label = "Emails" if source == "messages" else "Contacts"
            self.prog_widgets[source]["lbl"].configure(
                text=(
                    f"Users: {users_proc - users_fail} succeeded , {users_fail}"
                    f" failed | {label}: {cumulative:,}"
                )
            )
      elif mtype == "complete":
        self.show_results_content(msg["data"])
      elif mtype == "error":
        messagebox.showerror(
            "Operation Failed", msg.get("message", "An unknown error occurred")
        )
        self.show_config_view()

  def process_log_queue(self):
    try:
      while not self.log_queue.empty():
        self.update_progress(self.log_queue.get_nowait())
    except:
      pass
    finally:
      self.after(100, self.process_log_queue)

  def start_scan(self):
    disclaimer_text = (
        "The estimations provided by this tool are calculated projections"
        " intended for preliminary planning only. Actual migration timelines"
        " (ETAs) and wave execution may vary based on real-time network"
        " conditions, source/target throttling policies, migration"
        " configurations, and the volume of delta migrations. These figures do"
        " not constitute a performance guarantee or a binding service level"
        " agreement (SLA)."
    )
    should_proceed = messagebox.askokcancel(
        title="Estimation Disclaimer",
        message=disclaimer_text,
        parent=self,
    )
    if not should_proceed:
      return

    self.stop_scan_event.clear()
    with self.log_lock:
      self.log_buffer = []
    self.spinners_active = {}
    self.spinner_indices = {}
    for w in self.scan_container.winfo_children():
      w.destroy()
    self.prog_widgets = {}

    self.create_progress_row(
        self.scan_container, "users", "Scanning Users", is_user=True
    )
    self.prog_user.start()
    if self.scan_email.get():
      self.create_progress_row(
          self.scan_container, "messages", "Scanning Emails"
      )
    if self.scan_contact.get():
      self.create_progress_row(
          self.scan_container, "contacts", "Scanning Contacts"
      )
    if self.scan_calendar.get():
      self.create_progress_row(
          self.scan_container, "calendars", "Scanning Calendars"
      )
    self.create_progress_row(
        self.scan_container, "plan_generation", "Generating Migration Plan"
    )

    threading.Thread(target=self.execute_migration_scan).start()

  def stop_scan_logic(self):
    self.btn_action_primary.configure(state="disabled", text="Stopping scan...")
    self.stop_scan_event.set()
    with self.log_lock:
      self.log_buffer.append("Scan Stopped.")

  def log_msg(self, text):
    with self.log_lock:
      self.log_buffer.append(text)

  def ui_update(self, type, **kwargs):
    data = {"type": type}
    data.update(kwargs)
    self.log_queue.put(data)

  def execute_migration_scan(self):
    """Orchestrates the end-to-end migration estimation scan."""
    monitor = None
    try:
      self.log_msg("--- Starting Batch Scan ---")
      monitor = ResourceMonitor()
      monitor.start()
      start_time = time.time()

      # 1. Preparation
      config = self._get_scan_configuration()

      # 2. Authentication
      manager = self._authenticate_if_needed(config)

      # 3. User Discovery
      all_users, existing_data = self._resolve_target_users(config, manager)

      # 4. Build Batch List
      csv_rows, stats = self._prepare_batch_list(
          config, all_users, existing_data
      )

      # 5. Execution
      self._run_scan_phases(config, manager, csv_rows, stats)

      # 6. Analysis & Reporting
      self._generate_final_report(config, csv_rows, stats, monitor, start_time)

    except Exception as e:
      self.log_msg(f"Process failed: {e}")
      print(f"Process failed: {e}")
      self.ui_update("error", message=str(e))
    finally:
      if monitor is not None:
        monitor.stop()

  def _get_scan_configuration(self) -> ScanConfig:
    """Reads configuration variables from UI controls."""
    return ScanConfig(
        tenant_id=self.tenant_id.get().strip(),
        client_ids=[
            x.strip() for x in self.client_ids.get().split(",") if x.strip()
        ],
        client_secrets=[
            x.strip() for x in self.client_secrets.get().split(",") if x.strip()
        ],
        user_source=self.user_source.get(),
        csv_path=self.user_csv_path.get(),
        scan_email=self.scan_email.get(),
        scan_contact=self.scan_contact.get(),
        scan_calendar=self.scan_calendar.get(),
        concurrency=self.concurrency.get(),
        load_multiplier=self.load_multiplier.get(),
        retries=self.retries.get(),
        backoff=self.backoff.get(),
        eta_max_users=self.eta_max_users.get(),
        parallel_waves=self.parallel_waves.get(),
    )

  def _authenticate_if_needed(
      self, config: ScanConfig
  ) -> Optional[TokenManager]:
    """Authenticates with MS Graph if any scanning is required."""
    if config.tenant_id and config.client_ids and config.client_secrets:
      manager = TokenManager(
          config.tenant_id,
          config.client_ids,
          config.client_secrets,
          config.concurrency,
          config.retries,
          config.backoff,
      )
      return manager
    return None

  def _resolve_target_users(
      self, config: ScanConfig, manager: Optional[TokenManager]
  ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Resolves the list of users to process, either from CSV or Tenant."""
    existing_data = {}
    users_to_resolve = []
    all_users = []

    # Flags to determine if we need to scan
    have_email = False
    have_contact = False
    have_calendar = False

    # 1. Parse CSV if applicable
    if config.user_source == "csv":
      if not config.csv_path or not os.path.exists(config.csv_path):
        raise Exception("CSV path invalid or file not found.")

      df_input = pd.read_csv(config.csv_path)
      df_input.columns = df_input.columns.str.strip()
      rename_map = {
          "Email Id": "User Principal Name",
          "Calendar Event Count": "Event Count",
      }
      df_input.rename(columns=rename_map, inplace=True)
      col = next(
          (
              c
              for c in ["User Principal Name", "Email", "UPN"]
              if c in df_input.columns
          ),
          None,
      )

      if col:
        if "Email Count" in df_input.columns:
          have_email = True
        if "Contact Count" in df_input.columns:
          have_contact = True
        if (
            "Calendar Count" in df_input.columns
            and "Event Count" in df_input.columns
        ):
          have_calendar = True

        for _, row in df_input.iterrows():
          upn = str(row[col]).lower().strip()
          existing_data[upn] = row.to_dict()

        users_to_resolve = df_input[col].dropna().unique().tolist()
      else:
        raise Exception(
            "CSV must contain 'Email Id' or 'User Principal Name' column."
        )

    # 2. Determine if we need live scanning
    scanning_required = (
        (config.scan_email and not have_email)
        or (config.scan_contact and not have_contact)
        or (config.scan_calendar and not have_calendar)
    )

    # 3. Authenticate if required
    if scanning_required or config.user_source == "tenant":
      if not manager:
        # If we need to scan but have no manager (missing creds)
        if config.user_source == "tenant":
          raise Exception("Missing Credentials for Tenant Scan.")
        else:
          raise Exception(
              "Missing Credentials for Delta Scan (CSV missing some columns)."
          )

      # Determine scopes based on what is missing
      required_scopes = ["User.Read.All"]
      if config.scan_email and not have_email:
        required_scopes.append("Mail.Read")
      if config.scan_contact and not have_contact:
        required_scopes.append("Contacts.Read")
      if config.scan_calendar and not have_calendar:
        required_scopes.append("Calendars.Read")

      manager.authenticate_all(self.log_msg, required_scopes=required_scopes)
    else:
      self.log_msg(
          "Skipping Authentication (All required data present in CSV)."
      )

    self.ui_update("user_discovery", status="Fetching...", count=0)

    # 4. Resolve Users
    if config.user_source == "csv":
      if scanning_required:
        self.log_msg("Delta Scan required. Resolving User IDs...")
        all_users = self._resolve_users_from_csv(manager, users_to_resolve)
      else:
        self.log_msg("Using CSV data directly...")
        all_users = [
            {"userPrincipalName": u, "id": None} for u in users_to_resolve
        ]
    else:
      all_users = self._get_all_users_graph(manager)

    # Apply Load Multiplier
    mult = max(1, config.load_multiplier)
    if mult > 1:
      all_users = all_users * mult

    self.ui_update("user_discovery", status="Done", count=len(all_users))
    return all_users, existing_data

  def _prepare_batch_list(
      self,
      config: ScanConfig,
      all_users: List[Dict[str, Any]],
      existing_data: Dict[str, Any],
  ) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Prepares the initial list of user rows and stats."""
    csv_rows = []

    # Check what we have in existing_data
    sample = next(iter(existing_data.values())) if existing_data else {}
    have_email = "Email Count" in sample
    have_contact = "Contact Count" in sample
    have_calendar = "Calendar Count" in sample and "Event Count" in sample

    def safe_int(val):
      try:
        return int(float(val))
      except:
        return 0

    for u in all_users:
      upn = u["userPrincipalName"]
      key = str(upn).lower().strip()
      row = {
          "User Principal Name": upn,
          "User ID": u["id"],
          "Email Count": 0,
          "Contact Count": 0,
          "Calendar Count": 0,
          "Event Count": 0,
      }
      if key in existing_data:
        src = existing_data[key]
        if have_email and config.scan_email:
          row["Email Count"] = safe_int(src.get("Email Count", 0))
        if have_contact and config.scan_contact:
          row["Contact Count"] = safe_int(src.get("Contact Count", 0))
        if have_calendar and config.scan_calendar:
          row["Calendar Count"] = safe_int(src.get("Calendar Count", 0))
          row["Event Count"] = safe_int(src.get("Event Count", 0))
      csv_rows.append(row)

    stats = {
        "emails": sum(r["Email Count"] for r in csv_rows),
        "contacts": sum(r["Contact Count"] for r in csv_rows),
        "calendars": sum(r["Calendar Count"] for r in csv_rows),
        "events": sum(r["Event Count"] for r in csv_rows),
    }
    return csv_rows, stats

  def _run_scan_phases(
      self,
      config: ScanConfig,
      manager: Optional[TokenManager],
      csv_rows: List[Dict[str, Any]],
      stats: Dict[str, int],
  ) -> None:
    """Executes the data fetching phases."""
    num_apps = len(config.client_ids) if config.client_ids else 1
    workers = num_apps * config.concurrency
    batch_size = min(self.concurrency.get() // 10, 20)
    user_chunks = [
        csv_rows[i : i + batch_size]
        for i in range(0, len(csv_rows), batch_size)
    ]
    total_users = len(csv_rows)

    has_email_data = any(r["Email Count"] > 0 for r in csv_rows)
    has_contact_data = any(r["Contact Count"] > 0 for r in csv_rows)
    has_calendar_data = any(r["Calendar Count"] > 0 for r in csv_rows)
    failed_emails = []
    failed_contacts = []
    failed_calendars = []

    can_scan = manager is not None

    if config.scan_email:
      if can_scan and (not has_email_data or config.user_source == "tenant"):
        self.ui_update("phase_status", source="messages", status="running")
        failed_emails = self.run_batch_phase_ui(
            user_chunks, "messages", manager, workers, stats, total_users
        )
        self.ui_update("phase_status", source="messages", status="complete")
      else:
        self.log_msg("Skipping Email Scan (Data present or No Auth)")
        self.ui_update("phase_status", source="messages", status="running")
        self.ui_update(
            "scan_progress",
            source="messages",
            progress=1.0,
            cumulative=stats["emails"],
            processed=total_users,
            total=total_users,
        )
        self.ui_update("phase_status", source="messages", status="complete")

    if config.scan_contact:
      if can_scan and (not has_contact_data or config.user_source == "tenant"):
        self.ui_update("phase_status", source="contacts", status="running")
        failed_contacts = self.run_batch_phase_ui(
            user_chunks, "contacts", manager, workers, stats, total_users
        )
        self.ui_update("phase_status", source="contacts", status="complete")
      else:
        self.log_msg("Skipping Contact Scan (Data present or No Auth)")
        self.ui_update("phase_status", source="contacts", status="running")
        self.ui_update(
            "scan_progress",
            source="contacts",
            progress=1.0,
            cumulative=stats["contacts"],
            processed=total_users,
            total=total_users,
        )
        self.ui_update("phase_status", source="contacts", status="complete")

    if config.scan_calendar:
      if can_scan and (not has_calendar_data or config.user_source == "tenant"):
        self.ui_update("phase_status", source="calendars", status="running")
        failed_calendars = self.run_batch_phase_ui(
            user_chunks, "calendars", manager, workers, stats, total_users
        )
        self.ui_update("phase_status", source="calendars", status="complete")
      else:
        self.log_msg("Skipping Calendar Scan (Data present or No Auth)")
        self.ui_update("phase_status", source="calendars", status="running")
        extra = (
            f"Calendars: {stats['calendars']:,} | Events: {stats['events']:,}"
        )
        self.ui_update(
            "scan_progress",
            source="calendars",
            progress=1.0,
            cumulative=0,
            processed=total_users,
            total=total_users,
            extra_text=extra,
        )
        self.ui_update("phase_status", source="calendars", status="complete")

    # --- LOG FAILED USERS SUMMARY ---
    if failed_emails or failed_calendars or failed_contacts:
      self.log_msg("\n" + "=" * 40)

      if failed_emails:
        self.log_msg("Email Migration Failures")
        for f in failed_emails:
          self.log_msg(f"User: {f['user']} | Cause: {f['cause']}")
        self.log_msg("")  # Add blank line

      if failed_calendars:
        self.log_msg("Calendar Migration Failures")
        for f in failed_calendars:
          self.log_msg(f"User: {f['user']} | Cause: {f['cause']}")
        self.log_msg("")  # Add blank line

      if failed_contacts:
        self.log_msg("Contacts Migration Failures")
        for f in failed_contacts:
          self.log_msg(f"User: {f['user']} | Cause: {f['cause']}")
        self.log_msg("")  # Add blank line
      self.log_msg("=" * 40)

  def _generate_final_report(
      self,
      config: ScanConfig,
      csv_rows: List[Dict[str, Any]],
      stats: Dict[str, int],
      monitor: ResourceMonitor,
      start_time: float,
  ) -> None:
    """Calculates final stats, waves, and exports data."""
    self.ui_update("phase_status", source="plan_generation", status="running")
    time.sleep(0.5)

    self.ui_update(
        "scan_progress",
        source="plan_generation",
        progress=0.33,
        extra_text="Calculating ETAs...",
    )
    time.sleep(0.5)

    df = pd.DataFrame(csv_rows)
    df, waves, total_eta, buckets = self.calculate_migration_waves(df)

    monitor.stop()
    monitor.join()
    elapsed = str(timedelta(seconds=int(time.time() - start_time)))
    avg_cpu, max_cpu, avg_ram, max_ram = monitor.get_stats()
    total_ram_gb = psutil.virtual_memory().total / (1024**3)
    total_cpu_cores = psutil.cpu_count(logical=True)

    self.log_msg("\n" + "=" * 40)
    self.log_msg(f"TOTAL TIME: {elapsed}")
    self.log_msg(f"Total Users: {len(csv_rows)}")
    self.log_msg(
        f"Emails: {stats['emails']} | Contacts: {stats['contacts']} |"
        f" Calendars: {stats['calendars']} | Events: {stats['events']}"
    )
    self.log_msg(f"System: {total_cpu_cores} Cores, {total_ram_gb:.1f}GB RAM")
    self.log_msg(f"CPU Avg/Peak: {avg_cpu:.1f}% / {max_cpu:.1f}%")
    self.log_msg(f"RAM Avg/Peak: {avg_ram:.1f}% / {max_ram:.1f}%")
    self.log_msg("=" * 40)

    self.ui_update(
        "scan_progress",
        source="plan_generation",
        progress=0.66,
        extra_text="Generating reports...",
    )
    time.sleep(0.5)

    # Prepare Export
    output_map = {
        "User Principal Name": "Email Id",
        "Event Count": "Calendar Event Count",
    }
    df_output = df.rename(columns=output_map)

    final_columns = ["Email Id", "Suggested Wave"]
    if config.scan_email:
      final_columns.append("Email Count")
    if config.scan_contact:
      final_columns.append("Contact Count")
    if config.scan_calendar:
      final_columns.extend(["Calendar Count", "Calendar Event Count"])

    final_columns = [c for c in final_columns if c in df_output.columns]
    df_output = df_output[final_columns]

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = os.path.join("outputs", ts)
    os.makedirs(output_dir, exist_ok=True)

    report_path = os.path.join(output_dir, f"user_report_{ts}.csv")
    logs_path = os.path.join(output_dir, f"logs_{ts}.log")

    df_output.to_csv(report_path, index=False)

    waves_dir = os.path.join(output_dir, "suggested waves")
    os.makedirs(waves_dir, exist_ok=True)

    unique_waves = df_output["Suggested Wave"].unique()
    for wave in unique_waves:
      if not wave:
        continue
      wave_data = df_output[df_output["Suggested Wave"] == wave].copy()
      wave_export = wave_data[["Email Id"]].rename(
          columns={"Email Id": "Source Exchange Email"}
      )
      safe_name = wave.replace(" ", "")
      wave_path = os.path.join(waves_dir, f"{safe_name}.csv")
      wave_export.to_csv(wave_path, index=False)

    with self.log_lock:
      log_content = "\n".join(self.log_buffer)
    with open(logs_path, "w", encoding="utf-8") as f:
      f.write(log_content)

    result_data = {
        "total_users": len(df),
        "total_emails": stats["emails"],
        "total_contacts": stats["contacts"],
        "total_calendars": stats["calendars"],
        "total_events": stats["events"],
        "total_items": (
            stats["emails"]
            + stats["contacts"]
            + stats["calendars"]
            + stats["events"]
        ),
        "total_eta": total_eta,
        "waves": waves,
        "df": df_output,
        "buckets": buckets,
    }
    self.ui_update("phase_status", source="plan_generation", status="complete")
    time.sleep(2)
    self.ui_update("complete", data=result_data)

  def run_batch_phase_ui(
      self, chunks, res_type, manager, workers, stats, total_users
  ):
    if self.stop_scan_event.is_set():
      return
    self.log_msg(f"\n--- Starting {res_type.upper()} Scan ---")

    completed_chunks = 0
    users_processed = 0
    users_failed = 0
    phase_failures = []
    phase_total = 0
    phase_events = 0
    phase_cals = 0

    executor = ThreadPoolExecutor(max_workers=workers)
    try:
      # Map the Future to its specific chunk so we know exactly how many users it contained
      futures = {
          executor.submit(
              fetch_user_batch_data,
              chunk,
              res_type,
              manager,
              self.log_msg,
              self.stop_scan_event,
          ): chunk
          for chunk in chunks
      }

      for f in as_completed(futures):
        if self.stop_scan_event.is_set():
          executor.shutdown(wait=False, cancel_futures=True)
          return []

        # Get the original chunk associated with this completed future
        chunk = futures[f]

        try:
          r = f.result()
          users_failed += r.get("failed", 0)
          phase_failures.extend(r.get("failed_details", []))

          if res_type == "messages":
            val = r["emails"]
            stats["emails"] += val
            phase_total += val
          elif res_type == "contacts":
            val = r["contacts"]
            stats["contacts"] += val
            phase_total += val
          elif res_type == "calendars":
            c = r["calendars"]
            e = r["events"]
            stats["calendars"] += c
            stats["events"] += e
            phase_cals += c
            phase_events += e
        except Exception as e:
          users_failed += len(chunk)
          for u in chunk:
            phase_failures.append({
                "user": u["User Principal Name"],
                "cause": f"Chunk failed: {e}",
            })
          pass

        completed_chunks += 1
        users_processed += len(chunk)

        log_freq = max(10, workers // 5)

        if completed_chunks % log_freq == 0 or users_processed == total_users:
          if res_type == "calendars":
            self.log_msg(
                f"Processed {users_processed}/{total_users} | Failed:"
                f" {users_failed}/{total_users} | Calendars: {phase_cals} |"
                f" Events: {phase_events}"
            )
          else:
            label = "messages" if res_type == "messages" else "contacts"
            self.log_msg(
                f"Processed {users_processed}/{total_users} | Failed:"
                f" {users_failed}/{total_users} | {label}: {phase_total}"
            )

        prog = users_processed / total_users if total_users > 0 else 0
        extra = ""
        if res_type == "calendars":
          extra = f"Calendars: {phase_cals:,} | Events: {phase_events:,}"

        self.ui_update(
            "scan_progress",
            source=res_type,
            progress=prog,
            cumulative=phase_total,
            processed=users_processed,
            failed=users_failed,
            total=total_users,
            extra_text=extra,
        )
    finally:
      executor.shutdown(wait=True)

    return phase_failures

  def calculate_migration_waves(self, df):
    # Ensure numeric columns
    target_cols = ["Email Count", "Contact Count", "Event Count"]
    for col in target_cols:
      if col not in df.columns:
        df[col] = 0
      else:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # 1. Sort Users (Descending - Heaviest first for optimal packing)
    df["SortMetric"] = df.apply(
        lambda x: max(
            x["Email Count"], (x["Event Count"] + x["Contact Count"])
        ),
        axis=1,
    )
    df_sorted_base = df.sort_values(by="SortMetric", ascending=False).copy()

    user_min_limit = self.eta_min_users.get()
    user_max_limit = self.eta_max_users.get()
    num_parallel = max(1, self.parallel_waves.get())
    max_allowed_waves = self.eta_max_waves.get()

    # Define automated candidate targets in hours (3h, 6h, 12h, 18h, 1d, 1.5d, 2d, 3d, 5d, 7d, 10d, 15d, 20d, 30d, 45d, 60d)
    candidate_hours = [
        3,
        6,
        12,
        18,
        24,
        36,
        48,
        72,
        120,
        168,
        240,
        360,
        480,
        720,
        1080,
        1440,
    ]

    self.log_msg(
        "\n--- 🧠 Auto-Optimizing Target ETA (Constraints: Max"
        f" {max_allowed_waves} Waves, Target Range:"
        f" {user_min_limit}-{user_max_limit} Users Per Wave) ---"
    )

    best_total_eta = float("inf")
    best_plan = None
    fallback_plan = None
    min_waves_seen = float("inf")

    # Helper: Calculate ETA for subset
    def get_batch_eta(subset_df):
      eta_email = 0.0
      if ENABLE_EMAIL_ETA:
        eta_email = calculate_wave_duration(
            subset_df["Email Count"].tolist(),
            ETA_EMAIL_GLOBAL_LIMIT,
            ETA_EMAIL_USER_LIMIT,
            ETA_EMAIL_BATCH_SIZE,
            ETA_EMAIL_BATCH_TIME,
        )
      eta_calendar = 0.0
      if ENABLE_CALENDAR_ETA:
        eta_calendar = calculate_wave_duration(
            subset_df["Event Count"].tolist(),
            ETA_CALENDAR_GLOBAL_LIMIT,
            ETA_CALENDAR_USER_LIMIT,
            ETA_CALENDAR_BATCH_SIZE,
            ETA_CALENDAR_BATCH_TIME,
        )
      eta_contact = 0.0
      if ENABLE_CONTACT_ETA:
        eta_contact = calculate_wave_duration(
            subset_df["Contact Count"].tolist(),
            ETA_CONTACT_GLOBAL_LIMIT,
            ETA_CONTACT_USER_LIMIT,
            ETA_CONTACT_BATCH_SIZE,
            ETA_CONTACT_BATCH_TIME,
        )
      return max(eta_email, eta_calendar + eta_contact)

    # 2. Iterate through candidates
    for target_hours in candidate_hours:
      df_sorted = df_sorted_base.copy()
      df_sorted["Suggested Wave"] = ""

      total_users = len(df_sorted)
      start_idx = 0
      raw_chunks = []

      # Partitioning Loop
      while start_idx < total_users:
        remaining_users = total_users - start_idx
        current_max = min(remaining_users, user_max_limit)
        current_min = min(user_min_limit, remaining_users)

        # Binary Search for Optimal Size based on the current target_hours
        min_subset = df_sorted.iloc[start_idx : start_idx + current_min]
        if get_batch_eta(min_subset) > target_hours:
          chosen_size = current_min
        else:
          max_subset = df_sorted.iloc[start_idx : start_idx + current_max]
          if get_batch_eta(max_subset) <= target_hours:
            chosen_size = current_max
          else:
            low = current_min
            high = current_max
            chosen_size = high
            while low <= high:
              mid = (low + high) // 2
              subset = df_sorted.iloc[start_idx : start_idx + mid]
              eta = get_batch_eta(subset)

              if eta > target_hours:
                chosen_size = mid
                high = mid - 1
              else:
                low = mid + 1

        end_idx = start_idx + chosen_size
        final_subset = df_sorted.iloc[start_idx:end_idx]
        w_eta = get_batch_eta(final_subset)

        # Store the chunk data
        raw_chunks.append({
            "start_idx": start_idx,
            "end_idx": end_idx,
            "users": len(final_subset),
            "total_emails": int(final_subset["Email Count"].sum()),
            "total_contacts": int(final_subset["Contact Count"].sum()),
            "total_events": int(final_subset["Event Count"].sum()),
            "eta": w_eta,
        })
        start_idx = end_idx

      # 3. Schedule Chunks into Buckets
      num_buckets = min(num_parallel, len(raw_chunks))

      if num_buckets == 0:
        total_eta = 0
        buckets = []
        final_waves_list = []
      else:
        buckets = [
            {"id": i + 1, "total": 0.0, "waves": []} for i in range(num_buckets)
        ]
        for chunk in raw_chunks:
          target = min(buckets, key=lambda b: b["total"])
          target["waves"].append(chunk)
          target["total"] += chunk["eta"]

        total_eta = max(b["total"] for b in buckets)

        # Reverse and Name
        all_chunks_with_time = []
        buckets.reverse()

        for b_idx, b in enumerate(buckets):
          waves_list = b["waves"]
          if isinstance(waves_list, list):
            waves_list.reverse()
          current_time = 0.0
          for chunk in waves_list:
            chunk["start_time"] = current_time
            chunk["bucket_idx"] = b_idx
            current_time += chunk["eta"]
            all_chunks_with_time.append(chunk)

        all_chunks_with_time.sort(
            key=lambda x: (x["start_time"], x["bucket_idx"])
        )

        final_waves_list = []
        for i, chunk in enumerate(all_chunks_with_time):
          wave_name = f"Wave {i+1}"
          chunk["name"] = wave_name
          final_waves_list.append(chunk)
          col_idx = df_sorted.columns.get_loc("Suggested Wave")
          df_sorted.iloc[chunk["start_idx"] : chunk["end_idx"], col_idx] = (
              wave_name
          )

      num_waves = len(final_waves_list)
      self.log_msg(
          f"Evaluated Target {target_hours}h: Generated {num_waves} waves |"
          f" Total ETA: {self.format_eta(total_eta)}"
      )

      # 4. Selection Logic
      if num_waves <= max_allowed_waves:
        if total_eta < best_total_eta:
          best_total_eta = total_eta
          best_plan = (df_sorted, final_waves_list, total_eta, buckets)

      # Keep a fallback in case NO plan has <= 50 waves
      if num_waves < min_waves_seen:
        min_waves_seen = num_waves
        fallback_plan = (df_sorted, final_waves_list, total_eta, buckets)

    # 5. Assign Final Best Plan
    if best_plan is not None:
      df_final, final_waves_list, total_eta, buckets = best_plan
    else:
      df_final, final_waves_list, total_eta, buckets = fallback_plan

    self.log_msg(f"\n" + "=" * 60)
    self.log_msg(
        f"🏆 OPTIMAL PLAN SELECTED: {len(final_waves_list)} Waves | TOTAL"
        f" PROJECT ETA: {self.format_eta(total_eta)}"
    )
    self.log_msg("=" * 60 + "\n")

    # Log the final details of the best plan
    for chunk in final_waves_list:
      self.log_msg(
          f"{chunk['name']}: {chunk['users']} Users | ETA:"
          f" {self.format_eta(chunk['eta'])} | Starts @"
          f" {self.format_eta(chunk['start_time'])}"
      )

    return df_final, final_waves_list, total_eta, buckets

  def _get_all_users_graph(self, manager):
    users = []
    url = f"{GRAPH_BASE_URL}/users?$select=id,userPrincipalName&$top=999"
    token_data = manager.get_valid_token_slot(self.log_msg)
    token = token_data["token"]
    session = manager.get_session()
    headers = {"Authorization": f"Bearer {token}"}
    try:
      while url and not self.stop_scan_event.is_set():
        # Check mid-loop for extremely long tenant scans
        if time.time() > token_data["expires_at"]:
          manager.return_token_slot(token_data)
          token_data = manager.get_valid_token_slot(self.log_msg)
          token = token_data["token"]
          headers = {"Authorization": f"Bearer {token}"}

        r = session.get(url, headers=headers)
        if r.status_code != 200:
          break
        d = r.json()
        users.extend(d.get("value", []))
        url = d.get("@odata.nextLink")
        self.ui_update(
            "user_discovery", count=len(users), status="Scanning Tenant..."
        )
    finally:
      manager.return_token_slot(token_data)
    return users

  def _resolve_users_from_csv(self, manager, emails):
    resolved = []

    def resolve_one(email):
      if self.stop_scan_event.is_set():
        return None
      token_data = manager.get_valid_token_slot(self.log_msg)
      t = token_data["token"]
      s = manager.get_session()
      h = {"Authorization": f"Bearer {t}", "ConsistencyLevel": "eventual"}
      try:
        cln = email.replace("'", "''")
        u = (
            f"{GRAPH_BASE_URL}/users?$filter=userPrincipalName eq"
            f" '{cln}'&$select=id,userPrincipalName"
        )
        r = s.get(u, headers=h)
        if r.status_code == 200 and r.json().get("value"):
          return r.json()["value"][0]
      except:
        pass
      finally:
        manager.return_token_slot(token_data)

    with ThreadPoolExecutor(max_workers=min(50, len(emails))) as exc:
      futures = [exc.submit(resolve_one, e) for e in emails]
      for f in as_completed(futures):
        if self.stop_scan_event.is_set():
          break
        res = f.result()
        if res:
          resolved.append(res)
          self.ui_update(
              "user_discovery", count=len(resolved), status="Resolving CSV..."
          )
    return resolved


def main():
  """Application entry point."""
  # Suppress SSL warnings for cleaner console output
  urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

  # Configure High DPI scaling if necessary (Windows)
  try:
    from ctypes import windll

    windll.shcore.SetProcessDpiAwareness(1)
  except Exception:
    pass

  app = MigrationEstimatorTool()
  app.mainloop()


if __name__ == "__main__":
  main()
