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

"""Resilient batch networking engine supporting dynamic backoffs and throttles."""

import asyncio
import logging
import os
import random
import threading
import time
from typing import Any
import typing

import aiohttp
import requests
from util.auth_manager import TokenManager
from util.rate_limiter import HybridLimiter
from util.state_registry import AuditLogger
from util.state_registry import MetricsRegistry


logger = logging.getLogger(__name__)


class GraphBatchClient:
  """Central communication engine handling batch negotiation and resilient retries."""

  def __init__(self, log_func: typing.Callable[[str], None] | None = None) -> None:
    self.log_func = log_func

  def _log(self, message: str) -> None:
    if self.log_func:
      self.log_func(message)

  def execute_batch_request(
      self,
      session: requests.Session,
      batch_url: str,
      token_manager: TokenManager,
      token_data: dict[str, Any],
      requests_payload: list[dict[str, Any]],
      metrics: MetricsRegistry,
      limiter: HybridLimiter,
      stop_event: threading.Event | None = None,
      context: str = "",
      use_msft_backoff: bool = False,
      default_backoff: int = 2,
  ) -> dict[str, Any]:
    """Executes multi-payload batch against MS Graph enforcing dynamic throttle windows."""
    successful_responses = {}
    pending_requests = requests_payload
    max_retries = token_manager.retries
    current_try = 0
    last_exception: Exception | None = None
    timeout_val = float(os.environ.get("BATCH_CLIENT_TIMEOUT", 180.0))

    def _sleep(duration):
      if stop_event:
        return stop_event.wait(timeout=duration)
      time.sleep(duration)
      return False

    while pending_requests and current_try < max_retries:
      if stop_event and stop_event.is_set():
        break
      current_try += 1

      # Consistency Requirement: Apply injected throttles explicitly for batch
      # flows.
      limiter.wait()

      payload = {"requests": pending_requests}
      headers = {
          "Authorization": f"Bearer {token_data['token']}",
          "Content-Type": "application/json",
      }

      try:
        start_time = time.monotonic()
        # Hardened network timeout guard.
        response = session.post(
            batch_url, headers=headers, json=payload, timeout=timeout_val
        )
        latency = time.monotonic() - start_time

        # Consistency Requirement: Feed batch outer statuses into metrics
        # aggregator.
        metrics.record_api_call(latency, response.status_code)

        if response.status_code == 200:
          try:
            batch_responses = response.json().get("responses", [])
          except ValueError:
            self._log("Invalid JSON retrieved from batch. Awaiting retry.")
            if _sleep(default_backoff):
              break
            continue

          current_batch_map = {
              request["id"]: request for request in pending_requests
          }
          next_retry_requests = []
          retry_after_delay = 0
          needs_refresh = False

          for response_item in batch_responses:
            request_id = response_item.get("id")
            status = response_item.get("status")
            if status == 429:
              headers_429 = response_item.get("headers", {})
              try:
                wait_sec = int(float(headers_429.get("Retry-After", 0)))
              except (ValueError, TypeError):
                wait_sec = 2
              retry_after_delay = max(retry_after_delay, wait_sec)
              if request_id in current_batch_map:
                next_retry_requests.append(current_batch_map[request_id])
            elif status == 401:
              needs_refresh = True
              if request_id in current_batch_map:
                next_retry_requests.append(current_batch_map[request_id])
                retry_after_delay = max(retry_after_delay, 2)
            elif status in [500, 502, 503, 504]:
              if request_id in current_batch_map:
                next_retry_requests.append(current_batch_map[request_id])
                retry_after_delay = max(retry_after_delay, 2)
            else:
              successful_responses[request_id] = response_item

          if next_retry_requests:
            if stop_event and stop_event.is_set():
              break

            if needs_refresh:
              self._log(
                  f"Batch individual items failed auth in {context}. Performing inline refresh."
              )
              token_manager.refresh_token_data(token_data)

            if retry_after_delay > 0 and use_msft_backoff:
              sleep_time = float(retry_after_delay) + random.uniform(0, 1.0)
            else:
              sleep_time = default_backoff ** (current_try - 1)

            sleep_time = min(sleep_time, 300.0)
            self._log(
                f"Batch partial fail ({len(next_retry_requests)} items). Throttle sleep: {sleep_time:.1f}s"
            )
            if stop_event and stop_event.is_set():
              break

            if _sleep(sleep_time):
              break
            pending_requests = next_retry_requests
          else:
            pending_requests = []
        elif response.status_code == 429:
          if stop_event and stop_event.is_set():
            break
          try:
            raw_retry_after_seconds = int(
                float(response.headers.get("Retry-After", 5))
            )
          except (ValueError, TypeError):
            raw_retry_after_seconds = 5 * current_try

          wait = min(float(raw_retry_after_seconds), 300.0) + random.uniform(
              0, 1.0
          )
          self._log(f"Outer Batch 429 Throttled. Pausing for {wait:.1f}s")
          if _sleep(wait):
            break
        elif response.status_code == 401:
          if stop_event and stop_event.is_set():
            break
          self._log("Outer Batch 401 Unauthorized. Attempting refresh...")
          token_manager.refresh_token_data(token_data)
          if _sleep(default_backoff):
            break
        elif response.status_code in [500, 502, 503, 504]:
          if stop_event and stop_event.is_set():
            break
          wait = min(float(default_backoff ** (current_try - 1)), 30.0)
          self._log(
              f"Outer Batch Server Error {response.status_code}. Waiting {wait:.1f}s"
          )
          if _sleep(wait):
            break
        else:
          self._log(
              f"Batch terminal unrecoverable failure {response.status_code}"
          )
          raise ConnectionError(
              f"Terminal outer batch failure code {response.status_code}"
          )
      except Exception as error:
        # Catch specific connection errors without swallowing.
        last_exception = error
        self._log(
            f"Network exception trapped during outer batch ({current_try}/{max_retries}): {error}"
        )
        if current_try < max_retries:
          if stop_event and stop_event.is_set():
            break
          sleep_duration = min(float(default_backoff**current_try), 30.0)
          if _sleep(sleep_duration):
            break
          continue
        break

    if pending_requests:
      message = (
          f"CRITICAL: Max retries ({max_retries}) reached for batch in"
          f" {context} context. Failed to deliver {len(pending_requests)}"
          " sub-items."
      )
      self._log(message)
      if last_exception:
        raise RuntimeError(message) from last_exception
      raise RuntimeError(message)

    return successful_responses

  def get_with_retry(
      self,
      session: requests.Session,
      url: str,
      headers: dict[str, str],
      token_manager: TokenManager,
      token_data: dict[str, Any],
      metrics: MetricsRegistry,
      limiter: HybridLimiter,
      audit_logger: AuditLogger | None = None,
      max_retries: int = 3,
      backoff: int = 2,
  ) -> requests.Response:
    """Executes synchronous HTTP GET, mitigating rate exhaustion natively."""
    retries = 0
    last_exception = None
    while retries < max_retries:
      limiter.wait()
      retries += 1
      try:
        start_call = time.monotonic()
        response = session.get(url, headers=headers, timeout=30.0)
        latency = time.monotonic() - start_call
        metrics.record_api_call(latency, response.status_code)

        payload_count = None
        if response.status_code == 200:
          try:
            data = response.json()
            payload_count = len(
                data.get("value", []) or data.get("responses", [])
            )
          except ValueError:
            payload_count = 1

        if audit_logger:
          audit_logger.log_event({
              "type": "api_call",
              "url": url,
              "status_code": response.status_code,
              "latency_sec": round(latency, 3),
              "retry_attempt": retries - 1,
              "returned_elements": payload_count,
          })

        if response.status_code == 200:
          return response
        elif response.status_code == 401:
          self._log("401 Unauthenticated captured in GET operation.")
          # Safe consumption: don't decrease retry count.
          if token_manager.refresh_token_data(token_data):
            headers["Authorization"] = f"Bearer {token_data['token']}"
            time.sleep(1.0)
        elif response.status_code == 429:
          try:
            retry_after = int(float(response.headers.get("Retry-After", 0)))
          except (ValueError, TypeError):
            retry_after = backoff * retries
          retry_val = retry_after or (backoff * retries)
          self._log(f"429 Throttle active. Sleeping {retry_val:.1f}s")
          time.sleep(retry_val)
        elif response.status_code in [500, 502, 503, 504]:
          self._log(f"Server Error {response.status_code} captured on {url}. Retrying...")
          time.sleep(backoff * retries)
        elif response.status_code in [403, 404]:
          self._log(f"WARNING: API call to {url} failed with status {response.status_code}: {response.text[:200]}")
          return response
        else:
          self._log(f"WARNING: API call to {url} returned unexpected status {response.status_code}: {response.text[:200]}")
          time.sleep(backoff * retries)
      except Exception as error:
        last_exception = error
        self._log(f"GET operation exception encountered on {url}: {error}")
        time.sleep(backoff * retries)

    if last_exception:
      raise ConnectionError(
          f"Exhausted {max_retries} attempts on {url}"
      ) from last_exception
    raise ConnectionError(f"Exhausted {max_retries} attempts on {url}")

  async def get_with_retry_async(
      self,
      session: aiohttp.ClientSession,
      url: str,
      headers: dict[str, str],
      token_manager: TokenManager,
      token_data: dict[str, Any],
      metrics: MetricsRegistry,
      limiter: HybridLimiter,
      max_retries: int = 3,
      backoff: int = 2,
  ) -> tuple[dict[str, Any] | None, int]:
    """Executes non-blocking async GET utilizing native ClientSession."""
    retries = 0
    last_exception = None
    while retries < max_retries:
      async with limiter:
        retries += 1
        try:
          start_call = time.monotonic()
          async with session.get(
              url, headers=headers, timeout=30.0
          ) as response:
            status = response.status
            latency = time.monotonic() - start_call
            metrics.record_api_call(latency, status)

            if status == 200:
              try:
                payload = await response.json()
              except Exception:
                text = await response.text()
                payload = {"text": text}
              return payload, status

            response_headers = response.headers.copy()

          if status == 401:
            self._log("Async 401 detected. Initiating secure off-thread refresh.")
            refreshed = await asyncio.to_thread(
                token_manager.refresh_token_data, token_data
            )
            if refreshed:
              headers["Authorization"] = f"Bearer {token_data['token']}"
              await asyncio.sleep(1.0)
          elif status == 429:
            try:
              retry_after = int(float(response_headers.get("Retry-After", 5)))
            except (ValueError, TypeError):
              retry_after = 5
            self._log(f"Async 429 detected. Suspending {retry_after:.1f}s")
            await asyncio.sleep(retry_after)
          elif status in [500, 502, 503, 504]:
            self._log(f"Async Server Error {status} captured on {url}. Retrying...")
            await asyncio.sleep(backoff * retries)
          elif status in [403, 404]:
            self._log(f"WARNING: Async API call to {url} failed with status {status}")
            return None, status
          else:
            self._log(f"WARNING: Async API call to {url} returned unexpected status {status}")
            await asyncio.sleep(backoff * retries)
        except Exception as error:
          last_exception = error
          self._log(f"Async exception trapped on {url}: {error}")
          await asyncio.sleep(backoff * retries)

    if last_exception:
      raise ConnectionError(
          f"Async exhaust failure ({max_retries} attempts) on {url}"
      ) from last_exception
    raise ConnectionError(
        f"Async exhaust failure ({max_retries} attempts) on {url}"
    )
