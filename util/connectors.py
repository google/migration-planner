import random
import threading

from util.auth_manager import TokenManager
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from typing import Any, Callable, Dict, List, Optional
from util.utils import get_success_responses, get_failed_responses_that_can_be_retried
import requests
import queue
import time
import base64
import json

TOKEN_URL_TEMPLATE = "https://login.microsoftonline.com/{0}/oauth2/v2.0/token"
GRAPH_BETA_URL = "https://graph.microsoft.com/beta"
GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
MAX_RETRIES = 30
BACKOFF = 2
SHOW_LOAD_MULTIPLIER = False
USE_MSFT_BACKOFF = True

class UrlInvoker():
    def __init__(self, token_manager: TokenManager, batch_retry_count: int, batch_backoff: int, initial_delay: int, jitter: float):
        self.token_manager = token_manager
        self.batch_retry_count = batch_retry_count
        self.batch_backoff = batch_backoff
        self.initial_delay = initial_delay
        self.jitter = jitter
        self.throttle_until = 0.0
        self.lock = threading.Lock()

    # Warning: Doesn't have support for paginated responses. They are handled in the callers themselves
    def invoke(
        self, 
        url: str, 
        batch: List[Dict[str, Any]], 
        logger: Callable[[str], None], 
        stop_event: Optional[threading.Event] = None,
        context: str = ""
    ) -> List[Dict[str, Any]]:

        try:
            # Failsafe
            if logger is None:
                logger = lambda x: None

            token_data = self.token_manager.get_valid_token_slot(logger)
            session = self.token_manager.get_session()

            batch_url = f"{url}/$batch"
            
            final_responses = []
            failed_responses = []

            curr_batch = batch.copy()

            retry_count = 0
            while retry_count < self.batch_retry_count:
                responses = self.execute_batch_request(
                        session,
                        batch_url,
                        self.token_manager,
                        token_data,
                        curr_batch,
                        logger,
                        stop_event,
                        context
                    )
                
                final_responses += get_success_responses(responses)
                failed_responses = get_failed_responses_that_can_be_retried(responses)
                failed_response_ids = [response["id"] for response in failed_responses]

                curr_batch = [request for request in curr_batch if str(request["id"]) in failed_response_ids]

                if len(failed_responses) > 0:
                    wait_time = self.initial_delay * pow(self.batch_backoff, retry_count) + random.uniform(0, self.jitter)
                    retry_count += 1
                    time.sleep(wait_time)
                else:
                    break

            if len(failed_responses) > 0:
                logger(f"Consistent failures observed for the following: {",".join(response.get("body") for response in failed_responses)}")

        except Exception as e:
            logger(f"Error in {context}: {e}")
        finally:
            self.token_manager.return_token_slot(token_data)

        return final_responses + failed_responses

    """
        @returns Map of the request ID in batch to the relevant response.
    """
    def execute_batch_request(
        self,
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

            with self.lock:
                now = time.time()
                if now < self.throttle_until:
                    sleep_time = self.throttle_until - now
                    logger(f"Global throttling active. Sleeping for {sleep_time:.1f}s before sending request...")
                    time.sleep(sleep_time)
            
            # Add jitter to stagger requests after lock is released
            # Sleeps randomly between 0.1 and 2.0 seconds
            time.sleep(random.uniform(0.1, 2.0)) 

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
                        
                        if "body" not in response_item or not response_item["body"]:
                            logger(f"WARNING: Response item {req_id} in {context} has missing or empty body! Status: {status}")
                        if status == 429:
                            logger(f"Received Throttling error for {batch_url} | Response: {response_item}")
                            headers_429 = response_item.get("headers", {})
                            try:
                                wait_sec = int(float(headers_429.get("Retry-After", 0)))
                            except (ValueError, TypeError):
                                wait_sec = 2
                            
                            retry_after_delay = max(retry_after_delay, wait_sec)
                            self.throttle_until = max(self.throttle_until, time.time() + wait_sec)
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
                        elif "error" in response_item["body"]:
                            # Failsafe for uncaught errors.
                            logger(f"Error encountered in batch response in {context}: {response_item['body']['error']}")
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
                            token_manager.refresh_token_data(token_data)

                        # If we have a specific Retry-After delay (retry_after_delay > 0), use it directly.
                        # Otherwise, use exponential backoff.
                        if retry_after_delay > 0 and USE_MSFT_BACKOFF:
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

                        if stop_event:
                            if stop_event.wait(timeout=sleep_time):
                                break
                        else:
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
                    if stop_event:
                        if stop_event.wait(timeout=wait):
                            break
                    else:
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
                    if token_manager.refresh_token_data(token_data):
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
                    if stop_event:
                        if stop_event.wait(timeout=min(2 * current_try, 30)):
                            break
                    else:
                        time.sleep(min(2 * current_try, 30))
                    continue
                else:
                    logger(f"Max retries reached for batch in {context}. Data lost.")
                    break

        if pending_requests and (stop_event is None or not stop_event.is_set()):
            logger(
                f"WARNING: Max retries ({max_retries}) reached in {context}."
                f" {len(pending_requests)} items dropped permanently."
            )
        elif pending_requests and stop_event is not None and stop_event.is_set():
            logger(
                f"WARNING: Not retrying requests in {context} due to stop event"
            )

        return successful_responses
