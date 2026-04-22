import random
import threading

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
USE_MSFT_BACKOFF = False

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

    print(f"Token Queue Size: {self.token_queue.qsize()}")
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

class UrlInvoker():
    def __init__(self, token_manager: TokenManager, batch_retry_count: int, batch_backoff: int, initial_delay: int, jitter: float):
        self.token_manager = token_manager
        self.batch_retry_count = batch_retry_count
        self.batch_backoff = batch_backoff
        self.initial_delay = initial_delay
        self.jitter = jitter

    # Warning: Doesn't have support for paginated responses. They are handled in the callers themselvesß
    def invoke(
        self, 
        url: str, 
        batch: List[Dict[str, Any]], 
        logger: Callable[[str], None], 
        stop_event: Optional[threading.Event] = None,
        context: str = ""
    ) -> List[Dict[str, Any]]:

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
            print(f"Retry Count: {retry_count} for url {curr_batch[0]['url']}")
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
