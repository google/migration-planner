from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple
from util.enums import FailureType, ResourceType

@dataclass
class ScanConfig:
  """Holds configuration for the current scan job."""

  tenant_id: str
  client_ids: List[str]
  client_secrets: List[str]
  user_source: str
  csv_path: str
  scan_email: bool = False
  scan_chat: bool = False
  scan_contact: bool = False
  scan_calendar: bool = False
  scan_in_place_archives: bool = False
  scan_shared_mail_boxes: bool = False
  scan_group_mail_boxes: bool = False
  concurrency: int = 10
  load_multiplier: int = 1
  retries: int = 30
  backoff: int = 2
  eta_max_users: int = 5000
  parallel_batches: int = 10
  hierarchial_crawl_batch_limit: int = 4
  mode: str = "heuristics"
  sample_percentage: int = 10
  bucket_ranges: List[Tuple[int, int]] = field(default_factory=lambda: [(0, 10240), (10241, 102400), (102401, 1048576), (1048577, float("inf"))])
  large_resource_count_limit: int = 500000
  includePersonalSites: bool = True
  includeTeamSites: bool = True
  max_allowed_depth: int = 100

@dataclass
class RequestResponsePair:
  request: Dict[str, Any]
  response: Dict[str, Any]

@dataclass
class Bucket:
    sizeRange: Tuple[int, int]  # (Low, High) in MBs
    count: int

@dataclass
class FileSizeDistribution:
    buckets: List[Bucket]

@dataclass
class LargeResource:
    Type: ResourceType
    Id: str
    subTreeCount: int
    Limit: int

RETRYABLE_ERROR_CODES = [429, 500, 502, 503, 504]


def create_batches(
    api_url: str,
    placeholder_list: List[Dict[str, Any]],
    batch_size: int,
    use_identification_headers: bool = False,
    delta_api_page_size: Optional[int] = None,
) -> List[List[Dict[str, Any]]]:
  batches: List[List[Dict[str, Any]]] = []
  batch_requests: List[Dict[str, Any]] = []
  request_id = 0

  headers = {"ConsistencyLevel": "eventual"}
  if delta_api_page_size is not None:
    headers["Prefer"] = f"odata.maxpagesize={delta_api_page_size}"

  for placeholder in placeholder_list:
    if request_id >= batch_size:
      batches.append(batch_requests)
      batch_requests = []
      request_id = 0

    try:
      formatted_api = api_url.format(**placeholder)
      batch_requests.append({
          "id": request_id,
          "method": "GET",
          "url": formatted_api,
          "headers": (
              headers | (placeholder if use_identification_headers else {})
          ),  # TODO Create better method for mapping to reduce payload size
      })
      request_id += 1
    except:
      raise Exception("Incorrect Payload passed to create batch")

  if len(batch_requests) > 0:
    batches.append(batch_requests)

  return batches


def group_responses_by_key(
    required_map: Dict[str, List[Dict[str, Any]]],
    batch_requests: List[Dict[str, Any]],
    batch_responses: List[Dict[str, Any]],
    grouping_key: str,
):

  batch_responses_map: Dict[int, Dict[str, Any]] = {
      int(response["id"]): response for response in batch_responses
  }
  id_to_request_mapping: Dict[str, Dict[str, Any]] = {}
  id_to_response_mapping: Dict[str, List[Dict[str, Any]]] = {}

  for request in batch_requests:
    id_to_request_mapping[request["id"]] = request
    if request["id"] not in batch_responses_map:
      id_to_response_mapping[request["id"]] = []
    elif "body" not in batch_responses_map[request["id"]]:
      id_to_response_mapping[request["id"]] = []
    elif "value" not in batch_responses_map[request["id"]]["body"]:
      id_to_response_mapping[request["id"]] = []
    else:
      id_to_response_mapping[request["id"]] = batch_responses_map[
          request["id"]
      ]["body"]["value"]

  for request_id, response in id_to_response_mapping.items():
    if (
        id_to_request_mapping[request_id]["headers"][grouping_key]
        not in required_map
    ):
      required_map[
          id_to_request_mapping[request_id]["headers"][grouping_key]
      ] = []
    required_map[
        id_to_request_mapping[request_id]["headers"][grouping_key]
    ] += response


def get_success_responses(responses: Dict[str, Any]) -> List[Dict[str, Any]]:
  return [
      response
      for response in responses.values()
      if "body" in response
      and response["status"] >= 200
      and response["status"] < 300
  ]


def get_failed_responses(responses: Dict[str, Any]) -> List[Dict[str, Any]]:
  return [
      response
      for response in responses.values()
      if not (response["status"] >= 200 and response["status"] < 300)
  ]


def get_failed_responses_that_can_be_retried(
    responses: Dict[str, Any],
) -> List[Dict[str, Any]]:
  return [
      response
      for response in responses.values()
      if "body" in response and response["status"] in RETRYABLE_ERROR_CODES
  ]


def get_relative_url(url: str, base_url: str) -> str:
  if url.startswith(base_url):
    relative_url = url[len(base_url) :]
    if relative_url.startswith("/"):
      relative_url = relative_url[1:]
    return relative_url
  elif url.startswith("https://graph.microsoft.com/beta/"):
    return url[len("https://graph.microsoft.com/beta/") :]
  return url


def process_pagination_responses(
    batch: List[Dict[str, Any]],
    responses: List[Dict[str, Any]],
    orig_map: Dict[str, Any],
    grouping_key: str,
    base_url: str,
    failures: Optional[List[Dict[str, Any]]] = None,
    is_partial: bool = False,
    progress_callback: Optional[Callable[[List], None]] = None
) -> List[Dict[str, Any]]:
    next_items = []
    batch_responses_map = {int(resp["id"]): resp for resp in responses}
    
    for req in batch:
        req_id = req["id"]
        key = req["headers"][grouping_key]

        if req_id in batch_responses_map:
            resp = batch_responses_map[req_id] 
            # Retrieve original response object
            orig_entry = orig_map[key]
            orig_resp = orig_entry["resp"] if isinstance(orig_entry, dict) and "resp" in orig_entry else orig_entry
            
            if "body" in resp and "value" in resp["body"]:
                orig_resp["body"]["value"] += resp["body"]["value"]
                if progress_callback is not None:
                    progress_callback(resp["body"]["value"], "@odata.nextLink" in resp["body"])
                
                if "@odata.nextLink" in resp["body"]:
                    next_url = resp["body"]["@odata.nextLink"]
                    relative_url = get_relative_url(next_url, base_url)
                    
                    # Create next item with all original headers preserved
                    next_item = dict(req["headers"])
                    next_item["url"] = relative_url
                    next_items.append(next_item)
            elif "body" in resp and "error" in resp["body"] and failures is not None:
                failures.append({
                    "mailboxId": key,
                    "isPartial": is_partial,
                    "type": FailureType.FAILURE_STATUS_CODE_ERROR,
                    "statusCode" : resp["status"],
                    "message": resp["body"]["error"]["message"]
                })
        else:
          failures.append({
            "mailboxId": key,
            "isPartial": is_partial,
            "type": FailureType.NOT_FOUND,
            "statusCode": None,
            "message": "No response found for API Call."
          })
                    
    return next_items

def create_request_to_response_map(
    batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]],
    batch_id_to_responses_map: Dict[int, List[Dict[str, Any]]],
    failures: Optional[List[Dict[str, str]]] = None,
) -> List[RequestResponsePair]:
  request_to_response_map_list: List[RequestResponsePair] = []

  for batch_id, batch in batch_id_to_batch_map.items():
    batch_response = batch_id_to_responses_map[batch_id]
    try:
      temp_request_id_to_response_map = {
          int(response["id"]): response for response in batch_response
      }
    except Exception as error:
      if failures is not None:
        failures.append({
            "type": FailureType.INVALID_DATA,
            "statusCode": 200,
            "message": (
                f"Invalid data - Unable to convert id to integer: {error}"
            ),
        })
        continue

    for request in batch:
      if request["id"] not in temp_request_id_to_response_map:
        if failures is not None:
          failures.append({
              "userId": (
                  request["headers"]["userId"]
                  if "userId" in request["headers"]
                  else None
              ),
              "isPartial": False,
              "type": FailureType.NOT_FOUND,
              "message": "No response received for the request",
          })
        continue  # TODO Check why and add logs for possible failure
      if (
          temp_request_id_to_response_map[request["id"]]["status"] < 200
          or temp_request_id_to_response_map[request["id"]]["status"] >= 300
      ):
        if failures is not None:
          error_message = (
              temp_request_id_to_response_map[request["id"]]["body"]["error"][
                  "message"
              ]
              if "body" in temp_request_id_to_response_map[request["id"]]
              and "error"
              in temp_request_id_to_response_map[request["id"]]["body"]
              else "Unknown Error"
          )
          failures.append({
              "userId": (
                  request["headers"]["userId"]
                  if "userId" in request["headers"]
                  else None
              ),
              "isPartial": False,
              "type": FailureType.FAILURE_STATUS_CODE_ERROR,
              "statusCode": temp_request_id_to_response_map[request["id"]][
                  "status"
              ],
              "message": (
                  "The request failed with status code"
                  f" {temp_request_id_to_response_map[request['id']]['status']}"
                  f" and error message: {error_message}"
              ),
          })
        continue
      request_to_response_map_list.append(
          RequestResponsePair(
              request=request,
              response=temp_request_id_to_response_map[request["id"]],
          )
      )

  return request_to_response_map_list


def get_batch_responses_map(
    responses: List[Dict[str, Any]],
    logger: Optional[Callable[[str], None]] = None,
):
  batch_responses_map = {}
  for response in responses:
    if "id" in response:
      try:
        batch_responses_map[int(response["id"])] = response
      except ValueError:
        if logger:
          logger(
              "Warning: Received response with non-numeric ID: "
              f"{response['id']}"
          )
    else:
      if logger:
        logger("Warning: Received response missing 'id' field")
  return batch_responses_map
