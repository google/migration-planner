from dataclasses import dataclass
from typing import Any, Dict, List

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
  parallel_batches: int
  hierarchial_crawl_batch_limit: int = 4

RETRYABLE_ERROR_CODES = [429, 500, 502, 503, 504]

def create_batches(
    api: str, 
    placeholder_list: List[Dict[str, Any]], 
    batch_size: int,
    useIdentificationHeaders: bool = False
) -> List[List[Dict[str, Any]]]:
    batches = []
    batch_requests = []
    req_id = 0

    headers = {
        "ConsistencyLevel": "eventual"
    }

    for placeholder in placeholder_list:
        if (req_id >= batch_size):
            batches.append(batch_requests)
            batch_requests = []
            req_id = 0

        try:
            formatted_api = api.format(**placeholder)
            batch_requests.append({
                "id": req_id,
                "method": "GET",
                "url": formatted_api,
                "headers": headers | (placeholder if useIdentificationHeaders else {}),         # TODO Create better method for mapping to reduce payload size
            })
            req_id += 1
        except:
            raise Exception("Incorrect Payload passed to create batch")
    
    if len(batch_requests) > 0:
        batches.append(batch_requests)
    
    return batches

def group_responses_by_key(
        required_map: Dict[str, List[Dict[str, Any]]], 
        batch_requests: List[Dict[str, Any]], 
        batch_responses: List[Dict[str, Any]], 
        grouping_key: str
    ):

    batch_responses_map: Dict[int, Dict[str, Any]] = {int(response["id"]): response for response in batch_responses}
    id_to_request_mapping: Dict[str, Dict[str, Any]] = {}
    id_to_response_mapping: Dict[str, Dict[str, Any]] = {}

    for request in batch_requests:
        id_to_request_mapping[request["id"]] = request
        id_to_response_mapping[request["id"]] = batch_responses_map[request["id"]]["body"]["value"]
    
    for request_id, response in id_to_response_mapping.items():
        if id_to_request_mapping[request_id]["headers"][grouping_key] not in required_map:
            required_map[id_to_request_mapping[request_id]["headers"][grouping_key]] = []
        required_map[id_to_request_mapping[request_id]["headers"][grouping_key]] += response

def get_success_responses(responses: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [response for response in responses.values() 
            if "body" in response and response["status"] >= 200 and 
            response["status"] < 300]

def get_failed_responses(responses: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [response for response in responses.values() 
            if not (response["status"] >= 200 and 
            response["status"] < 300)]

def get_failed_responses_that_can_be_retried(responses: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [response for response in responses.values() 
            if "body" in response and response["status"] in RETRYABLE_ERROR_CODES]