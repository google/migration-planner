from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from util.enums import FailureType

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
    scan_in_place_archives: bool
    scan_group_mail_boxes: bool
    concurrency: int
    load_multiplier: int
    retries: int
    backoff: int
    eta_max_users: int
    parallel_batches: int
    hierarchial_crawl_batch_limit: int = 4

@dataclass
class RequestResponsePair:
    request: Dict[str, Any]
    response: Dict[str, Any]


RETRYABLE_ERROR_CODES = [429, 500, 502, 503, 504]

def create_batches(
    api: str, 
    placeholder_list: List[Dict[str, Any]], 
    batch_size: int,
    useIdentificationHeaders: bool = False
) -> List[List[Dict[str, Any]]]:
    batches: List[List[Dict[str, Any]]] = []
    batch_requests: List[Dict[str, Any]] = []
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

def get_relative_url(url: str, base_url: str) -> str:
    if url.startswith(base_url):
        rel = url[len(base_url):]
        if rel.startswith("/"):
            rel = rel[1:]
        return rel
    elif url.startswith("https://graph.microsoft.com/beta/"):
        return url[len("https://graph.microsoft.com/beta/"):]
    return url

def process_pagination_responses(
    batch: List[Dict[str, Any]],
    responses: List[Dict[str, Any]],
    orig_map: Dict[str, Any],
    grouping_key: str,
    base_url: str,
    failures: Optional[List[Dict[str, Any]]] = None,
    is_partial: bool = False
) -> List[Dict[str, Any]]:
    next_items = []
    batch_responses_map = {int(resp["id"]): resp for resp in responses}
    
    for req in batch:
        req_id = req["id"]
        if req_id in batch_responses_map:
            resp = batch_responses_map[req_id]
            key = req["headers"][grouping_key]
            
            # Retrieve original response object
            orig_entry = orig_map[key]
            orig_resp = orig_entry["resp"] if isinstance(orig_entry, dict) and "resp" in orig_entry else orig_entry
            
            if "body" in resp and "value" in resp["body"]:
                orig_resp["body"]["value"] += resp["body"]["value"]
                
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
                    
    return next_items

def create_request_to_response_map(
        batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]], 
        batch_id_to_responses_map: Dict[int, List[Dict[str, Any]]], 
        failures: Optional[List[Dict[str, str]]] = None
    ) -> List[RequestResponsePair]:
    request_to_response_map_list: List[RequestResponsePair] = []

    for batch_id, batch in batch_id_to_batch_map.items():
        batch_response = batch_id_to_responses_map[batch_id]
        temp_request_id_to_response_map = {int(response["id"]): response for response in batch_response}

        for request in batch:
            if request["id"] not in temp_request_id_to_response_map:
                if failures is not None:
                    failures.append({
                        "userId": request["headers"]["userId"] if "userId" in request["headers"] else None,
                        "isPartial": False,
                        "type": FailureType.NOT_FOUND,
                        "message": "No response received for the request"
                    })
                continue        # TODO Check why and add logs for possible failure
            if temp_request_id_to_response_map[request["id"]]["status"] < 200 or temp_request_id_to_response_map[request["id"]]["status"] >= 300:
                if failures is not None:
                    error_message = (
                        temp_request_id_to_response_map[request["id"]]["body"][
                            "error"
                        ]["message"]
                        if "body" in temp_request_id_to_response_map[request["id"]]
                        and "error"
                        in temp_request_id_to_response_map[request["id"]][
                            "body"
                        ]
                        else "Unknown Error"
                    )                    
                    failures.append({
                        "userId": request["headers"]["userId"] if "userId" in request["headers"] else None,
                        "isPartial": False,
                        "type": FailureType.FAILURE_STATUS_CODE_ERROR,
                        "statusCode" : temp_request_id_to_response_map[request["id"]]["status"],
                        "message": f"The request failed with status code {temp_request_id_to_response_map[request["id"]]["status"]} and error message: {error_message}"
                    })
                continue
            request_to_response_map_list.append(RequestResponsePair(request=request, response=temp_request_id_to_response_map[request["id"]]))

    return request_to_response_map_list
