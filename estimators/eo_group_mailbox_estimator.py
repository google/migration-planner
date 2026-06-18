from concurrent.futures import Future, ThreadPoolExecutor
import threading
from typing import Any, Callable, Dict, List, Optional

from estimators.estimator import Estimator
from util.auth_manager import TokenManager
from util.connectors import UrlInvoker
from util.utils import ScanConfig, create_batches, create_request_to_response_map, get_batch_responses_map, get_relative_url, process_pagination_responses, group_responses_by_key
from util.enums import FailureType

import json

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"

class EOGroupMailBoxEstimator(Estimator):
    def __init__(self,
        config: ScanConfig, 
        url_invoker: UrlInvoker, 
        logger: Optional[Callable[[str], None]] = None, 
        stop_event: Optional[threading.Event] = None
    ):
        self.config = config
        self.url_invoker = url_invoker
        self.logger = logger
        self.stop_event = stop_event
        self.archive_executor = ThreadPoolExecutor(max_workers=self.config.concurrency)

    def is_hard_stop_requested(self):
        if self.stop_event is None:
            return False
        
        return self.stop_event.is_set()
    
    

    def get_group_id_to_mail_mapping(self):
        group_ids, group_mails = self._get_group_ids_for_tenant()
        return [{"id": group_id, "mail": mail_id} for group_id, mail_id in zip(group_ids, group_mails)]

    # @param data --> Dictionary of param name to its value.
    # @param failures --> List of failures (it will be updated in place)
    # @returns Dictionary of group id to count of group posts.
    def calculate_resource_count(self, data: Dict[str, Any], failures: List[Dict[str, str]]) -> Dict[str, int]:
        group_id_to_thread_count = {}
        try:
            group_ids: List[str] = None if "group_ids" not in data else data["group_ids"]
            if not group_ids or None in group_ids:
                group_ids, group_mails = self._get_group_ids_for_tenant()

            group_id_to_thread_ids = self._get_thread_ids_for_groups(group_ids, failures)    # test using the first group

            thread_id_to_post_count = self._get_post_count_for_threads(group_id_to_thread_ids, failures)
            group_id_to_thread_count = self._consolidate_thread_post_counts_for_each_group(group_id_to_thread_ids, thread_id_to_post_count)
            group_id_to_thread_ids_count = {group_id: len(threads) for group_id, threads in group_id_to_thread_ids.items()}

        except Exception as e:
            if self.logger:
                self.logger(f"Error calculating resource count: {e}")
            failures.append({
                "groupId": "ALL",
                "error": str(e),
                "failureType": FailureType.UNKNOWN.value,
            })
            return group_id_to_thread_count, {}
            
        return group_id_to_thread_count, group_id_to_thread_ids_count

    def calculate_migration_eta(self, data: Dict[str, Any]) -> float:
        return super().calculate_migration_eta(data)
    
    def get_resource_type(self):
        return "GROUP_MAILBOX"
    
    def get_migration_type(self):
        return "EXCHANGE_ONLINE"

    def _get_group_ids_for_tenant(self) -> List:    
        group_ids: List[str] = []
        graph_user_endpoint = "/groups?$filter=not(resourceProvisioningOptions/any(x:x eq 'Team'))&$select=id,mail&$top=999"
        
        # We use a dummy placeholder to create a batch request.
        # This allows us to reuse the batching and pagination utilities.
        placeholders = [{"tenant_query": "all_groups"}]
        user_batches = create_batches(graph_user_endpoint, placeholders, self.config.parallel_batches, True)
        
        futures_map: Dict[int, Future[List[Dict[str, Any]]]] = {}
        batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]] = {}
        idx = 0
        for batch in user_batches:
            futures_map[idx] = self.archive_executor.submit(self.url_invoker.invoke, GRAPH_BASE_URL, batch, self.logger, self.stop_event, self.get_resource_type())
            batch_id_to_batch_map[idx] = batch
            idx += 1

        response_map: Dict[int, List[Dict[str, Any]]] = {}
        for batch_id, future in futures_map.items():
            response_map[batch_id] = future.result()

        query_to_resp_map: Dict[str, Dict[str, Any]] = {}
        pending_next_items = []
        
        for batch_id, responses in response_map.items():
            batch = batch_id_to_batch_map[batch_id]
            batch_responses_map = get_batch_responses_map(responses, self.logger)
            for req in batch:
                req_id = req["id"]
                if req_id in batch_responses_map:
                    resp = batch_responses_map[req_id]
                    query_key = req["headers"]["tenant_query"]
                    query_to_resp_map[query_key] = resp
                    
                    if "body" in resp and "@odata.nextLink" in resp["body"]:
                        next_url = resp["body"]["@odata.nextLink"]
                        relative_url = get_relative_url(next_url, GRAPH_BASE_URL)
                        pending_next_items.append({
                            "tenant_query": query_key,
                            "url": relative_url
                        })
                    elif "body" in resp and "error" in resp["body"]:
                        if self.logger:
                            self.logger(f"Error fetching groups: {resp['body']['error']['message']}")
                else:
                     if self.logger:
                            self.logger("[WARNING] No response found for group API.")

        while pending_next_items and not self.is_hard_stop_requested():
            batches = create_batches("{url}", pending_next_items, self.config.parallel_batches, True)
            
            next_futures_map: Dict[int, Future[List[Dict[str, Any]]]] = {}
            next_batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]] = {}
            idx = 0
            for batch in batches:
                next_futures_map[idx] = self.archive_executor.submit(self.url_invoker.invoke, GRAPH_BASE_URL, batch, self.logger, self.stop_event, self.get_resource_type())
                next_batch_id_to_batch_map[idx] = batch
                idx += 1
                
            next_response_map: Dict[int, List[Dict[str, Any]]] = {}
            for batch_id, future in next_futures_map.items():
                next_response_map[batch_id] = future.result()
                
            new_pending_next_items = []
            
            for batch_id, responses in next_response_map.items():
                batch = next_batch_id_to_batch_map[batch_id]
                new_pending_next_items.extend(process_pagination_responses(batch, responses, query_to_resp_map, "tenant_query", GRAPH_BASE_URL, None, False))
                
            pending_next_items = new_pending_next_items

        # Now extract IDs and Mails
        group_mails: List[str] = []
        for query_key, resp in query_to_resp_map.items():
            if "body" in resp and "value" in resp["body"]:
                for item in resp["body"]["value"]:
                    if "id" in item:
                        group_ids.append(item["id"])
                        group_mails.append(item.get("mail", ""))
                        
        return group_ids, group_mails

    def _get_thread_ids_for_groups(self, group_ids: List[str], failures: List[Dict[str, str]]) -> Dict[str, List[str]]:
        # print("Starting Thread Count")
        group_to_thread_ids: Dict[str, List[str]] = {group_id: [] for group_id in group_ids}
        
        if not group_ids:
            return group_to_thread_ids
            
        thread_api = "groups/{group_id}/threads?$select=id&$top=10"
        
        group_id_maps = [{"group_id": group_id} for group_id in group_ids]
        group_batches = create_batches(thread_api, group_id_maps, self.config.parallel_batches, True)
        
        futures_map: Dict[int, Future[List[Dict[str, Any]]]] = {}
        batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]] = {}
        idx = 0
        for batch in group_batches:
            futures_map[idx] = self.archive_executor.submit(self.url_invoker.invoke, GRAPH_BASE_URL, batch, self.logger, self.stop_event, self.get_resource_type())
            batch_id_to_batch_map[idx] = batch
            idx += 1

        response_map: Dict[int, List[Dict[str, Any]]] = {}
        for batch_id, future in futures_map.items():
            response_map[batch_id] = future.result()

        group_to_resp_map: Dict[str, Dict[str, Any]] = {}
        pending_next_items = []
        
        for batch_id, responses in response_map.items():
            batch = batch_id_to_batch_map[batch_id]
            batch_responses_map = get_batch_responses_map(responses, self.logger)
            for req in batch:
                req_id = req["id"]
                if req_id in batch_responses_map:
                    resp = batch_responses_map[req_id]
                    group_id = req["headers"]["group_id"]
                    group_to_resp_map[group_id] = resp
                    
                    if "body" in resp and "@odata.nextLink" in resp["body"]:
                        next_url = resp["body"]["@odata.nextLink"]
                        relative_url = get_relative_url(next_url, GRAPH_BASE_URL)
                        pending_next_items.append({
                            "group_id": group_id,
                            "url": relative_url
                        })
                    elif "body" in resp and "error" in resp["body"]:
                        failures.append({
                            "groupId": group_id,
                            "isPartial": False,
                            "type": FailureType.FAILURE_STATUS_CODE_ERROR,
                            "statusCode": resp["status"],
                            "message": resp["body"]["error"]["message"]
                        })
                else:
                    failures.append({
                        "groupId": req["headers"]["group_id"],
                        "isPartial": False,
                        "type": FailureType.NOT_FOUND,
                        "statusCode": None,
                        "message": "No response from thread API."
                    })

        while pending_next_items and not self.is_hard_stop_requested():
            batches = create_batches("{url}", pending_next_items, self.config.parallel_batches, True)
            
            next_futures_map: Dict[int, Future[List[Dict[str, Any]]]] = {}
            next_batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]] = {}
            idx = 0
            for batch in batches:
                next_futures_map[idx] = self.archive_executor.submit(self.url_invoker.invoke, GRAPH_BASE_URL, batch, self.logger, self.stop_event, self.get_resource_type())
                next_batch_id_to_batch_map[idx] = batch
                idx += 1
                
            next_response_map: Dict[int, List[Dict[str, Any]]] = {}
            for batch_id, future in next_futures_map.items():
                next_response_map[batch_id] = future.result()
                
            new_pending_next_items = []
            
            for batch_id, responses in next_response_map.items():
                batch = next_batch_id_to_batch_map[batch_id]
                new_pending_next_items.extend(process_pagination_responses(batch, responses, group_to_resp_map, "group_id", GRAPH_BASE_URL, None, False))
                
            pending_next_items = new_pending_next_items

        # Now group responses by key
        threads_by_group: Dict[str, List[Dict[str, Any]]] = {}
        for batch_id, responses in response_map.items():
            batch = batch_id_to_batch_map[batch_id]
            group_responses_by_key(threads_by_group, batch, responses, "group_id")

        # Extract thread IDs
        for group_id, threads in threads_by_group.items():
            for thread in threads:
                if "id" in thread:
                    group_to_thread_ids[group_id].append(thread["id"])
                    
        return group_to_thread_ids

    def _get_post_count_for_threads(self, group_id_to_thread_ids: Dict[str, List[str]], failures: List[Dict[str, str]]) -> Dict[str, int]:
        # print("Starting Post Count")
        thread_to_count: Dict[str, int] = {}
        
        thread_to_group_map = {tid: gid for gid, tids in group_id_to_thread_ids.items() for tid in tids}
        
        # Flatten and interleave to avoid adjacent requests for same group
        flattened_items = []
        
        max_threads = max(len(tids) for tids in group_id_to_thread_ids.values()) if group_id_to_thread_ids else 0
        
        for i in range(max_threads):
            for group_id, thread_ids in group_id_to_thread_ids.items():
                if i < len(thread_ids):
                    flattened_items.append({
                        "group_id": group_id,
                        "thread_id": thread_ids[i],
                        "userId": thread_ids[i]  # Used for failure tracking in create_request_to_response_map
                    })
                
        if not flattened_items:
            return thread_to_count
            
        post_api = "groups/{group_id}/threads/{thread_id}/posts?$count=true&$select=id&$top=1"
        
        # Optimize Batch Size
        optimal_batch_size = min(self.config.parallel_batches, min(20, len(group_id_to_thread_ids)))
        batches = create_batches(post_api, flattened_items, optimal_batch_size, True)
        
        # Refine batches to ensure no duplicate groups in any batch
        refined_batches = []
        for batch in batches:
            group_counts = {}
            for req in batch:
                gid = req["headers"]["group_id"]
                group_counts[gid] = group_counts.get(gid, 0) + 1
            
            if max(group_counts.values(), default=0) <= 1:
                refined_batches.append(batch)
            else:
                # Split batch so each sub-batch has only threads from distinct groups
                grouped_by_gid = {}
                for req in batch:
                    gid = req["headers"]["group_id"]
                    if gid not in grouped_by_gid:
                        grouped_by_gid[gid] = []
                    grouped_by_gid[gid].append(req)
                
                max_len = max(len(reqs) for reqs in grouped_by_gid.values())
                for i in range(max_len):
                    new_sub_batch = []
                    for gid, reqs in grouped_by_gid.items():
                        if i < len(reqs):
                            new_sub_batch.append(reqs[i])
                    refined_batches.append(new_sub_batch)
                    
        batches = refined_batches
        
        futures_map: Dict[int, Future[List[Dict[str, Any]]]] = {}
        batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]] = {}
        idx = 0
        
        # Optimize Threadpool Size locally
        optimal_concurrency = min(4, self.config.concurrency)
        response_map: Dict[int, List[Dict[str, Any]]] = {}
        
        with ThreadPoolExecutor(max_workers=optimal_concurrency) as local_executor:
            for batch in batches:
                futures_map[idx] = local_executor.submit(self.url_invoker.invoke, GRAPH_BASE_URL, batch, self.logger, self.stop_event, self.get_resource_type())
                batch_id_to_batch_map[idx] = batch
                idx += 1

            for batch_id, future in futures_map.items():
                response_map[batch_id] = future.result()

        granular_request_to_response_pairs = create_request_to_response_map(batch_id_to_batch_map, response_map, failures)
        
        # Enrich failures with groupId
        for failure in failures:
            if "userId" in failure and failure["userId"] in thread_to_group_map:
                failure["groupId"] = thread_to_group_map[failure["userId"]]
                
        for request_response_pair in granular_request_to_response_pairs:
            request = request_response_pair.request
            response = request_response_pair.response
            thread_id = request["headers"]["thread_id"]
            
            if "body" in response and "@odata.count" in response["body"]:
                try:
                    thread_to_count[thread_id] = int(response["body"]["@odata.count"])
                except Exception as e:
                    failures.append({
                        "userId": thread_id,
                        "type": FailureType.INVALID_DATA,
                        "statusCode": 200,
                        "message": f"Invalid data - Unable to convert count to integer: {e}"
                    })
                    thread_to_count[thread_id] = 0
            else:
                failures.append({
                    "userId": thread_id,
                    "type": FailureType.INVALID_DATA,
                    "statusCode": 200,
                    "message": "Invalid data - No count present in response"
                })
                thread_to_count[thread_id] = 0
                
        return thread_to_count

    def _consolidate_thread_post_counts_for_each_group(self, group_id_to_thread_ids: Dict[str, List[str]], thread_id_to_post_count: Dict[str, int]) -> Dict[str, int]:
        group_to_post_count: Dict[str, int] = {}
        for group_id, thread_ids in group_id_to_thread_ids.items():
            total_count = 0
            for thread_id in thread_ids:
                total_count += thread_id_to_post_count.get(thread_id, 0)
            group_to_post_count[group_id] = total_count
        return group_to_post_count
