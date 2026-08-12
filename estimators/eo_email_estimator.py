from typing import Any, Callable, Dict, List, Optional
from estimators.estimator import Estimator
from util.connectors import UrlInvoker, TokenManager
from util.utils import ScanConfig, group_responses_by_key, process_pagination_responses, get_relative_url, get_batch_responses_map, create_batches
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from util.thread_safe_ds import AtomicInt
from util.enums import FailureType
import re
from util.constants import GRAPH_BASE_URL

class EOEmailEstimator(Estimator):
    def __init__(
            self, 
            config: ScanConfig, 
            url_invoker: UrlInvoker, 
            child_folder_url_invoker: UrlInvoker, 
            logger: Optional[Callable[[str], None]] = None, 
            stop_event: Optional[threading.Event] = None
        ):
        super().__init__()
        self.config = config
        self.url_invoker = url_invoker
        self.child_folder_url_invoker = child_folder_url_invoker
        self.logger = logger
        self.stop_event = stop_event

        self.archive_executor = ThreadPoolExecutor(max_workers=self.config.concurrency)
        self.tree_executor = ThreadPoolExecutor(max_workers=self.config.concurrency)

    def calculate_migration_eta(self, data: Dict[str, Any]) -> float:
        return super().calculate_migration_eta(data)

    def get_resource_type(self):
        return "EO_EMAIL"

    def get_migration_type(self):
        return "EXCHANGE_ONLINE"

    def is_hard_stop_requested(self):
        if self.stop_event is None:
            return False
        return self.stop_event.is_set()

    def calculate_resource_count(self, data: Dict[str, Any], failures: List[Dict[str, str]]) -> Dict[str, int]:
        user_ids = data["user_ids"]
        if not user_ids or None in user_ids:
            raise Exception("Invalid user ids provided. Please check the list and ensure all the IDs are non-null.")
        return self.get_email_count(user_ids, failures)

    def get_email_count(self, user_ids: List[str], failures: List[Dict[str, str]]) -> Dict[str, int]:
        if self.is_hard_stop_requested():
            return {user_id: 0 for user_id in user_ids}

        # Extract all the top level folders.
        user_id_maps = [{"userId": user_id} for user_id in user_ids]
        folder_api = "users/{userId}/mailFolders?$select=id,displayName,childFolderCount,totalItemCount&$top=999"

        top_level_folders: Dict[str, List[Dict[str, Any]]] = {}      # Map of User to top level folder list.
        user_batches = create_batches(folder_api, user_id_maps, self.config.parallel_batches, True)

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
        
        user_to_resp_map: Dict[str, Dict[str, Any]] = {}
        pending_next_items = []
        
        for batch_id, responses in response_map.items():
            batch = batch_id_to_batch_map[batch_id]
            
            batch_responses_map = get_batch_responses_map(responses, self.logger)
            for req in batch:
                req_id = req["id"]
                if req_id in batch_responses_map:
                    resp = batch_responses_map[req_id]
                    user_id = req["headers"]["userId"]
                    user_to_resp_map[user_id] = resp
                    
                    if "body" in resp and "@odata.nextLink" in resp["body"]:
                        next_url = resp["body"]["@odata.nextLink"]
                        relative_url = get_relative_url(next_url, GRAPH_BASE_URL)
                        pending_next_items.append({
                            "userId": user_id,
                            "url": relative_url
                        })
                    elif "body" in resp and "error" in resp["body"]:
                        failures.append({
                            "userId": user_id,
                            "isPartial": False,
                            "type": FailureType.FAILURE_STATUS_CODE_ERROR,
                            "statusCode": resp["status"],
                            "message": resp["body"]["error"]["message"]
                        })
                else:
                    failures.append({
                        "userId": req["headers"]["userId"],
                        "isPartial": False,
                        "type": FailureType.NOT_FOUND,
                        "statusCode": None,
                        "message": "No response found for folder API."
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
                new_pending_next_items.extend(process_pagination_responses(batch, responses, user_to_resp_map, "userId", GRAPH_BASE_URL, failures, True))
                
            pending_next_items = new_pending_next_items
            
        for batch_id, responses in response_map.items():
            batch = batch_id_to_batch_map[batch_id]
            group_responses_by_key(top_level_folders, batch, responses, "userId")

        email_count: Dict[str, AtomicInt] = {}
        for user_id in user_ids:
            email_count[user_id] = AtomicInt(0)

        parseable_sub_folders = self.get_parseable_folders_and_update_counts(
            top_level_folders,
            email_count,
            failures
        )

        active_thread_count = AtomicInt(0)
        condition = threading.Condition()

        if not self.is_hard_stop_requested():
            self.submit_child_folder_requests_to_executor(
                condition,
                parseable_sub_folders,
                email_count,
                active_thread_count,
                failures
            )
        
        while active_thread_count.get_value() > 0:
            with condition:
                condition.wait()

        final_count: Dict[str, int] = {}
        for user_id, count in email_count.items():
            final_count[user_id] = count.get_value()

        return final_count

    def get_parseable_folders_and_update_counts(
        self,
        child_folders: Dict[str, List[Dict[str, Any]]],
        email_count: Dict[str, AtomicInt],
        failures: List[Dict[str, Any]],
    ) -> Dict[str, List[Dict[str, Any]]]:
        parseable_sub_folders: Dict[str, List[Dict[str, Any]]] = {}

        for user_id, sub_folders in child_folders.items():
            for sub_folder in sub_folders:
                # 1. Safely handle totalItemCount
                if "totalItemCount" in sub_folder:
                    try:
                        count = int(sub_folder["totalItemCount"])
                        email_count[user_id].increment(count)
                    except (ValueError, TypeError):
                        if self.logger:
                            user_email = self.get_display_name_from_id(user_id)
                            self.logger(f"Warning: Invalid totalItemCount '{sub_folder.get('totalItemCount')}' for user {user_email}. Skipping count.")
                        failures.append({
                            "userId": user_id,
                            "isPartial": True,
                            "type": FailureType.INVALID_DATA,
                            "statusCode": None,
                            "message": f"Invalid totalItemCount '{sub_folder.get('totalItemCount')}'"
                        })
                
                # 2. Safely handle childFolderCount
                child_count = 0
                if "childFolderCount" in sub_folder and sub_folder["childFolderCount"] is not None:
                    try:
                        child_count = int(sub_folder["childFolderCount"])
                    except (ValueError, TypeError):
                        if self.logger:
                            user_email = self.get_display_name_from_id(user_id)
                            self.logger(f"Warning: Invalid childFolderCount '{sub_folder.get('childFolderCount')}' for user {user_email}. Assuming 0.")
                        failures.append({
                            "userId": user_id,
                            "isPartial": True,
                            "type": FailureType.INVALID_DATA,
                            "statusCode": None,
                            "message": f"Invalid childFolderCount '{sub_folder.get('childFolderCount')}'"
                        })
                
                if child_count > 0:
                    if user_id not in parseable_sub_folders:
                        parseable_sub_folders[user_id] = []
                    parseable_sub_folders[user_id].append(sub_folder)

        return parseable_sub_folders

    def parse_and_count_mails_in_child_folders(
            self,
            condition: threading.Condition, 
            folders: Dict[str, List[Dict[str, Any]]], 
            email_count: Dict[str, AtomicInt], 
            active_thread_count: AtomicInt,
            failures: List[Dict[str, Any]]
    ) -> None:
        try:
            child_folder_api = "users/{userId}/mailFolders/{folderId}/childFolders?$select=id,displayName,childFolderCount,totalItemCount&$top=999"

            user_id_to_folder_id: List[Dict[str, Any]] = []
            for user_id, folder_list in folders.items():
                for folder in folder_list:
                    user_id_to_folder_id.append({"userId": user_id, "folderId": folder["id"]})
            
            batches = create_batches(child_folder_api, user_id_to_folder_id, self.config.hierarchial_crawl_batch_limit, True)

            child_folders: Dict[str, List[Dict[str, Any]]] = {}
            
            all_initial_responses = []
            folder_context_map = {}

            futures_map: Dict[int, Future[List[Dict[str, Any]]]] = {}
            batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]] = {}
            idx = 0
            for batch in batches:
                futures_map[idx] = self.archive_executor.submit(self.child_folder_url_invoker.invoke, GRAPH_BASE_URL, batch, self.logger, self.stop_event, self.get_resource_type())
                batch_id_to_batch_map[idx] = batch
                idx += 1

            response_map: Dict[int, List[Dict[str, Any]]] = {}
            for batch_id, future in futures_map.items():
                response_map[batch_id] = future.result()
            
            for batch_id, responses in response_map.items():
                batch = batch_id_to_batch_map[batch_id]
                all_initial_responses.append((batch, responses))
                
                batch_responses_map = get_batch_responses_map(responses, self.logger)
                for req in batch:
                    req_id = req["id"]
                    if req_id in batch_responses_map and batch_responses_map[req_id]["status"] == 200:
                        resp = batch_responses_map[req_id]
                        folder_id = req["headers"]["folderId"]
                        folder_context_map[folder_id] = {
                            "resp": resp,
                            "userId": req["headers"]["userId"]
                        }
                        
            pending_next_items = []
            for batch, responses in all_initial_responses:
                batch_responses_map = get_batch_responses_map(responses, self.logger)
                for req in batch:
                    req_id = req["id"]
                    if req_id in batch_responses_map:
                        resp = batch_responses_map[req_id]
                        folder_id = req["headers"]["folderId"]
                        
                        if "body" in resp and "@odata.nextLink" in resp["body"]:
                            next_url = resp["body"]["@odata.nextLink"]
                            relative_url = get_relative_url(next_url, GRAPH_BASE_URL)
                            pending_next_items.append({
                                "folderId": folder_id,
                                "url": relative_url,
                                "userId": req["headers"]["userId"]
                            })
                        elif "body" in resp and "error" in resp["body"]:
                            failures.append({
                                "userId": req["headers"]["userId"],
                                "folderId": req["headers"]["folderId"],
                                "isPartial": True,
                                "type": FailureType.FAILURE_STATUS_CODE_ERROR,
                                "statusCode": resp["status"],
                                "message": resp["body"]["error"]["message"]
                            })
                    else:
                        failures.append({
                            "userId": req["headers"]["userId"],
                            "folderId": req["headers"]["folderId"],
                            "isPartial": True,
                            "type": FailureType.NOT_FOUND,
                            "statusCode": None,
                            "message": "Invalid response received for the child folder"
                        })
                    
            while pending_next_items:
                batches = create_batches("{url}", pending_next_items, self.config.hierarchial_crawl_batch_limit, True)
                
                new_pending_next_items = []
                
                futures_map = {}
                batch_id_to_batch_map = {}
                idx = 0
                for batch in batches:
                    futures_map[idx] = self.archive_executor.submit(self.child_folder_url_invoker.invoke, GRAPH_BASE_URL, batch, self.logger, self.stop_event, self.get_resource_type())
                    batch_id_to_batch_map[idx] = batch
                    idx += 1

                response_map = {}
                for batch_id, future in futures_map.items():
                    response_map[batch_id] = future.result()

                for batch_id, responses in response_map.items():
                    batch = batch_id_to_batch_map[batch_id]
                    new_pending_next_items.extend(process_pagination_responses(batch, responses, folder_context_map, "folderId", GRAPH_BASE_URL, failures, True))
                    
                pending_next_items = new_pending_next_items
                
            for batch, responses in all_initial_responses:
                group_responses_by_key(child_folders, batch, responses, "userId")

            parseable_sub_folders = self.get_parseable_folders_and_update_counts(
                child_folders,
                email_count,
                failures
            )

            self.submit_child_folder_requests_to_executor(
                condition,
                parseable_sub_folders,
                email_count,
                active_thread_count,
                failures
            )
        finally:
            active_thread_count.decrement(1)
            with condition:
                condition.notify_all()

    def submit_child_folder_requests_to_executor(
        self,
        condition: threading.Condition,
        parseable_sub_folders: Dict[str, List[Dict[str, Any]]],
        email_count: Dict[str, AtomicInt],
        active_thread_count: AtomicInt,
        failures: List[Dict[str, Any]],
    ) -> None:
        if not parseable_sub_folders or len(parseable_sub_folders) == 0 or self.is_hard_stop_requested():
            return

        try:
            active_thread_count.increment()
            self.tree_executor.submit(self.parse_and_count_mails_in_child_folders, condition, parseable_sub_folders, email_count, active_thread_count, failures)
        except:
            active_thread_count.decrement()
            with condition:
                condition.notify_all()

    def shutdown(self):
        if hasattr(self, 'archive_executor') and self.archive_executor:
            self.archive_executor.shutdown(wait=False)
        if hasattr(self, 'tree_executor') and self.tree_executor:
            self.tree_executor.shutdown(wait=False)
