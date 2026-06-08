from typing import Any, Callable, Dict, List, Optional

from estimators.estimator import Estimator
from util.connectors import UrlInvoker
from util.utils import ScanConfig, group_responses_by_key, process_pagination_responses, get_relative_url, get_batch_responses_map, create_batches
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from util.thread_safe_ds import AtomicInt
from util.enums import FailureType

GRAPH_BETA_URL = "https://graph.microsoft.com/beta"

class EOInPlaceArchiveEstimator(Estimator):
    def __init__(
            self, 
            config: ScanConfig, 
            url_invoker: UrlInvoker, 
            child_folder_url_invoker: UrlInvoker, 
            logger: Optional[Callable[[str], None]] = None, 
            stop_event: Optional[threading.Event] = None,
            use_delta_api: bool = False
        ):
        super().__init__()
        self.config = config
        self.url_invoker = url_invoker
        self.child_folder_url_invoker = child_folder_url_invoker
        self.logger = logger
        self.stop_event = stop_event
        self.use_delta_api = use_delta_api
        
        self.archive_executor = ThreadPoolExecutor(max_workers=self.config.concurrency)
        if self.use_delta_api is False:
            self.tree_executor = ThreadPoolExecutor(max_workers=self.config.concurrency)

    def calculate_migration_eta(self, data: Dict[str, Any]) -> float:
        return super().calculate_migration_eta(data)

    def get_resource_type(self):
        return "EO_IN_PLACE_ARCHIVE"

    def get_migration_type(self):
        return "EXCHANGE_ONLINE"

    def is_hard_stop_requested(self):
        if self.stop_event is None:
            return False
        
        return self.stop_event.is_set()

    """
        @param List of Dictionary of param name to its value
        @param List of failures (it will be updated in place)
        @returns Dictionary of user id to in-place archived mail count
    """
    def calculate_resource_count(self, data: Dict[str, Any], failures: List[Dict[str, str]]) -> Dict[str, int]:
        user_ids = data["user_ids"]
        if not user_ids or None in user_ids:
            raise Exception("Invalid user ids provided. Please check the list and ensure all the IDs are non-null.")
        return self.get_in_place_archive_count(user_ids, failures)

    def get_in_place_archive_count(self, user_ids: List[str], failures: List[Dict[str, str]]) -> Dict[str, int]:
        # Fetch the in-place archive mail box id for the user
        exchange_api = "users/{userId}/settings/exchange"
        
        user_id_maps = [{"userId": user_id} for user_id in user_ids]
        user_batches = create_batches(exchange_api, user_id_maps, self.config.parallel_batches, True)
        
        futures_map: Dict[int, Future[List[Dict[str, Any]]]] = {}
        batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]] = {}
        idx = 0
        for batch in user_batches:
            futures_map[idx] = self.archive_executor.submit(self.url_invoker.invoke, GRAPH_BETA_URL, batch, self.logger, self.stop_event, self.get_resource_type())
            batch_id_to_batch_map[idx] = batch
            idx += 1

        response_map: Dict[int, List[Dict[str, Any]]] = {}
        for batch_id, future in futures_map.items():
            response_map[batch_id] = future.result()

        user_to_mailbox: Dict[str, str] = {}
        mailbox_to_user: Dict[str, str] = {}
        for batch_id, responses in response_map.items():
            batch = batch_id_to_batch_map[batch_id]
            batch_responses_map = get_batch_responses_map(responses, self.logger)
            for req in batch:
                req_id = req["id"]
                if req_id in batch_responses_map:
                    resp = batch_responses_map[req_id]
                    user_id = req["headers"]["userId"]
                    if "body" in resp and "inPlaceArchiveMailboxId" in resp["body"]:
                        user_to_mailbox[user_id] = resp["body"]["inPlaceArchiveMailboxId"]
                        mailbox_to_user[resp["body"]["inPlaceArchiveMailboxId"]] = user_id
                    elif "body" in resp and "error" in resp["body"]:
                        failures.append({
                            "userId": user_id,
                            "isPartial": False,
                            "type": FailureType.FAILURE_STATUS_CODE_ERROR,
                            "statusCode" : resp["status"],
                            "message": resp["body"]["error"]["message"]
                        })
                else:
                    failures.append({
                        "userId": req["headers"]["userId"],
                        "isPartial": False,
                        "type": FailureType.NOT_FOUND,
                        "statusCode": None,
                        "message": "In-place archive mailbox not found for the user."
                    })

        mail_box_ids = list(set(user_to_mailbox.values()))
        mailbox_failures = []
        mail_box_to_count = self.parse_and_count_in_place_archive_mail_box(mail_box_ids, mailbox_failures)
        
        user_to_count = {user_id: 0 for user_id in user_ids}
        for user_id, mail_box_id in user_to_mailbox.items():
            user_to_count[user_id] = mail_box_to_count.get(mail_box_id, 0)

        failures.extend([{
            "userId": mailbox_to_user[mailbox_failure["mailboxId"]],
            "isPartial": mailbox_failure["isPartial"],
            "folderId": mailbox_failure["folderId"] if "folderId" in mailbox_failure else None,
            "type": mailbox_failure["type"],
            "statusCode": mailbox_failure["statusCode"],
            "message": mailbox_failure["message"]
        } for mailbox_failure in mailbox_failures])
            
        return user_to_count

    def parse_and_count_in_place_archive_mail_box(self, mail_box_ids: List[str], failures: List[Dict[str, str]]) -> Dict[str, int]:
        if self.is_hard_stop_requested():
            return {mail_box_id: 0 for mail_box_id in mail_box_ids}

        # Extract all the top level folders. This is done separately as a different API is used for top level folders compared to child folders
        mail_box_id_maps = [{"mailboxId": mail_box_id} for mail_box_id in mail_box_ids]
        folder_api = ""
        
        if self.use_delta_api is True:
            folder_api = "admin/exchange/mailboxes/{mailboxId}/folders/delta?$select=id,childFolderCount,totalItemCount"
        else:
            folder_api = "admin/exchange/mailboxes/{mailboxId}/folders?$select=id,childFolderCount,totalItemCount&$top=999"

        top_level_folders: Dict[str, List[Dict[str, Any]]] = {}      # Map of Mail box to top level folder list.
        mail_box_batches = create_batches(folder_api, mail_box_id_maps, self.config.parallel_batches, True)

        futures_map: Dict[int, Future[List[Dict[str, Any]]]] = {}
        batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]] = {}
        idx = 0
        for batch in mail_box_batches:
            futures_map[idx] = self.archive_executor.submit(self.url_invoker.invoke, GRAPH_BETA_URL, batch, self.logger, self.stop_event, self.get_resource_type())
            batch_id_to_batch_map[idx] = batch
            idx += 1

        response_map: Dict[int, List[Dict[str, Any]]] = {}
        for batch_id, future in futures_map.items():
            response_map[batch_id] = future.result()
        
        # Create a map of mailboxId -> original_response_object
        # And identify next links
        mailbox_to_resp_map: Dict[str, Dict[str, Any]] = {}
        pending_next_items = []
        
        for batch_id, responses in response_map.items():
            batch = batch_id_to_batch_map[batch_id]
            
            # Initialize mapping and check for next links manually
            batch_responses_map = get_batch_responses_map(responses, self.logger)
            for req in batch:
                req_id = req["id"]
                if req_id in batch_responses_map:
                    resp = batch_responses_map[req_id]
                    mailbox_id = req["headers"]["mailboxId"]
                    mailbox_to_resp_map[mailbox_id] = resp
                    
                    if "body" in resp and "@odata.nextLink" in resp["body"]:
                        next_url = resp["body"]["@odata.nextLink"]
                        relative_url = get_relative_url(next_url, GRAPH_BETA_URL)
                        pending_next_items.append({
                            "mailboxId": mailbox_id,
                            "url": relative_url
                        })
                    elif "body" in resp and "error" in resp["body"]:
                        failures.append({
                            "mailboxId": mailbox_id,
                            "isPartial": False,                                                 # Call the /folders would not result in partial failure
                            "type": FailureType.FAILURE_STATUS_CODE_ERROR,
                            "statusCode": resp["status"],
                            "message": resp["body"]["error"]["message"]
                        })
                else:
                    failures.append({
                        "mailboxId": req["headers"]["mailboxId"],
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
                next_futures_map[idx] = self.archive_executor.submit(self.url_invoker.invoke, GRAPH_BETA_URL, batch, self.logger, self.stop_event, self.get_resource_type())
                next_batch_id_to_batch_map[idx] = batch
                idx += 1
                
            next_response_map: Dict[int, List[Dict[str, Any]]] = {}
            for batch_id, future in next_futures_map.items():
                next_response_map[batch_id] = future.result()
                
            new_pending_next_items = []
            
            for batch_id, responses in next_response_map.items():
                batch = next_batch_id_to_batch_map[batch_id]
                new_pending_next_items.extend(process_pagination_responses(batch, responses, mailbox_to_resp_map, "mailboxId", GRAPH_BETA_URL, failures, True))
                
            pending_next_items = new_pending_next_items
            
        # Now that all pages are fetched and merged into original objects,
        # populate top_level_folders using original batch structure.
        for batch_id, responses in response_map.items():
            batch = batch_id_to_batch_map[batch_id]
            group_responses_by_key(top_level_folders, batch, responses, "mailboxId")

        # Maintaining a global count of mails to avoid waiting for each thread
        archived_mail_count: Dict[str, AtomicInt] = {}        # Dict with key as mail_box_id and value as the mail count atomic variable

        for mail_box_id in mail_box_ids:
            # Synchronization not needed for archived_mail_count as a whole as we would only be doing GET operations on the keys.
            archived_mail_count[mail_box_id] = AtomicInt(0)

        parseable_sub_folders = self.get_parseable_folders_and_update_counts(
            top_level_folders   ,
            archived_mail_count,
            failures
        )

        if self.use_delta_api is False:
            # Maintaining this count to ensure that every child folder is parsed before returning the final count. 
            active_thread_count = AtomicInt(0)
            condition = threading.Condition()

            if not self.is_hard_stop_requested():
                self.submit_child_folder_requests_to_executor (
                    condition,
                    parseable_sub_folders,
                    archived_mail_count,
                    active_thread_count,
                    failures
                )
            
            # Non blocking wait to ensure that the parsing is complete before returning the result. Note that it is always expected to be non-zero unless the parsing is over as we increment the count before decrementing it sequentially for a particular folder.
            while active_thread_count.get_value() > 0:
                with condition:
                    condition.wait()

        mail_count: Dict[str, int] = {}
        for mail_box_id, count in archived_mail_count.items():
            mail_count[mail_box_id] = count.get_value()

        return mail_count

    def parse_and_count_mails_in_child_folders(
            self,
            condition: threading.Condition, 
            folders: Dict[str, List[Dict[str, Any]]], 
            archived_mail_count: Dict[str, AtomicInt], 
            active_thread_count: AtomicInt,
            failures: List[Dict[str, Any]]
    ) -> None:
        try:
            child_folder_api = "admin/exchange/mailboxes/{mailboxId}/folders/{folderId}/childFolders?$select=id,childFolderCount,totalItemCount&$top=999"

            mail_box_id_to_folder_id: List[Dict[str, Any]] = []
            for mail_box_id, folder_list in folders.items():
                for folder in folder_list:
                    mail_box_id_to_folder_id.append({"mailboxId": mail_box_id, "folderId": folder["id"]})
            
            batches = create_batches(child_folder_api, mail_box_id_to_folder_id, self.config.hierarchial_crawl_batch_limit, True)

            child_folders: Dict[str, List[Dict[str, Any]]] = {}
            
            all_initial_responses = []
            folder_context_map = {}

            futures_map: Dict[int, Future[List[Dict[str, Any]]]] = {}
            batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]] = {}
            idx = 0
            for batch in batches:
                futures_map[idx] = self.archive_executor.submit(self.child_folder_url_invoker.invoke, GRAPH_BETA_URL, batch, self.logger, self.stop_event, self.get_resource_type())
                batch_id_to_batch_map[idx] = batch
                idx += 1

            response_map: Dict[int, List[Dict[str, Any]]] = {}
            for batch_id, future in futures_map.items():
                response_map[batch_id] = future.result()
            
            # Using sequential processing here as each batch would have 4 requests which is = throttling quota of same mailbox requests.
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
                            "mailboxId": req["headers"]["mailboxId"]
                        }
                        
            # Now check for next links
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
                            relative_url = get_relative_url(next_url, GRAPH_BETA_URL)
                            pending_next_items.append({
                                "folderId": folder_id,
                                "url": relative_url,
                                "mailboxId": req["headers"]["mailboxId"]
                            })
                        elif "body" in resp and "error" in resp["body"]:
                            failures.append({
                                "mailboxId": req["headers"]["mailboxId"],
                                "folderId": req["headers"]["folderId"],
                                "isPartial": True,                   # As we can only reach this point if the top level folder scan is successful
                                "type": FailureType.FAILURE_STATUS_CODE_ERROR,
                                "statusCode": resp["status"],
                                "message": resp["body"]["error"]["message"]
                            })
                    else:
                        failures.append({
                            "mailboxId": req["headers"]["mailboxId"],
                            "folderId": req["headers"]["folderId"],
                            "isPartial": True,                   # As we can only reach this point if the top level folder scan is successful
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
                    futures_map[idx] = self.archive_executor.submit(self.child_folder_url_invoker.invoke, GRAPH_BETA_URL, batch, self.logger, self.stop_event, self.get_resource_type())
                    batch_id_to_batch_map[idx] = batch
                    idx += 1

                response_map = {}
                for batch_id, future in futures_map.items():
                    response_map[batch_id] = future.result()

                for batch_id, responses in response_map.items():
                    batch = batch_id_to_batch_map[batch_id]
                    new_pending_next_items.extend(process_pagination_responses(batch, responses, folder_context_map, "folderId", GRAPH_BETA_URL, True))
                    
                pending_next_items = new_pending_next_items
                
            # Finally, group responses by key using original batches
            for batch, responses in all_initial_responses:
                group_responses_by_key(child_folders, batch, responses, "mailboxId")

            parseable_sub_folders = self.get_parseable_folders_and_update_counts(
                child_folders,
                archived_mail_count,
                failures
            )

            self.submit_child_folder_requests_to_executor (
                condition,
                parseable_sub_folders,
                archived_mail_count,
                active_thread_count,
                failures
            )
        finally:
            active_thread_count.decrement(1)
            with condition:
                condition.notify_all()

    def get_parseable_folders_and_update_counts(
        self,
        child_folders: Dict[str, List[Dict[str, Any]]],
        archived_mail_count: List[AtomicInt],
        failures: List[Dict[str, Any]],
    ) -> Dict[str, List[Dict[str, Any]]]:
        parseable_sub_folders: Dict[str, List[Dict[str, Any]]] = {}
        for mail_box_id, sub_folders in child_folders.items():
            for sub_folder in sub_folders:
                # 1. Safely handle totalItemCount
                if "totalItemCount" in sub_folder:
                    try:
                        count = int(sub_folder["totalItemCount"])
                        archived_mail_count[mail_box_id].increment(count)
                    except (ValueError, TypeError):
                        if self.logger:
                            self.logger(f"Warning: Invalid totalItemCount '{sub_folder.get('totalItemCount')}' for mailbox {self.get_display_name_from_id(mail_box_id)}. Skipping count.")
                        failures.append({
                            "mailboxId": mail_box_id,
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
                            self.logger(f"Warning: Invalid childFolderCount '{sub_folder.get('childFolderCount')}' for mailbox {self.get_display_name_from_id(mail_box_id)}. Assuming 0.")
                        failures.append({
                            "mailboxId": mail_box_id,
                            "isPartial": True,
                            "type": FailureType.INVALID_DATA,
                            "statusCode": None,
                            "message": f"Invalid childFolderCount '{sub_folder.get('childFolderCount')}'"
                        })
                
                if child_count > 0:
                    if mail_box_id not in parseable_sub_folders:
                        parseable_sub_folders[mail_box_id] = []
                    parseable_sub_folders[mail_box_id].append(sub_folder)

        return parseable_sub_folders

    def submit_child_folder_requests_to_executor (
        self,
        condition: threading.Condition,
        parseable_sub_folders: Dict[str, List[Dict[str, Any]]],
        archived_mail_count: Dict[str, AtomicInt],
        active_thread_count: AtomicInt,
        failures: List[Dict[str, Any]],
    ) -> None:
        if not parseable_sub_folders or len(parseable_sub_folders) == 0 or self.is_hard_stop_requested():
            return

        try:
            active_thread_count.increment()

            #TODO Use a retry template and failure callback instead of try, except
            self.tree_executor.submit(self.parse_and_count_mails_in_child_folders, condition, parseable_sub_folders, archived_mail_count, active_thread_count, failures)
        except:
            active_thread_count.decrement()
            with condition:
                condition.notify_all()
