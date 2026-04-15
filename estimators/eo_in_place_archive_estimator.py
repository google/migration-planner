from typing import Any, Callable, Dict, List, Optional

import concurrent
from estimators.estimator import Estimator
from util.connectors import TokenManager
from util.connectors import UrlInvoker
from util.utils import ScanConfig, group_responses_by_key
import threading
from concurrent.futures import ThreadPoolExecutor
from util.utils import create_batches
from util.atomic_int import AtomicInt
import json
import time

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"

TOKEN_URL_TEMPLATE = "https://login.microsoftonline.com/{0}/oauth2/v2.0/token"
GRAPH_BETA_URL = "https://graph.microsoft.com/beta"

class EOInPlaceArchiveEstimator(Estimator):
    def __init__(
            self, 
            manager: TokenManager, 
            config: ScanConfig, 
            url_invoker: UrlInvoker, 
            logger: Optional[Callable[[str], None]] = None, 
            stop_event: Optional[threading.Event] = None
        ):
        super().__init__()
        self.manager = manager
        self.config = config
        self.url_invoker = url_invoker
        self.logger = logger
        self.stop_event = stop_event
        self.archive_executor = ThreadPoolExecutor(max_workers=self.config.concurrency)

    def calculate_migration_eta(self, data):
        # Implementation for calculating migration ETA
        pass

    def get_resource_type(self):
        return "EO_IN_PLACE_ARCHIVE"

    def get_migration_type(self):
        return "EXCHANGE_ONLINE"

    """
        @param List of Dictionary of param name to its value
        @returns Dictionary of user id to in-place archived mail count
    """
    def calculate_resource_count(self, data: List[Dict[str, Any]]) -> Dict[str, int]:
        user_ids = [entry["user_id"] for entry in data]
        return self.get_in_place_archive_count(user_ids)

    def get_in_place_archive_count(self, user_ids: List[str]) -> Dict[str, int]:
        # Fetch the in-place archive mail box id for the user
        exchange_api = "users/{userId}/settings/exchange"
        mail_box_ids = []
        
        user_id_maps = [{"userId": user_id} for user_id in user_ids]
        user_batches = create_batches(exchange_api, user_id_maps, self.config.parallel_batches)
        
        futures = []
        for batch in user_batches:
            futures.append(self.archive_executor.submit(self.url_invoker.invoke, GRAPH_BETA_URL, batch, self.logger, self.stop_event, self.get_resource_type()))

        responses = []
        for future in futures:
            responses += future.result()

        for response in responses:
            if "body" not in response:
                continue
            if "inPlaceArchiveMailboxId" not in response["body"]:
                continue
            
            mail_box_ids.append(response["body"]["inPlaceArchiveMailboxId"])

        # Start the BFS crawl
        return self.parse_and_count_in_place_archive_mail_box(mail_box_ids)

    def parse_and_count_in_place_archive_mail_box(self, mail_box_ids: List[str]) -> Dict[str, int]:
        # Extract all the top level folders. This is done separately as a different API is used for top level folders compared to child folders
        mail_box_id_maps = [{"mailboxId": mail_box_id} for mail_box_id in mail_box_ids]
        folder_api = "admin/exchange/mailboxes/{mailboxId}/folders?$select=id,childFolderCount,totalItemCount&$top=100"     # TODO Add support for a configurable page size

        top_level_folders: Dict[str, List[Dict[str, Any]]] = {}      # Map of Mail box to top level folder list.
        mail_box_batches = create_batches(folder_api, mail_box_id_maps, self.config.parallel_batches, True)

        futures = []
        for batch in mail_box_batches:
            futures.append(self.archive_executor.submit(self.url_invoker.invoke, GRAPH_BETA_URL, batch, self.logger, self.stop_event, self.get_resource_type()))
        
        response_list = []
        for future in futures:
            response_list.append(future.result())
        
        for responses in response_list:
            group_responses_by_key(top_level_folders, batch, responses, "mailboxId")

        # Maintaining a global count of mails to avoid waiting for each thread
        archived_mail_count: Dict[str, AtomicInt] = {}        # Dict with key as mail_box_id and value as the mail count atomic variable

        for mail_box_id in mail_box_ids:
            # Synchronization not needed for archived_mail_count as a whole as we would only be doing GET operations on the keys.
            archived_mail_count[mail_box_id] = AtomicInt(0)
        
        # Maintaining this count to ensure that every child folder is parsed before returning the final count. 
        active_thread_count = AtomicInt(0)

        # TODO Add support for a thread count per user for progress bars

        condition = threading.Condition()
        
        self.submit_child_folder_requests_to_executor (
            condition,
            top_level_folders,
            archived_mail_count,
            active_thread_count
        )

        # Non blocking wait to ensure that the parsing is complete before returning the result. Note that it is always expected to be non-zero unless the parsing is over as we increment the count before decrementing it sequentially for a particular folder.
        while active_thread_count.get_value() > 0:
            with condition:
                condition.wait()

        mail_count = {}
        for mail_box_id, count in archived_mail_count.items():
            mail_count[mail_box_id] = count.get_value()

        return mail_count

    def parse_and_count_mails_in_child_folders(
            self,
            condition: threading.Condition, 
            folders: Dict[str, List[Dict[str, Any]]], 
            archived_mail_count: Dict[str, AtomicInt], 
            active_thread_count: AtomicInt,
    ) -> None:
        child_folder_api = "admin/exchange/mailboxes/{mailBoxId}/folders/{folderId}/childFolders?$select=id,childFolderCount,totalItemCount"

        mail_box_id_to_folder_id = []
        for mail_box_id, folder_list in folders.items():
            for folder in folder_list:
                mail_box_id_to_folder_id.append({"mailBoxId": mail_box_id, "folderId": folder["id"]})
        
        batches = create_batches(child_folder_api, mail_box_id_to_folder_id, self.config.hierarchial_crawl_batch_limit, True)

        child_folders: Dict[str, List[Dict[str, Any]]] = {}
        
        for batch in batches:
            responses = self.url_invoker.invoke(GRAPH_BETA_URL, batch, self.logger, self.stop_event, self.get_resource_type())
            group_responses_by_key(child_folders, batch, responses, "mailBoxId")

        self.submit_child_folder_requests_to_executor (
            condition,
            child_folders,
            archived_mail_count,
            active_thread_count
        )

        active_thread_count.decrement(1);
        with condition:
            condition.notify_all()

    def submit_child_folder_requests_to_executor (
        self,
        condition: threading.Condition,
        child_folders: Dict[str, List[Dict[str, Any]]],
        archived_mail_count: Dict[str, AtomicInt],
        active_thread_count: AtomicInt
    ) -> None:

        parseable_sub_folders = {}
        for mail_box_id, sub_folders in child_folders.items():
            for sub_folder in sub_folders:
                archived_mail_count[mail_box_id].increment(sub_folder["totalItemCount"]) if "totalItemCount" in sub_folder else None
                if "childFolderCount" in sub_folder and sub_folder["childFolderCount"] is not None and sub_folder["childFolderCount"] > 0:
                    if mail_box_id not in parseable_sub_folders:
                        parseable_sub_folders[mail_box_id] = []
                    parseable_sub_folders[mail_box_id].append(sub_folder)

        if len(parseable_sub_folders) > 0:
            try:
                active_thread_count.increment()

                #TODO Use a retry template and failure callback instead of try, except
                self.archive_executor.submit(self.parse_and_count_mails_in_child_folders, condition, parseable_sub_folders, archived_mail_count, active_thread_count)
            except:
                active_thread_count.decrement()
                with condition:
                    condition.notify_all()

