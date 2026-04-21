from concurrent.futures import Future, ThreadPoolExecutor
import threading
from typing import Any, Callable, Dict, List, Optional

from estimators.estimator import Estimator
from util.connectors import TokenManager, UrlInvoker
from util.utils import ScanConfig, create_batches, create_request_to_response_map

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"

class EOGroupMailBoxEstimator(Estimator):
    def __init__(self,
        manager: TokenManager, 
        config: ScanConfig, 
        url_invoker: UrlInvoker, 
        logger: Optional[Callable[[str], None]] = None, 
        stop_event: Optional[threading.Event] = None
    ):
        self.manager = manager
        self.config = config
        self.url_invoker = url_invoker
        self.logger = logger
        self.stop_event = stop_event
        self.archive_executor = ThreadPoolExecutor(max_workers=self.config.concurrency)

    def calculate_resource_count(self, data: Dict[str, Any], failures: List[Dict[str, str]]) -> Dict[str, int]:
        user_ids: List[str] = data["user_ids"]
        if not user_ids or None in user_ids:
            raise Exception("Invalid user ids provided. Please check the list and ensure all the IDs are non-null.")
        mailbox_to_count = self.get_group_mail_box_count(user_ids, failures)
        
        # Creating a new map to account for the fact that the user might not have a group mailbox. 
        user_to_count = {user_id: 0 for user_id in user_ids}
        for user_id, count in mailbox_to_count.items():
            user_to_count[user_id] = count
            
        return user_to_count

    def calculate_migration_eta(self, data: Dict[str, Any]) -> float:
        return super().calculate_migration_eta(data)
    
    def get_resource_type(self):
        return "GROUP_MAILBOX"
    
    def get_migration_type(self):
        return "EXCHANGE_ONLINE"


    def get_group_mail_box_count(self, user_ids: List[str], failures: List[Dict[str, str]]) -> Dict[str, int]:
        # Filter out the shared mail boxes the provided list. 
        mailbox_setting_endpoint = "/users/{userId}/mailboxSettings/userPurpose"
        
        batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]] = {}
        batch_id_to_future_map: Dict[int, Future[List[Dict[str, Any]]]] = {}
        user_id_maps = [{"userId": user_id} for user_id in user_ids]
        user_batches = create_batches(mailbox_setting_endpoint, user_id_maps, self.config.parallel_batches, True)

        idx = 0
        for batch in user_batches:
            batch_id_to_batch_map[idx] = batch
            batch_id_to_future_map[idx] = self.archive_executor.submit(self.url_invoker.invoke, GRAPH_BASE_URL, batch, self.logger, self.stop_event, self.get_resource_type())
            idx += 1

        batch_id_to_responses_map: Dict[int, List[Dict[str, Any]]] = {}

        for batch_id, future in batch_id_to_future_map.items():
            batch_id_to_responses_map[batch_id] = future.result()

        granular_request_to_response_pairs = create_request_to_response_map(batch_id_to_batch_map, batch_id_to_responses_map, failures)
        group_mail_box_ids = [request_response_pair.request["headers"]["userId"] for request_response_pair in granular_request_to_response_pairs if request_response_pair.response["body"]["value"] == "shared"]

        group_mail_count_endpoint = "/users/{userId}/messages?$count=true&$top=1&$select=id"
        group_mail_box_batches = create_batches(group_mail_count_endpoint, [{"userId": mail_id} for mail_id in group_mail_box_ids], self.config.parallel_batches, True)

        batch_id_to_future_map.clear()
        batch_id_to_batch_map.clear()
        idx = 0
        for batch in group_mail_box_batches:
            batch_id_to_batch_map[idx] = batch
            batch_id_to_future_map[idx] = self.archive_executor.submit(self.url_invoker.invoke, GRAPH_BASE_URL, batch, self.logger, self.stop_event, self.get_resource_type())
            idx += 1

        batch_id_to_responses_map.clear()
        for batch_id, future in batch_id_to_future_map.items():
            batch_id_to_responses_map[batch_id] = future.result()
            
        granular_request_to_response_pairs = create_request_to_response_map(batch_id_to_batch_map, batch_id_to_responses_map, failures)

        group_mailbox_to_count: Dict[str, int] = {}
        for request_response_pair in granular_request_to_response_pairs:
            request = request_response_pair.request
            response = request_response_pair.response
            group_mailbox_to_count[request["headers"]["userId"]] = response["body"]["@odata.count"]

        return group_mailbox_to_count
        
