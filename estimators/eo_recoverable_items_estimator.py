from collections import Counter, deque
from dataclasses import dataclass
import threading
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple
import urllib.parse

from estimators.estimator import Estimator
from util.connectors import UrlInvoker
from util.constants import (
    GRAPH_BASE_URL,
    RECOVERABLE_ITEMS_FOLDERS,
    RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH,
)
from util.enums import FailureType
from util.thread_safe_ds import AtomicInt
from util.utils import ScanConfig, get_relative_url

# Only plain emails are counted in $count mode.
IPM_NOTE_FILTER = "type eq 'IPM.Note'"
FOLDER_SELECT = "$select=id,childFolderCount,totalItemCount"


@dataclass
class _WorkItem:
    """One Graph request pending for a mailbox."""
    kind: str               # "count", "root" or "children"
    label: str              # Recoverable Items folder label
    url: str                # Relative URL sent in the $batch
    depth: int = 0          # Depth below the Recoverable Items folder (children only)
    folder_id: Optional[str] = None


class EORecoverableItemsEstimator(Estimator):
    """Counts emails in the Recoverable Items folders of primary mailboxes.

    Mailboxes are identified by UPN. Two modes, chosen by the Recursive setting:
    - $count mode: IPM.Note $count of each folder.
    - Hierarchy mode: sum of totalItemCount over each folder and its subfolders.

    Every request is a work item in its mailbox's queue. Each round takes at
    most RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH items per mailbox
    and sends them as sequential $batch calls of at most graph_batch_size
    requests, keeping a mailbox's items for the round in the same $batch. A
    call never has more than one $batch in flight, so a mailbox owned by one
    call never sees more than that many concurrent requests. Callers must not
    pass the same mailbox to two concurrent calls.
    """

    def __init__(
            self,
            config: ScanConfig,
            url_invoker: UrlInvoker,
            logger: Optional[Callable[[str], None]] = None,
            stop_event: Optional[threading.Event] = None
        ):
        super().__init__()
        self.config = config
        self.url_invoker = url_invoker
        self.logger = logger
        self.stop_event = stop_event
        # No thread pool: concurrency comes from the caller running several
        # calls in parallel over disjoint mailboxes.
        self.archive_executor = None
        self.folder_totals: Dict[str, AtomicInt] = {
            label: AtomicInt(0) for label in RECOVERABLE_ITEMS_FOLDERS
        }

    def calculate_migration_eta(self, data: Dict[str, Any]) -> float:
        return super().calculate_migration_eta(data)

    def get_resource_type(self):
        return "EO_RECOVERABLE_ITEMS"

    def get_migration_type(self):
        return "EXCHANGE_ONLINE"

    def is_hard_stop_requested(self):
        if self.stop_event is None:
            return False
        return self.stop_event.is_set()

    def calculate_resource_count(self, data: Dict[str, Any], failures: List[Dict[str, Any]]) -> Dict[str, int]:
        """@returns Map of UPN to its total Recoverable Items count."""
        folder_counts = self.get_folder_counts(data["user_ids"], failures)
        return {upn: sum(counts.values()) for upn, counts in folder_counts.items()}

    def get_folder_counts(self, mailboxes: List[str], failures: List[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
        """@returns Map of UPN to {folder label: count}. Failed or unscanned folders count 0."""
        if not mailboxes or None in mailboxes:
            raise Exception("Invalid user ids provided. Please check the list and ensure all the IDs are non-null.")

        # De-duplicate case-insensitively so a mailbox is only requested once.
        unique_mailboxes: Dict[str, str] = {}
        for mailbox in mailboxes:
            unique_mailboxes.setdefault(mailbox.strip().lower(), mailbox.strip())

        counts: Dict[str, Dict[str, int]] = {
            key: {label: 0 for label in RECOVERABLE_ITEMS_FOLDERS} for key in unique_mailboxes
        }

        self._count_mailboxes(list(unique_mailboxes), unique_mailboxes, counts, failures)

        for folder_counts in counts.values():
            for label, count in folder_counts.items():
                self.folder_totals[label].increment(count)

        return {mailbox: dict(counts[mailbox.strip().lower()]) for mailbox in mailboxes}

    def get_folder_totals(self) -> Dict[str, int]:
        """@returns Aggregate count per folder label across all calls."""
        return {label: total.get_value() for label, total in self.folder_totals.items()}

    def _count_mailboxes(
            self,
            keys: List[str],
            unique_mailboxes: Dict[str, str],
            counts: Dict[str, Dict[str, int]],
            failures: List[Dict[str, Any]]
        ) -> None:
        hierarchy_mode = self.config.recursive_email_scan
        queues: Dict[str, Deque[_WorkItem]] = {
            key: deque(self._initial_items(unique_mailboxes[key], hierarchy_mode)) for key in keys
        }
        visited: Dict[str, Set[str]] = {key: set() for key in keys}
        folder_failures: Dict[str, List[Dict[str, Any]]] = {key: [] for key in keys}
        failed_folders: Dict[str, Set[str]] = {key: set() for key in keys}
        mailboxes_per_batch = max(1, self.config.graph_batch_size // RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH)

        while not self.is_hard_stop_requested():
            # Take up to the per-mailbox limit of pending items from every mailbox.
            round_items: List[Tuple[str, List[_WorkItem]]] = []
            for key in keys:
                taken = []
                while queues[key] and len(taken) < RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH:
                    taken.append(queues[key].popleft())
                if taken:
                    round_items.append((key, taken))
            if not round_items:
                break

            for start in range(0, len(round_items), mailboxes_per_batch):
                if self.is_hard_stop_requested():
                    break
                group = round_items[start:start + mailboxes_per_batch]
                requests, request_map = self._build_batch(group)
                responses = self._execute_batch(requests)

                for req_id, (key, item) in request_map.items():
                    item_failures, request_failed = self._handle_response(
                        responses.get(req_id), item, unique_mailboxes[key],
                        counts[key], queues[key], visited[key]
                    )
                    folder_failures[key].extend(item_failures)
                    if request_failed and item.kind in ("count", "root"):
                        failed_folders[key].add(item.label)

        for key, mailbox_failures in folder_failures.items():
            if not mailbox_failures:
                continue
            if len(failed_folders[key]) == len(RECOVERABLE_ITEMS_FOLDERS):
                first = mailbox_failures[0]
                failures.append({
                    "userId": unique_mailboxes[key],
                    "isPartial": False,
                    "type": first["type"],
                    "statusCode": first["statusCode"],
                    "message": f"All Recoverable Items folders failed. First error: {first['message']}"
                })
            else:
                failures.extend(mailbox_failures)

    def _initial_items(self, mailbox: str, hierarchy_mode: bool) -> List[_WorkItem]:
        encoded_mailbox = urllib.parse.quote(mailbox, safe="@")
        items = []
        for label, well_known_name in RECOVERABLE_ITEMS_FOLDERS.items():
            folder_url = f"/admin/exchange/mailboxes/{encoded_mailbox}/folders/{well_known_name}"
            if hierarchy_mode:
                items.append(_WorkItem("root", label, f"{folder_url}?{FOLDER_SELECT}"))
            else:
                encoded_filter = urllib.parse.quote(IPM_NOTE_FILTER, safe="")
                items.append(_WorkItem(
                    "count", label,
                    f"{folder_url}/items?$filter={encoded_filter}&$count=true&$top=1&$select=id"
                ))
        return items

    def _children_item(self, mailbox: str, label: str, folder_id: str, depth: int) -> _WorkItem:
        encoded_mailbox = urllib.parse.quote(mailbox, safe="@")
        encoded_folder = urllib.parse.quote(folder_id, safe="")
        return _WorkItem(
            "children", label,
            f"/admin/exchange/mailboxes/{encoded_mailbox}/folders/{encoded_folder}/childFolders?{FOLDER_SELECT}&$top=999",
            depth=depth, folder_id=folder_id
        )

    def _build_batch(
            self,
            group: List[Tuple[str, List[_WorkItem]]]
        ) -> Tuple[List[Dict[str, Any]], Dict[str, Tuple[str, _WorkItem]]]:
        requests: List[Dict[str, Any]] = []
        request_map: Dict[str, Tuple[str, _WorkItem]] = {}

        for key, items in group:
            for item in items:
                req_id = str(len(requests))
                request = {"id": req_id, "method": "GET", "url": item.url}
                if item.kind == "count":
                    request["headers"] = {"ConsistencyLevel": "eventual"}
                requests.append(request)
                request_map[req_id] = (key, item)

        per_mailbox = Counter(key for key, _ in request_map.values())
        if per_mailbox and max(per_mailbox.values()) > RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH:
            raise RuntimeError("Recoverable Items $batch exceeds the per-mailbox request limit.")

        return requests, request_map

    def _execute_batch(self, requests: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Sends one $batch synchronously. @returns Map of request id to response."""
        logger = self.logger if self.logger is not None else (lambda x: None)
        token_manager = self.url_invoker.token_manager
        token_data = token_manager.get_valid_token_slot(logger)
        try:
            return self.url_invoker.execute_batch_request(
                token_manager.get_session(),
                f"{GRAPH_BASE_URL}/$batch",
                token_manager,
                token_data,
                requests,
                logger,
                self.stop_event,
                self.get_resource_type(),
            )
        except Exception as e:
            logger(f"Error in {self.get_resource_type()}: {e}")
            return {}
        finally:
            token_manager.return_token_slot(token_data)

    def _handle_response(
            self,
            response: Optional[Dict[str, Any]],
            item: _WorkItem,
            mailbox: str,
            counts: Dict[str, int],
            queue: Deque[_WorkItem],
            visited: Set[str]
        ) -> Tuple[List[Dict[str, Any]], bool]:
        """Applies one response to the mailbox's counts and queue.

        @returns (failures, whether the request itself failed)
        """
        resource = item.label if item.folder_id is None else f"{item.label} ({item.folder_id})"

        def failure(failure_type: FailureType, status_code: Optional[int], message: str) -> Dict[str, Any]:
            return {
                "userId": mailbox,
                "folderId": resource,
                "isPartial": True,
                "type": failure_type,
                "statusCode": status_code,
                "message": message
            }

        if not response:
            return [failure(FailureType.NOT_FOUND, None, "No response received for the request")], True

        status = response.get("status")
        body = response.get("body") or {}
        if status != 200:
            error_message = body.get("error", {}).get("message") if isinstance(body, dict) else None
            if status == 404:
                return [failure(FailureType.NOT_FOUND, status, error_message or "Folder not found")], True
            return [failure(FailureType.FAILURE_STATUS_CODE_ERROR, status, error_message or "Unknown error")], True

        if item.kind == "count":
            if "@odata.count" not in body:
                return [failure(FailureType.INVALID_DATA, status, "No @odata.count in response")], True
            try:
                counts[item.label] += int(body["@odata.count"])
            except (ValueError, TypeError):
                return [failure(FailureType.INVALID_DATA, status, f"Invalid @odata.count '{body['@odata.count']}'")], True
            return [], False

        if item.kind == "root":
            if "id" not in body:
                return [failure(FailureType.INVALID_DATA, status, "No folder id in response")], True
            return self._apply_folders([body], item.label, 0, mailbox, counts, queue, visited, failure), False

        # Child folder page.
        if "value" not in body:
            return [failure(FailureType.INVALID_DATA, status, "No value in childFolders response")], True
        item_failures = self._apply_folders(body["value"], item.label, item.depth, mailbox, counts, queue, visited, failure)
        if "@odata.nextLink" in body:
            relative_url = get_relative_url(body["@odata.nextLink"], GRAPH_BASE_URL)
            if relative_url.startswith("http"):
                item_failures.append(failure(FailureType.INVALID_DATA, status, "Unexpected @odata.nextLink host"))
            else:
                queue.append(_WorkItem("children", item.label, f"/{relative_url}", depth=item.depth, folder_id=item.folder_id))
        return item_failures, False

    def _apply_folders(
            self,
            folders: List[Dict[str, Any]],
            label: str,
            depth: int,
            mailbox: str,
            counts: Dict[str, int],
            queue: Deque[_WorkItem],
            visited: Set[str],
            failure: Callable[[FailureType, Optional[int], str], Dict[str, Any]]
        ) -> List[Dict[str, Any]]:
        """Adds folders' totalItemCount and queues their children. @returns Failures."""
        item_failures = []
        for folder in folders:
            folder_id = folder.get("id")
            if folder_id is None or folder_id in visited:
                continue
            visited.add(folder_id)

            try:
                counts[label] += int(folder.get("totalItemCount", 0))
            except (ValueError, TypeError):
                item_failures.append(failure(FailureType.INVALID_DATA, 200, f"Invalid totalItemCount '{folder.get('totalItemCount')}' for folder {folder_id}"))

            try:
                child_count = int(folder.get("childFolderCount") or 0)
            except (ValueError, TypeError):
                item_failures.append(failure(FailureType.INVALID_DATA, 200, f"Invalid childFolderCount '{folder.get('childFolderCount')}' for folder {folder_id}"))
                child_count = 0

            if child_count > 0:
                if depth + 1 > self.config.max_allowed_depth:
                    message = f"Depth limit {self.config.max_allowed_depth} reached under {label}; deeper folders not counted"
                    if self.logger:
                        self.logger(f"[WARNING] Recoverable Items: {mailbox} | {message}")
                    item_failures.append(failure(FailureType.INVALID_DATA, 200, message))
                else:
                    queue.append(self._children_item(mailbox, label, folder_id, depth + 1))
        return item_failures
