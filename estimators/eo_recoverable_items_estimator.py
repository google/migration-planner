from concurrent.futures import Future, ThreadPoolExecutor
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple
import urllib.parse

from estimators.estimator import Estimator
from util.connectors import UrlInvoker
from util.constants import (
    GRAPH_BASE_URL,
    RECOVERABLE_ITEMS_FOLDERS,
    RECOVERABLE_ITEMS_ITEM_TYPES,
    RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH,
)
from util.enums import FailureType
from util.thread_safe_ds import AtomicInt
from util.utils import ScanConfig, create_batches, get_batch_responses_map

# The admin API rejects `type in (...)`, so the item types are or-ed together.
ITEM_TYPE_FILTER = " or ".join(f"type eq '{item_type}'" for item_type in RECOVERABLE_ITEMS_ITEM_TYPES)
FOLDER_COUNT_API = (
    "admin/exchange/mailboxes/{mailbox}/folders/{folder}/items?$filter="
    + urllib.parse.quote(ITEM_TYPE_FILTER, safe="")
    + "&$count=true&$top=1&$select=id"
)


class EORecoverableItemsEstimator(Estimator):
    """Counts emails in the Recoverable Items folders of primary mailboxes.

    Mailboxes are identified by UPN. These folders never contain subfolders, so
    each folder needs a single server-side $count request over the item types in
    RECOVERABLE_ITEMS_ITEM_TYPES, whatever the Recursive setting (totalItemCount
    can't be filtered by item type).

    The folders are requested in sequential rounds of
    RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH folders, so a mailbox
    never has more than that many requests in flight. Callers must not pass the
    same mailbox to two concurrent calls.
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
        self.archive_executor = ThreadPoolExecutor(max_workers=self.config.concurrency)
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
        folder_failures: Dict[str, List[Dict[str, Any]]] = {key: [] for key in unique_mailboxes}

        labels = list(RECOVERABLE_ITEMS_FOLDERS)
        for start in range(0, len(labels), RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH):
            if self.is_hard_stop_requested():
                break
            round_labels = labels[start:start + RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH]
            self._count_round(unique_mailboxes, round_labels, counts, folder_failures)

        for key, mailbox_failures in folder_failures.items():
            if not mailbox_failures:
                continue
            if len(mailbox_failures) == len(RECOVERABLE_ITEMS_FOLDERS):
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

        for folder_counts in counts.values():
            for label, count in folder_counts.items():
                self.folder_totals[label].increment(count)

        return {mailbox: dict(counts[mailbox.strip().lower()]) for mailbox in mailboxes}

    def get_folder_totals(self) -> Dict[str, int]:
        """@returns Aggregate count per folder label across all calls."""
        return {label: total.get_value() for label, total in self.folder_totals.items()}

    def _count_round(
            self,
            unique_mailboxes: Dict[str, str],
            round_labels: List[str],
            counts: Dict[str, Dict[str, int]],
            folder_failures: Dict[str, List[Dict[str, Any]]]
        ) -> None:
        # Map each request's identification headers back to (mailbox key, folder label).
        placeholder_to_target: Dict[Tuple[str, str], Tuple[str, str]] = {}
        placeholders = []
        for key, mailbox in unique_mailboxes.items():
            encoded_mailbox = urllib.parse.quote(mailbox, safe="@")
            for label in round_labels:
                well_known_name = RECOVERABLE_ITEMS_FOLDERS[label]
                placeholders.append({"mailbox": encoded_mailbox, "folder": well_known_name})
                placeholder_to_target[(encoded_mailbox, well_known_name)] = (key, label)

        batches = create_batches(FOLDER_COUNT_API, placeholders, self.config.graph_batch_size, True)

        futures_map: Dict[int, Future[List[Dict[str, Any]]]] = {}
        batch_id_to_batch_map: Dict[int, List[Dict[str, Any]]] = {}
        idx = 0
        for batch in batches:
            futures_map[idx] = self.archive_executor.submit(self.url_invoker.invoke, GRAPH_BASE_URL, batch, self.logger, self.stop_event, self.get_resource_type())
            batch_id_to_batch_map[idx] = batch
            idx += 1

        for batch_id, future in futures_map.items():
            try:
                responses = future.result()
            except Exception as e:
                if self.logger:
                    self.logger(f"Error in {self.get_resource_type()}: {e}")
                responses = []

            batch_responses_map = get_batch_responses_map(responses, self.logger)
            for req in batch_id_to_batch_map[batch_id]:
                key, label = placeholder_to_target[(req["headers"]["mailbox"], req["headers"]["folder"])]
                count, failure = self._parse_response(batch_responses_map.get(req["id"]), unique_mailboxes[key], label)
                if failure is not None:
                    folder_failures[key].append(failure)
                else:
                    counts[key][label] = count

    def _parse_response(
            self,
            response: Optional[Dict[str, Any]],
            mailbox: str,
            label: str
        ) -> Tuple[int, Optional[Dict[str, Any]]]:
        """@returns (count, None) on success, or (0, failure) on any error."""
        def failure(failure_type: FailureType, status_code: Optional[int], message: str):
            return 0, {
                "userId": mailbox,
                "folderId": label,
                "isPartial": True,
                "type": failure_type,
                "statusCode": status_code,
                "message": message
            }

        if not response:
            return failure(FailureType.NOT_FOUND, None, "No response found for the request")

        status = response.get("status")
        body = response.get("body") or {}
        if not isinstance(status, int) or status < 200 or status >= 300:
            error_message = body.get("error", {}).get("message") if isinstance(body, dict) else None
            return failure(FailureType.FAILURE_STATUS_CODE_ERROR, status, error_message or "Unknown error")

        if "@odata.count" not in body:
            return failure(FailureType.INVALID_DATA, status, "No @odata.count in response")
        try:
            return int(body["@odata.count"]), None
        except (ValueError, TypeError):
            return failure(FailureType.INVALID_DATA, status, f"Invalid @odata.count '{body['@odata.count']}'")
