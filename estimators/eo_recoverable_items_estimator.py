from collections import Counter
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple
import urllib.parse

from estimators.estimator import Estimator
from util.connectors import UrlInvoker
from util.constants import (
    GRAPH_BASE_URL,
    RECOVERABLE_ITEMS_FOLDERS,
    RECOVERABLE_ITEMS_MAILBOXES_PER_TASK,
    RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH,
)
from util.enums import FailureType
from util.thread_safe_ds import AtomicInt
from util.utils import ScanConfig

# Only plain emails are counted in $count mode.
IPM_NOTE_FILTER = "type eq 'IPM.Note'"


class EORecoverableItemsEstimator(Estimator):
    """Counts emails in the Recoverable Items folders of primary mailboxes.

    Mailboxes are identified by UPN. Each call processes its mailboxes in
    sequential groups of RECOVERABLE_ITEMS_MAILBOXES_PER_TASK, and each group in
    sequential rounds of one $batch. A round never carries more than
    RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH requests for the same
    mailbox, and a call never has more than one $batch in flight, so a mailbox
    owned by one call never sees more than that many concurrent requests.
    Callers must not pass the same mailbox to two concurrent calls.
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
        if self.config.recursive_email_scan:
            raise NotImplementedError("Folder hierarchy mode for Recoverable Items is not implemented yet.")

        # De-duplicate case-insensitively so a mailbox is only requested once.
        unique_mailboxes: Dict[str, str] = {}
        for mailbox in mailboxes:
            unique_mailboxes.setdefault(mailbox.strip().lower(), mailbox.strip())

        counts: Dict[str, Dict[str, int]] = {
            key: {label: 0 for label in RECOVERABLE_ITEMS_FOLDERS} for key in unique_mailboxes
        }

        keys = list(unique_mailboxes)
        for start in range(0, len(keys), RECOVERABLE_ITEMS_MAILBOXES_PER_TASK):
            if self.is_hard_stop_requested():
                break
            group = keys[start:start + RECOVERABLE_ITEMS_MAILBOXES_PER_TASK]
            self._count_group(group, unique_mailboxes, counts, failures)

        for folder_counts in counts.values():
            for label, count in folder_counts.items():
                self.folder_totals[label].increment(count)

        return {mailbox: dict(counts[mailbox.strip().lower()]) for mailbox in mailboxes}

    def get_folder_totals(self) -> Dict[str, int]:
        """@returns Aggregate count per folder label across all calls."""
        return {label: total.get_value() for label, total in self.folder_totals.items()}

    def _count_group(
            self,
            group: List[str],
            unique_mailboxes: Dict[str, str],
            counts: Dict[str, Dict[str, int]],
            failures: List[Dict[str, Any]]
        ) -> None:
        labels = list(RECOVERABLE_ITEMS_FOLDERS)
        rounds = [
            labels[i:i + RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH]
            for i in range(0, len(labels), RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH)
        ]
        folder_failures: Dict[str, List[Dict[str, Any]]] = {key: [] for key in group}

        for round_labels in rounds:
            if self.is_hard_stop_requested():
                break
            requests, request_map = self._build_round(group, unique_mailboxes, round_labels)
            responses = self._execute_batch(requests)

            for req_id, (key, label) in request_map.items():
                count, failure = self._parse_count_response(
                    responses.get(req_id), unique_mailboxes[key], label
                )
                if failure is not None:
                    folder_failures[key].append(failure)
                else:
                    counts[key][label] = count

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

    def _build_round(
            self,
            group: List[str],
            unique_mailboxes: Dict[str, str],
            round_labels: List[str]
        ) -> Tuple[List[Dict[str, Any]], Dict[str, Tuple[str, str]]]:
        requests: List[Dict[str, Any]] = []
        request_map: Dict[str, Tuple[str, str]] = {}
        encoded_filter = urllib.parse.quote(IPM_NOTE_FILTER, safe="")

        for key in group:
            encoded_mailbox = urllib.parse.quote(unique_mailboxes[key], safe="@")
            for label in round_labels:
                req_id = str(len(requests))
                requests.append({
                    "id": req_id,
                    "method": "GET",
                    "url": (
                        f"/admin/exchange/mailboxes/{encoded_mailbox}/folders/"
                        f"{RECOVERABLE_ITEMS_FOLDERS[label]}/items"
                        f"?$filter={encoded_filter}&$count=true&$top=1&$select=id"
                    ),
                    "headers": {"ConsistencyLevel": "eventual"},
                })
                request_map[req_id] = (key, label)

        per_mailbox = Counter(key for key, _ in request_map.values())
        if per_mailbox and max(per_mailbox.values()) > RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH:
            raise RuntimeError("Recoverable Items round exceeds the per-mailbox request limit.")

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

    def _parse_count_response(
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
            return failure(FailureType.NOT_FOUND, None, "No response received for the request")

        status = response.get("status")
        body = response.get("body") or {}
        error_message = body.get("error", {}).get("message") if isinstance(body, dict) else None

        if status == 200:
            if "@odata.count" not in body:
                return failure(FailureType.INVALID_DATA, status, "No @odata.count in response")
            try:
                return int(body["@odata.count"]), None
            except (ValueError, TypeError):
                return failure(FailureType.INVALID_DATA, status, f"Invalid @odata.count '{body['@odata.count']}'")
        if status == 404:
            return failure(FailureType.NOT_FOUND, status, error_message or "Folder not found")
        return failure(FailureType.FAILURE_STATUS_CODE_ERROR, status, error_message or "Unknown error")
