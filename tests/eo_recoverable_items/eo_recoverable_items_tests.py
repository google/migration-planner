import threading
import time
import unittest
import urllib.parse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock

from estimators.eo_recoverable_items_estimator import EORecoverableItemsEstimator
from util.constants import (
    RECOVERABLE_ITEMS_COLUMNS,
    RECOVERABLE_ITEMS_FOLDERS,
    RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH,
)
from util.enums import FailureType
from util.utils import ScanConfig

WELL_KNOWN_TO_LABEL = {wk: label for label, wk in RECOVERABLE_ITEMS_FOLDERS.items()}
RETRYABLE_STATUSES = (429, 500, 502, 503, 504)


def request_target(req: Dict[str, Any]) -> Tuple[str, str]:
    """@returns (mailbox, folder label) of a $batch sub-request."""
    return urllib.parse.unquote(req["headers"]["mailbox"]), WELL_KNOWN_TO_LABEL[req["headers"]["folder"]]


class FakeGraph:
    """Fake for UrlInvoker.invoke that records every $batch.

    Like UrlInvoker.invoke, it only returns 2xx responses and retryable
    (429/5xx) failures; other error responses are dropped. Responses default to
    200 with @odata.count = `counts[(mailbox, label)]`.
    `responses[(mailbox, label)]` overrides the response.
    """

    def __init__(self, delay: float = 0.0):
        self.counts: Dict[Tuple[str, str], int] = {}
        self.responses: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self.delay = delay
        self.batches: List[List[Dict[str, Any]]] = []
        self.lock = threading.Lock()
        self.in_flight_per_mailbox: Dict[str, int] = {}
        self.max_in_flight_per_mailbox = 0
        self.raise_error = False
        self.on_batch = None

    def invoke(self, url, batch, logger, stop_event=None, context=""):
        mailboxes = [request_target(req)[0] for req in batch]
        with self.lock:
            self.batches.append(batch)
            for mailbox in mailboxes:
                self.in_flight_per_mailbox[mailbox] = self.in_flight_per_mailbox.get(mailbox, 0) + 1
                self.max_in_flight_per_mailbox = max(self.max_in_flight_per_mailbox, self.in_flight_per_mailbox[mailbox])
        try:
            if self.delay:
                time.sleep(self.delay)
            if self.on_batch is not None:
                self.on_batch()
            if self.raise_error:
                raise ConnectionError("network down")
            results = []
            for req in batch:
                target = request_target(req)
                count = self.counts.get(target, 0)
                response = self.responses.get(target, {"status": 200, "body": {"@odata.count": count}})
                status = response["status"]
                if 200 <= status < 300 or status in RETRYABLE_STATUSES:
                    results.append(dict(response, id=str(req["id"])))
            return results
        finally:
            with self.lock:
                for mailbox in mailboxes:
                    self.in_flight_per_mailbox[mailbox] -= 1


class TestEORecoverableItemsEstimator(unittest.TestCase):

    def setUp(self):
        self.config = ScanConfig(
            tenant_id="test-tenant",
            client_ids=["test-client"],
            client_secrets=["test-secret"],
            user_source="tenant",
            csv_path="",
            scan_email=True,
            scan_recoverable_items=True,
            recursive_email_scan=False,
            concurrency=10,
            retries=1,
            backoff=1,
        )
        self.stop_event = threading.Event()
        self.graph = FakeGraph()
        self.url_invoker = MagicMock()
        self.url_invoker.invoke.side_effect = self.graph.invoke
        self.estimator = self._make_estimator()

    def tearDown(self):
        self.estimator.shutdown()

    def _make_estimator(self):
        return EORecoverableItemsEstimator(
            config=self.config,
            url_invoker=self.url_invoker,
            stop_event=self.stop_event,
        )

    def _requests(self) -> List[Dict[str, Any]]:
        return [req for batch in self.graph.batches for req in batch]

    def test_counts_per_folder_and_totals(self):
        self.graph.counts = {
            ("a@x.com", "Deletions"): 5, ("a@x.com", "Purges"): 7,
            ("a@x.com", "DiscoveryHolds"): 11, ("a@x.com", "Versions"): 13,
            ("b@x.com", "Purges"): 2,
        }
        failures = []
        result = self.estimator.get_folder_counts(["a@x.com", "b@x.com"], failures)

        self.assertEqual(result["a@x.com"], {"Deletions": 5, "Purges": 7, "DiscoveryHolds": 11, "Versions": 13})
        self.assertEqual(result["b@x.com"], {"Deletions": 0, "Purges": 2, "DiscoveryHolds": 0, "Versions": 0})
        self.assertEqual(failures, [])
        self.assertEqual(self.estimator.get_folder_totals(), {"Deletions": 5, "Purges": 9, "DiscoveryHolds": 11, "Versions": 13})

    def test_calculate_resource_count_returns_totals(self):
        self.graph.counts = {("a@x.com", label): 1 for label in RECOVERABLE_ITEMS_FOLDERS}
        result = self.estimator.calculate_resource_count({"user_ids": ["a@x.com"]}, [])
        self.assertEqual(result, {"a@x.com": 4})

    def test_count_request_shape(self):
        self.estimator.get_folder_counts(["o'neil#x@x.com"], [])
        requests = self._requests()

        self.assertEqual(len(requests), len(RECOVERABLE_ITEMS_FOLDERS))
        for req in requests:
            self.assertEqual(req["method"], "GET")
            self.assertEqual(req["headers"]["ConsistencyLevel"], "eventual")
            self.assertTrue(req["url"].startswith("admin/exchange/mailboxes/o%27neil%23x@x.com/folders/"))
            self.assertTrue(req["url"].endswith("/items?$filter=type%20eq%20%27IPM.Note%27&$count=true&$top=1&$select=id"))
        self.assertEqual({request_target(req)[1] for req in requests}, set(RECOVERABLE_ITEMS_FOLDERS))

    def test_recursive_setting_does_not_change_requests(self):
        self.estimator.get_folder_counts(["a@x.com"], [])
        urls_off = sorted(req["url"] for req in self._requests())
        self.graph.batches.clear()
        self.config.recursive_email_scan = True
        self.estimator.get_folder_counts(["a@x.com"], [])
        urls_on = sorted(req["url"] for req in self._requests())

        self.assertEqual(urls_off, urls_on)
        self.assertTrue(all("/items?$filter=" in url for url in urls_on))

    def test_missing_count_is_partial_failure(self):
        self.graph.responses = {("a@x.com", "Purges"): {"status": 200, "body": {"value": []}}}
        failures = []
        result = self.estimator.get_folder_counts(["a@x.com"], failures)

        self.assertEqual(result["a@x.com"]["Purges"], 0)
        self.assertEqual(len(failures), 1)
        self.assertTrue(failures[0]["isPartial"])
        self.assertEqual(failures[0]["type"], FailureType.INVALID_DATA)
        self.assertEqual(failures[0]["folderId"], "Purges")
        self.assertEqual(failures[0]["userId"], "a@x.com")

    def test_error_responses_are_partial_failures(self):
        self.graph.counts = {("a@x.com", "Deletions"): 3}
        self.graph.responses = {
            ("a@x.com", "DiscoveryHolds"): {"status": 404, "body": {"error": {"message": "Folder not found."}}},
            ("a@x.com", "Versions"): {"status": 503, "body": {"error": {"message": "Service unavailable."}}},
        }
        failures = []
        result = self.estimator.get_folder_counts(["a@x.com"], failures)

        self.assertEqual(result["a@x.com"]["Deletions"], 3)
        by_folder = {f["folderId"]: f for f in failures}
        self.assertEqual(set(by_folder), {"DiscoveryHolds", "Versions"})
        # invoke drops non-retryable errors, so the 404 surfaces as a missing response.
        self.assertEqual(by_folder["DiscoveryHolds"]["type"], FailureType.NOT_FOUND)
        self.assertEqual(by_folder["Versions"]["type"], FailureType.FAILURE_STATUS_CODE_ERROR)
        self.assertEqual(by_folder["Versions"]["statusCode"], 503)
        self.assertEqual(by_folder["Versions"]["message"], "Service unavailable.")
        self.assertTrue(all(f["isPartial"] for f in failures))

    def test_all_folders_failed_is_single_full_failure(self):
        for label in RECOVERABLE_ITEMS_FOLDERS:
            self.graph.responses[("a@x.com", label)] = {"status": 404, "body": {"error": {"message": "Mailbox not found."}}}
        self.graph.counts = {("b@x.com", "Deletions"): 4}
        failures = []
        result = self.estimator.get_folder_counts(["a@x.com", "b@x.com"], failures)

        self.assertEqual(sum(result["a@x.com"].values()), 0)
        self.assertEqual(result["b@x.com"]["Deletions"], 4)
        self.assertEqual(len(failures), 1)
        self.assertFalse(failures[0]["isPartial"])
        self.assertEqual(failures[0]["userId"], "a@x.com")

    def test_batch_exception_fails_all_folders(self):
        self.graph.raise_error = True
        failures = []
        result = self.estimator.get_folder_counts(["a@x.com"], failures)

        self.assertEqual(sum(result["a@x.com"].values()), 0)
        self.assertEqual(len(failures), 1)
        self.assertFalse(failures[0]["isPartial"])

    def test_batch_composition_and_rounds(self):
        mailboxes = [f"user{i}@x.com" for i in range(25)]
        self.estimator.get_folder_counts(mailboxes, [])

        for batch in self.graph.batches:
            self.assertLessEqual(len(batch), self.config.graph_batch_size)
        # Round 1 (first two folders) completes before round 2 starts.
        labels = list(RECOVERABLE_ITEMS_FOLDERS)
        batch_labels = [{request_target(req)[1] for req in batch} for batch in self.graph.batches]
        first_round = set(labels[:RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH])
        round_of_batch = [0 if labels_in_batch <= first_round else 1 for labels_in_batch in batch_labels]
        self.assertEqual(round_of_batch, sorted(round_of_batch))
        # Every (mailbox, folder) requested exactly once.
        targets = [request_target(req) for req in self._requests()]
        self.assertEqual(len(targets), len(set(targets)))
        self.assertEqual(len(targets), len(mailboxes) * len(RECOVERABLE_ITEMS_FOLDERS))

    def test_duplicate_mailboxes_requested_once(self):
        self.graph.counts = {("A@x.com", "Deletions"): 9}
        result = self.estimator.get_folder_counts(["A@x.com", "a@X.com", "b@x.com"], [])

        requested = [request_target(req)[0].lower() for req in self._requests()]
        self.assertEqual(requested.count("a@x.com"), len(RECOVERABLE_ITEMS_FOLDERS))
        self.assertEqual(result["A@x.com"]["Deletions"], 9)
        self.assertEqual(result["a@X.com"]["Deletions"], 9)
        self.assertEqual(self.estimator.get_folder_totals()["Deletions"], 9)

    def test_parallel_calls_never_exceed_per_mailbox_limit(self):
        # Mirrors the scan pipeline: concurrent calls over disjoint mailbox
        # chunks of min(concurrency // 10, 20).
        self.graph.delay = 0.003
        mailboxes = [f"user{i}@x.com" for i in range(200)]
        for concurrency in (10, 30, 100):
            with self.subTest(concurrency=concurrency):
                self.config.concurrency = concurrency
                estimator = self._make_estimator()
                chunk_size = min(concurrency // 10, 20)
                tasks = [mailboxes[i:i + chunk_size] for i in range(0, len(mailboxes), chunk_size)]
                try:
                    with ThreadPoolExecutor(max_workers=concurrency) as executor:
                        list(executor.map(lambda task: estimator.get_folder_counts(task, []), tasks))
                finally:
                    estimator.shutdown()

                self.assertLessEqual(self.graph.max_in_flight_per_mailbox, RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH)

    def test_stop_event_halts_next_round(self):
        def stop_after_first_batch():
            self.stop_event.set()
        self.graph.on_batch = stop_after_first_batch
        self.graph.counts = {("a@x.com", "Deletions"): 1, ("a@x.com", "Versions"): 1}
        failures = []
        result = self.estimator.get_folder_counts(["a@x.com"], failures)

        self.assertEqual(len(self.graph.batches), 1)
        self.assertEqual(result["a@x.com"]["Deletions"], 1)
        self.assertEqual(result["a@x.com"]["Versions"], 0)
        self.assertEqual(failures, [])

    def test_invalid_input(self):
        with self.assertRaises(Exception):
            self.estimator.get_folder_counts([], [])
        with self.assertRaises(Exception):
            self.estimator.get_folder_counts(["a@x.com", None], [])


class _Var:
    """Minimal stand-in for a Tk variable."""

    def __init__(self, value):
        self._value = value

    def get(self):
        return self._value


class FakeRecoverableItemsEstimator:
    """Records get_folder_counts calls and returns fixed per-folder counts."""

    def __init__(self, counts: Dict[str, Dict[str, int]], partial_failures: Optional[Dict[str, str]] = None):
        self.counts = counts
        self.partial_failures = partial_failures or {}
        self.calls: List[List[str]] = []
        self.lock = threading.Lock()
        self.on_call = None

    def get_folder_counts(self, mailboxes, failures):
        with self.lock:
            self.calls.append(list(mailboxes))
        if self.on_call is not None:
            self.on_call()
        for mailbox in mailboxes:
            if mailbox.lower() in self.partial_failures:
                failures.append({
                    "userId": mailbox, "folderId": self.partial_failures[mailbox.lower()], "isPartial": True,
                    "type": FailureType.NOT_FOUND, "statusCode": None, "message": "No response found for the request",
                })
        empty = {label: 0 for label in RECOVERABLE_ITEMS_FOLDERS}
        return {mailbox: dict(self.counts.get(mailbox.lower(), empty)) for mailbox in mailboxes}

    def shutdown(self):
        pass


def make_row(upn: str, row_type: str = "User") -> Dict[str, Any]:
    row = {
        "User Principal Name / Group Mail": upn,
        "User ID / Group ID": f"id-{upn}",
        "Type": row_type,
        "Email Count": 0,
        "Encrypted Email Count": 0,
        "Contact Count": 0,
        "Calendar Count": 0,
        "Event Count": 0,
        "In Place Archive Count": 0,
        "Shared Mail Count": 0,
        "Group Post Count": 0,
        "Group Thread Count": 0,
    }
    row.update({column: 0 for column in RECOVERABLE_ITEMS_COLUMNS.values()})
    return row


class TestRecoverableItemsScanPipeline(unittest.TestCase):

    def setUp(self):
        from ui.exchange_online_ui import MigrationEstimatorTool

        self.config = ScanConfig(
            tenant_id="test-tenant",
            client_ids=["test-client"],
            client_secrets=["test-secret"],
            user_source="tenant",
            csv_path="",
            scan_email=False,
            scan_recoverable_items=True,
            concurrency=30,
            retries=1,
            backoff=1,
        )
        self.tool = object.__new__(MigrationEstimatorTool)
        self.tool.stop_scan_event = threading.Event()
        self.tool.log_msg = MagicMock()
        self.tool.ui_update = MagicMock()
        self.tool.concurrency = _Var(self.config.concurrency)
        self.tool.id_to_display_name = {}
        self.tool.factory = MagicMock()
        self.stats = {"recoverable_items": 0}

    def _logs(self) -> List[str]:
        return [call.args[0] for call in self.tool.log_msg.call_args_list]

    def test_phase_deduplicates_chunks_and_fans_out(self):
        counts = {label: i + 1 for i, label in enumerate(RECOVERABLE_ITEMS_FOLDERS)}  # 1, 2, 3, 4
        estimator = FakeRecoverableItemsEstimator(
            {"a@x.com": counts, "s@x.com": counts}, partial_failures={"s@x.com": "Purges"}
        )
        self.tool.factory.get_recoverable_items_estimator.return_value = estimator
        rows = [make_row("a@x.com"), make_row("s@x.com", "Shared"), make_row("A@x.com")]  # duplicate of a@x.com
        rows += [make_row(f"u{i}@x.com") for i in range(5)]

        failures, partial = self.tool._run_recoverable_items_phase(self.config, MagicMock(), rows, self.stats)

        requested = [mailbox.lower() for call in estimator.calls for mailbox in call]
        self.assertEqual(len(requested), len(set(requested)))
        self.assertEqual(set(requested), {"a@x.com", "s@x.com"} | {f"u{i}@x.com" for i in range(5)})
        self.assertTrue(all(len(call) <= min(self.config.concurrency // 10, 20) for call in estimator.calls))
        for row in rows[:3]:
            self.assertEqual([row[column] for column in RECOVERABLE_ITEMS_COLUMNS.values()], [1, 2, 3, 4])
        self.assertEqual(self.stats["recoverable_items"], 30)
        self.assertEqual(failures, [])
        self.assertEqual(len(partial), 1)
        self.assertEqual(partial[0]["user"], "s@x.com")
        self.assertEqual(partial[0]["failed_resource"], "Purges")

        logs = self._logs()
        self.assertTrue(any("8 rows -> 7 unique mailboxes (6 User / 1 Shared)" in line for line in logs))
        self.assertIn("Recoverable Items Summary (IPM.Note)", logs)
        self.assertTrue(any(line.strip() == "Deletions 2 | Purges 4 | DiscoveryHolds 6 | Versions 8" for line in logs))
        self.assertTrue(any("Failed mailboxes  : 0 | Partially failed: 1" in line for line in logs))

    def test_scan_phases_only_send_user_and_shared_rows(self):
        captured = {}

        def fake_phase(config, manager, rows, stats):
            captured["rows"] = rows
            return [], []

        self.tool._run_recoverable_items_phase = fake_phase
        users = [make_row("a@x.com")]
        shared = [make_row("s@x.com", "Shared")]
        groups = [make_row("g@x.com", "Group Mailbox")]
        stats = {key: 0 for key in ("emails", "encrypted_emails", "contacts", "calendars", "events", "in_place_archives",
                                    "shared_mails", "group_mails", "group_threads", "recoverable_items")}

        self.tool._run_scan_phases(self.config, MagicMock(), None, users, shared, groups, stats)

        self.assertEqual([row["User Principal Name / Group Mail"] for row in captured["rows"]], ["a@x.com", "s@x.com"])

    def test_permission_scope_only_when_selected(self):
        self.tool._get_all_users_graph = MagicMock(return_value=[])
        for selected in (True, False):
            with self.subTest(selected=selected):
                self.config.scan_recoverable_items = selected
                manager = MagicMock()
                self.tool._resolve_target_users(self.config, manager)
                scopes = manager.authenticate_all.call_args.kwargs["required_scopes"]
                self.assertEqual("MailboxItem.Read.All" in scopes, selected)

    def test_stop_is_logged(self):
        estimator = FakeRecoverableItemsEstimator({})
        estimator.on_call = self.tool.stop_scan_event.set
        self.tool.factory.get_recoverable_items_estimator.return_value = estimator
        self.config.concurrency = 10
        rows = [make_row(f"u{i}@x.com") for i in range(50)]

        self.tool._run_recoverable_items_phase(self.config, MagicMock(), rows, self.stats)

        self.assertTrue(any(line.startswith("Recoverable Items scan stopped:") for line in self._logs()))


if __name__ == "__main__":
    unittest.main()
