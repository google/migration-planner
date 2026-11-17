import glob
import os
import tempfile
import threading
import time
import unittest
import urllib.parse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

from estimators.eo_recoverable_items_estimator import EORecoverableItemsEstimator
from util.constants import (
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
    """Records calls and returns fixed per-folder counts, like the real estimator's API."""

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

    def calculate_resource_count(self, data, failures):
        folder_counts = self.get_folder_counts(data["user_ids"], failures)
        return {upn: sum(counts.values()) for upn, counts in folder_counts.items()}

    def get_folder_totals(self):
        requested = {mailbox.lower() for call in self.calls for mailbox in call}
        return {
            label: sum(self.counts.get(mailbox, {}).get(label, 0) for mailbox in requested)
            for label in RECOVERABLE_ITEMS_FOLDERS
        }

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
    row["Recoverable Email Count"] = 0
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
            self.assertEqual(row["Recoverable Email Count"], 10)
        self.assertEqual(self.stats["recoverable_items"], 30)
        self.assertEqual(self.tool.recoverable_folder_totals, {"Deletions": 2, "Purges": 4, "DiscoveryHolds": 6, "Versions": 8})
        self.assertEqual(failures, [])
        self.assertEqual(len(partial), 1)
        self.assertEqual(partial[0]["user"], "s@x.com")
        self.assertEqual(partial[0]["failed_resource"], "Purges")


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

    def test_phase_logs_same_events_as_primary_scan(self):
        # Like the primary mailbox scan: a phase header and periodic progress lines only.
        self.tool.factory.get_recoverable_items_estimator.return_value = FakeRecoverableItemsEstimator({})
        rows = [make_row(f"u{i}@x.com") for i in range(100)]

        self.tool._run_recoverable_items_phase(self.config, MagicMock(), rows, self.stats)

        logs = self._logs()
        self.assertEqual(logs[0], "\n--- Starting RECOVERABLE_ITEMS Scan ---")
        self.assertTrue(all(line.startswith("Processed ") for line in logs[1:]))
        self.assertTrue(logs[-1].startswith("Processed 100/100"))

    def test_runs_right_after_primary_email_scan(self):
        order = []
        self.config.scan_email = True
        self.config.scan_encrypted_email = True
        self.config.scan_contact = True
        self.tool.run_batch_phase_ui = lambda chunks, res_type, *args: order.append(res_type) or []
        self.tool._run_recoverable_items_phase = lambda *args: order.append("recoverable_items") or ([], [])
        stats = {key: 0 for key in ("emails", "encrypted_emails", "contacts", "calendars", "events", "in_place_archives",
                                    "shared_mails", "group_mails", "group_threads", "recoverable_items")}

        self.tool._run_scan_phases(self.config, MagicMock(), None, [make_row("a@x.com")], [], [], stats)

        self.assertEqual(order, ["messages", "recoverable_items", "encrypted_messages", "contacts"])


ALL_STATS_KEYS = ("emails", "encrypted_emails", "contacts", "calendars", "events", "in_place_archives",
                  "shared_mails", "group_mails", "group_threads", "recoverable_items")


class TestRecoverableItemsCsv(unittest.TestCase):

    def setUp(self):
        from ui.exchange_online_ui import MigrationEstimatorTool

        self.tmp = tempfile.TemporaryDirectory()
        self.config = ScanConfig(
            tenant_id="", client_ids=[], client_secrets=[],
            user_source="csv", csv_path=os.path.join(self.tmp.name, "input.csv"),
            scan_email=False, scan_recoverable_items=True,
            concurrency=10, retries=1, backoff=1,
        )
        self.tool = object.__new__(MigrationEstimatorTool)
        self.tool.stop_scan_event = threading.Event()
        self.tool.log_msg = MagicMock()
        self.tool.ui_update = MagicMock()
        self.tool.concurrency = _Var(self.config.concurrency)
        self.tool.id_to_display_name = {}
        self.tool.factory = MagicMock()

    def tearDown(self):
        self.tmp.cleanup()

    def _write_csv(self, columns: List[str], rows: List[List[Any]]):
        import pandas as pd
        pd.DataFrame(rows, columns=columns).to_csv(self.config.csv_path, index=False)

    def _logs(self) -> List[str]:
        return [call.args[0] for call in self.tool.log_msg.call_args_list]

    def test_csv_with_column_is_reused_without_scanning(self):
        self._write_csv(["Email Id", "Type", "Recoverable Email Count"], [["a@x.com", "User", 10], ["s@x.com", "Shared", 26]])

        all_users, existing_data, groups, shared = self.tool._resolve_target_users(self.config, None)
        rows, stats = self.tool._prepare_batch_list(self.config, all_users, existing_data, groups, shared)
        self.tool._run_scan_phases(self.config, None, None, rows, [], [], dict(stats))

        self.assertIn("Skipping Authentication (All required data present in CSV).", self._logs())
        self.assertIn("Skipping Recoverable Emails Scan (Data present or No Auth)", self._logs())
        by_upn = {row["User Principal Name / Group Mail"]: row for row in rows}
        self.assertEqual(by_upn["a@x.com"]["Recoverable Email Count"], 10)
        self.assertEqual(by_upn["s@x.com"]["Recoverable Email Count"], 26)
        self.assertEqual(stats["recoverable_items"], 36)
        self.tool.factory.get_recoverable_items_estimator.assert_not_called()

    def test_older_report_without_column_triggers_delta_scan(self):
        self.config.scan_email = True
        self._write_csv(["Email Id", "Type", "Email Count"], [["a@x.com", "User", 5]])
        self.tool._resolve_from_csv = MagicMock(return_value=[{"userPrincipalName": "a@x.com", "id": "id-a"}])

        with self.assertRaises(Exception):
            self.tool._resolve_target_users(self.config, None)

        manager = MagicMock()
        all_users, existing_data, groups, shared = self.tool._resolve_target_users(self.config, manager)
        self.assertEqual(manager.authenticate_all.call_args.kwargs["required_scopes"], ["User.Read.All", "MailboxItem.Read.All"])
        rows, _ = self.tool._prepare_batch_list(self.config, all_users, existing_data, groups, shared)
        self.assertEqual(rows[0]["Email Count"], 5)
        self.assertEqual(rows[0]["Recoverable Email Count"], 0)

    def test_older_report_is_unaffected_when_unselected(self):
        self.config.scan_email = True
        self.config.scan_recoverable_items = False
        self._write_csv(["Email Id", "Type", "Email Count"], [["a@x.com", "User", 5]])

        all_users, existing_data, groups, shared = self.tool._resolve_target_users(self.config, None)
        rows, stats = self.tool._prepare_batch_list(self.config, all_users, existing_data, groups, shared)

        self.assertIn("Skipping Authentication (All required data present in CSV).", self._logs())
        self.assertEqual(rows[0]["Email Count"], 5)
        self.assertEqual(stats["recoverable_items"], 0)

    def test_blank_values_load_as_zero(self):
        self._write_csv(["Email Id", "Type", "Recoverable Email Count"], [["a@x.com", "User", None], ["b@x.com", "User", 7]])

        all_users, existing_data, groups, shared = self.tool._resolve_target_users(self.config, None)
        rows, _ = self.tool._prepare_batch_list(self.config, all_users, existing_data, groups, shared)

        by_upn = {row["User Principal Name / Group Mail"]: row for row in rows}
        self.assertEqual(by_upn["a@x.com"]["Recoverable Email Count"], 0)
        self.assertEqual(by_upn["b@x.com"]["Recoverable Email Count"], 7)

    def _export(self, scan_recoverable_items: bool, folder_totals=None) -> "pd.DataFrame":
        import pandas as pd

        self.config.scan_email = True
        self.config.scan_recoverable_items = scan_recoverable_items
        self.tool.recoverable_folder_totals = folder_totals
        rows = [make_row("a@x.com"), make_row("b@x.com")]
        rows[0]["Email Count"] = 10
        rows[0]["Recoverable Email Count"] = 10   # b@x.com keeps 0, as after a failed scan
        stats = {key: 0 for key in ALL_STATS_KEYS}
        stats["recoverable_items"] = 10
        monitor = MagicMock()
        monitor.get_stats.return_value = (0.0, 0.0, 0.0, 0.0)
        self.tool.calculate_migration_batches = lambda df: (df.assign(**{"Suggested Batch": "Batch 1"}), [], 0.0, [])

        cwd = os.getcwd()
        os.chdir(self.tmp.name)
        try:
            with patch("ui.exchange_online_ui.time.sleep"):
                self.tool._generate_final_report(self.config, rows, [], [], stats, monitor, time.time())
            report = glob.glob(os.path.join("outputs", "*", "user_report_*.csv"))[0]
            return pd.read_csv(report)
        finally:
            os.chdir(cwd)

    def test_report_has_recoverable_email_count_column(self):
        df = self._export(True, {"Deletions": 1, "Purges": 2, "DiscoveryHolds": 3, "Versions": 4})
        columns = list(df.columns)
        self.assertEqual(columns[columns.index("Email Count") + 1], "Recoverable Email Count")
        self.assertEqual(df["Recoverable Email Count"].tolist(), [10, 0])
        self.assertIn("Recoverable Emails: 10 (Deletions 1 | Purges 2 | DiscoveryHolds 3 | Versions 4)", self._logs())

    def test_final_totals_line_without_breakdown_when_reused_from_csv(self):
        self._export(True, None)
        self.assertIn("Recoverable Emails: 10", self._logs())

    def test_report_has_no_recoverable_column_when_unselected(self):
        df = self._export(False)
        self.assertNotIn("Recoverable Email Count", df.columns)
        self.assertFalse(any(line.startswith("Recoverable Emails:") for line in self._logs()))


class _EtaEstimator:
    """Uses the base Estimator ETA model without any network resources."""

    def calculate_migration_eta(self, data):
        from estimators.estimator import Estimator
        return Estimator.calculate_migration_eta(self, data)

    def shutdown(self):
        pass


class TestRecoverableItemsEta(unittest.TestCase):

    def setUp(self):
        from ui.exchange_online_ui import MigrationEstimatorTool

        self.config = ScanConfig(
            tenant_id="", client_ids=[], client_secrets=[], user_source="tenant", csv_path="",
            scan_email=True, scan_recoverable_items=True, concurrency=10, retries=1, backoff=1,
        )
        self.tool = object.__new__(MigrationEstimatorTool)
        self.tool.log_msg = MagicMock()
        self.tool.ui_update = MagicMock()
        self.tool.eta_min_users = _Var(1)
        self.tool.eta_max_users = _Var(1)
        self.tool.eta_max_batches = _Var(50)
        self.tool.parallel_batches = _Var(1)
        self.tool._get_scan_configuration = lambda: self.config
        self.tool.factory = MagicMock()
        self.tool.factory.get_in_place_archive_estimator.return_value = _EtaEstimator()
        self.tool.factory.get_shared_mailbox_estimator.return_value = _EtaEstimator()
        self.tool.factory.get_group_mailbox_estimator.return_value = _EtaEstimator()

    def _plan(self, rows: List[Dict[str, Any]], drop_recoverable_column: bool = False):
        import pandas as pd
        df = pd.DataFrame(rows)
        if drop_recoverable_column:
            df = df.drop(columns=["Recoverable Email Count"])
        return self.tool.calculate_migration_batches(df)

    def _row(self, upn: str, row_type: str = "User", email: int = 0, shared: int = 0, recoverable: int = 0):
        row = make_row(upn, row_type)
        row["Email Count"] = email
        row["Shared Mail Count"] = shared
        row["Recoverable Email Count"] = recoverable
        return row

    def test_older_data_without_column_matches_zero_counts(self):
        rows = [self._row("a@x.com", email=500), self._row("s@x.com", "Shared", shared=300)]
        _, batches_without, eta_without, _ = self._plan([dict(r) for r in rows], drop_recoverable_column=True)
        _, batches_zero, eta_zero, _ = self._plan([dict(r) for r in rows])

        self.assertEqual(eta_without, eta_zero)
        self.assertEqual([b["users"] for b in batches_without], [b["users"] for b in batches_zero])
        self.assertTrue(all(b["total_recoverable_emails"] == 0 for b in batches_without))

    def test_user_recoverable_emails_join_the_email_stream(self):
        _, _, eta_with_recoverable, _ = self._plan([self._row("a@x.com", email=1000, recoverable=500)])
        _, _, eta_as_email, _ = self._plan([self._row("a@x.com", email=1500)])

        self.assertGreater(eta_with_recoverable, 0)
        self.assertAlmostEqual(eta_with_recoverable, eta_as_email)

    def test_shared_recoverable_emails_join_the_shared_stream(self):
        _, _, eta_with_recoverable, _ = self._plan([self._row("s@x.com", "Shared", shared=1000, recoverable=500)])
        _, _, eta_as_shared, _ = self._plan([self._row("s@x.com", "Shared", shared=1500)])

        self.assertGreater(eta_with_recoverable, 0)
        self.assertAlmostEqual(eta_with_recoverable, eta_as_shared)

    def test_recoverable_emails_sort_heavy_mailboxes_first(self):
        df_final, _, _, _ = self._plan([self._row("a@x.com", email=100), self._row("b@x.com", email=50, recoverable=200)])
        self.assertEqual(df_final["User Principal Name / Group Mail"].tolist(), ["b@x.com", "a@x.com"])

    def test_batches_report_recoverable_totals(self):
        self.tool.eta_max_users = _Var(5)
        self.tool.eta_min_users = _Var(5)
        _, batches, _, _ = self._plan([self._row("a@x.com", email=10, recoverable=3),
                                       self._row("s@x.com", "Shared", shared=10, recoverable=4)])
        self.assertEqual(sum(b["total_recoverable_emails"] for b in batches), 7)

    def test_result_data_includes_recoverable_totals(self):
        stats = {key: 0 for key in ALL_STATS_KEYS}
        stats.update({"emails": 100, "recoverable_items": 40})
        rows = [self._row("a@x.com", email=100, recoverable=40)]
        monitor = MagicMock()
        monitor.get_stats.return_value = (0.0, 0.0, 0.0, 0.0)
        self.tool.recoverable_folder_totals = None
        with tempfile.TemporaryDirectory() as tmp:
            cwd = os.getcwd()
            os.chdir(tmp)
            try:
                for selected, expected in ((True, 40), (False, None)):
                    with self.subTest(selected=selected):
                        self.config.scan_recoverable_items = selected
                        with patch("ui.exchange_online_ui.time.sleep"):
                            self.tool._generate_final_report(self.config, [dict(r) for r in rows], [], [], stats, monitor, time.time())
                        data = self.tool.ui_update.call_args_list[-1].kwargs["data"]
                        self.assertEqual(data["total_recoverable_emails"], expected)
                        self.assertEqual(data["total_items"], 140)
            finally:
                os.chdir(cwd)

if __name__ == "__main__":
    unittest.main()
