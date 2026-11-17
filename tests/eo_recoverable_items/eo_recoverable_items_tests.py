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
    200 with `counts[(mailbox, label)]` in both @odata.count and totalItemCount.
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
                response = self.responses.get(target, {"status": 200, "body": {"@odata.count": count, "totalItemCount": count}})
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

    def test_recursive_setting_reads_folder_total_item_count(self):
        self.config.recursive_email_scan = True
        self.graph.counts = {("a@x.com", "Deletions"): 8, ("a@x.com", "Versions"): 3}
        failures = []
        result = self.estimator.get_folder_counts(["a@x.com"], failures)

        self.assertEqual(result["a@x.com"], {"Deletions": 8, "Purges": 0, "DiscoveryHolds": 0, "Versions": 3})
        self.assertEqual(failures, [])
        for req in self._requests():
            self.assertRegex(req["url"], r"^admin/exchange/mailboxes/a@x\.com/folders/recoverableitems\w+\?\$select=id,totalItemCount$")

    def test_missing_count_field_is_partial_failure(self):
        for recursive in (False, True):
            with self.subTest(recursive=recursive):
                self.config.recursive_email_scan = recursive
                self.graph.responses = {("a@x.com", "Purges"): {"status": 200, "body": {"id": "x"}}}
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
        # chunks of min(concurrency // 10, 20), in both modes.
        self.graph.delay = 0.003
        mailboxes = [f"user{i}@x.com" for i in range(200)]
        for recursive in (False, True):
            for concurrency in (10, 30, 100):
                with self.subTest(recursive=recursive, concurrency=concurrency):
                    self.config.recursive_email_scan = recursive
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


if __name__ == "__main__":
    unittest.main()
