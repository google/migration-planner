from collections import Counter
import threading
import time
import unittest
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock

from estimators.eo_recoverable_items_estimator import EORecoverableItemsEstimator
from util.constants import (
    GRAPH_BASE_URL,
    RECOVERABLE_ITEMS_FOLDERS,
    RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH,
)
from util.enums import FailureType
from util.utils import ScanConfig

WELL_KNOWN_TO_LABEL = {wk: label for label, wk in RECOVERABLE_ITEMS_FOLDERS.items()}


def parse_request_url(url: str) -> Tuple[str, str]:
    """@returns (mailbox, folder label) from a recoverable items request URL."""
    path = url.split("?")[0]
    parts = path.strip("/").split("/")
    # admin/exchange/mailboxes/{mailbox}/folders/{wellKnown}/items
    return urllib.parse.unquote(parts[3]), WELL_KNOWN_TO_LABEL[parts[5]]


class FakeGraph:
    """Fake for UrlInvoker.execute_batch_request that records every $batch.

    Responses default to 200 with @odata.count from `counts`. Per (mailbox,
    label) overrides in `responses` replace the response; the value None drops
    the response entirely (as if retries were exhausted).
    """

    def __init__(self, counts: Optional[Dict[Tuple[str, str], int]] = None, delay: float = 0.0):
        self.counts = counts or {}
        self.responses: Dict[Tuple[str, str], Optional[Dict[str, Any]]] = {}
        self.delay = delay
        self.batches: List[List[Dict[str, Any]]] = []
        self.lock = threading.Lock()
        self.in_flight_per_mailbox: Dict[str, int] = {}
        self.max_in_flight_per_mailbox = 0
        self.raise_error = False
        self.on_batch = None

    def execute_batch_request(self, session, batch_url, token_manager, token_data, requests, logger, stop_event=None, context=""):
        mailboxes = [parse_request_url(req["url"])[0] for req in requests]
        with self.lock:
            self.batches.append(requests)
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
            result = {}
            for req in requests:
                key = parse_request_url(req["url"])
                if key in self.responses:
                    if self.responses[key] is not None:
                        result[req["id"]] = dict(self.responses[key], id=req["id"])
                    continue
                result[req["id"]] = {"id": req["id"], "status": 200, "body": {"@odata.count": self.counts.get(key, 0)}}
            return result
        finally:
            with self.lock:
                for mailbox in mailboxes:
                    self.in_flight_per_mailbox[mailbox] -= 1


class TestEORecoverableItemsCountMode(unittest.TestCase):

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
        self.url_invoker.execute_batch_request.side_effect = self.graph.execute_batch_request
        self.estimator = self._make_estimator()

    def _make_estimator(self):
        return EORecoverableItemsEstimator(
            config=self.config,
            url_invoker=self.url_invoker,
            stop_event=self.stop_event,
        )

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

    def test_request_shape(self):
        self.estimator.get_folder_counts(["o'neil#x@x.com"], [])
        requests = [req for batch in self.graph.batches for req in batch]

        self.assertEqual(len(requests), len(RECOVERABLE_ITEMS_FOLDERS))
        for req in requests:
            self.assertEqual(req["method"], "GET")
            self.assertEqual(req["headers"], {"ConsistencyLevel": "eventual"})
            self.assertTrue(req["url"].startswith("/admin/exchange/mailboxes/o%27neil%23x@x.com/folders/"))
            self.assertIn("/items?$filter=type%20eq%20%27IPM.Note%27&$count=true&$top=1&$select=id", req["url"])
        requested_folders = {parse_request_url(req["url"])[1] for req in requests}
        self.assertEqual(requested_folders, set(RECOVERABLE_ITEMS_FOLDERS))

    def test_missing_count_is_partial_failure(self):
        self.graph.counts = {("a@x.com", "Deletions"): 3}
        self.graph.responses[("a@x.com", "Purges")] = {"status": 200, "body": {"value": []}}
        failures = []
        result = self.estimator.get_folder_counts(["a@x.com"], failures)

        self.assertEqual(result["a@x.com"]["Purges"], 0)
        self.assertEqual(result["a@x.com"]["Deletions"], 3)
        self.assertEqual(len(failures), 1)
        self.assertTrue(failures[0]["isPartial"])
        self.assertEqual(failures[0]["type"], FailureType.INVALID_DATA)
        self.assertEqual(failures[0]["folderId"], "Purges")
        self.assertEqual(failures[0]["userId"], "a@x.com")

    def test_error_statuses_are_partial_failures(self):
        self.graph.responses[("a@x.com", "DiscoveryHolds")] = {
            "status": 404, "body": {"error": {"code": "ErrorFolderNotFound", "message": "Folder not found."}}
        }
        self.graph.responses[("a@x.com", "Versions")] = {
            "status": 403, "body": {"error": {"code": "ErrorAccessDenied", "message": "Access is denied."}}
        }
        self.graph.responses[("a@x.com", "Purges")] = None  # dropped after retries
        failures = []
        result = self.estimator.get_folder_counts(["a@x.com"], failures)

        self.assertEqual(sum(result["a@x.com"].values()), 0)
        by_folder = {f["folderId"]: f for f in failures}
        self.assertEqual(set(by_folder), {"DiscoveryHolds", "Versions", "Purges"})
        self.assertEqual(by_folder["DiscoveryHolds"]["type"], FailureType.NOT_FOUND)
        self.assertEqual(by_folder["DiscoveryHolds"]["statusCode"], 404)
        self.assertEqual(by_folder["Versions"]["type"], FailureType.FAILURE_STATUS_CODE_ERROR)
        self.assertEqual(by_folder["Versions"]["message"], "Access is denied.")
        self.assertEqual(by_folder["Purges"]["type"], FailureType.NOT_FOUND)
        self.assertIsNone(by_folder["Purges"]["statusCode"])
        self.assertTrue(all(f["isPartial"] for f in failures))

    def test_all_folders_failed_is_single_full_failure(self):
        for label in RECOVERABLE_ITEMS_FOLDERS:
            self.graph.responses[("a@x.com", label)] = {
                "status": 404, "body": {"error": {"message": "Mailbox not found."}}
            }
        self.graph.counts = {("b@x.com", "Deletions"): 4}
        failures = []
        result = self.estimator.get_folder_counts(["a@x.com", "b@x.com"], failures)

        self.assertEqual(sum(result["a@x.com"].values()), 0)
        self.assertEqual(result["b@x.com"]["Deletions"], 4)
        self.assertEqual(len(failures), 1)
        self.assertFalse(failures[0]["isPartial"])
        self.assertEqual(failures[0]["userId"], "a@x.com")
        self.assertIn("Mailbox not found.", failures[0]["message"])

    def test_batch_exception_fails_all_folders(self):
        self.graph.raise_error = True
        failures = []
        result = self.estimator.get_folder_counts(["a@x.com"], failures)

        self.assertEqual(sum(result["a@x.com"].values()), 0)
        self.assertEqual(len(failures), 1)
        self.assertFalse(failures[0]["isPartial"])

    def test_batch_composition_limits(self):
        mailboxes = [f"user{i}@x.com" for i in range(25)]
        self.estimator.get_folder_counts(mailboxes, [])

        mailboxes_per_batch = self.config.graph_batch_size // RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH
        batches_per_round = -(-len(mailboxes) // mailboxes_per_batch)
        rounds = -(-len(RECOVERABLE_ITEMS_FOLDERS) // RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH)
        self.assertEqual(len(self.graph.batches), batches_per_round * rounds)
        for batch in self.graph.batches:
            self.assertLessEqual(len(batch), self.config.graph_batch_size)
            per_mailbox: Dict[str, int] = {}
            for req in batch:
                mailbox = parse_request_url(req["url"])[0]
                per_mailbox[mailbox] = per_mailbox.get(mailbox, 0) + 1
            self.assertLessEqual(max(per_mailbox.values()), RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH)
        # Every (mailbox, folder) requested exactly once.
        requested = [parse_request_url(req["url"]) for batch in self.graph.batches for req in batch]
        self.assertEqual(len(requested), len(set(requested)))
        self.assertEqual(len(requested), len(mailboxes) * len(RECOVERABLE_ITEMS_FOLDERS))

    def test_duplicate_mailboxes_requested_once(self):
        self.graph.counts = {("A@x.com", "Deletions"): 9}
        result = self.estimator.get_folder_counts(["A@x.com", "a@X.com", "b@x.com"], [])

        requested = [parse_request_url(req["url"])[0].lower() for batch in self.graph.batches for req in batch]
        self.assertEqual(requested.count("a@x.com"), len(RECOVERABLE_ITEMS_FOLDERS))
        self.assertEqual(result["A@x.com"]["Deletions"], 9)
        self.assertEqual(result["a@X.com"]["Deletions"], 9)
        self.assertEqual(self.estimator.get_folder_totals()["Deletions"], 9)

    def test_parallel_calls_never_exceed_per_mailbox_limit(self):
        # Mirrors the scan pipeline: concurrent calls over disjoint mailbox
        # chunks of min(concurrency // 10, 20), for several slider values.
        self.graph.delay = 0.005
        mailboxes = [f"user{i}@x.com" for i in range(200)]
        for concurrency in (10, 30, 100):
            with self.subTest(concurrency=concurrency):
                chunk_size = min(concurrency // 10, 20)
                tasks = [mailboxes[i:i + chunk_size] for i in range(0, len(mailboxes), chunk_size)]
                with ThreadPoolExecutor(max_workers=concurrency) as executor:
                    list(executor.map(lambda task: self.estimator.get_folder_counts(task, []), tasks))

                self.assertLessEqual(self.graph.max_in_flight_per_mailbox, RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH)

    def test_stop_event_halts_further_batches(self):
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


class FakeFolderTreeGraph:
    """Fake for UrlInvoker.execute_batch_request serving folder hierarchies.

    `trees[(mailbox, label)]` is a nested node {"id", "count", "children"}.
    childFolders responses are paginated by `page_size`. `failures[url_part]`
    returns the given response for any request whose URL contains url_part.
    """

    def __init__(self, page_size: int = 999, delay: float = 0.0):
        self.trees: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self.failures: Dict[str, Dict[str, Any]] = {}
        self.page_size = page_size
        self.delay = delay
        self.batches: List[List[Dict[str, Any]]] = []
        self.lock = threading.Lock()
        self.in_flight_per_mailbox: Dict[str, int] = {}
        self.max_in_flight_per_mailbox = 0
        self.next_link_override: Optional[str] = None

    @staticmethod
    def node(folder_id: str, count: int, children: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        return {"id": folder_id, "count": count, "children": children or []}

    def _find(self, mailbox: str, folder_id: str) -> Optional[Dict[str, Any]]:
        stack = [tree for (m, _), tree in self.trees.items() if m == mailbox]
        while stack:
            node = stack.pop()
            if node["id"] == folder_id:
                return node
            stack.extend(node["children"])
        return None

    @staticmethod
    def _folder_body(node: Dict[str, Any]) -> Dict[str, Any]:
        return {"id": node["id"], "totalItemCount": node["count"], "childFolderCount": len(node["children"])}

    def _respond(self, url: str) -> Optional[Dict[str, Any]]:
        for url_part, response in self.failures.items():
            if url_part in url:
                return response
        path, _, query = url.partition("?")
        parts = path.strip("/").split("/")
        mailbox = urllib.parse.unquote(parts[3])
        folder = urllib.parse.unquote(parts[5])
        if len(parts) == 6:
            node = self.trees.get((mailbox, WELL_KNOWN_TO_LABEL[folder]))
            if node is None:
                return {"status": 404, "body": {"error": {"message": "Folder not found."}}}
            return {"status": 200, "body": self._folder_body(node)}

        node = self._find(mailbox, folder)
        if node is None:
            return {"status": 404, "body": {"error": {"message": "Folder not found."}}}
        skip = int(urllib.parse.parse_qs(query).get("$skiptoken", ["0"])[0])
        page = node["children"][skip:skip + self.page_size]
        body: Dict[str, Any] = {"value": [self._folder_body(child) for child in page]}
        if skip + self.page_size < len(node["children"]):
            body["@odata.nextLink"] = self.next_link_override or (
                f"{GRAPH_BASE_URL}/admin/exchange/mailboxes/{parts[3]}/folders/{parts[5]}"
                f"/childFolders?$skiptoken={skip + self.page_size}"
            )
        return {"status": 200, "body": body}

    def execute_batch_request(self, session, batch_url, token_manager, token_data, requests, logger, stop_event=None, context=""):
        mailboxes = [urllib.parse.unquote(req["url"].strip("/").split("/")[3]) for req in requests]
        with self.lock:
            self.batches.append(requests)
            for mailbox in mailboxes:
                self.in_flight_per_mailbox[mailbox] = self.in_flight_per_mailbox.get(mailbox, 0) + 1
                self.max_in_flight_per_mailbox = max(self.max_in_flight_per_mailbox, self.in_flight_per_mailbox[mailbox])
        try:
            if self.delay:
                time.sleep(self.delay)
            result = {}
            for req in requests:
                response = self._respond(req["url"])
                if response is not None:
                    result[req["id"]] = dict(response, id=req["id"])
            return result
        finally:
            with self.lock:
                for mailbox in mailboxes:
                    self.in_flight_per_mailbox[mailbox] -= 1


class TestEORecoverableItemsHierarchyMode(unittest.TestCase):

    def setUp(self):
        self.config = ScanConfig(
            tenant_id="test-tenant",
            client_ids=["test-client"],
            client_secrets=["test-secret"],
            user_source="tenant",
            csv_path="",
            scan_email=True,
            scan_recoverable_items=True,
            recursive_email_scan=True,
            concurrency=10,
            retries=1,
            backoff=1,
        )
        self.stop_event = threading.Event()
        self.graph = FakeFolderTreeGraph()
        self.url_invoker = MagicMock()
        self.url_invoker.execute_batch_request.side_effect = self.graph.execute_batch_request
        self.logger = MagicMock()
        self.estimator = EORecoverableItemsEstimator(
            config=self.config,
            url_invoker=self.url_invoker,
            logger=self.logger,
            stop_event=self.stop_event,
        )

    def _set_flat_trees(self, mailbox: str, count: int = 0):
        for label in RECOVERABLE_ITEMS_FOLDERS:
            self.graph.trees[(mailbox, label)] = FakeFolderTreeGraph.node(f"{mailbox}-{label}", count)

    def _assert_per_batch_limit(self):
        for batch in self.graph.batches:
            self.assertLessEqual(len(batch), self.config.graph_batch_size)
            per_mailbox = Counter(req["url"].strip("/").split("/")[3] for req in batch)
            self.assertLessEqual(max(per_mailbox.values()), RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH)

    def test_sums_total_item_count_across_levels(self):
        node = FakeFolderTreeGraph.node
        self._set_flat_trees("a@x.com")
        self.graph.trees[("a@x.com", "DiscoveryHolds")] = node("dh", 10, [
            node("dh-1", 5, [node("dh-1-1", 2), node("dh-1-2", 3)]),
            node("dh-2", 7),
        ])
        self.graph.trees[("a@x.com", "Purges")] = node("p", 4)
        failures = []
        result = self.estimator.get_folder_counts(["a@x.com"], failures)

        self.assertEqual(result["a@x.com"], {"Deletions": 0, "Purges": 4, "DiscoveryHolds": 27, "Versions": 0})
        self.assertEqual(failures, [])
        self._assert_per_batch_limit()
        urls = [req["url"] for batch in self.graph.batches for req in batch]
        self.assertTrue(all("/items" not in url for url in urls))
        self.assertTrue(all("$select=id,childFolderCount,totalItemCount" in url for url in urls))

    def test_follows_child_folder_pagination(self):
        node = FakeFolderTreeGraph.node
        self.graph.page_size = 2
        self._set_flat_trees("a@x.com")
        self.graph.trees[("a@x.com", "Versions")] = node("v", 1, [node(f"v-{i}", 1) for i in range(5)])
        failures = []
        result = self.estimator.get_folder_counts(["a@x.com"], failures)

        self.assertEqual(result["a@x.com"]["Versions"], 6)
        self.assertEqual(failures, [])
        child_requests = [req["url"] for batch in self.graph.batches for req in batch if "childFolders" in req["url"]]
        self.assertEqual(len(child_requests), 3)

    def test_child_failure_keeps_other_counts(self):
        node = FakeFolderTreeGraph.node
        self._set_flat_trees("a@x.com")
        self.graph.trees[("a@x.com", "Deletions")] = node("d", 10, [
            node("d-1", 5, [node("d-1-1", 100)]),
            node("d-2", 3),
        ])
        self.graph.failures["/folders/d-1/childFolders"] = {
            "status": 500, "body": {"error": {"message": "Server error."}}
        }
        failures = []
        result = self.estimator.get_folder_counts(["a@x.com"], failures)

        self.assertEqual(result["a@x.com"]["Deletions"], 18)
        self.assertEqual(len(failures), 1)
        self.assertTrue(failures[0]["isPartial"])
        self.assertEqual(failures[0]["folderId"], "Deletions (d-1)")
        self.assertEqual(failures[0]["type"], FailureType.FAILURE_STATUS_CODE_ERROR)

    def test_root_failures(self):
        self._set_flat_trees("a@x.com", count=2)
        del self.graph.trees[("a@x.com", "Purges")]
        failures = []
        result = self.estimator.get_folder_counts(["a@x.com"], failures)

        self.assertEqual(result["a@x.com"], {"Deletions": 2, "Purges": 0, "DiscoveryHolds": 2, "Versions": 2})
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]["folderId"], "Purges")
        self.assertEqual(failures[0]["type"], FailureType.NOT_FOUND)

    def test_all_roots_failed_is_single_full_failure(self):
        failures = []
        result = self.estimator.get_folder_counts(["missing@x.com"], failures)

        self.assertEqual(sum(result["missing@x.com"].values()), 0)
        self.assertEqual(len(failures), 1)
        self.assertFalse(failures[0]["isPartial"])

    def test_depth_limit(self):
        node = FakeFolderTreeGraph.node
        self.config.max_allowed_depth = 1
        self._set_flat_trees("a@x.com")
        self.graph.trees[("a@x.com", "Deletions")] = node("d", 1, [node("d-1", 1, [node("d-1-1", 50)])])
        failures = []
        result = self.estimator.get_folder_counts(["a@x.com"], failures)

        self.assertEqual(result["a@x.com"]["Deletions"], 2)
        self.assertEqual(len(failures), 1)
        self.assertIn("Depth limit 1", failures[0]["message"])
        self.assertTrue(any("Depth limit 1" in call.args[0] for call in self.logger.call_args_list))

    def test_revisited_folder_not_double_counted(self):
        node = FakeFolderTreeGraph.node
        self._set_flat_trees("a@x.com")
        shared_child = node("shared", 9)
        self.graph.trees[("a@x.com", "Deletions")] = node("d", 1, [node("d-1", 1, [shared_child]), node("d-2", 1, [shared_child])])
        result = self.estimator.get_folder_counts(["a@x.com"], [])

        self.assertEqual(result["a@x.com"]["Deletions"], 12)

    def test_foreign_next_link_not_followed(self):
        node = FakeFolderTreeGraph.node
        self.graph.page_size = 1
        self.graph.next_link_override = "https://evil.example.com/next"
        self._set_flat_trees("a@x.com")
        self.graph.trees[("a@x.com", "Deletions")] = node("d", 1, [node("d-1", 1), node("d-2", 1)])
        failures = []
        result = self.estimator.get_folder_counts(["a@x.com"], failures)

        self.assertEqual(result["a@x.com"]["Deletions"], 2)
        urls = [req["url"] for batch in self.graph.batches for req in batch]
        self.assertFalse(any("evil.example.com" in url for url in urls))
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]["type"], FailureType.INVALID_DATA)

    def test_folder_ids_are_url_encoded(self):
        node = FakeFolderTreeGraph.node
        self._set_flat_trees("a@x.com")
        self.graph.trees[("a@x.com", "Deletions")] = node("d", 1, [node("AA/B+c=", 1, [node("x", 1)])])
        result = self.estimator.get_folder_counts(["a@x.com"], [])

        urls = [req["url"] for batch in self.graph.batches for req in batch]
        self.assertTrue(any("/folders/AA%2FB%2Bc%3D/childFolders" in url for url in urls))
        self.assertEqual(result["a@x.com"]["Deletions"], 3)

    def test_parallel_deep_trees_never_exceed_per_mailbox_limit(self):
        node = FakeFolderTreeGraph.node
        self.graph.delay = 0.002
        self.graph.page_size = 2
        mailboxes = [f"user{i}@x.com" for i in range(60)]
        for mailbox in mailboxes:
            for label in RECOVERABLE_ITEMS_FOLDERS:
                self.graph.trees[(mailbox, label)] = node(f"{mailbox}-{label}", 1, [
                    node(f"{mailbox}-{label}-{i}", 1, [node(f"{mailbox}-{label}-{i}-{j}", 1) for j in range(3)])
                    for i in range(3)
                ])
        for concurrency in (10, 30, 100):
            with self.subTest(concurrency=concurrency):
                chunk_size = min(concurrency // 10, 20)
                tasks = [mailboxes[i:i + chunk_size] for i in range(0, len(mailboxes), chunk_size)]
                with ThreadPoolExecutor(max_workers=concurrency) as executor:
                    results = list(executor.map(lambda task: self.estimator.get_folder_counts(task, []), tasks))

                self.assertLessEqual(self.graph.max_in_flight_per_mailbox, RECOVERABLE_ITEMS_MAX_REQUESTS_PER_MAILBOX_PER_BATCH)
                self.assertTrue(all(counts == {label: 13 for label in RECOVERABLE_ITEMS_FOLDERS}
                                    for result in results for counts in result.values()))
        self._assert_per_batch_limit()


if __name__ == "__main__":
    unittest.main()
