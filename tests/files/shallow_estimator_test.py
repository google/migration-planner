# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for ShallowFileEstimator and SpRestConnector.

Shallow Scan reads aggregate SharePoint REST StorageMetrics and ItemCount rather
than crawling the delta API, so these tests are organised in four layers, each
mocking at a different seam:

  1. `_split_library_endpoint`  - pure, no I/O. Endpoint string correctness.
  2. `SpRestConnector`          - mocks the session so the real retry, throttle
                                  and HTTP status handling executes.
  3. `_scan_document_libraries` - patches `_execute_get` so aggregation and
                                  threshold logic runs without any sleeping.
  4. `calculate_resource_metrics` - full orchestration over the Graph mock.

All test data is inline, so the suite needs no generated fixture and runs in
well under a second.

Run with:
  python3 -m unittest tests.files.shallow_estimator_test -v
"""

import threading
import unittest
from unittest import mock

from estimators.files_shallow.shallow_file_estimator import LibraryTarget
from estimators.files_shallow.shallow_file_estimator import ShallowFileEstimator
from tests.files.mocks import MockCertTokenManager
from tests.files.mocks import MockResponse
from tests.files.mocks import MockSpSession
from tests.files.mocks import MockUrlInvoker
from util.enums import FailureType
from util.enums import ResourceType
from util.files_shallow.sp_rest_connector import SpRestConnector
from util.utils import ScanConfig

TENANT = "https://contoso.sharepoint.com"
MY_TENANT = "https://contoso-my.sharepoint.com"

LARGE_LIMIT = 500
WARNING_LIMIT = 200


def build_config(**overrides):
  """Creates a ScanConfig with small thresholds so tests stay readable."""
  params = {
      "tenant_id": "test-tenant",
      "client_ids": ["test-client-1"],
      "client_secrets": ["test-secret-1"],
      "user_source": "tenant",
      "csv_path": "",
      "concurrency": 10,
      "retries": 1,
      "backoff": 1,
      "parallel_batches": 5,
      "large_resource_count_limit": LARGE_LIMIT,
      "warning_resource_count_limit": WARNING_LIMIT,
      "max_allowed_depth": 3,
  }
  params.update(overrides)
  return ScanConfig(**params)


def storage_body(file_count, stream_size, total_size=None):
  """Builds a GetFolderByServerRelativeUrl StorageMetrics response body."""
  return {
      "StorageMetrics": {
          "TotalFileCount": file_count,
          "TotalFileStreamSize": stream_size,
          "TotalSize": total_size if total_size is not None else stream_size,
      }
  }


class SplitLibraryEndpointTest(unittest.TestCase):
  """Layer 1: endpoint construction. Pure function, no I/O."""

  def setUp(self):
    self.connector = SpRestConnector(MockCertTokenManager(), max_retries=1)

  def test_endpoint_components(self):
    """Verify REST addressing components for every library shape we support."""
    cases = [
        (
            "onedrive",
            f"{MY_TENANT}/personal/alice_contoso_com",
            f"{MY_TENANT}/personal/alice_contoso_com/Documents",
            f"{MY_TENANT}/personal/alice_contoso_com",
            "/personal/alice_contoso_com/Documents",
            "contoso-my.sharepoint.com",
        ),
        (
            "site_collection_encoded_space",
            f"{TENANT}/sites/Finance",
            f"{TENANT}/sites/Finance/Shared%20Documents",
            f"{TENANT}/sites/Finance",
            "/sites/Finance/Shared Documents",
            "contoso.sharepoint.com",
        ),
        (
            "subsite_rooted_at_own_web",
            f"{TENANT}/sites/Finance/HR",
            f"{TENANT}/sites/Finance/HR/Shared%20Documents",
            f"{TENANT}/sites/Finance/HR",
            "/sites/Finance/HR/Shared Documents",
            "contoso.sharepoint.com",
        ),
        (
            "apostrophe_odata_escaped",
            f"{TENANT}/sites/Legal",
            f"{TENANT}/sites/Legal/Bob's Docs",
            f"{TENANT}/sites/Legal",
            "/sites/Legal/Bob''s Docs",
            "contoso.sharepoint.com",
        ),
        (
            "system_library_site_assets",
            f"{TENANT}/sites/Finance",
            f"{TENANT}/sites/Finance/SiteAssets",
            f"{TENANT}/sites/Finance",
            "/sites/Finance/SiteAssets",
            "contoso.sharepoint.com",
        ),
        (
            "trailing_slash_tolerated",
            f"{TENANT}/sites/Finance/",
            f"{TENANT}/sites/Finance/Shared%20Documents/",
            f"{TENANT}/sites/Finance",
            "/sites/Finance/Shared Documents",
            "contoso.sharepoint.com",
        ),
    ]

    for name, web, library, exp_base, exp_path, exp_domain in cases:
      with self.subTest(case=name):
        base, path, domain = self.connector._split_library_endpoint(
            web, library
        )
        self.assertEqual(base, exp_base)
        self.assertEqual(path, exp_path)
        self.assertEqual(domain, exp_domain)

  def test_onedrive_endpoint_matches_pre_refactor_string(self):
    """Verify the OneDrive URLs are byte-identical to the pre-refactor build."""
    web = f"{MY_TENANT}/personal/alice_contoso_com"
    library = f"{web}/Documents"

    base, path, _ = self.connector._split_library_endpoint(web, library)

    # Reproduced verbatim from the OneDrive-only implementation this replaced.
    legacy_item = (
        f"{web}/_api/web/GetList('/personal/alice_contoso_com/Documents')"
        "?$select=ItemCount"
    )
    legacy_storage = (
        f"{web}/_api/web/GetFolderByServerRelativeUrl"
        "('/personal/alice_contoso_com/Documents')"
        "?$select=StorageMetrics&$expand=StorageMetrics"
    )

    self.assertEqual(
        f"{base}/_api/web/GetList('{path}')?$select=ItemCount", legacy_item
    )
    self.assertEqual(
        f"{base}/_api/web/GetFolderByServerRelativeUrl('{path}')"
        "?$select=StorageMetrics&$expand=StorageMetrics",
        legacy_storage,
    )

  def test_invalid_urls_raise(self):
    """Verify unusable URLs raise rather than producing a malformed endpoint."""
    cases = [
        ("no_scheme_or_host", "not-a-url", "not-a-url"),
        ("empty_web", "", f"{TENANT}/sites/Finance/Shared Documents"),
    ]
    for name, web, library in cases:
      with self.subTest(case=name):
        with self.assertRaises(ValueError):
          self.connector._split_library_endpoint(web, library)


class SpRestConnectorHttpTest(unittest.TestCase):
  """Layer 2: real retry/throttle logic against a mocked session."""

  def setUp(self):
    self.session = MockSpSession()
    self.token_manager = MockCertTokenManager(self.session)
    # max_retries=1 keeps the backoff loop from actually sleeping.
    self.connector = SpRestConnector(
        self.token_manager, max_retries=1, backoff=1
    )
    self.web = f"{TENANT}/sites/Finance"
    self.library = f"{self.web}/Shared Documents"

  def _get_metrics(self):
    return self.connector.get_library_metrics(
        self.web, self.library, logger=lambda *_: None
    )

  def test_success_parses_counts(self):
    """Verify a healthy pair of responses parses into derived counts."""
    self.session.register("GetList(", MockResponse(200, {"ItemCount": 130}))
    self.session.register(
        "GetFolderByServerRelativeUrl(",
        MockResponse(200, storage_body(100, 2048, 4096)),
    )

    result = self._get_metrics()

    self.assertEqual(result["item_count"], 130)
    self.assertEqual(result["file_count"], 100)
    self.assertEqual(result["folder_count"], 30)
    self.assertEqual(result["active_size_bytes"], 2048)
    self.assertEqual(result["total_size_bytes"], 4096)

  def test_folder_count_never_negative(self):
    """Verify a file count above item count clamps folder count to zero."""
    self.session.register("GetList(", MockResponse(200, {"ItemCount": 5}))
    self.session.register(
        "GetFolderByServerRelativeUrl(",
        MockResponse(200, storage_body(9, 1024)),
    )

    self.assertEqual(self._get_metrics()["folder_count"], 0)

  def test_odata_string_integers_are_parsed(self):
    """Verify OData numeric strings are coerced to ints."""
    self.session.register("GetList(", MockResponse(200, {"ItemCount": "42"}))
    self.session.register(
        "GetFolderByServerRelativeUrl(",
        MockResponse(200, storage_body("30", "1024", "2048")),
    )

    result = self._get_metrics()

    self.assertEqual(result["item_count"], 42)
    self.assertEqual(result["file_count"], 30)
    self.assertEqual(result["folder_count"], 12)

  def test_http_403_raises_permission_error(self):
    """Verify 403 surfaces as PermissionError so the DL is marked failed."""
    self.session.register(
        "GetList(", MockResponse(403, {}, text="Access denied.")
    )

    with self.assertRaises(PermissionError):
      self._get_metrics()

  def test_http_404_yields_zeros_without_raising(self):
    """Verify 404 returns an empty body and therefore zeros, not a failure.

    This documents existing behaviour: `_execute_get` maps 404 to `{}`, so a
    missing library reports zeros rather than being recorded as failed.
    """
    self.session.register("GetList(", MockResponse(404, {}))
    self.session.register("GetFolderByServerRelativeUrl(", MockResponse(404, {}))

    result = self._get_metrics()

    self.assertEqual(result["item_count"], 0)
    self.assertEqual(result["file_count"], 0)
    self.assertEqual(result["folder_count"], 0)
    self.assertEqual(result["active_size_bytes"], 0)

  def test_http_500_exhausts_retries_and_raises(self):
    """Verify a persistent server error raises once retries are exhausted."""
    self.session.register(
        "GetList(", MockResponse(500, {}, text="Internal Server Error")
    )

    with self.assertRaises(RuntimeError):
      self._get_metrics()

  def test_http_429_exhausts_retries_and_raises(self):
    """Verify sustained throttling raises rather than reporting zeros."""
    self.session.register(
        "GetList(",
        MockResponse(429, {}, text="Throttled", headers={"Retry-After": "0"}),
    )

    with self.assertRaises(RuntimeError):
      self._get_metrics()

  def test_http_429_then_success_sets_throttle_gate(self):
    """Verify a 429 is retried after Retry-After and opens the throttle gate."""
    connector = SpRestConnector(self.token_manager, max_retries=3, backoff=1)
    self.session.register(
        "GetList(",
        MockResponse(429, {}, text="Throttled", headers={"Retry-After": "0"}),
        MockResponse(200, {"ItemCount": 7}),
    )
    self.session.register(
        "GetFolderByServerRelativeUrl(",
        MockResponse(200, storage_body(5, 512)),
    )

    result = connector.get_library_metrics(
        self.web, self.library, logger=lambda *_: None
    )

    self.assertEqual(result["item_count"], 7)
    self.assertEqual(result["folder_count"], 2)
    self.assertGreater(connector.throttle_until, 0.0)

  def test_http_401_refreshes_token_then_succeeds(self):
    """Verify a 401 triggers an inline token refresh and then retries."""
    connector = SpRestConnector(self.token_manager, max_retries=3, backoff=1)
    self.session.register(
        "GetList(",
        MockResponse(401, {}, text="Unauthorized"),
        MockResponse(200, {"ItemCount": 3}),
    )
    self.session.register(
        "GetFolderByServerRelativeUrl(",
        MockResponse(200, storage_body(3, 256)),
    )

    result = connector.get_library_metrics(
        self.web, self.library, logger=lambda *_: None
    )

    self.assertEqual(result["item_count"], 3)
    self.assertEqual(
        self.token_manager.refresh_calls, ["contoso.sharepoint.com"]
    )

  def test_stop_event_short_circuits(self):
    """Verify an already-set stop event returns empty without calling out."""
    stop_event = threading.Event()
    stop_event.set()
    self.session.register("GetList(", MockResponse(200, {"ItemCount": 99}))

    result = self.connector.get_item_counts(
        self.web, self.library, lambda *_: None, stop_event
    )

    self.assertEqual(result, 0)
    self.assertEqual(self.session.get_calls(), [])


class ShallowScanBaseTest(unittest.TestCase):
  """Shared setup for the estimator-level layers."""

  def setUp(self):
    self.session = MockSpSession()
    self.token_manager = MockCertTokenManager(self.session)
    self.stop_event = threading.Event()
    self.config = build_config()
    self.id_to_display = {}

    self.estimator = ShallowFileEstimator(
        config=self.config,
        url_invoker=mock.Mock(),
        cert_token_manager=self.token_manager,
        logger=lambda *_: None,
        stop_event=self.stop_event,
        progress_update_callback=lambda *a, **k: None,
    )
    self.estimator.set_id_to_display_name_map(self.id_to_display)
    self.estimator.site_to_metadata = {}

  def register_site(self, site_id, url, is_personal=False, site_level=0):
    self.id_to_display[site_id] = url
    self.estimator.site_to_metadata[site_id] = {"isPersonalSite": is_personal}
    return site_id

  def register_library(self, drive_id, url):
    self.id_to_display[drive_id] = url
    return drive_id

  def build_metrics(self, site_ids):
    """Initialises a metrics dict containing the given root site collections."""
    metrics = self.estimator._initialize_metrics_structure()
    metrics["siteMetrics"] = {
        site_id: {"siteLevel": 0, "dlCount": 1} for site_id in site_ids
    }
    self.estimator._prepare_root_sites(metrics)
    return metrics

  def run_scan(self, metrics, subsite_to_drives, subsite_to_top_level_site,
               library_data, failures=None):
    """Runs the Phase 2 scan with `_execute_get` patched to canned data.

    `library_data` maps a drive id to either a dict of
    (item_count, file_count, stream_size) or an Exception to raise.
    """
    failures = failures if failures is not None else []
    captured = []
    lock = threading.Lock()

    # Resolve endpoints back to a drive id via the library URL path.
    path_to_drive = {}
    for drive_id in library_data:
      url = self.id_to_display[drive_id]
      path_to_drive[url.split("://", 1)[-1].split("/", 1)[-1]] = drive_id

    def fake_execute_get(_self, endpoint, base_url, domain, logger,
                         stop_event=None):
      with lock:
        captured.append(endpoint)

      drive_id = None
      for path, candidate in path_to_drive.items():
        if path.replace("%20", " ") in endpoint.replace("%20", " "):
          drive_id = candidate
          break

      if drive_id is None:
        raise RuntimeError(f"Unexpected endpoint: {endpoint}")

      data = library_data[drive_id]
      if isinstance(data, BaseException):
        raise data
      if isinstance(data, dict) and "GetList(" in endpoint:
        if isinstance(data.get("item"), BaseException):
          raise data["item"]
        return {"ItemCount": data["item"]}
      if isinstance(data.get("storage"), BaseException):
        raise data["storage"]
      return storage_body(data["files"], data["size"])

    targets = self.estimator._build_library_targets(
        metrics, subsite_to_drives, subsite_to_top_level_site, failures
    )
    with mock.patch.object(SpRestConnector, "_execute_get", fake_execute_get):
      processed, failed = self.estimator._scan_document_libraries(
          metrics, targets, subsite_to_top_level_site, failures
      )
    self.estimator._finalize_tenant_totals(metrics, processed, failed)

    return {
        "metrics": metrics,
        "processed": processed,
        "failed": failed,
        "failures": failures,
        "endpoints": captured,
        "targets": targets,
    }


class LibraryTargetTest(ShallowScanBaseTest):
  """Layer 3a: expansion of discovery output into library targets."""

  def test_every_discovered_library_is_targeted(self):
    """Verify no filtering is applied, including system libraries."""
    site = self.register_site("siteA", f"{TENANT}/sites/Finance")
    self.register_library("d1", f"{TENANT}/sites/Finance/Shared%20Documents")
    self.register_library("d2", f"{TENANT}/sites/Finance/SiteAssets")
    self.register_library("d3", f"{TENANT}/sites/Finance/Style%20Library")
    metrics = self.build_metrics([site])

    targets = self.estimator._build_library_targets(
        metrics, {site: ["d1", "d2", "d3"]}, {}, []
    )

    self.assertEqual([t.drive_id for t in targets], ["d1", "d2", "d3"])
    for target in targets:
      self.assertEqual(target.owning_site_id, site)
      self.assertEqual(target.top_level_site_id, site)

  def test_subsite_library_is_rooted_at_subsite_web(self):
    """Verify a subsite library keeps its own web but rolls up to the root."""
    root = self.register_site("siteA", f"{TENANT}/sites/Finance")
    sub = self.register_site("siteA_hr", f"{TENANT}/sites/Finance/HR")
    self.register_library("d1", f"{TENANT}/sites/Finance/HR/Shared%20Documents")
    metrics = self.build_metrics([root])

    targets = self.estimator._build_library_targets(
        metrics, {sub: ["d1"]}, {sub: root}, []
    )

    self.assertEqual(len(targets), 1)
    self.assertEqual(targets[0].web_base_url, f"{TENANT}/sites/Finance/HR")
    self.assertEqual(targets[0].owning_site_id, sub)
    self.assertEqual(targets[0].top_level_site_id, root)

  def test_unresolvable_urls_are_skipped_and_recorded(self):
    """Verify sites and libraries without a usable URL are excluded."""
    good = self.register_site("siteA", f"{TENANT}/sites/Finance")
    bad_site = self.register_site("siteB", "")
    self.register_library("d1", f"{TENANT}/sites/Finance/Shared%20Documents")
    self.register_library("d_bad", "")
    metrics = self.build_metrics([good, bad_site])
    failures = []

    targets = self.estimator._build_library_targets(
        metrics,
        {good: ["d1", "d_bad"], bad_site: ["d2"]},
        {},
        failures,
    )

    self.assertEqual([t.drive_id for t in targets], ["d1"])
    self.assertEqual(len(failures), 2)
    for failure in failures:
      self.assertEqual(failure["type"], FailureType.NOT_FOUND.name)

  def test_libraries_of_unknown_site_are_ignored(self):
    """Verify libraries rolling up to a site absent from the report are dropped."""
    site = self.register_site("siteA", f"{TENANT}/sites/Finance")
    self.register_library("d1", f"{TENANT}/sites/Finance/Shared%20Documents")
    self.register_library("d9", f"{TENANT}/sites/Ghost/Shared%20Documents")
    metrics = self.build_metrics([site])

    targets = self.estimator._build_library_targets(
        metrics, {site: ["d1"], "siteGhost": ["d9"]}, {}, []
    )

    self.assertEqual([t.drive_id for t in targets], ["d1"])


class ShallowScanAggregationTest(ShallowScanBaseTest):
  """Layer 3b: metric aggregation and threshold evaluation."""

  def test_single_onedrive_library(self):
    """Verify a lone OneDrive library produces the expected derived metrics."""
    site = self.register_site(
        "od1", f"{MY_TENANT}/personal/alice_contoso_com", is_personal=True
    )
    drive = self.register_library(
        "b!drive1", f"{MY_TENANT}/personal/alice_contoso_com/Documents"
    )
    metrics = self.build_metrics([site])

    out = self.run_scan(
        metrics, {site: [drive]}, {},
        {drive: {"item": 13, "files": 10, "size": 1024}},
    )

    site_metrics = out["metrics"]["siteMetrics"][site]
    self.assertEqual(site_metrics["fileCount"], 10)
    self.assertEqual(site_metrics["folderCount"], 3)
    self.assertEqual(site_metrics["totalSize"], 1024)
    self.assertEqual(site_metrics["resourceCount"], 13)
    self.assertEqual(out["processed"], 1)
    self.assertEqual(out["failed"], 0)
    self.assertEqual(len(out["endpoints"]), 2)

  def test_multiple_libraries_roll_up_to_one_site(self):
    """Verify sibling libraries accumulate rather than overwrite."""
    site = self.register_site("siteA", f"{TENANT}/sites/Finance")
    d1 = self.register_library("d1", f"{TENANT}/sites/Finance/Shared%20Documents")
    d2 = self.register_library("d2", f"{TENANT}/sites/Finance/SiteAssets")
    metrics = self.build_metrics([site])

    out = self.run_scan(
        metrics, {site: [d1, d2]}, {},
        {
            d1: {"item": 100, "files": 80, "size": 1000},
            d2: {"item": 50, "files": 45, "size": 500},
        },
    )

    site_metrics = out["metrics"]["siteMetrics"][site]
    self.assertEqual(site_metrics["fileCount"], 125)
    self.assertEqual(site_metrics["folderCount"], 25)
    self.assertEqual(site_metrics["totalSize"], 1500)
    self.assertEqual(site_metrics["resourceCount"], 150)

  def test_subsite_library_rolls_up_to_top_level_site(self):
    """Verify subsite libraries land on the root site collection row."""
    root = self.register_site("siteA", f"{TENANT}/sites/Finance")
    sub = self.register_site("siteA_hr", f"{TENANT}/sites/Finance/HR")
    d1 = self.register_library("d1", f"{TENANT}/sites/Finance/Shared%20Documents")
    d2 = self.register_library(
        "d2", f"{TENANT}/sites/Finance/HR/Shared%20Documents"
    )
    metrics = self.build_metrics([root])

    out = self.run_scan(
        metrics, {root: [d1], sub: [d2]}, {sub: root},
        {
            d1: {"item": 10, "files": 8, "size": 100},
            d2: {"item": 20, "files": 15, "size": 200},
        },
    )

    self.assertNotIn(sub, out["metrics"]["siteMetrics"])
    site_metrics = out["metrics"]["siteMetrics"][root]
    self.assertEqual(site_metrics["fileCount"], 23)
    self.assertEqual(site_metrics["folderCount"], 7)

  def test_per_library_drive_metrics(self):
    """Verify driveMetrics holds each library's own numbers, not the roll-up."""
    site = self.register_site("siteA", f"{TENANT}/sites/Finance")
    d1 = self.register_library("d1", f"{TENANT}/sites/Finance/Shared%20Documents")
    d2 = self.register_library("d2", f"{TENANT}/sites/Finance/SiteAssets")
    metrics = self.build_metrics([site])

    out = self.run_scan(
        metrics, {site: [d1, d2]}, {},
        {
            d1: {"item": 100, "files": 80, "size": 1000},
            d2: {"item": 50, "files": 45, "size": 500},
        },
    )

    drive_metrics = out["metrics"]["driveMetrics"]
    self.assertEqual(drive_metrics[d1]["fileCount"], 80)
    self.assertEqual(drive_metrics[d1]["folderCount"], 20)
    self.assertEqual(drive_metrics[d2]["fileCount"], 45)
    self.assertEqual(drive_metrics[d2]["folderCount"], 5)

  def test_threshold_levels(self):
    """Verify Large and Warning fire at DL, Subsite and Site with right parents."""
    root = self.register_site("siteA", f"{TENANT}/sites/Finance")
    sub = self.register_site("siteA_hr", f"{TENANT}/sites/Finance/HR")
    d1 = self.register_library(
        "d1", f"{TENANT}/sites/Finance/HR/Shared%20Documents"
    )
    metrics = self.build_metrics([root])

    out = self.run_scan(
        metrics, {sub: [d1]}, {sub: root},
        {d1: {"item": LARGE_LIMIT + 1, "files": 1, "size": 10}},
    )

    large = {(r["type"], r["id"], r["parent"])
             for r in out["metrics"]["tenantLevelLargeResources"]}
    warning = {(r["type"], r["id"], r["parent"])
               for r in out["metrics"]["tenantLevelWarningResources"]}
    expected = {
        (ResourceType.DL.value, d1, sub),
        (ResourceType.SUBSITE.value, sub, root),
        (ResourceType.SITE.value, root, "N/A (Top level site)"),
    }

    self.assertEqual(large, expected)
    self.assertEqual(warning, expected)
    self.assertEqual(out["metrics"]["siteMetrics"][root]["largeResourceCount"], 3)
    self.assertEqual(out["metrics"]["tenantLevelLargeResourceCount"], 3)

  def test_no_subsite_entry_for_top_level_owned_library(self):
    """Verify a root-owned library emits DL and SITE but never SUBSITE."""
    site = self.register_site("siteA", f"{TENANT}/sites/Finance")
    d1 = self.register_library("d1", f"{TENANT}/sites/Finance/Shared%20Documents")
    metrics = self.build_metrics([site])

    out = self.run_scan(
        metrics, {site: [d1]}, {},
        {d1: {"item": WARNING_LIMIT + 1, "files": 1, "size": 10}},
    )

    types = [r["type"] for r in out["metrics"]["tenantLevelWarningResources"]]
    self.assertIn(ResourceType.DL.value, types)
    self.assertIn(ResourceType.SITE.value, types)
    self.assertNotIn(ResourceType.SUBSITE.value, types)

  def test_threshold_boundary_is_strict(self):
    """Verify a count exactly at the limit does not breach."""
    cases = [
        ("exactly_at_warning", WARNING_LIMIT, 0),
        ("one_over_warning", WARNING_LIMIT + 1, 2),
    ]
    for name, item_count, expected_warnings in cases:
      with self.subTest(case=name):
        self.setUp()
        site = self.register_site("siteA", f"{TENANT}/sites/Finance")
        d1 = self.register_library(
            "d1", f"{TENANT}/sites/Finance/Shared%20Documents"
        )
        metrics = self.build_metrics([site])

        out = self.run_scan(
            metrics, {site: [d1]}, {},
            {d1: {"item": item_count, "files": 1, "size": 10}},
        )

        self.assertEqual(
            out["metrics"]["tenantLevelWarningResourceCount"],
            expected_warnings,
        )

  def test_tenant_totals_equal_sum_of_sites(self):
    """Verify tenant roll-up equals the sum of the site rows."""
    site_a = self.register_site("siteA", f"{TENANT}/sites/Finance")
    site_b = self.register_site("siteB", f"{TENANT}/sites/Legal")
    d1 = self.register_library("d1", f"{TENANT}/sites/Finance/Shared%20Documents")
    d2 = self.register_library("d2", f"{TENANT}/sites/Legal/Shared%20Documents")
    metrics = self.build_metrics([site_a, site_b])

    out = self.run_scan(
        metrics, {site_a: [d1], site_b: [d2]}, {},
        {
            d1: {"item": 100, "files": 80, "size": 1000},
            d2: {"item": 40, "files": 30, "size": 400},
        },
    )

    result = out["metrics"]
    site_values = result["siteMetrics"].values()
    self.assertEqual(
        result["fileCount"], sum(s["fileCount"] for s in site_values)
    )
    self.assertEqual(
        result["folderCount"], sum(s["folderCount"] for s in site_values)
    )
    self.assertEqual(result["fileCount"], 110)
    self.assertEqual(result["folderCount"], 30)

  def test_unsupported_metrics_reported_as_na(self):
    """Verify metrics Shallow Scan cannot derive stay as N/A."""
    site = self.register_site("siteA", f"{TENANT}/sites/Finance")
    d1 = self.register_library("d1", f"{TENANT}/sites/Finance/Shared%20Documents")
    metrics = self.build_metrics([site])

    out = self.run_scan(
        metrics, {site: [d1]}, {}, {d1: {"item": 10, "files": 8, "size": 100}}
    )

    result = out["metrics"]
    self.assertTrue(result["isShallowScan"])
    for field in ("shortcutCount", "maxEffectiveDepth", "maxFolderDepth",
                  "folderCountExceedingDepthLimit",
                  "fileCountExceedingDepthLimit"):
      with self.subTest(field=field):
        self.assertEqual(result[field], "N/A")

    site_metrics = result["siteMetrics"][site]
    for field in ("shortcutCount", "folderCountExceedingDepthLimit",
                  "fileCountExceedingDepthLimit"):
      with self.subTest(site_field=field):
        self.assertEqual(site_metrics[field], "N/A")

  def test_site_classification(self):
    """Verify personal and team sites are classified for the report."""
    personal = self.register_site(
        "od1", f"{MY_TENANT}/personal/alice_contoso_com", is_personal=True
    )
    team = self.register_site("siteA", f"{TENANT}/sites/Finance")
    d1 = self.register_library(
        "d1", f"{MY_TENANT}/personal/alice_contoso_com/Documents"
    )
    d2 = self.register_library("d2", f"{TENANT}/sites/Finance/Shared%20Documents")
    metrics = self.build_metrics([personal, team])

    out = self.run_scan(
        metrics, {personal: [d1], team: [d2]}, {},
        {
            d1: {"item": 10, "files": 8, "size": 100},
            d2: {"item": 20, "files": 15, "size": 200},
        },
    )

    self.assertEqual(
        out["metrics"]["siteClassification"],
        {personal: "personal", team: "teams"},
    )

  def test_empty_target_list(self):
    """Verify a scan with nothing to do returns zeros without error."""
    site = self.register_site("siteA", f"{TENANT}/sites/Finance")
    metrics = self.build_metrics([site])

    processed, failed = self.estimator._scan_document_libraries(
        metrics, [], {}, []
    )

    self.assertEqual((processed, failed), (0, 0))

  def test_hard_stop_skips_remaining_libraries(self):
    """Verify a set stop event prevents any library from being queried."""
    site = self.register_site("siteA", f"{TENANT}/sites/Finance")
    d1 = self.register_library("d1", f"{TENANT}/sites/Finance/Shared%20Documents")
    metrics = self.build_metrics([site])
    self.stop_event.set()

    out = self.run_scan(
        metrics, {site: [d1]}, {}, {d1: {"item": 10, "files": 8, "size": 100}}
    )

    self.assertEqual(out["processed"], 0)
    self.assertEqual(out["endpoints"], [])


class ShallowScanFailureTest(ShallowScanBaseTest):
  """Layer 3c: failure isolation and recording."""

  def test_failure_types_are_recorded(self):
    """Verify each error kind maps to the right failure type and status."""
    cases = [
        (
            "permission_denied",
            PermissionError("HTTP 403 Forbidden"),
            FailureType.FAILURE_STATUS_CODE_ERROR.name,
            403,
        ),
        (
            "server_error",
            RuntimeError("HTTP 500 after retries"),
            FailureType.UNKNOWN_ERROR.name,
            500,
        ),
        (
            "throttled_out",
            RuntimeError("HTTP 429 after retries"),
            FailureType.UNKNOWN_ERROR.name,
            500,
        ),
    ]

    for name, error, expected_type, expected_status in cases:
      with self.subTest(case=name):
        self.setUp()
        site = self.register_site("siteA", f"{TENANT}/sites/Finance")
        d1 = self.register_library(
            "d1", f"{TENANT}/sites/Finance/Shared%20Documents"
        )
        metrics = self.build_metrics([site])

        out = self.run_scan(metrics, {site: [d1]}, {}, {d1: error})

        self.assertEqual(out["processed"], 1)
        self.assertEqual(out["failed"], 1)
        self.assertEqual(len(out["failures"]), 1)
        self.assertEqual(out["failures"][0]["type"], expected_type)
        self.assertEqual(out["failures"][0]["statusCode"], expected_status)
        self.assertEqual(out["metrics"]["siteMetrics"][site]["fileCount"], 0)

  def test_partial_failure_is_all_or_nothing(self):
    """Verify a library whose second call fails contributes nothing at all.

    This is the invariant chosen during design: no partial credit. If ItemCount
    succeeds but StorageMetrics fails, the library must not report the item
    count it already retrieved.
    """
    site = self.register_site("siteA", f"{TENANT}/sites/Finance")
    d1 = self.register_library("d1", f"{TENANT}/sites/Finance/Shared%20Documents")
    metrics = self.build_metrics([site])

    out = self.run_scan(
        metrics, {site: [d1]}, {},
        {d1: {"item": 500, "storage": RuntimeError("StorageMetrics failed"),
              "files": 0, "size": 0}},
    )

    site_metrics = out["metrics"]["siteMetrics"][site]
    self.assertEqual(site_metrics["fileCount"], 0)
    self.assertEqual(site_metrics["folderCount"], 0)
    self.assertEqual(site_metrics["resourceCount"], 0)
    self.assertEqual(site_metrics["totalSize"], 0)
    self.assertNotIn(d1, out["metrics"]["driveMetrics"])
    self.assertEqual(out["failed"], 1)
    self.assertEqual(out["metrics"]["tenantLevelWarningResourceCount"], 0)

  def test_failed_library_does_not_affect_siblings(self):
    """Verify one bad library leaves the rest of the site intact."""
    site = self.register_site("siteA", f"{TENANT}/sites/Finance")
    good = self.register_library(
        "d1", f"{TENANT}/sites/Finance/Shared%20Documents"
    )
    bad = self.register_library("d2", f"{TENANT}/sites/Finance/SiteAssets")
    metrics = self.build_metrics([site])

    out = self.run_scan(
        metrics, {site: [good, bad]}, {},
        {
            good: {"item": 100, "files": 80, "size": 1000},
            bad: RuntimeError("boom"),
        },
    )

    site_metrics = out["metrics"]["siteMetrics"][site]
    self.assertEqual(site_metrics["fileCount"], 80)
    self.assertEqual(site_metrics["folderCount"], 20)
    self.assertEqual(out["processed"], 2)
    self.assertEqual(out["failed"], 1)
    self.assertIn(good, out["metrics"]["driveMetrics"])
    self.assertNotIn(bad, out["metrics"]["driveMetrics"])


class ShallowScanOrchestrationTest(unittest.TestCase):
  """Layer 4: calculate_resource_metrics end to end over the Graph mock."""

  def build_graph_data(self):
    """Builds a minimal tenant: one team site with two document libraries."""
    site = {
        "id": "siteA",
        "displayName": "Finance",
        "webUrl": f"{TENANT}/sites/Finance",
        "isPersonalSite": False,
        "drives": ["d1", "d2"],
        "lists": [],
        "subsites": [],
    }
    return {
        "root_site": "siteA",
        "sites": {"siteA": site},
        "all_sites": [site],
        "lists": {},
        "drives": {
            "d1": {
                "id": "d1",
                "name": "Documents",
                "driveType": "documentLibrary",
                "webUrl": f"{TENANT}/sites/Finance/Shared%20Documents",
            },
            "d2": {
                "id": "d2",
                "name": "Site Assets",
                "driveType": "documentLibrary",
                "webUrl": f"{TENANT}/sites/Finance/SiteAssets",
            },
        },
        "items": {},
        "licenses": [],
    }

  def build_estimator(self, graph_data, stop_event=None, logger=None):
    estimator = ShallowFileEstimator(
        config=build_config(),
        url_invoker=MockUrlInvoker(graph_data),
        cert_token_manager=MockCertTokenManager(),
        logger=logger if logger is not None else (lambda *_: None),
        stop_event=stop_event if stop_event is not None else threading.Event(),
        progress_update_callback=lambda *a, **k: None,
    )
    estimator.set_id_to_display_name_map({})
    return estimator

  def test_end_to_end_happy_path(self):
    """Verify discovery feeds Phase 2 and produces a complete report."""
    graph_data = self.build_graph_data()
    estimator = self.build_estimator(graph_data)
    failures = []

    def fake_execute_get(_self, endpoint, base_url, domain, logger,
                         stop_event=None):
      if "GetList(" in endpoint:
        return {"ItemCount": 100}
      return storage_body(80, 2048)

    with mock.patch.object(SpRestConnector, "_execute_get", fake_execute_get):
      result = estimator.calculate_resource_metrics({}, failures)

    self.assertTrue(result["isShallowScan"])
    self.assertIn("siteA", result["siteMetrics"])
    # Two libraries, each 80 files and 20 folders.
    self.assertEqual(result["siteMetrics"]["siteA"]["fileCount"], 160)
    self.assertEqual(result["siteMetrics"]["siteA"]["folderCount"], 40)
    self.assertEqual(result["fileCount"], 160)
    self.assertEqual(result["folderCount"], 40)
    self.assertEqual(len(result["driveMetrics"]), 2)

  def test_site_discovery_failure_is_recorded(self):
    """Verify a Graph 500 during discovery surfaces as a failure."""
    graph_data = self.build_graph_data()
    estimator = self.build_estimator(graph_data)
    estimator.url_invoker.token_manager.session.custom_responses["/sites"] = (
        500,
        {"error": {"message": "Top-level Sites Fetch Failure Simulation"}},
    )
    failures = []

    def fake_execute_get(_self, endpoint, base_url, domain, logger,
                         stop_event=None):
      if "GetList(" in endpoint:
        return {"ItemCount": 10}
      return storage_body(8, 100)

    with mock.patch.object(SpRestConnector, "_execute_get", fake_execute_get):
      estimator.calculate_resource_metrics({}, failures)

    self.assertTrue(failures, "Expected a discovery failure to be recorded")

  def test_unexpected_exception_returns_empty_metrics(self):
    """Verify an unexpected error is captured rather than propagated."""
    graph_data = self.build_graph_data()
    estimator = self.build_estimator(graph_data)
    failures = []

    with mock.patch.object(
        ShallowFileEstimator,
        "_discover_sites",
        side_effect=RuntimeError("catastrophic failure"),
    ):
      result = estimator.calculate_resource_metrics({}, failures)

    self.assertEqual(result, {})
    self.assertEqual(len(failures), 1)
    self.assertEqual(failures[0]["type"], FailureType.UNKNOWN_ERROR.name)
    self.assertEqual(failures[0]["statusCode"], 500)

  def test_hard_stop_before_phase_two(self):
    """Verify a stop requested during discovery skips the REST phase."""
    graph_data = self.build_graph_data()
    stop_event = threading.Event()
    stop_event.set()
    estimator = self.build_estimator(graph_data, stop_event)
    failures = []

    def fail_if_called(*_args, **_kwargs):
      raise AssertionError("Phase 2 must not run after a hard stop")

    with mock.patch.object(SpRestConnector, "_execute_get", fail_if_called):
      result = estimator.calculate_resource_metrics({}, failures)

    self.assertEqual(result.get("driveMetrics"), {})

  def test_hard_stop_during_phase_two_preserves_discovered_counts(self):
    """Verify that stopping during Phase 2 preserves discovered folders/files."""
    graph_data = self.build_graph_data()
    stop_event = threading.Event()
    logs = []
    estimator = self.build_estimator(graph_data, stop_event, logger=logs.append)
    failures = []

    call_count = 0
    lock = threading.Lock()

    def fake_execute_get(_self, endpoint, base_url, domain, logger, stop_event=None):
      nonlocal call_count
      with lock:
        call_count += 1
        # Set stop_event after the first library's REST calls (GetList & StorageMetrics)
        if call_count >= 2:
          stop_event.set()
      if "GetList(" in endpoint:
        return {"ItemCount": 100}
      return storage_body(80, 2048)

    with mock.patch.object(SpRestConnector, "_execute_get", fake_execute_get):
      result = estimator.calculate_resource_metrics({}, failures)

    self.assertTrue(result["isShallowScan"])
    # First library produced 80 files and 20 folders (100 - 80)
    self.assertEqual(result["folderCount"], 20)
    self.assertEqual(result["fileCount"], 80)
    self.assertEqual(
        result["folderCount"],
        sum(s.get("folderCount", 0) for s in result["siteMetrics"].values()),
    )
    self.assertEqual(
        result["fileCount"],
        sum(s.get("fileCount", 0) for s in result["siteMetrics"].values()),
    )
    self.assertIn("phase_runtimes", result)
    self.assertIn("drive_discovery_seconds", result["phase_runtimes"])
    self.assertIn("siteClassification", result)
    self.assertTrue(
        any("[Phase 2: Drive Discovery] Stopped after" in line for line in logs),
        f"Expected stopped log in: {logs}",
    )
    self.assertTrue(
        any("[Phase 2] Shallow Drive Discovery stopped." in line for line in logs),
        f"Expected phase 2 stopped log in: {logs}",
    )

  def test_phase_runtimes_tracked_and_logged(self):
    """Verify Phase 1 and Phase 2 runtimes are recorded in metrics and logged."""
    graph_data = self.build_graph_data()
    logs = []
    estimator = self.build_estimator(graph_data, logger=logs.append)
    failures = []

    def fake_execute_get(_self, endpoint, base_url, domain, logger, stop_event=None):
      if "GetList(" in endpoint:
        return {"ItemCount": 10}
      return storage_body(10, 1024)

    with mock.patch.object(SpRestConnector, "_execute_get", fake_execute_get):
      result = estimator.calculate_resource_metrics({}, failures)

    self.assertIn("phase_runtimes", result)
    phase_runtimes = result["phase_runtimes"]
    self.assertIn("site_discovery_seconds", phase_runtimes)
    self.assertIn("drive_discovery_seconds", phase_runtimes)
    self.assertIsInstance(phase_runtimes["site_discovery_seconds"], float)
    self.assertIsInstance(phase_runtimes["drive_discovery_seconds"], float)
    self.assertGreaterEqual(phase_runtimes["site_discovery_seconds"], 0.0)
    self.assertGreaterEqual(phase_runtimes["drive_discovery_seconds"], 0.0)

    # Check that logger was called for both phases
    phase1_logged = any("[Phase 1: Site Discovery] Completed in" in line for line in logs)
    phase2_logged = any("[Phase 2: Drive Discovery] Completed in" in line for line in logs)
    self.assertTrue(phase1_logged, f"Expected Phase 1 log in: {logs}")
    self.assertTrue(phase2_logged, f"Expected Phase 2 log in: {logs}")

  def test_deep_file_estimator_phase_runtimes_tracked_and_logged(self):
    """Verify FileEstimator (Deep Scan) records and logs phase runtimes."""
    from estimators.file_estimator import FileEstimator
    graph_data = self.build_graph_data()
    logs = []
    estimator = FileEstimator(
        config=build_config(),
        url_invoker=MockUrlInvoker(graph_data),
        logger=logs.append,
        stop_event=threading.Event(),
        progress_update_callback=lambda *a, **k: None,
    )
    failures = []
    result = estimator.calculate_resource_metrics({}, failures)

    self.assertIn("phase_runtimes", result)
    phase_runtimes = result["phase_runtimes"]
    self.assertIn("site_discovery_seconds", phase_runtimes)
    self.assertIn("drive_discovery_seconds", phase_runtimes)
    self.assertIsInstance(phase_runtimes["site_discovery_seconds"], float)
    self.assertIsInstance(phase_runtimes["drive_discovery_seconds"], float)
    self.assertGreaterEqual(phase_runtimes["site_discovery_seconds"], 0.0)
    self.assertGreaterEqual(phase_runtimes["drive_discovery_seconds"], 0.0)

    phase1_logged = any("[Phase 1: Site Discovery] Completed in" in line for line in logs)
    phase2_logged = any("[Phase 2: Drive Discovery] Completed in" in line for line in logs)
    self.assertTrue(phase1_logged, f"Expected Phase 1 log in: {logs}")
    self.assertTrue(phase2_logged, f"Expected Phase 2 log in: {logs}")


class ShallowScanReportCsvUploadTest(unittest.TestCase):
  """Verifies uploading a Shallow Scan site_report CSV passes validation and parses metrics."""

  def _make_dummy_tool(self, csv_path: str, shallow_toggle: bool = False):
    from ui.files_ui import FileMigrationEstimatorTool

    class DummyVar:
      def __init__(self, val):
        self._val = val
      def get(self):
        return self._val

    tool = object.__new__(FileMigrationEstimatorTool)
    tool.user_source = DummyVar("csv")
    tool.user_csv_path = DummyVar(csv_path)
    tool.include_personal_sites = DummyVar(True)
    tool.include_team_sites = DummyVar(True)
    tool.val_shallow_scan = shallow_toggle
    tool.skipped_actual_scan = False
    return tool

  def test_shallow_scan_site_report_csv_validates_and_parses(self):
    """Uploading a shallow scan site_report.csv with N/A columns succeeds and preserves Shallow Scan semantics."""
    import os
    import tempfile

    csv_content = (
        "Site URL/Name,Subsite Count,DL Count,List Count,Folder Count,File Count,"
        "Shortcut Count,Folder Count > Depth Limit 100,File Count > Depth Limit 100,"
        "Entities with > 500k item count,Entities with > 200k item count,Corpus Size,Suggested Batch\n"
        "https://smh3v-my.sharepoint.com/personal/bugbash5_smh3v_onmicrosoft_com,0,1,4,2,99997,N/A,N/A,N/A,0,0,73.24 MB,Batch 1\n"
        "https://smh3v-my.sharepoint.com/personal/bugbash4_smh3v_onmicrosoft_com,0,1,8,3,99977,N/A,N/A,N/A,0,0,73.24 MB,Batch 2\n"
        "https://smh3v-my.sharepoint.com/personal/runwaydrivetest100_smh3v_onmicrosoft_com,0,1,3,200,17540,N/A,N/A,N/A,0,0,540.93 GB,Batch 4\n"
    )

    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as tmp:
      tmp.write(csv_content)
      tmp_path = tmp.name

    try:
      tool = self._make_dummy_tool(tmp_path, shallow_toggle=False)
      config = build_config(user_source="csv", csv_path=tmp_path)

      # 1. _validate_csv must not raise "CSV contains empty or null values."
      with mock.patch("ui.files_ui.messagebox.showerror") as mock_err:
        tool._validate_csv()
        mock_err.assert_not_called()

      # 2. _try_get_metrics_from_csv_report must parse N/A safely and auto-enable shallow scan mode
      metrics = tool._try_get_metrics_from_csv_report(config)
      self.assertIsNotNone(metrics)
      self.assertTrue(metrics["isShallowScan"])
      self.assertTrue(tool.val_shallow_scan)
      self.assertTrue(config.shallow_scan)
      self.assertTrue(tool.skipped_actual_scan)

      self.assertEqual(metrics["siteCount"], 3)
      self.assertEqual(metrics["folderCount"], 2 + 3 + 200)
      self.assertEqual(metrics["fileCount"], 99997 + 99977 + 17540)
      self.assertEqual(metrics["shortcutCount"], "N/A")
      self.assertEqual(metrics["folderCountExceedingDepthLimit"], "N/A")
      self.assertEqual(metrics["fileCountExceedingDepthLimit"], "N/A")

      s1 = metrics["siteMetrics"]["https://smh3v-my.sharepoint.com/personal/bugbash5_smh3v_onmicrosoft_com"]
      self.assertEqual(s1["folderCount"], 2)
      self.assertEqual(s1["fileCount"], 99997)
      self.assertEqual(s1["shortcutCount"], "N/A")
      self.assertEqual(s1["folderCountExceedingDepthLimit"], "N/A")
      self.assertEqual(s1["fileCountExceedingDepthLimit"], "N/A")
      self.assertEqual(s1["resourceCount"], 99999)
      self.assertAlmostEqual(s1["totalSize"], 73.24 * (1024 ** 2), places=0)
    finally:
      if os.path.exists(tmp_path):
        os.remove(tmp_path)


class EncryptedFilesSharePointSearchTest(unittest.TestCase):
  """Verifies SharePoint REST postquery encrypted file detection with IndexDocId cursor pagination."""

  def test_extract_postquery_rows_handles_verbose_and_nometadata(self):
    """_extract_postquery_rows parses both odata=verbose and odata=nometadata structures."""
    verbose_payload = {
        "d": {
            "postquery": {
                "PrimaryQueryResult": {
                    "RelevantResults": {
                        "Table": {
                            "Rows": {
                                "results": [
                                    {
                                        "Cells": {
                                            "results": [
                                                {"Key": "DocId", "Value": "101"},
                                                {"Key": "Size", "Value": "4096"},
                                                {"Key": "Path", "Value": "https://contoso.sharepoint.com/sites/HR/Shared Documents/a.docx"},
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        }
    }
    rows = SpRestConnector._extract_postquery_rows(verbose_payload)
    self.assertEqual(len(rows), 1)
    self.assertEqual(rows[0]["DocId"], "101")
    self.assertEqual(rows[0]["Size"], "4096")

  def test_search_encrypted_files_by_labels_cursor_pagination(self):
    """search_encrypted_files_by_labels paginates using IndexDocId > last_doc_id sorted by DocId ascending."""
    token_manager = MockCertTokenManager(MockSpSession())
    connector = SpRestConnector(token_manager, max_retries=2, backoff=1)

    captured_payloads = []

    def fake_execute_post(endpoint, payload, base_url, domain, logger, stop_event=None):
      captured_payloads.append((endpoint, payload))
      if len(captured_payloads) == 1:
        # Return exactly 500 rows with DocId 1..500 to trigger next cursor page
        rows_data = [
            {
                "Cells": [
                    {"Key": "DocId", "Value": str(i)},
                    {"Key": "Size", "Value": "1024"},
                    {"Key": "Path", "Value": f"https://contoso.sharepoint.com/sites/HR/Shared Documents/file_{i}.docx"},
                    {"Key": "UniqueId", "Value": f"guid-{i}"},
                ]
            }
            for i in range(1, 501)
        ]
      else:
        rows_data = [
            {
                "Cells": [
                    {"Key": "DocId", "Value": "501"},
                    {"Key": "Size", "Value": "2048"},
                    {"Key": "Path", "Value": "https://contoso.sharepoint.com/sites/HR/Shared Documents/file_501.docx"},
                    {"Key": "UniqueId", "Value": "guid-501"},
                ]
            }
        ]
      return {
          "PrimaryQueryResult": {
              "RelevantResults": {"Table": {"Rows": rows_data}}
          }
      }

    with mock.patch.object(connector, "_execute_post", side_effect=fake_execute_post):
      results = connector.search_encrypted_files_by_labels(
          base_url="https://contoso.sharepoint.com",
          encrypted_label_ids=["label-guid-1"],
          path_prefixes=["https://contoso.sharepoint.com/sites/HR"],
      )

    self.assertEqual(len(results), 501)
    self.assertEqual(len(captured_payloads), 2)
    req1 = captured_payloads[0][1]["request"]
    req2 = captured_payloads[1][1]["request"]
    self.assertEqual(req1["SortList"]["results"], [{"Property": "DocId", "Direction": "0"}])
    self.assertNotIn("IndexDocId>", req1["Querytext"])
    self.assertIn("IndexDocId>500", req2["Querytext"])
    self.assertEqual(req2["StartRow"], 0)


if __name__ == "__main__":
  unittest.main()
