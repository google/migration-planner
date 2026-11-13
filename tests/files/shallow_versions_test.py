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

"""Tests for the Shallow Scan file versions estimate.

Run with:
  python3 -m unittest tests.files.shallow_versions_test -v
"""

import os
import tempfile
import unittest
from unittest import mock

import pandas as pd

from estimators.files_shallow.shallow_file_estimator import estimate_library_versions
from tests.files.mocks import MockCertTokenManager
from tests.files.mocks import MockResponse
from tests.files.mocks import MockSpSession
from tests.files.shallow_estimator_test import ShallowScanBaseTest
from tests.files.shallow_estimator_test import TENANT
from tests.files.shallow_estimator_test import build_config
from tests.files.shallow_estimator_test import storage_body
from ui.files_shallow import shallow_ui_helpers
from util.files_shallow.sp_rest_connector import SpRestConnector

V_CORPUS = shallow_ui_helpers.VERSIONS_CORPUS_COL
V_FILES = shallow_ui_helpers.VERSIONS_FILE_COUNT_COL


class EstimateLibraryVersionsTest(unittest.TestCase):
  """Covers the per-library formula and its edge cases."""

  def test_no_versions_gets_exact_ten_percent_buffer(self):
    """10 files and no versions -> exactly 11 (no float rounding to 12)."""
    self.assertEqual(estimate_library_versions(10, 1000, 1000, 0), (1000, 11))

  def test_versions_are_extrapolated_from_average_file_size(self):
    """corpus = 3500 - 500 = 3000; avg = 100; 30 files * 1.1 = 33."""
    self.assertEqual(estimate_library_versions(10, 1000, 3500, 500), (3000, 33))

  def test_fractional_estimate_rounds_up(self):
    """avg = 100; 1050 / 100 = 10.5; * 1.1 = 11.55 -> 12."""
    self.assertEqual(estimate_library_versions(7, 700, 1050, 0), (1050, 12))

  def test_corpus_never_below_active_size(self):
    """TotalSize - AdditionalFileStreamSize < TotalFileStreamSize is clamped."""
    self.assertEqual(estimate_library_versions(10, 1000, 900, 200), (1000, 11))

  def test_no_files(self):
    self.assertEqual(estimate_library_versions(0, 0, 5000, 100), (0, 0))

  def test_empty_files(self):
    """Files with zero stream size -> ceil(1.1 * files)."""
    self.assertEqual(estimate_library_versions(5, 0, 800, 0), (800, 6))

  def test_large_values_stay_exact(self):
    files, active = 4_000_000, 7_500_000_000_000
    corpus, est = estimate_library_versions(files, active, active * 3, 0)
    self.assertEqual(corpus, active * 3)
    self.assertEqual(est, 13_200_000)


class ConnectorAdditionalSizeTest(unittest.TestCase):
  """Covers parsing AdditionalFileStreamSize from StorageMetrics."""

  def setUp(self):
    self.session = MockSpSession()
    self.connector = SpRestConnector(
        MockCertTokenManager(self.session), max_retries=1, backoff=1
    )
    self.web = f"{TENANT}/sites/Finance"
    self.library = f"{self.web}/Shared Documents"

  def _metrics(self, body):
    self.session.register("GetList(", MockResponse(200, {"ItemCount": 12}))
    self.session.register("GetFolderByServerRelativeUrl(", MockResponse(200, body))
    return self.connector.get_library_metrics(self.web, self.library, logger=lambda *_: None)

  def test_additional_size_parsed(self):
    result = self._metrics(storage_body(10, 1000, 3500, "500"))
    self.assertEqual(result["total_size_bytes"], 3500)
    self.assertEqual(result["additional_size_bytes"], 500)

  def test_missing_additional_size_defaults_to_zero(self):
    self.assertEqual(self._metrics(storage_body(10, 1000))["additional_size_bytes"], 0)


class ShallowEstimatorVersionsTest(ShallowScanBaseTest):
  """Covers per-DL aggregation of the versions estimate in the estimator."""

  def _scan(self, include_versions, library_data):
    self.config.include_file_versions = include_versions
    site = self.register_site("siteA", f"{TENANT}/sites/Finance")
    self.register_library("d1", f"{TENANT}/sites/Finance/Shared%20Documents")
    self.register_library("d2", f"{TENANT}/sites/Finance/Archive")
    metrics = self.build_metrics([site])
    return site, self.run_scan(metrics, {site: ["d1", "d2"]}, {}, library_data)

  def test_versions_aggregated_per_site_drive_and_tenant(self):
    site, out = self._scan(True, {
        "d1": {"item": 12, "files": 10, "size": 1000, "total": 3500, "additional": 500},
        "d2": {"item": 7, "files": 7, "size": 700, "total": 1050, "additional": 0},
    })
    metrics = out["metrics"]
    s_meta = metrics["siteMetrics"][site]

    self.assertTrue(metrics["hasVersionEstimates"])
    self.assertEqual(s_meta["versionsCorpusSize"], 3000 + 1050)
    self.assertEqual(s_meta["versionsEstimatedFileCount"], 33 + 12)
    self.assertEqual(metrics["versionsCorpusSize"], 4050)
    self.assertEqual(metrics["versionsEstimatedFileCount"], 45)
    self.assertEqual(metrics["driveMetrics"]["d1"]["versionsEstimatedFileCount"], 33)
    # Active metrics are unchanged.
    self.assertEqual(s_meta["totalSize"], 1700)
    self.assertEqual(s_meta["fileCount"], 17)

  def test_failed_library_contributes_nothing(self):
    site, out = self._scan(True, {
        "d1": {"item": 12, "files": 10, "size": 1000, "total": 3500, "additional": 500},
        "d2": RuntimeError("boom"),
    })
    s_meta = out["metrics"]["siteMetrics"][site]
    self.assertEqual(s_meta["versionsCorpusSize"], 3000)
    self.assertEqual(s_meta["versionsEstimatedFileCount"], 33)

  def test_versions_off_adds_no_fields(self):
    site, out = self._scan(False, {
        "d1": {"item": 12, "files": 10, "size": 1000, "total": 3500, "additional": 500},
        "d2": {"item": 7, "files": 7, "size": 700},
    })
    metrics = out["metrics"]
    self.assertNotIn("hasVersionEstimates", metrics)
    self.assertNotIn("versionsCorpusSize", metrics["siteMetrics"][site])
    self.assertNotIn("versionsCorpusSize", metrics["driveMetrics"]["d1"])

  def test_large_resource_threshold_ignores_versions(self):
    """Versions push the estimate to 3,300 files (> limit 500), but active items are 400."""
    site, out = self._scan(True, {
        "d1": {"item": 400, "files": 300, "size": 300, "total": 3000, "additional": 0},
        "d2": {"item": 1, "files": 1, "size": 1},
    })
    metrics = out["metrics"]
    self.assertEqual(metrics["driveMetrics"]["d1"]["versionsEstimatedFileCount"], 3300)
    self.assertEqual(metrics["siteMetrics"][site]["largeResourceCount"], 0)
    self.assertEqual(metrics["tenantLevelLargeResources"], [])


class UsesVersionEstimatesTest(unittest.TestCase):

  def test_requires_checkbox_and_data(self):
    on, off = mock.Mock(val_include_file_versions=True), mock.Mock(val_include_file_versions=False)
    self.assertTrue(shallow_ui_helpers.uses_version_estimates(on, {"hasVersionEstimates": True}))
    self.assertFalse(shallow_ui_helpers.uses_version_estimates(on, {}))
    self.assertFalse(shallow_ui_helpers.uses_version_estimates(off, {"hasVersionEstimates": True}))


class VersionsPlanningTest(unittest.TestCase):
  """Covers ETA planning on versions-inclusive values with active values restored."""

  def _df(self, with_versions=True):
    rows = [
        {"Site Id": "a", "File Count": 10, "Corpus Size": 1000, "Resource Count": 10},
        {"Site Id": "b", "File Count": 5, "Corpus Size": 500, "Resource Count": 5},
    ]
    if with_versions:
      rows[0].update({V_CORPUS: 3000, V_FILES: 33})
      rows[1].update({V_CORPUS: 500, V_FILES: 6})
    return pd.DataFrame(rows)

  def _tool(self):
    tool = mock.Mock()
    tool.val_shallow_scan = True
    seen = {}

    def fake_plan(df, _lm):
      seen["df"] = df.copy()
      batch = {"df_subset": df, "corpus_size": float(df["Corpus Size"].sum()),
               "file_count": int(df["File Count"].sum()), "shortcut_count": 0}
      return df.assign(**{"Suggested Batch": "Batch 1"}), [batch], 1.0, [{"batches": [batch]}]

    tool.calculate_migration_batches.side_effect = fake_plan
    return tool, seen

  def test_eta_uses_versions_and_display_restores_active(self):
    tool, seen = self._tool()
    df_final, batches, _, buckets = shallow_ui_helpers.calculate_shallow_scan_batches(
        tool, self._df(), {}
    )

    self.assertEqual(seen["df"]["Corpus Size"].tolist(), [3000, 500])
    self.assertEqual(seen["df"]["File Count"].tolist(), [33, 6])
    self.assertEqual(df_final["Corpus Size"].tolist(), [1000, 500])
    self.assertEqual(df_final["File Count"].tolist(), [10, 5])
    self.assertEqual(df_final[V_FILES].tolist(), [33, 6])
    self.assertEqual(batches[0]["corpus_size"], 1500.0)
    self.assertEqual(batches[0]["file_count"], 15)
    self.assertIs(buckets[0]["batches"][0], batches[0])

  def test_without_versions_columns_planning_is_unchanged(self):
    tool, seen = self._tool()
    shallow_ui_helpers.calculate_shallow_scan_batches(tool, self._df(False), {})
    self.assertEqual(seen["df"]["Corpus Size"].tolist(), [1000, 500])
    self.assertEqual(seen["df"]["File Count"].tolist(), [10, 5])

  def test_deep_scan_recommended_ignores_versions_estimate(self):
    """Only the active '>500k' column flags sites, even with a huge versions estimate."""
    tool, _ = self._tool()
    df = self._df()
    df[V_FILES] = [900_000, 2_000_000]
    df[shallow_ui_helpers.LARGE_RESOURCE_COL] = [0, 1]

    df_final, _, _, _ = shallow_ui_helpers.calculate_shallow_scan_batches(tool, df, {})
    mask = shallow_ui_helpers.get_deep_scan_recommended_mask(tool, df_final)

    self.assertEqual(df_final.loc[mask, "Site Id"].tolist(), ["b"])

  def test_versions_increase_eta_with_real_planner(self):
    """End-to-end with the real P90 planner: versions-inclusive ETA is larger."""
    from ui.files_ui import FileMigrationEstimatorTool

    class _StubEstimator:
      def calculate_migration_eta(self, data):
        return sum(item["files"] for item in data["items"]) / 1000.0

    tool = object.__new__(FileMigrationEstimatorTool)
    tool.val_parallel_batches = 1
    tool.val_eta_max_users = 5000
    tool.val_include_team_sites = False
    tool.val_shallow_scan = True
    tool.factory = mock.Mock()
    tool.factory.get_files_estimator.return_value = _StubEstimator()
    tool.log_msg = mock.Mock()

    _, _, eta_active, _ = shallow_ui_helpers.calculate_shallow_scan_batches(tool, self._df(False), {})
    _, batches, eta_versions, _ = shallow_ui_helpers.calculate_shallow_scan_batches(tool, self._df(), {})

    self.assertAlmostEqual(eta_active, 0.015)
    self.assertAlmostEqual(eta_versions, 0.039)
    self.assertEqual(sum(b["file_count"] for b in batches), 15)


class SiteReportUploadVersionsTest(unittest.TestCase):
  """Covers backward-compatible parsing of the versions site report columns."""

  HEADER = (
      "Site URL/Name,Subsite Count,DL Count,Failed DL Count,List Count,Folder Count,File Count,"
      "Shortcut Count,Folder Count > Depth Limit 100,File Count > Depth Limit 100,"
      "Entities with > 500k item count,Corpus Size"
  )

  def _parse(self, csv_content):
    from ui.files_ui import FileMigrationEstimatorTool

    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, encoding="utf-8") as tmp:
      tmp.write(csv_content)
      path = tmp.name
    try:
      tool = object.__new__(FileMigrationEstimatorTool)
      tool.user_source = mock.Mock(get=lambda: "csv")
      tool.user_csv_path = mock.Mock(get=lambda: path)
      tool.include_personal_sites = mock.Mock(get=lambda: True)
      tool.include_team_sites = mock.Mock(get=lambda: True)
      with mock.patch("ui.files_ui.messagebox.showerror") as mock_err:
        tool._validate_csv()
        mock_err.assert_not_called()
      return tool._try_get_metrics_from_csv_report(build_config(user_source="csv", csv_path=path))
    finally:
      os.remove(path)

  def test_new_report_columns_are_parsed(self):
    metrics = self._parse(
        f"{self.HEADER},{V_CORPUS},{V_FILES},Suggested Batch\n"
        "https://t.sharepoint.com/sites/a,0,1,0,1,2,10,N/A,N/A,N/A,0,1.00 KB,3.00 KB,33,Batch 1\n"
    )
    s = metrics["siteMetrics"]["https://t.sharepoint.com/sites/a"]
    self.assertTrue(metrics["hasVersionEstimates"])
    self.assertEqual(s["versionsCorpusSize"], 3072)
    self.assertEqual(s["versionsEstimatedFileCount"], 33)
    self.assertEqual(metrics["versionsEstimatedFileCount"], 33)
    self.assertEqual(metrics["versionFallbackRows"], 0)

  def test_old_report_without_columns_is_compatible(self):
    metrics = self._parse(
        f"{self.HEADER},Suggested Batch\n"
        "https://t.sharepoint.com/sites/a,0,1,0,1,2,10,N/A,N/A,N/A,0,1.00 KB,Batch 1\n"
    )
    self.assertIsNotNone(metrics)
    self.assertNotIn("hasVersionEstimates", metrics)
    self.assertNotIn("versionsCorpusSize", metrics["siteMetrics"]["https://t.sharepoint.com/sites/a"])

  def test_blank_rows_fall_back_to_active_values(self):
    metrics = self._parse(
        f"{self.HEADER},{V_CORPUS},{V_FILES}\n"
        "https://t.sharepoint.com/sites/a,0,1,0,1,2,10,N/A,N/A,N/A,0,1.00 KB,3.00 KB,33\n"
        "https://t.sharepoint.com/sites/b,0,1,0,1,2,4,N/A,N/A,N/A,0,2.00 KB,,\n"
    )
    b = metrics["siteMetrics"]["https://t.sharepoint.com/sites/b"]
    self.assertEqual(b["versionsCorpusSize"], 2048)
    self.assertEqual(b["versionsEstimatedFileCount"], 4)
    self.assertEqual(metrics["versionFallbackRows"], 1)

  def test_all_blank_columns_count_as_old_report(self):
    metrics = self._parse(
        f"{self.HEADER},{V_CORPUS},{V_FILES}\n"
        "https://t.sharepoint.com/sites/a,0,1,0,1,2,10,N/A,N/A,N/A,0,1.00 KB,N/A,N/A\n"
    )
    self.assertNotIn("hasVersionEstimates", metrics)


class MigrationReportExportVersionsTest(unittest.TestCase):
  """Covers the exported migration report with the versions estimate."""

  def _export(self, include_versions):
    import csv
    from ui.files_ui import FileMigrationEstimatorTool

    site = "https://t.sharepoint.com/sites/a"
    tool = object.__new__(FileMigrationEstimatorTool)
    tool.val_include_file_versions = include_versions
    tool.val_include_recycle_bin_contents = False
    tool.val_scan_encrypted_files = False
    tool.show_eta = True
    tool.id_to_display_name = {}
    tool.last_scan_data = {
        "isShallowScan": True,
        "hasVersionEstimates": True,
        "siteMetrics": {site: {
            "dlCount": 1, "fileCount": 10, "folderCount": 2, "totalSize": 1024,
            "shortcutCount": "N/A", "versionsCorpusSize": 3072,
            "versionsEstimatedFileCount": 33,
        }},
        "siteClassification": {site: "teams"},
        "df": pd.DataFrame([{"Site Id": site, "Suggested Batch": "Batch 1"}]),
    }
    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as tmp:
      path = tmp.name
    try:
      with mock.patch("tkinter.filedialog.asksaveasfilename", return_value=path):
        tool.export_current_report()
      with open(path, encoding="utf-8") as f:
        return list(csv.reader(f))
    finally:
      os.remove(path)

  def test_versions_summary_rows_and_site_columns(self):
    rows = self._export(True)
    summary = {r[0]: r[1] for r in rows if len(r) == 2}
    self.assertEqual(summary["Total Corpus Size"], "1.00 KB")
    self.assertEqual(summary["Total Corpus Size (incl. Versions)"], "3.00 KB")
    self.assertEqual(summary["Estimated File Count (incl. Versions)"], "33")
    self.assertFalse(any("will not be migrated" in r[0] for r in rows if r))

    header = next(r for r in rows if r and r[0] == "Site Collection")
    site_row = next(r for r in rows if r and r[0].startswith("https://"))
    idx = header.index("Corpus Size")
    self.assertEqual(header[idx + 1:idx + 3], [V_CORPUS, V_FILES])
    self.assertEqual(site_row[idx:idx + 3], ["1.00 KB", "3.00 KB", "33"])
    self.assertEqual(len(header), len(site_row))
    self.assertNotIn("Historical File Version Count", header)

  def test_versions_unticked_exports_as_before(self):
    rows = self._export(False)
    header = next(r for r in rows if r and r[0] == "Site Collection")
    self.assertNotIn(V_CORPUS, header)
    self.assertFalse(any(r and r[0] == "Total Corpus Size (incl. Versions)" for r in rows))


if __name__ == "__main__":
  unittest.main()
