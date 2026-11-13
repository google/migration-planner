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

"""Tests for license fetching (live scans and site report uploads).

Run with:
  python3 -m unittest tests.files.license_fetch_test -v
"""

import time
import unittest
from unittest import mock

import pandas as pd

from estimators.file_estimator import FileEstimator
from tests.files.mocks import MockResponse
from tests.files.shallow_estimator_test import build_config


def _sku(units, applies_to="User", sharepoint=True):
  plan = "SharePoint" if sharepoint else "Exchange"
  return {
      "prepaidUnits": {"enabled": units},
      "consumedUnits": units // 2,
      "appliesTo": applies_to,
      "servicePlans": [{"servicePlanType": plan}],
  }


class GetLicenseMetricsTest(unittest.TestCase):
  """Covers FileEstimator._get_license_metrics success and failure handling."""

  def _estimator(self, responses):
    estimator = object.__new__(FileEstimator)
    estimator.config = build_config(retries=1, backoff=1)
    estimator.logger = lambda *_: None
    estimator.stop_event = None
    session = mock.Mock()
    session.get.side_effect = responses
    token_manager = mock.Mock()
    token_manager.get_valid_token_slot.return_value = {"token": "t", "expires_at": time.time() + 3600}
    token_manager.get_session.return_value = session
    estimator.url_invoker = mock.Mock(token_manager=token_manager)
    return estimator, token_manager

  def test_success_sums_sharepoint_skus_across_pages(self):
    estimator, token_manager = self._estimator([
        MockResponse(200, {"value": [_sku(10000), _sku(500, sharepoint=False)],
                           "@odata.nextLink": "https://next"}),
        MockResponse(200, {"value": [_sku(6000, applies_to="Company")]}),
    ])
    failures = []

    metrics = estimator.get_license_metrics(failures)

    self.assertNotIn("fetchError", metrics)
    self.assertEqual(metrics["totalAllotedUnits"], {"User": 10000, "Company": 6000})
    self.assertEqual(failures, [])
    token_manager.return_token_slot.assert_called_once()

  def test_http_error_on_later_page_discards_partial_results(self):
    estimator, _ = self._estimator([
        MockResponse(200, {"value": [_sku(10000)], "@odata.nextLink": "https://next"}),
        MockResponse(403, {"error": {"message": "Insufficient privileges"}}),
    ])
    failures = []

    metrics = estimator.get_license_metrics(failures)

    self.assertEqual(metrics["fetchError"], "license API returned HTTP 403")
    self.assertEqual(metrics["totalAllotedUnits"], {"User": 0, "Company": 0})
    self.assertEqual(failures[0]["statusCode"], 403)
    self.assertIn("Insufficient privileges", failures[0]["message"])

  def test_request_exception_after_retries(self):
    estimator, _ = self._estimator([ConnectionError("boom"), ConnectionError("boom")])
    with mock.patch("estimators.file_estimator.time.sleep"):
      metrics = estimator.get_license_metrics([])
    self.assertEqual(metrics["fetchError"], "license API request failed")

  def test_tenant_without_sharepoint_skus_is_not_a_failure(self):
    estimator, _ = self._estimator([MockResponse(200, {"value": [_sku(50, sharepoint=False)]})])
    metrics = estimator.get_license_metrics([])
    self.assertNotIn("fetchError", metrics)
    self.assertEqual(metrics["totalAllotedUnits"], {"User": 0, "Company": 0})


class ReportUploadLicenseFetchTest(unittest.TestCase):
  """Covers FileMigrationEstimatorTool._get_license_metrics_for_report_upload."""

  def _tool(self):
    from ui.files_ui import FileMigrationEstimatorTool

    tool = object.__new__(FileMigrationEstimatorTool)
    tool.log_msg = mock.Mock()
    tool.ui_update = mock.Mock()
    tool.factory = mock.Mock()
    return tool

  def _config(self, tenant="t", ids=("c",), secrets=("s",)):
    return build_config(tenant_id=tenant, client_ids=list(ids), client_secrets=list(secrets))

  def test_blank_credentials_skip_fetch(self):
    tool = self._tool()
    result = tool._get_license_metrics_for_report_upload(self._config("", (), ()), [])
    self.assertEqual(result, {"fetchError": "no credentials provided"})
    tool.factory.get_manager.assert_not_called()

  def test_incomplete_credentials(self):
    tool = self._tool()
    for cfg in (self._config(secrets=()), self._config(tenant=""), self._config(ids=("a", "b"))):
      with self.subTest(cfg=cfg):
        result = tool._get_license_metrics_for_report_upload(cfg, [])
        self.assertEqual(result, {"fetchError": "incomplete credentials"})
    tool.factory.get_manager.assert_not_called()

  def test_missing_permission(self):
    tool = self._tool()
    tool.factory.get_manager.return_value.authenticate_all.side_effect = ValueError(
        "Missing required permissions for App c: LicenseAssignment.Read.All"
    )
    result = tool._get_license_metrics_for_report_upload(self._config(), [])
    self.assertEqual(
        result, {"fetchError": "app is missing the LicenseAssignment.Read.All permission"}
    )

  def test_sign_in_failure(self):
    tool = self._tool()
    tool.factory.get_manager.return_value.authenticate_all.side_effect = ConnectionError("bad")
    result = tool._get_license_metrics_for_report_upload(self._config(), [])
    self.assertIn("sign-in failed", result["fetchError"])

  def test_success_signs_in_before_creating_estimator(self):
    tool = self._tool()
    calls = []
    manager = tool.factory.get_manager.return_value
    manager.authenticate_all.side_effect = lambda *a, **k: calls.append("auth")
    estimator = mock.Mock()
    estimator.get_license_metrics.return_value = {"totalAllotedUnits": {"User": 20000, "Company": 0}}

    def make_estimator(**_):
      calls.append("estimator")
      return estimator

    tool.factory.get_files_estimator.side_effect = make_estimator

    result = tool._get_license_metrics_for_report_upload(self._config(), [])

    self.assertEqual(calls, ["auth", "estimator"])
    self.assertEqual(result["totalAllotedUnits"]["User"], 20000)
    _, kwargs = manager.authenticate_all.call_args
    self.assertEqual(kwargs["required_scopes"], ["LicenseAssignment.Read.All"])


class LicenseBannerAndEtaTest(unittest.TestCase):
  """Covers the fallback banner and the effect of licenses on ETAs."""

  def test_banner_only_when_fetch_failed(self):
    from ui.files_ui import get_license_fallback_banner_text

    self.assertIsNone(get_license_fallback_banner_text({"licenseMetrics": {}}))
    self.assertIsNone(get_license_fallback_banner_text({}))
    text = get_license_fallback_banner_text({"licenseMetrics": {"fetchError": "no credentials provided"}})
    self.assertIn("(no credentials provided)", text)
    self.assertIn("lowest license tier", text)

  def test_total_license_count(self):
    from ui.files_ui import get_total_license_count

    self.assertEqual(get_total_license_count({"totalAllotedUnits": {"User": 3, "Company": 4}}), 7)
    self.assertEqual(get_total_license_count({"fetchError": "x"}), 0)
    self.assertEqual(get_total_license_count(None), 0)

  def test_fetched_licenses_lower_eta(self):
    from ui.files_ui import FileMigrationEstimatorTool

    class _RealEta:
      def calculate_migration_eta(self, data):
        return FileEstimator.calculate_migration_eta(None, data)

    tool = object.__new__(FileMigrationEstimatorTool)
    tool.val_parallel_batches = 1
    tool.val_eta_max_users = 5000
    tool.val_include_team_sites = False
    tool.factory = mock.Mock()
    tool.factory.get_files_estimator.return_value = _RealEta()
    tool.log_msg = mock.Mock()
    df = pd.DataFrame([
        {"Site Id": "a", "File Count": 1_000_000, "Corpus Size": 1024 ** 3, "Resource Count": 1_000_000},
    ])

    _, _, eta_fallback, _ = tool.calculate_migration_batches(df.copy(), {"fetchError": "x"})
    _, _, eta_licensed, _ = tool.calculate_migration_batches(
        df.copy(), {"totalAllotedUnits": {"User": 20000, "Company": 0}}
    )

    # 20k licenses is the 4x QPS tier (vs 1x for <=1,000).
    self.assertAlmostEqual(eta_fallback / eta_licensed, 4.0, places=3)


if __name__ == "__main__":
  unittest.main()
