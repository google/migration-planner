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

"""Tests for the Files P90-tail batch planner.

Run with:
  python3 -m unittest tests.files.batch_planner_test -v
"""

import unittest
from unittest import mock

from util import files_batch_planner as planner


def _sum_eta(etas):
  return lambda positions: sum(etas[p] for p in positions)


def _all_positions(plan):
  return sorted(p for lane in plan.lanes for positions, _ in lane for p in positions)


class TailGroupSizeTest(unittest.TestCase):
  """Covers n_tail = clamp(floor(tail / lanes), 1, k)."""

  def test_exact_division(self):
    self.assertEqual(planner.compute_tail_group_size(10, 2, 5), 5)

  def test_fraction_is_floored(self):
    self.assertEqual(planner.compute_tail_group_size(10, 3, 5), 3)  # 3.33 -> 3
    self.assertEqual(planner.compute_tail_group_size(14, 3, 5), 4)  # 4.67 -> 4

  def test_clipped_to_cap(self):
    self.assertEqual(planner.compute_tail_group_size(50, 2, 5), 5)
    self.assertEqual(planner.compute_tail_group_size(50, 2, 3), 3)
    self.assertEqual(planner.compute_tail_group_size(50, 2, 1), 1)

  def test_below_cap_is_unchanged(self):
    self.assertEqual(planner.compute_tail_group_size(4, 2, 3), 2)  # 2 < k=3

  def test_clipped_to_one(self):
    self.assertEqual(planner.compute_tail_group_size(3, 5, 5), 1)  # 0.6 -> 1

  def test_empty_tail(self):
    self.assertEqual(planner.compute_tail_group_size(0, 3, 5), 1)


class LicenseTierCapTest(unittest.TestCase):
  """Covers k by license tier: <=1,000 -> 1; 1,001-5,000 -> 2; >5,000 -> 3."""

  def test_tier_boundaries(self):
    cases = [(0, 1), (1000, 1), (1001, 2), (5000, 2), (5001, 3), (100000, 3)]
    for licenses, k in cases:
      with self.subTest(licenses=licenses):
        self.assertEqual(planner.get_max_tail_group_size(licenses), k)

  def test_cap_limits_p90_group_size(self):
    """10 P90 sites, 2 lanes: floor(10/2)=5 is capped to k (1, 2 or 3)."""
    etas = [float(v) for v in range(100, 0, -1)]
    for k, expected_sizes in [(1, [1] * 10), (2, [2] * 5), (3, [3, 3, 4])]:
      with self.subTest(k=k):
        plan = planner.plan_p90_lanes(etas, 2, 5000, _sum_eta(etas), k)
        self.assertEqual(plan.tail_group_size, k)
        self.assertEqual(
            planner.split_tail_into_groups(plan.tail_count, plan.tail_group_size),
            expected_sizes,
        )
        self.assertEqual(_all_positions(plan), list(range(100)))

  def test_invalid_cap_is_clamped_to_one(self):
    etas = [float(v) for v in range(100, 0, -1)]
    plan = planner.plan_p90_lanes(etas, 2, 5000, _sum_eta(etas), 0)
    self.assertEqual(plan.tail_group_size, 1)


class SplitTailTest(unittest.TestCase):
  """Covers grouping the tail with leftovers spread onto the smallest groups."""

  def test_even_split(self):
    self.assertEqual(planner.split_tail_into_groups(10, 5), [5, 5])

  def test_leftover_goes_to_smallest_group(self):
    self.assertEqual(planner.split_tail_into_groups(10, 3), [3, 3, 4])

  def test_leftovers_spread_one_each_from_the_end(self):
    self.assertEqual(planner.split_tail_into_groups(14, 4), [4, 5, 5])

  def test_leftovers_wrap_with_single_group(self):
    self.assertEqual(planner.split_tail_into_groups(9, 5), [9])

  def test_empty_tail(self):
    self.assertEqual(planner.split_tail_into_groups(0, 3), [])


class PlanP90LanesTest(unittest.TestCase):
  """Covers end-to-end lane and batch planning."""

  def test_empty_input(self):
    plan = planner.plan_p90_lanes([], 3, 5000, lambda _p: 0.0, 5)
    self.assertEqual(plan.lanes, [[], [], []])
    self.assertEqual(plan.tail_count, 0)

  def test_single_site_has_no_tail(self):
    plan = planner.plan_p90_lanes([5.0], 2, 5000, _sum_eta([5.0]), 5)
    self.assertEqual(plan.tail_count, 0)
    self.assertEqual(plan.lanes, [[([0], False)], []])

  def test_identical_etas_have_no_tail(self):
    etas = [2.0] * 20
    plan = planner.plan_p90_lanes(etas, 2, 5000, _sum_eta(etas), 5)
    self.assertEqual(plan.tail_count, 0)
    self.assertEqual([len(lane[0][0]) for lane in plan.lanes], [10, 10])

  def test_zero_eta_sites_stay_out_of_tail(self):
    etas = [9.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
    plan = planner.plan_p90_lanes(etas, 2, 5000, _sum_eta(etas), 5)
    self.assertEqual(plan.tail_count, 1)
    self.assertEqual(_all_positions(plan), list(range(10)))

  def test_user_example_two_lanes(self):
    """100 sites, 2 lanes: 10 P90 sites -> two P90 batches of 5 (top 5, next 5)."""
    etas = [float(v) for v in range(100, 0, -1)]
    plan = planner.plan_p90_lanes(etas, 2, 5000, _sum_eta(etas), 5)

    self.assertEqual(plan.tail_count, 10)
    self.assertEqual(plan.tail_group_size, 5)
    self.assertEqual(plan.lanes[0][0], ([0, 1, 2, 3, 4], True))
    self.assertEqual(plan.lanes[1][0], ([5, 6, 7, 8, 9], True))
    self.assertEqual(_all_positions(plan), list(range(100)))

  def test_user_example_three_lanes(self):
    """10 P90 sites with 3 lanes -> P90 groups of 3, 3, 4."""
    etas = [float(v) for v in range(100, 0, -1)]
    plan = planner.plan_p90_lanes(etas, 3, 5000, _sum_eta(etas), 5)

    p90_groups = [positions for lane in plan.lanes for positions, is_p90 in lane if is_p90]
    self.assertEqual(sorted(len(g) for g in p90_groups), [3, 3, 4])
    self.assertEqual(sorted(p for g in p90_groups for p in g), list(range(10)))

  def test_p90_batches_come_first_in_each_lane(self):
    etas = [float(v) for v in range(100, 0, -1)]
    plan = planner.plan_p90_lanes(etas, 3, 5000, _sum_eta(etas), 5)
    for lane in plan.lanes:
      flags = [is_p90 for _, is_p90 in lane]
      self.assertEqual(flags, sorted(flags, reverse=True))

  def test_p90_batches_in_same_lane_are_merged(self):
    """With 1 lane every P90 group lands in the same lane and is merged into one batch."""
    etas = [float(v) for v in range(200, 0, -1)]
    plan = planner.plan_p90_lanes(etas, 1, 5000, _sum_eta(etas), 5)
    p90_batches = [positions for positions, is_p90 in plan.lanes[0] if is_p90]
    self.assertEqual(len(p90_batches), 1)
    self.assertEqual(p90_batches[0], list(range(plan.tail_count)))

  def test_p90_merge_respects_max_sites(self):
    etas = [float(v) for v in range(200, 0, -1)]  # 20 P90 sites, groups of 5
    plan = planner.plan_p90_lanes(etas, 1, 12, _sum_eta(etas), 5)
    p90_sizes = [len(positions) for positions, is_p90 in plan.lanes[0] if is_p90]
    self.assertEqual(p90_sizes, [10, 10])

  def test_remaining_sites_chunked_by_max_sites_in_descending_eta(self):
    etas = [float(v) for v in range(100, 0, -1)]
    plan = planner.plan_p90_lanes(etas, 2, 7, _sum_eta(etas), 5)
    for lane in plan.lanes:
      rest = [positions for positions, is_p90 in lane if not is_p90]
      self.assertTrue(all(len(chunk) <= 7 for chunk in rest))
      self.assertTrue(all(len(chunk) == 7 for chunk in rest[:-1]))
      flat = [p for chunk in rest for p in chunk]
      self.assertEqual(flat, sorted(flat, key=lambda p: -etas[p]))

  def test_remaining_sites_go_to_least_loaded_lane(self):
    """A lane loaded by a heavy P90 batch receives fewer remaining sites."""
    etas = [1000.0] + [1.0] * 19
    plan = planner.plan_p90_lanes(etas, 2, 5000, _sum_eta(etas), 5)
    self.assertEqual(plan.tail_count, 1)
    self.assertEqual(plan.lanes[0], [([0], True)])
    self.assertEqual(len(plan.lanes[1][0][0]), 19)

  def test_fewer_sites_than_lanes(self):
    etas = [3.0, 2.0]
    plan = planner.plan_p90_lanes(etas, 5, 5000, _sum_eta(etas), 5)
    self.assertEqual(_all_positions(plan), [0, 1])
    self.assertEqual(sum(1 for lane in plan.lanes if lane), 2)

  def test_invalid_settings_are_clamped(self):
    etas = [3.0, 2.0, 1.0]
    plan = planner.plan_p90_lanes(etas, 0, 0, _sum_eta(etas), 5)
    self.assertEqual(len(plan.lanes), 1)
    self.assertTrue(all(len(positions) == 1 for positions, _ in plan.lanes[0]))


class CalculateMigrationBatchesTest(unittest.TestCase):
  """Integration test for FileMigrationEstimatorTool.calculate_migration_batches."""

  def _make_tool(self, num_lanes=2, max_sites=5000):
    from ui.files_ui import FileMigrationEstimatorTool

    class _StubEstimator:
      def calculate_migration_eta(self, data):
        return sum(item["files"] for item in data["items"]) / 1000.0

    tool = object.__new__(FileMigrationEstimatorTool)
    tool.val_parallel_batches = num_lanes
    tool.val_eta_max_users = max_sites
    tool.val_include_team_sites = False
    tool.factory = mock.Mock()
    tool.factory.get_files_estimator.return_value = _StubEstimator()
    tool.log_msg = mock.Mock()
    return tool

  def _df(self, file_counts):
    import pandas as pd

    return pd.DataFrame([
        {"Site Id": f"site{i}", "File Count": c, "Corpus Size": c * 10, "Resource Count": c}
        for i, c in enumerate(file_counts)
    ])

  def test_plan_marks_p90_batches_and_assigns_every_site(self):
    from util.constants import COLOR_P90_BATCH

    tool = self._make_tool(num_lanes=2)
    df = self._df(list(range(1000, 0, -10)))  # 100 sites, shuffled order not required

    df_final, batches, total_eta, buckets = tool.calculate_migration_batches(df, {})

    self.assertTrue((df_final["Suggested Batch"] != "").all())
    p90 = [b for b in batches if b["is_p90"]]
    self.assertEqual(len(p90), 2)
    self.assertTrue(all(b["color"] == COLOR_P90_BATCH for b in p90))
    self.assertTrue(all("color" not in b for b in batches if not b["is_p90"]))
    self.assertEqual(sum(b["sites"] for b in batches), 100)
    self.assertEqual([b["name"] for b in batches], [f"Batch {i+1}" for i in range(len(batches))])
    self.assertEqual({b["name"] for b in p90}, {"Batch 1", "Batch 2"})
    self.assertAlmostEqual(total_eta, max(b["total"] for b in buckets))
    self.assertNotIn("SortMetric", df_final.columns)

  def test_sites_sorted_by_eta_descending(self):
    tool = self._make_tool(num_lanes=1)
    df = self._df([5, 500, 50])

    df_final, _, _, _ = tool.calculate_migration_batches(df, {})

    self.assertEqual(df_final["Site Id"].tolist(), ["site1", "site2", "site0"])

  def test_license_tier_sets_tail_group_cap(self):
    """k comes from licenseMetrics: 1 when unknown, 2 for 3k, 3 for 20k licenses."""
    cases = [
        ({}, 1),
        ({"fetchError": "no credentials provided"}, 1),
        ({"totalAllotedUnits": {"User": 3000, "Company": 0}}, 2),
        ({"totalAllotedUnits": {"User": 15000, "Company": 5000}}, 3),
    ]
    for license_metrics, expected_k in cases:
      with self.subTest(license_metrics=license_metrics):
        tool = self._make_tool(num_lanes=2)
        with mock.patch.object(
            planner, "plan_p90_lanes", wraps=planner.plan_p90_lanes
        ) as spy:
          tool.calculate_migration_batches(self._df(list(range(1000, 0, -10))), license_metrics)
        self.assertEqual(spy.call_args[0][4], expected_k)

  def test_empty_dataframe(self):
    import pandas as pd

    tool = self._make_tool()
    df = pd.DataFrame(columns=["Site Id", "File Count", "Corpus Size", "Resource Count"])

    df_final, batches, total_eta, buckets = tool.calculate_migration_batches(df, {})

    self.assertEqual((batches, total_eta, buckets), ([], 0, []))
    self.assertIn("Suggested Batch", df_final.columns)


if __name__ == "__main__":
  unittest.main()
