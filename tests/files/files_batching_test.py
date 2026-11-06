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

"""Tests for the OneDrive / SharePoint batching algorithm.

Run with:
  python3 -m unittest tests.files.files_batching_test -v
"""

import math
import random
import unittest
from unittest import mock

import pandas as pd

from batching import files_batching
from batching import files_eta
from util.constants import (
    FILES_GLOBAL_CORPUS_SIZE_LIMIT,
    FILES_MAX_SITES_PER_BATCH,
    FILES_MIN_SITES_PER_BATCH,
    SITE_SIMILARITY_GROUP_SIZE,
    SITE_SIMILARITY_MAX_RATIO,
)

GB = 1024**3
# Files/sec per batch used by the tests (36,000 files/hour).
FILES_PER_SEC = 10.0
COLUMNS = ["Site Id", "File Count", "Folder Count", "Resource Count", "Corpus Size"]


def _df(sites):
  """sites: list of (file_count, size_gb)."""
  return pd.DataFrame([
      {"Site Id": f"site{i}", "File Count": files, "Folder Count": 0,
       "Resource Count": files, "Corpus Size": size_gb * GB}
      for i, (files, size_gb) in enumerate(sites)
  ], columns=COLUMNS)


def _random_sites(n, seed):
  rnd = random.Random(seed)
  return [(int(rnd.lognormvariate(8, 1.6)), rnd.lognormvariate(21, 2) / GB) for _ in range(n)]


def _batch(df, parallel=1, max_sites=1000, max_batches=50, **kwargs):
  return files_batching.calculate_files_migration_batches(
      df, FILES_PER_SEC, max_sites=max_sites, num_parallel=parallel,
      max_batches=max_batches, **kwargs,
  )


def _fastest_possible(sites, lanes):
  """max(all sites as one batch / lanes, longest single site), in hours."""
  rate = FILES_PER_SEC
  total = files_eta.calculate_files_eta(sum(s for _, s in sites) * GB, sum(f for f, _ in sites), rate)
  longest = max(files_eta.calculate_files_eta(s * GB, f, rate) for f, s in sites)
  return max(total / lanes, longest)


def _is_similar(batch):
  """The 100k+ sites among the batch's 5 largest are within the ratio."""
  counts = sorted(batch["df_subset"]["File Count"], reverse=True)[:SITE_SIMILARITY_GROUP_SIZE]
  large = [c for c in counts if c >= 100_000]
  return len(large) <= 1 or large[0] / large[-1] <= SITE_SIMILARITY_MAX_RATIO


class FilesBatchingTest(unittest.TestCase):
  """Verifies batching keeps the total ETA minimal and largest sites similar."""

  def test_every_site_is_batched_once(self):
    sites = [(100_000, 50)] * 12 + [(20_000, 300)] * 40 + [(500, 5)] * 300
    df_final, batches, _, _ = _batch(_df(sites), parallel=3, max_sites=100)
    self.assertEqual(sum(b["sites"] for b in batches), len(sites))
    self.assertTrue((df_final["Suggested Batch"] != "").all())
    batched_ids = [sid for b in batches for sid in b["df_subset"]["Site Id"]]
    self.assertEqual(len(batched_ids), len(set(batched_ids)))

  def test_total_eta_matches_fastest_possible(self):
    """Similarity never makes the plan slower than balancing alone."""
    for seed, lanes in ((1, 7), (2, 5), (3, 3)):
      with self.subTest(seed=seed):
        sites = _random_sites(1500, seed)
        _, _, total_eta, _ = _batch(_df(sites), parallel=lanes, max_sites=500,
                                    max_batches=50)
        self.assertLessEqual(total_eta, _fastest_possible(sites, lanes) * 1.001)

  def test_all_lanes_used_with_5000_site_maximum(self):
    """With up to 5,000 sites per batch and 2,000 sites, all 7 lanes are used."""
    sites = _random_sites(2000, 4)
    _, batches, total_eta, buckets = _batch(_df(sites), parallel=7, max_sites=5000, max_batches=50)
    self.assertEqual(len(buckets), 7)
    self.assertLessEqual(total_eta, _fastest_possible(sites, 7) * 1.001)

  def test_largest_sites_in_each_batch_are_similar(self):
    """Clusters of similar sites: every batch's largest sites are within the ratio.

    Each cluster splits into groups of 10 per lane, so a fully similar plan is
    also the fastest one.
    """
    sites = ([(100_000, 50)] * 40 + [(30_000, 30)] * 40 + [(5_000, 10)] * 40
             + [(200, 1)] * 400)
    _, batches, _, _ = _batch(_df(sites), parallel=4, max_sites=200)
    for batch in batches:
      self.assertTrue(_is_similar(batch), batch["name"])
      self.assertFalse(batch["largest_sites_dissimilar"], batch["name"])

  def test_similar_large_sites_meet_the_rule(self):
    """A batch whose large sites are all within 1.5x meets the rule."""
    sites = [(100_000, 10)] * 8 + [(1_500, 1)] * 50
    _, batches, _, _ = _batch(_df(sites), parallel=1, max_batches=1)
    self.assertEqual(len(batches), 1)
    self.assertFalse(batches[0]["largest_sites_dissimilar"])

  def test_dissimilar_large_sites_are_a_miss(self):
    """A 300k-file site and 100k-file sites in one batch are more than 1.5x apart."""
    sites = [(300_000, 10)] + [(100_000, 10)] * 4 + [(1_500, 1)] * 50
    _, batches, _, _ = _batch(_df(sites), parallel=1, max_batches=1)
    self.assertTrue(batches[0]["largest_sites_dissimilar"])

  def test_one_large_site_is_not_mixed_into_a_batch_of_small_heads(self):
    """The customer case: 1 x 1M-file site and 49 x 10k-file sites."""
    sites = [(1_000_000, 10)] + [(10_000, 10)] * 49 + [(100, 1)] * 200
    _, batches, total_eta, _ = _batch(_df(sites), parallel=2, max_sites=60)
    # The giant needs at least 19 batch-mates; the cheapest are the 100-file sites.
    giant_with_smallest = (1_000_000 + 19 * 100) / 36_000
    self.assertAlmostEqual(total_eta, giant_with_smallest, places=3)
    for batch in batches:
      counts = sorted(batch["df_subset"]["File Count"], reverse=True)
      if counts[0] == 1_000_000:
        self.assertNotIn(10_000, counts)
        self.assertGreaterEqual(batch["sites"], FILES_MIN_SITES_PER_BATCH)

  def test_giant_site_shares_its_batch_with_size_heavy_sites(self):
    """A file-bound giant takes size-bound fillers; a lone giant isn't a miss."""
    sites = [(1_000_000, 10)] + [(10_000, 10)] * 40 + [(100, 400)] * 60
    _, batches, total_eta, _ = _batch(_df(sites), parallel=2)
    self.assertLessEqual(total_eta, _fastest_possible(sites, 2) * 1.01)
    giant_batch = next(b for b in batches if 1_000_000 in set(b["df_subset"]["File Count"]))
    self.assertGreaterEqual(giant_batch["sites"], FILES_MIN_SITES_PER_BATCH)
    self.assertFalse(giant_batch["largest_sites_dissimilar"])

  def test_dissimilar_batches_are_logged(self):
    log = mock.Mock()
    sites = [(300_000, 10)] + [(100_000, 10)] * 4 + [(1_500, 1)] * 50
    _, batches, _, _ = _batch(_df(sites), parallel=1, max_batches=1, log_fn=log)
    messages = " ".join(call.args[0] for call in log.call_args_list)
    self.assertIn("no similar-size peers", messages)
    self.assertIn(batches[0]["name"], messages)

  def test_lanes_are_balanced_and_total_is_busiest_lane(self):
    sites = [(50_000, 100)] * 30 + [(5_000, 50)] * 100 + [(300, 2)] * 500
    _, _, total_eta, buckets = _batch(_df(sites), parallel=5, max_sites=150)
    totals = [b["total"] for b in buckets]
    self.assertAlmostEqual(total_eta, max(totals), places=9)
    self.assertLessEqual(total_eta, _fastest_possible(sites, 5) * 1.001)
    for bucket in buckets:
      self.assertAlmostEqual(bucket["total"], sum(c["eta"] for c in bucket["batches"]), places=9)

  def test_never_more_than_5000_sites_per_batch(self):
    """The 5,000-site limit holds even if max batches or the setting would break it."""
    sites = [(100, 0.1)] * 12_000
    _, batches, _, _ = _batch(_df(sites), parallel=3, max_sites=10_000, max_batches=2)
    self.assertEqual(len(batches), math.ceil(12_000 / FILES_MAX_SITES_PER_BATCH))
    self.assertTrue(all(b["sites"] <= FILES_MAX_SITES_PER_BATCH for b in batches))

  def test_every_batch_has_at_least_20_sites(self):
    """The 20-site minimum holds across tenants, lane counts and settings."""
    for seed, lanes, n in ((5, 7, 300), (6, 5, 1500), (7, 3, 95), (8, 7, 35)):
      with self.subTest(seed=seed, lanes=lanes, sites=n):
        sites = _random_sites(n, seed)
        _, batches, total_eta, _ = _batch(_df(sites), parallel=lanes, max_sites=500, max_batches=50)
        self.assertEqual(sum(b["sites"] for b in batches), n)
        self.assertTrue(all(b["sites"] >= FILES_MIN_SITES_PER_BATCH for b in batches))
        self.assertLessEqual(len(batches), n // FILES_MIN_SITES_PER_BATCH)

  def test_twenty_to_thirty_nine_sites_form_one_batch(self):
    for n in (20, 39):
      with self.subTest(sites=n):
        _, batches, _, buckets = _batch(_df([(5_000, 5)] * n), parallel=5)
        self.assertEqual((len(batches), len(buckets)), (1, 1))

  def test_lanes_limited_by_20_site_minimum(self):
    """45 sites can form at most 2 batches, so only 2 of 5 lanes are used."""
    _, batches, _, buckets = _batch(_df([(5_000, 5)] * 45), parallel=5)
    self.assertEqual(len(batches), 2)
    self.assertEqual(len(buckets), 2)
    self.assertTrue(all(b["sites"] >= 20 for b in batches))

  def test_batches_in_a_lane_are_merged(self):
    """Batches in the same lane merge while they stay within 5,000 sites."""
    sites = _random_sites(3000, 9)
    sites = [(min(f, 9_000), s) for f, s in sites]  # No large sites: merges always allowed.
    _, batches, total_eta, buckets = _batch(_df(sites), parallel=2, max_sites=5000)
    self.assertEqual(len(batches), len(buckets))
    self.assertLessEqual(total_eta, _fastest_possible(sites, 2) * 1.001)

  def test_never_more_than_50_batches(self):
    """The max-batches setting is capped at 50."""
    sites = [(1_000, 1)] * 3000
    _, batches, _, _ = _batch(_df(sites), parallel=7, max_sites=60, max_batches=500)
    self.assertLessEqual(len(batches), 50)

  def test_respects_max_sites_and_max_batches(self):
    sites = [(1_000, 1)] * 500
    _, batches, _, _ = _batch(_df(sites), parallel=3, max_sites=50, max_batches=12)
    self.assertLessEqual(len(batches), 12)
    self.assertTrue(all(b["sites"] <= 50 for b in batches))

  def test_batches_are_named_by_start_time(self):
    sites = [(10_000, 10)] * 60 + [(100, 1)] * 200
    _, batches, _, _ = _batch(_df(sites), parallel=2, max_sites=60)
    self.assertEqual([b["name"] for b in batches], [f"Batch {i + 1}" for i in range(len(batches))])
    starts = [(b["start_time"], b["bucket_idx"]) for b in batches]
    self.assertEqual(starts, sorted(starts))

  def test_custom_eta_columns_drive_eta(self):
    """The size and file count columns passed in are used for the ETA."""
    df = _df([(10_000, 0)])
    df["Est Files"] = 30_000
    df["Est Size"] = 1024
    _, _, eta_default, _ = _batch(_df([(10_000, 0)]))
    _, _, eta_custom, _ = _batch(df, eta_size_col="Est Size", eta_file_col="Est Files")
    self.assertAlmostEqual(eta_custom, eta_default * 3, places=6)

  def test_folders_do_not_affect_eta_or_similarity(self):
    df = _df([(10_000, 10)] * 20)
    with_folders = df.assign(**{"Folder Count": 50_000, "Resource Count": 60_000})
    _, batches_a, eta_a, _ = _batch(df, parallel=2)
    _, batches_b, eta_b, _ = _batch(with_folders, parallel=2)
    self.assertAlmostEqual(eta_a, eta_b, places=9)
    self.assertEqual([b["sites"] for b in batches_a], [b["sites"] for b in batches_b])

  def test_worked_example(self):
    """The 10-site, 3-lane walkthrough (K = 3, 10 files/sec) reaches the true optimum.

    The best possible split of these sites over 3 lanes (found by trying every
    assignment) takes 12.0 h.
    """
    sites = [(360_000, 40), (108_000, 1600), (90_000, 800), (79_200, 1200), (36_000, 2000),
             (28_800, 400), (25_200, 2400), (7_200, 1600), (3_600, 800), (1_800, 3200)]
    df = _df(sites)
    df["Site Id"] = list("ABCDEFGHIJ")
    # The walkthrough predates the 20-site minimum and the 100k/10k thresholds,
    # so they are set back here; with them, 10 sites form a single batch.
    with mock.patch.object(files_batching, "SITE_SIMILARITY_GROUP_SIZE", 3), \
         mock.patch.object(files_batching, "FILES_MIN_SITES_PER_BATCH", 1), \
         mock.patch.object(files_batching, "BATCH_LEADER_MIN_FILES", 1000), \
         mock.patch.object(files_batching, "GROUP_MEMBER_MIN_FILES", 1000):
      _, _, total_eta, _ = _batch(df, parallel=3, max_sites=10, max_batches=6)
    self.assertAlmostEqual(total_eta, 12.0, places=3)


class FilesBatchingThresholdTest(unittest.TestCase):
  """Verifies which sites can start a batch, join a group, or only fill."""

  @staticmethod
  def _heads(file_counts):
    sites = files_batching._Sites(
        files=file_counts, sizes=[0.0] * len(file_counts), enc_files=[0] * len(file_counts),
        enc_sizes=[0.0] * len(file_counts), files_per_sec=10.0, k=5)
    order = sorted(range(len(file_counts)), key=lambda i: -file_counts[i])
    members = [i for i in order if file_counts[i] >= 10_000]
    groups = files_batching._form_heads(sites, members, float("inf"))
    return [[file_counts[i] for i in g] for g in groups]

  def test_only_100k_plus_sites_start_a_group(self):
    self.assertEqual(self._heads([90_000] * 12 + [5_000] * 20), [])

  def test_10k_to_100k_sites_join_a_100k_group(self):
    groups = self._heads([120_000] + [85_000] * 6 + [9_000] * 20)
    self.assertEqual(len(groups), 1)
    self.assertEqual(groups[0][:5], [120_000] + [85_000] * 4)

  def test_group_grows_up_to_5_large_sites(self):
    groups = self._heads([150_000] * 12)
    self.assertEqual([len(g) for g in groups], [5, 5, 2])

  def test_sites_under_10k_never_join_a_group(self):
    groups = self._heads([100_000] * 3 + [9_999] * 20)
    self.assertEqual(groups, [[100_000] * 3])

  def test_batches_led_by_smaller_sites_are_not_checked(self):
    sites = [(60_000, 10)] * 3 + [(500, 1)] * 40
    _, batches, _, _ = _batch(_df(sites), parallel=2)
    self.assertFalse(any(b["largest_sites_dissimilar"] for b in batches))


class FilesBatchingEdgeCaseTest(unittest.TestCase):
  """Verifies unusual reports and settings are handled."""

  def test_empty_report(self):
    df_final, batches, total_eta, buckets = _batch(_df([]))
    self.assertEqual((len(df_final), batches, total_eta, buckets), (0, [], 0.0, []))
    self.assertIn("Suggested Batch", df_final.columns)

  def test_all_sites_empty(self):
    df_final, batches, total_eta, _ = _batch(_df([(0, 0)] * 50), parallel=3, max_sites=10)
    self.assertEqual(total_eta, 0)
    self.assertEqual(sum(b["sites"] for b in batches), 50)
    self.assertTrue((df_final["Suggested Batch"] != "").all())
    self.assertFalse(any(b["largest_sites_dissimilar"] for b in batches))

  def test_all_sites_very_small(self):
    """Sites under 1,000 files never head a batch and are exempt from the rule."""
    sites = [(random.Random(i).randint(0, 999), 0.5) for i in range(800)]
    _, batches, total_eta, _ = _batch(_df(sites), parallel=4, max_sites=150,
                                      max_batches=50)
    self.assertLessEqual(total_eta, _fastest_possible(sites, 4) * 1.001)
    self.assertFalse(any(b["largest_sites_dissimilar"] for b in batches))
    self.assertTrue(all(b["sites"] <= 150 for b in batches))

  def test_fewer_sites_than_the_minimum_form_one_batch(self):
    """1-19 sites can't reach 20 per batch, so they form one batch."""
    _, batches, total_eta, buckets = _batch(_df([(36_000, 1)]), parallel=3)
    self.assertEqual((len(batches), len(buckets)), (1, 1))
    self.assertAlmostEqual(total_eta, 1.0, places=6)
    _, batches, _, buckets = _batch(_df([(5_000, 5), (3_000, 2)]), parallel=5)
    self.assertEqual((len(batches), len(buckets)), (1, 1))
    self.assertEqual(batches[0]["sites"], 2)

  def test_invalid_values_are_treated_as_zero(self):
    df = _df([(10_000, 10), (20_000, 10), (5_000, 1)])
    df["File Count"] = df["File Count"].astype(object)
    df.loc[0, "File Count"] = "N/A"
    df.loc[1, "Corpus Size"] = float("nan")
    df.loc[2, "File Count"] = -500
    df_final, batches, total_eta, _ = _batch(df, parallel=2)
    self.assertEqual(sum(b["sites"] for b in batches), 3)
    self.assertGreater(total_eta, 0)
    self.assertTrue((df_final["Suggested Batch"] != "").all())

  def test_duplicate_site_ids_each_get_a_batch(self):
    df = _df([(10_000, 10)] * 4)
    df["Site Id"] = "same-site"
    df_final, batches, _, _ = _batch(df, parallel=2, max_sites=2)
    self.assertEqual(sum(b["sites"] for b in batches), 4)
    self.assertTrue((df_final["Suggested Batch"] != "").all())

  def test_missing_site_id_column(self):
    df = _df([(10_000, 10)] * 3).drop(columns=["Site Id"])
    df_final, batches, _, _ = _batch(df)
    self.assertEqual(sum(b["sites"] for b in batches), 3)
    self.assertTrue((df_final["Suggested Batch"] != "").all())

  def test_invalid_settings_are_clamped(self):
    """Zero lanes or batches, and a minimum above the maximum, still plan safely."""
    sites = [(10_000, 10)] * 30
    _, batches, total_eta, _ = _batch(_df(sites), parallel=0, max_sites=10,
                                      max_batches=0)
    self.assertEqual(sum(b["sites"] for b in batches), 30)
    self.assertTrue(all(b["sites"] <= 10 for b in batches))
    self.assertGreater(total_eta, 0)


class FilesBatchingUiTest(unittest.TestCase):
  """Verifies the Files UI passes its settings and license-tier rate to batching."""

  def _make_tool(self, include_team_sites):
    from ui.files_ui import FileMigrationEstimatorTool

    tool = object.__new__(FileMigrationEstimatorTool)
    tool.val_eta_max_users = 5000
    tool.val_parallel_batches = 7
    tool.val_eta_max_batches = 50
    tool.val_include_team_sites = include_team_sites
    tool.log_msg = lambda *_: None
    return tool

  def test_rate_follows_license_tier(self):
    cases = [(0, 1), (1000, 1), (3000, 2), (10000, 3), (50000, 4), (50001, 5)]
    for include_team_sites, base in ((True, 4.4), (False, 4.8)):
      tool = self._make_tool(include_team_sites)
      for licenses, multiplier in cases:
        with self.subTest(team_sites=include_team_sites, licenses=licenses):
          rate = tool._get_qps_from_license_count(
              {"totalAllotedUnits": {"User": licenses, "Company": 0}}
          )
          self.assertAlmostEqual(rate, base * multiplier * 0.8, places=9)

  def test_settings_and_rate_are_passed_to_batching(self):
    tool = self._make_tool(include_team_sites=True)
    df = _df([(1_000, 1)])
    with mock.patch("ui.files_ui.files_batching.calculate_files_migration_batches") as batch:
      tool.calculate_migration_batches(df, {"totalAllotedUnits": {"User": 800, "Company": 300}})
    self.assertIs(batch.call_args.args[0], df)
    self.assertAlmostEqual(batch.call_args.args[1], 4.4 * 2 * 0.8, places=9)
    kwargs = batch.call_args.kwargs
    self.assertEqual(
        (kwargs["max_sites"], kwargs["num_parallel"], kwargs["max_batches"]), (5000, 7, 50)
    )


if __name__ == "__main__":
  unittest.main()
