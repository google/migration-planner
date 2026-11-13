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

"""Pure lane/batch planning logic for the Files P90-tail batching algorithm.

Sites are referenced by their position in a list sorted by ETA (descending), so
this module has no UI or DataFrame dependencies and can be unit-tested directly.
"""

from typing import Callable, List, NamedTuple, Sequence, Tuple

import pandas as pd

P90_QUANTILE = 0.9


def get_max_tail_group_size(license_count: int) -> int:
  """Returns k, the cap on n_tail, by license tier.

  <=1,000 licenses -> 1; 1,001-5,000 -> 2; >5,000 -> 3. When licenses could not
  be determined (count 0), the lowest tier applies.
  """
  if license_count <= 1000:
    return 1
  if license_count <= 5000:
    return 2
  return 3


class LanePlan(NamedTuple):
  """Result of plan_p90_lanes.

  lanes: One entry per lane; each is a list of (site positions, is_p90) batches
    in execution order (P90 batches first).
  p90_threshold: The P90 ETA value; tail sites have ETA strictly above it.
  tail_count: Number of sites in the P90 tail.
  tail_group_size: n_tail, the number of tail sites grouped per batch.
  lane_loads: Final load (hours) per lane: the sum of its batch ETAs.
  """

  lanes: List[List[Tuple[List[int], bool]]]
  p90_threshold: float
  tail_count: int
  tail_group_size: int
  lane_loads: List[float]


def get_p90_threshold(sorted_etas: Sequence[float]) -> float:
  """Returns the 90th-percentile ETA (linear interpolation), or 0.0 if empty."""
  if not sorted_etas:
    return 0.0
  return float(pd.Series(sorted_etas, dtype=float).quantile(P90_QUANTILE))


def compute_tail_group_size(
    tail_count: int, num_lanes: int, max_group_size: int
) -> int:
  """Returns n_tail = floor(tail_count / num_lanes), clamped to [1, max_group_size]."""
  if tail_count <= 0:
    return 1
  return max(1, min(max_group_size, tail_count // max(1, num_lanes)))


def split_tail_into_groups(tail_count: int, group_size: int) -> List[int]:
  """Splits the tail into floor(tail_count / group_size) groups, largest ETAs first.

  Leftover sites are spread one each onto the smallest (last) groups, wrapping
  around if there are more leftovers than groups. E.g. (10, 3) -> [3, 3, 4],
  (14, 4) -> [4, 5, 5], (9, 5) -> [9].
  """
  if tail_count <= 0:
    return []
  group_size = max(1, group_size)
  num_groups = max(1, tail_count // group_size)
  sizes = [group_size] * num_groups
  leftover = tail_count - group_size * num_groups
  for i in range(leftover):
    sizes[num_groups - 1 - (i % num_groups)] += 1
  return sizes


Totals = Tuple[float, ...]


class _BatchSeries:
  """A lane's sequence of batches of one kind (P90 or regular), built in order.

  Sites/groups are appended to the open (last) batch while it stays within
  max_sites, otherwise a new batch is opened. ETAs are evaluated on running
  totals, so load() always equals the sum of the final batch ETAs. Each append
  costs one ETA evaluation.
  """

  __slots__ = ("max_sites", "eta_fn", "batches", "open_totals", "open_eta", "closed_eta")

  def __init__(self, max_sites: int, eta_fn: Callable[[Totals], float]):
    self.max_sites = max_sites
    self.eta_fn = eta_fn
    self.batches: List[List[int]] = []
    self.open_totals: Totals = ()
    self.open_eta = 0.0
    self.closed_eta = 0.0

  def append(self, positions: List[int], totals: Totals) -> None:
    if self.batches and len(self.batches[-1]) + len(positions) <= self.max_sites:
      self.batches[-1].extend(positions)
      self.open_totals = tuple(a + b for a, b in zip(self.open_totals, totals))
    else:
      self.closed_eta += self.open_eta
      self.batches.append(list(positions))
      self.open_totals = tuple(totals)
    self.open_eta = self.eta_fn(self.open_totals)

  def load(self) -> float:
    return self.closed_eta + self.open_eta


def plan_p90_lanes(
    sorted_etas: Sequence[float],
    num_lanes: int,
    max_sites_per_batch: int,
    site_totals: Sequence[Totals],
    eta_from_totals: Callable[[Totals], float],
    max_tail_group_size: int,
) -> LanePlan:
  """Assigns sites to lanes and batches using the P90-tail algorithm.

  1. Sites with ETA strictly above the P90 ETA form the tail.
  2. n_tail = clamp(floor(tail / lanes), 1, k), where k is the license-tier cap
     (see get_max_tail_group_size); the tail is grouped n_tail at a time from
     the highest ETA down (see split_tail_into_groups).
  3. Tail groups are greedily assigned to the least-loaded lane. Consecutive
     tail groups in the same lane are merged while <= max_sites_per_batch.
  4. Remaining sites are greedily assigned site by site to the least-loaded
     lane and (per lane, in descending ETA order) cut into batches of
     max_sites_per_batch sites; the last batch may be smaller.

  Lane load is always the sum of the lane's batch ETAs as they will finally be
  merged/chunked, evaluated with eta_from_totals on summed per-site totals.
  Batch ETAs are not additive (e.g. max(files/QPS, size/throughput)), so this
  keeps the greedy decisions consistent with the displayed lane ETAs. Each
  assignment re-evaluates one batch ETA, so the cost is O(sites x lanes).

  Args:
    sorted_etas: Per-site ETAs (hours), sorted in descending order.
    num_lanes: Number of parallel lanes (Parallel Batches setting).
    max_sites_per_batch: Maximum number of sites in a single batch.
    site_totals: Per-site additive totals (same order as sorted_etas), e.g.
      (size, files, encrypted files, encrypted size).
    eta_from_totals: Returns the ETA (hours) of a batch from its summed totals.
    max_tail_group_size: k, the cap on n_tail (clamped to at least 1).

  Returns:
    A LanePlan. Lane ties are broken by the lowest lane index.
  """
  num_lanes = max(1, int(num_lanes))
  max_sites_per_batch = max(1, int(max_sites_per_batch))
  max_tail_group_size = max(1, int(max_tail_group_size))
  num_sites = len(sorted_etas)

  p90_threshold = get_p90_threshold(sorted_etas)
  # sorted_etas is descending, so the tail is always a prefix.
  tail_count = sum(1 for eta in sorted_etas if eta > p90_threshold)
  tail_group_size = compute_tail_group_size(
      tail_count, num_lanes, max_tail_group_size
  )

  p90_series = [_BatchSeries(max_sites_per_batch, eta_from_totals) for _ in range(num_lanes)]
  rest_series = [_BatchSeries(max_sites_per_batch, eta_from_totals) for _ in range(num_lanes)]
  # Cached lane loads; only the lane that receives work is recomputed.
  lane_loads = [0.0] * num_lanes

  def _least_loaded_lane() -> int:
    return min(range(num_lanes), key=lane_loads.__getitem__)

  def _update_load(lane: int) -> None:
    lane_loads[lane] = p90_series[lane].load() + rest_series[lane].load()

  start = 0
  for size in split_tail_into_groups(tail_count, tail_group_size):
    group = list(range(start, start + size))
    start += size
    group_totals = tuple(map(sum, zip(*(site_totals[p] for p in group))))
    lane = _least_loaded_lane()
    p90_series[lane].append(group, group_totals)
    _update_load(lane)

  # Sites arrive in descending ETA order, so each lane's regular batches are
  # filled in descending ETA order, as before.
  for pos in range(tail_count, num_sites):
    lane = _least_loaded_lane()
    rest_series[lane].append([pos], site_totals[pos])
    _update_load(lane)

  lanes: List[List[Tuple[List[int], bool]]] = []
  for lane_idx in range(num_lanes):
    batches = [(group, True) for group in p90_series[lane_idx].batches]
    batches.extend((chunk, False) for chunk in rest_series[lane_idx].batches)
    lanes.append(batches)

  return LanePlan(lanes, p90_threshold, tail_count, tail_group_size, lane_loads)
