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
MAX_TAIL_GROUP_SIZE = 5


class LanePlan(NamedTuple):
  """Result of plan_p90_lanes.

  lanes: One entry per lane; each is a list of (site positions, is_p90) batches
    in execution order (P90 batches first).
  p90_threshold: The P90 ETA value; tail sites have ETA strictly above it.
  tail_count: Number of sites in the P90 tail.
  tail_group_size: n_tail, the number of tail sites grouped per batch.
  """

  lanes: List[List[Tuple[List[int], bool]]]
  p90_threshold: float
  tail_count: int
  tail_group_size: int


def get_p90_threshold(sorted_etas: Sequence[float]) -> float:
  """Returns the 90th-percentile ETA (linear interpolation), or 0.0 if empty."""
  if not sorted_etas:
    return 0.0
  return float(pd.Series(sorted_etas, dtype=float).quantile(P90_QUANTILE))


def compute_tail_group_size(tail_count: int, num_lanes: int) -> int:
  """Returns n_tail = floor(tail_count / num_lanes), clamped to [1, 5]."""
  if tail_count <= 0:
    return 1
  return max(1, min(MAX_TAIL_GROUP_SIZE, tail_count // max(1, num_lanes)))


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


def _chunk(positions: List[int], max_size: int) -> List[List[int]]:
  return [positions[i:i + max_size] for i in range(0, len(positions), max_size)]


def _merge_consecutive(groups: List[List[int]], max_size: int) -> List[List[int]]:
  """Merges consecutive groups while the merged site count stays <= max_size."""
  merged: List[List[int]] = []
  for group in groups:
    if merged and len(merged[-1]) + len(group) <= max_size:
      merged[-1] = merged[-1] + group
    else:
      merged.append(list(group))
  return merged


def plan_p90_lanes(
    sorted_etas: Sequence[float],
    num_lanes: int,
    max_sites_per_batch: int,
    group_eta_fn: Callable[[List[int]], float],
) -> LanePlan:
  """Assigns sites to lanes and batches using the P90-tail algorithm.

  1. Sites with ETA strictly above the P90 ETA form the tail.
  2. n_tail = clamp(floor(tail / lanes), 1, 5); the tail is grouped n_tail at a
     time from the highest ETA down (see split_tail_into_groups).
  3. Tail groups are greedily assigned to the least-loaded lane. Consecutive
     tail groups in the same lane are merged while <= max_sites_per_batch.
  4. Remaining sites are greedily assigned site by site to the least-loaded
     lane, then (per lane, in descending ETA order) cut into batches of
     max_sites_per_batch sites; the last batch may be smaller.

  Args:
    sorted_etas: Per-site ETAs (hours), sorted in descending order.
    num_lanes: Number of parallel lanes (Parallel Batches setting).
    max_sites_per_batch: Maximum number of sites in a single batch.
    group_eta_fn: Returns the ETA of migrating the given site positions as one
      batch. Used to load lanes with tail group ETAs.

  Returns:
    A LanePlan. Lane ties are broken by the lowest lane index.
  """
  num_lanes = max(1, int(num_lanes))
  max_sites_per_batch = max(1, int(max_sites_per_batch))
  num_sites = len(sorted_etas)

  p90_threshold = get_p90_threshold(sorted_etas)
  # sorted_etas is descending, so the tail is always a prefix.
  tail_count = sum(1 for eta in sorted_etas if eta > p90_threshold)
  tail_group_size = compute_tail_group_size(tail_count, num_lanes)

  lane_times = [0.0] * num_lanes
  lane_tail_groups: List[List[List[int]]] = [[] for _ in range(num_lanes)]
  lane_rest: List[List[int]] = [[] for _ in range(num_lanes)]

  def _least_loaded_lane() -> int:
    return min(range(num_lanes), key=lambda i: lane_times[i])

  start = 0
  for size in split_tail_into_groups(tail_count, tail_group_size):
    group = list(range(start, start + size))
    start += size
    lane = _least_loaded_lane()
    lane_tail_groups[lane].append(group)
    lane_times[lane] += group_eta_fn(group)

  for pos in range(tail_count, num_sites):
    lane = _least_loaded_lane()
    lane_rest[lane].append(pos)
    lane_times[lane] += sorted_etas[pos]

  lanes: List[List[Tuple[List[int], bool]]] = []
  for lane_idx in range(num_lanes):
    batches = [
        (group, True)
        for group in _merge_consecutive(lane_tail_groups[lane_idx], max_sites_per_batch)
    ]
    rest_desc = sorted(lane_rest[lane_idx], key=lambda p: (-sorted_etas[p], p))
    batches.extend((chunk, False) for chunk in _chunk(rest_desc, max_sites_per_batch))
    lanes.append(batches)

  return LanePlan(lanes, p90_threshold, tail_count, tail_group_size)
