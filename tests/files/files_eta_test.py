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

"""Tests for the OneDrive / SharePoint ETA calculation.

Run with:
  python3 -m unittest tests.files.files_eta_test -v
"""

import unittest

from batching import files_eta
from util.constants import FILES_GLOBAL_CORPUS_SIZE_LIMIT


class FilesEtaTest(unittest.TestCase):
  """Verifies batch time = max(files / rate, bytes / size limit), plus encrypted files."""

  def test_file_bound_batch(self):
    eta = files_eta.calculate_files_eta(total_size=1024, total_files=36_000, files_per_sec=10)
    self.assertAlmostEqual(eta, 1.0, places=6)

  def test_size_bound_batch(self):
    size = FILES_GLOBAL_CORPUS_SIZE_LIMIT * 3600 * 2  # Two hours of bytes.
    eta = files_eta.calculate_files_eta(total_size=size, total_files=100, files_per_sec=10)
    self.assertAlmostEqual(eta, 2.0, places=6)

  def test_encrypted_files_migrate_at_half_speed(self):
    plain = files_eta.calculate_files_eta(1024, 36_000, 10)
    encrypted = files_eta.calculate_files_eta(1024, 36_000, 10, encrypted_files=36_000, encrypted_size=1024)
    self.assertAlmostEqual(encrypted, plain * 2, places=6)

  def test_empty_batch_takes_no_time(self):
    self.assertEqual(files_eta.calculate_files_eta(0, 0, 10), 0)


if __name__ == "__main__":
  unittest.main()
