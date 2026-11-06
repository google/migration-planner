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

"""ETA calculation for OneDrive and SharePoint migrations (Deep and Shallow Scan)."""

from util.constants import (
    ENCRYPTED_FILE_ETA_MULTIPLIER,
    FILES_GLOBAL_CORPUS_SIZE_LIMIT,
)


def calculate_files_eta(
    total_size: float,
    total_files: int,
    files_per_sec: float,
    encrypted_files: int = 0,
    encrypted_size: float = 0.0,
    size_limit: float = FILES_GLOBAL_CORPUS_SIZE_LIMIT,
) -> float:
  """Returns the migration time in HOURS for one batch.

  The batch is limited by whichever is slower: its files at files_per_sec, or
  its bytes at size_limit (bytes/sec). Encrypted files migrate at
  ENCRYPTED_FILE_ETA_MULTIPLIER of both limits and are added on top.

  Args:
    total_size: Total bytes in the batch, including encrypted files.
    total_files: Total files in the batch, including encrypted files.
    files_per_sec: Files per second the batch can migrate.
    encrypted_files: Encrypted files in the batch.
    encrypted_size: Bytes of encrypted files in the batch.
    size_limit: Bytes per second the batch can migrate.

  Returns:
    Estimated hours to migrate the batch.
  """
  unencrypted_size = total_size - encrypted_size
  unencrypted_files = total_files - encrypted_files

  # Unencrypted calculation
  average_unencrypted_file_size = unencrypted_size / unencrypted_files if unencrypted_files > 0 else 0
  max_qps_from_unencrypted_file_size = (
      size_limit / average_unencrypted_file_size if average_unencrypted_file_size > 0 else size_limit
  )
  qps_unencrypted = min(files_per_sec, max_qps_from_unencrypted_file_size)
  time_unencrypted = unencrypted_files / qps_unencrypted if qps_unencrypted > 0 else 0

  # Encrypted calculation
  average_encrypted_file_size = encrypted_size / encrypted_files if encrypted_files > 0 else 0
  encrypted_size_limit = size_limit * ENCRYPTED_FILE_ETA_MULTIPLIER
  encrypted_count_limit = files_per_sec * ENCRYPTED_FILE_ETA_MULTIPLIER
  max_qps_from_encrypted_file_size = (
      encrypted_size_limit / average_encrypted_file_size if average_encrypted_file_size > 0 else encrypted_size_limit
  )
  qps_encrypted = min(encrypted_count_limit, max_qps_from_encrypted_file_size)
  time_encrypted = encrypted_files / qps_encrypted if qps_encrypted > 0 else 0

  return (time_unencrypted + time_encrypted) / 3600
