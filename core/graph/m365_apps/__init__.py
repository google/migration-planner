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

"""Facade exposing M365 Apps data pipeline functions."""

from core.graph.m365_apps.active_users import run_o365_pipeline, process_active_user_detail
from core.graph.m365_apps.active_users_trend import run_o365_trend_pipeline, process_active_user_counts
from core.graph.m365_apps.app_usage import run_m365_pipeline, process_m365_app_user_detail
