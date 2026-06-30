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

"""Backward compatibility facade for Microsoft Entra Data telemetry."""

# Re-export pipeline from core backend
from core.graph.entra import run_devices_apps_pipeline
from core.graph.client import GraphClient
from core.graph.reports import ReportsService

# Re-export UI subframes and main container from telemetry package
from telemetry.entra.auth_methods import AuthMethodsSubFrame
from telemetry.entra.app_signins import AppSigninsSubFrame
from telemetry.entra.user_signins import UserSigninsSubFrame
from telemetry.entra.app_registrations import AppRegistrationsSubFrame
from telemetry.entra import DevicesAppsTelemetryFrame
