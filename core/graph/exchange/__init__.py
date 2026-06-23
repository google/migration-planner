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

"""Facade exposing Exchange data pipeline functions."""

from core.graph.exchange.mailbox import run_mailbox_usage_pipeline, format_bytes
from core.graph.exchange.calendar import run_calendar_telemetry_pipeline
from core.graph.exchange.integrated_apps import run_exchange_apps_pipeline
from core.graph.exchange.mail_security import run_mail_security_pipeline
from core.graph.exchange.transport_rules import run_transport_rules_pipeline
from core.graph.exchange.connectors import fetch_exchange_connectors_data
from core.graph.exchange.email_clients import run_email_client_usage_pipeline
from core.graph.exchange.pst_files import run_pst_discovery_pipeline
