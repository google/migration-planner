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

"""Exchange Online Transport Rules telemetry scanner data pipeline."""

import os
import logging
from core.powershell.transport_rules import TransportRulesFetcher

logger = logging.getLogger(__name__)

def run_transport_rules_pipeline(client_id: str, client_secret: str, tenant_id: str) -> dict:
    """Orchestrates PowerShell client to fetch Exchange Transport Rules and output to CSV."""
    logger.info("Starting Exchange Transport Rules Telemetry Pipeline...")
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    telemetry_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(script_dir))), "telemetry")
    reports_dir = os.path.join(telemetry_dir, "reports", f"{tenant_id}_{client_id}")
    os.makedirs(reports_dir, exist_ok=True)
    csv_path = os.path.join(reports_dir, "exchange_transport_rules.csv")
    
    fetcher = TransportRulesFetcher(tenant_id, client_id, client_secret)
    res = fetcher.fetch_rules(csv_path)
    
    if not res.get("success", False):
        errs = res.get("errors", {})
        first_err = list(errs.values())[0] if errs else "Unknown Script Error"
        raise ConnectionError(f"PowerShell Execution Error: {first_err}")
        
    return {"csv_path": csv_path, "success": True}
