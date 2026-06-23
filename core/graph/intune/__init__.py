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

"""Facade exposing Intune telemetry pipelines and legacy IntuneService class."""

import os
import csv
import logging
import threading
from collections import defaultdict
import pandas as pd

from core.graph.client import GraphClient
from core.graph.intune.mobile_apps import run_mobile_apps_pipeline
from core.graph.intune.detected_apps import run_detected_apps_pipeline
from core.graph.intune.device_configs import run_device_configs_pipeline

logger = logging.getLogger(__name__)

def run_intune_policies_pipeline(
    client_id: str, 
    client_secret: str, 
    tenant_id: str, 
    on_page_callback=None, 
    on_apps_page_callback=None, 
    is_cancelled_callback=None
) -> dict:
    """Consolidated pipeline to fetch Intune configuration policies, mobile apps, and detected apps in parallel (for backward compatibility)."""
    logger.info("Starting Intune Policies Pipeline in parallel...")
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    telemetry_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(script_dir))), "telemetry")
    reports_dir = os.path.join(telemetry_dir, "reports", f"{tenant_id}_{client_id}")
    os.makedirs(reports_dir, exist_ok=True)
    
    csv_path_device_configs = os.path.join(reports_dir, "intune_device_configs.csv")
    csv_path_config_policies = os.path.join(reports_dir, "intune_config_policies.csv")
    csv_path_apps = os.path.join(reports_dir, "intune_apps.csv")
    csv_path_detected_apps = os.path.join(reports_dir, "intune_detected_apps.csv")
    
    temp_path_device_configs = csv_path_device_configs + ".tmp"
    temp_path_config_policies = csv_path_config_policies + ".tmp"
    temp_path_apps = csv_path_apps + ".tmp"
    temp_path_detected_apps = csv_path_detected_apps + ".tmp"

    for path in [temp_path_device_configs, temp_path_config_policies]:
        with open(path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["displayName", "platform", "policyType"])
            
    with open(temp_path_apps, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["displayName"])
        
    errors = []
    
    def fetch_device_configs():
        try:
            run_device_configs_pipeline(
                client_id=client_id,
                client_secret=client_secret,
                tenant_id=tenant_id,
                endpoint_name="deviceConfigurations",
                csv_path=temp_path_device_configs,
                on_page_callback=on_page_callback,
                is_cancelled_callback=is_cancelled_callback
            )
        except Exception as e:
            errors.append(e)
            
    def fetch_config_policies():
        try:
            run_device_configs_pipeline(
                client_id=client_id,
                client_secret=client_secret,
                tenant_id=tenant_id,
                endpoint_name="configurationPolicies",
                csv_path=temp_path_config_policies,
                on_page_callback=on_page_callback,
                is_cancelled_callback=is_cancelled_callback
            )
        except Exception as e:
            errors.append(e)
            
    def fetch_mobile_apps():
        try:
            run_mobile_apps_pipeline(
                client_id=client_id,
                client_secret=client_secret,
                tenant_id=tenant_id,
                csv_path=temp_path_apps,
                on_page_callback=on_apps_page_callback,
                is_cancelled_callback=is_cancelled_callback
            )
        except Exception as e:
            errors.append(e)
            
    def fetch_detected_apps():
        try:
            run_detected_apps_pipeline(
                client_id=client_id,
                client_secret=client_secret,
                tenant_id=tenant_id,
                csv_path=temp_path_detected_apps,
                is_cancelled_callback=is_cancelled_callback
            )
        except Exception as e:
            errors.append(e)
            
    t1 = threading.Thread(target=fetch_device_configs, daemon=True)
    t2 = threading.Thread(target=fetch_config_policies, daemon=True)
    t3 = threading.Thread(target=fetch_mobile_apps, daemon=True)
    t4 = threading.Thread(target=fetch_detected_apps, daemon=True)
    
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    
    if len(errors) == 4:
        raise errors[0]

    for temp, final in [
        (temp_path_device_configs, csv_path_device_configs),
        (temp_path_config_policies, csv_path_config_policies),
        (temp_path_apps, csv_path_apps),
        (temp_path_detected_apps, csv_path_detected_apps)
    ]:
        if os.path.exists(temp):
            if os.path.exists(final):
                os.remove(final)
            os.rename(temp, final)

    counts = defaultdict(int)
    total_dc = 0
    total_cp = 0
    unique_apps = set()
    
    if os.path.exists(csv_path_device_configs):
        with open(csv_path_device_configs, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if len(row) >= 3:
                    platform, policy_type = row[1], row[2]
                    if platform and policy_type:
                        counts[(platform, policy_type)] += 1
                        total_dc += 1
                        
    if os.path.exists(csv_path_config_policies):
        with open(csv_path_config_policies, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if len(row) >= 3:
                    platform, policy_type = row[1], row[2]
                    if platform and policy_type:
                        counts[(platform, policy_type)] += 1
                        total_cp += 1
                        
    if os.path.exists(csv_path_apps):
        with open(csv_path_apps, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if len(row) >= 1:
                    app_name = row[0]
                    if app_name:
                        unique_apps.add(app_name)
                        
    rows = []
    for (platform, p_type), count in sorted(counts.items()):
        rows.append((platform, p_type, str(count)))
        
    detected_rows_for_ui = []
    if os.path.exists(csv_path_detected_apps):
        df_detected = pd.read_csv(csv_path_detected_apps)
        df_slice = df_detected.head(200).fillna("N/A")
        detected_rows_for_ui = df_slice.to_dict('records')
        
    return {
        "total_device_configs": total_dc,
        "total_config_policies": total_cp,
        "table_rows": rows,
        "mobile_apps": sorted(list(unique_apps)),
        "detected_apps": detected_rows_for_ui
    }


class IntuneService:
    """Legacy backward compatibility wrapper for Intune Graph query service."""
    def __init__(self, client: GraphClient):
        self.client = client

    def fetch_configuration_records(self, endpoint_name: str, csv_path: str, max_rows: int = 10000, on_page_callback=None, is_cancelled_callback=None) -> None:
        run_device_configs_pipeline(
            client_id=self.client.client_ids[0] if isinstance(self.client.client_ids, list) else self.client.client_ids,
            client_secret=self.client.client_secrets[0] if isinstance(self.client.client_secrets, list) else self.client.client_secrets,
            tenant_id=self.client.tenant_id,
            endpoint_name=endpoint_name,
            csv_path=csv_path,
            max_rows=max_rows,
            on_page_callback=on_page_callback,
            is_cancelled_callback=is_cancelled_callback
        )

    def fetch_mobile_apps(self, csv_path: str, max_rows: int = 5000, on_page_callback=None, is_cancelled_callback=None) -> None:
        run_mobile_apps_pipeline(
            client_id=self.client.client_ids[0] if isinstance(self.client.client_ids, list) else self.client.client_ids,
            client_secret=self.client.client_secrets[0] if isinstance(self.client.client_secrets, list) else self.client.client_secrets,
            tenant_id=self.client.tenant_id,
            csv_path=csv_path,
            max_rows=max_rows,
            on_page_callback=on_page_callback,
            is_cancelled_callback=is_cancelled_callback
        )

    def fetch_detected_apps(self, csv_path: str = None, max_rows: int = 10000, is_cancelled_callback=None) -> list:
        return run_detected_apps_pipeline(
            client_id=self.client.client_ids[0] if isinstance(self.client.client_ids, list) else self.client.client_ids,
            client_secret=self.client.client_secrets[0] if isinstance(self.client.client_secrets, list) else self.client.client_secrets,
            tenant_id=self.client.tenant_id,
            csv_path=csv_path,
            max_rows=max_rows,
            is_cancelled_callback=is_cancelled_callback
        )
