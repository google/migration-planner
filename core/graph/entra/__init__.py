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

"""Facade exposing Entra telemetry pipelines and consolidated runner."""

import os
import csv
import logging
import threading

from core.graph.client import GraphClient
from core.graph.reports import ReportsService

from core.graph.entra.auth_methods import run_auth_methods_pipeline
from core.graph.entra.app_signins import run_app_signins_pipeline
from core.graph.entra.user_signins import run_user_signins_pipeline
from core.graph.entra.app_registrations import run_app_registrations_pipeline

logger = logging.getLogger(__name__)

def run_devices_apps_pipeline(
    client_id: str, 
    client_secret: str, 
    tenant_id: str, 
    on_app_signins_page_callback=None,
    on_auth_methods_page_callback=None,
    on_user_signins_page_callback=None,
    on_app_registrations_page_callback=None,
    is_cancelled_callback=None
) -> dict:
    """Pipeline to fetch app sign-in summaries, auth methods, user signins, and app registrations in parallel."""
    logger.info("Starting Microsoft Entra Data Telemetry Pipeline in parallel...")
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    telemetry_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(script_dir))), "telemetry")
    reports_dir = os.path.join(telemetry_dir, "reports", f"{tenant_id}_{client_id}")
    os.makedirs(reports_dir, exist_ok=True)
    
    csv_path_app_signins = os.path.join(reports_dir, "entra_app_signins.csv")
    csv_path_auth_methods = os.path.join(reports_dir, "entra_auth_methods.csv")
    csv_path_user_signins = os.path.join(reports_dir, "entra_user_signins.csv")
    csv_path_app_registrations = os.path.join(reports_dir, "entra_app_registrations.csv")
    
    with open(csv_path_app_signins, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["appDisplayName", "successSignInCount"])
        
    with open(csv_path_auth_methods, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["authenticationMethod", "successActivityCount"])
        
    with open(csv_path_user_signins, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["appDisplayName", "operatingSystem", "browser", "isInteractive"])

    with open(csv_path_app_registrations, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["displayName", "appId", "createdDateTime", "signInAudience", "credentials"])
            
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=4,
        retries=5,
        backoff=2
    )
    client.authenticate()
    reports_service = ReportsService(client)
    
    errors = []
    
    def run_fetch_app_signins(path):
        try:
            reports_service.fetch_app_signin_summary(
                csv_path=path,
                max_rows=5000,
                on_page_callback=on_app_signins_page_callback,
                is_cancelled_callback=is_cancelled_callback
            )
        except Exception as thread_err:
            logger.error(f"Error in thread fetching app sign-ins: {thread_err}", exc_info=True)
            errors.append(thread_err)

    def run_fetch_auth_methods(path):
        try:
            reports_service.fetch_auth_methods_summary(
                csv_path=path,
                max_rows=5000,
                on_page_callback=on_auth_methods_page_callback,
                is_cancelled_callback=is_cancelled_callback
            )
        except Exception as thread_err:
            logger.error(f"Error in thread fetching auth methods: {thread_err}", exc_info=True)
            errors.append(thread_err)
            
    def run_fetch_user_signins(path):
        try:
            reports_service.fetch_user_signins(
                csv_path=path,
                max_rows=20000,
                on_page_callback=on_user_signins_page_callback,
                is_cancelled_callback=is_cancelled_callback
            )
        except Exception as thread_err:
            logger.error(f"Error in thread fetching user sign-ins: {thread_err}", exc_info=True)
            errors.append(thread_err)

    def run_fetch_app_registrations(path):
        try:
            reports_service.fetch_app_registrations(
                csv_path=path,
                max_rows=5000,
                on_page_callback=on_app_registrations_page_callback,
                is_cancelled_callback=is_cancelled_callback
            )
        except Exception as thread_err:
            logger.error(f"Error in thread fetching app registrations: {thread_err}", exc_info=True)
            errors.append(thread_err)
            
    try:
        t3 = threading.Thread(target=run_fetch_app_signins, args=(csv_path_app_signins,), daemon=True)
        t4 = threading.Thread(target=run_fetch_auth_methods, args=(csv_path_auth_methods,), daemon=True)
        t5 = threading.Thread(target=run_fetch_user_signins, args=(csv_path_user_signins,), daemon=True)
        t6 = threading.Thread(target=run_fetch_app_registrations, args=(csv_path_app_registrations,), daemon=True)
        
        t3.start()
        t4.start()
        t5.start()
        t6.start()
        
        t3.join()
        t4.join()
        t5.join()
        t6.join()
        
        if len(errors) == 4:
            raise errors[0]
            
        app_signins = []
        auth_methods = []
        app_registrations = []
        unique_apps = set()
        unique_os = set()
        unique_browsers = set()
        
        if os.path.exists(csv_path_app_signins):
            with open(csv_path_app_signins, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) >= 2:
                        app_signins.append((row[0], row[1]))
                        
        if os.path.exists(csv_path_auth_methods):
            with open(csv_path_auth_methods, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) >= 2:
                        auth_methods.append((row[0], row[1]))

        if os.path.exists(csv_path_app_registrations):
            with open(csv_path_app_registrations, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) >= 5:
                        app_registrations.append((row[0], row[1], row[2], row[3], row[4]))
                        
        if os.path.exists(csv_path_user_signins):
            with open(csv_path_user_signins, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) >= 4:
                        if row[0]: unique_apps.add(row[0])
                        if row[1]: unique_os.add(row[1])
                        if row[2]: unique_browsers.add(row[2])
                                
        return {
            "app_signins": app_signins,
            "auth_methods": auth_methods,
            "app_registrations": app_registrations,
            "user_signins": {
                "apps": sorted(list(unique_apps)),
                "os": sorted(list(unique_os)),
                "browsers": sorted(list(unique_browsers))
            }
        }
    finally:
        client.close()
