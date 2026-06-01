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

"""ReportsService encapsulating Microsoft Graph usage report generation and downloads."""

import os
import time
import logging
import concurrent.futures
from typing import List, Tuple
import requests
from core.graph.client import GraphClient

logger = logging.getLogger(__name__)

class ReportsService:
    """Service to fetch streaming M365 telemetry reports via Microsoft Graph API."""

    def __init__(self, client: GraphClient) -> None:
        self.client = client

    def download_report(self, api_url: str, output_filename: str, output_dir: str) -> None:
        """Downloads a single CSV report via a streaming connection utilizing GraphClient session."""
        token_slot = self.client.get_active_token()
        session = self.client.get_session()
        
        headers = {
            "Authorization": f"Bearer {token_slot['token']}"
        }
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, output_filename)

        try:
            logger.info("Calling Graph report endpoint for %s...", output_filename)
            resp = session.get(api_url, headers=headers, allow_redirects=False, stream=True)
            
            # Graph APIs return status code 302 redirect to a pre-authenticated S3/Azure Blob URL
            if resp.status_code == 302:
                resp.close()
                location_url = resp.headers.get("Location")
                if not location_url:
                    raise ConnectionError(f"302 redirect returned for {output_filename} but Location header is missing.")
                
                logger.info("Pre-authenticated storage redirect retrieved. Waiting 2 seconds before download...")
                time.sleep(2)

                max_retries = 5
                retry_interval = 30
                for attempt in range(1, max_retries + 1):
                    try:
                        logger.info("[Attempt %d/%d] Downloading report stream to %s...", attempt, max_retries, output_filename)
                        with requests.get(location_url, stream=True) as csv_response:
                            csv_response.raise_for_status()
                            with open(output_path, "wb") as f:
                                for chunk in csv_response.iter_content(chunk_size=8192):
                                    if chunk:
                                        f.write(chunk)
                        break
                    except requests.exceptions.RequestException as error:
                        if attempt < max_retries:
                            logger.warning("[Attempt %d/%d] Failed stream download. Retrying in %ds... (Error: %s)", attempt, max_retries, retry_interval, error)
                            time.sleep(retry_interval)
                        else:
                            logger.error("Failed downloading %s after %d attempts.", output_filename, max_retries, exc_info=True)
                            raise ConnectionError(f"Failed downloading report after %d attempts. Details: {error}")
                
                logger.info("Success! Saved report to: %s", output_path)

            elif resp.status_code == 200:
                logger.info("Unexpected 200 OK status returned. Saving direct streaming stream...")
                with open(output_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                resp.close()
                logger.info("Success! Saved report to: %s", output_path)
            else:
                resp.close()
                logger.error("Graph report request failed with status code %d: %s", resp.status_code, resp.text)
                raise ConnectionError(f"Microsoft Graph API request failed with status code {resp.status_code}")
        finally:
            self.client.release_token(token_slot)

    def download_reports_batch(self, reports: List[Tuple[str, str]], output_dir: str) -> None:
        """Downloads a list of reports concurrently using thread executors."""
        logger.info("Starting parallel batch download for %d reports...", len(reports))
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(reports)) as executor:
            futures = [
                executor.submit(self.download_report, url, filename, output_dir)
                for url, filename in reports
            ]
            # Gather all futures, raising exceptions if any thread failed
            for future in concurrent.futures.as_completed(futures):
                future.result()

    def download_o365_active_user_detail(self, output_dir: str) -> None:
        """Downloads the Office 365 active user details CSV report (180 days)."""
        url = "https://graph.microsoft.com/v1.0/reports/getOffice365ActiveUserDetail(period='D180')"
        self.download_report(url, "Office365ActiveUserDetail(180d).csv", output_dir)

    def download_o365_active_user_counts(self, output_dir: str) -> None:
        """Downloads the Office 365 30-day active user counts CSV report."""
        url = "https://graph.microsoft.com/v1.0/reports/getOffice365ActiveUserCounts(period='D30')"
        self.download_report(url, "Office365ActiveUserCounts(30d).csv", output_dir)

    def download_m365_app_details(self, output_dir: str) -> None:
        """Downloads both M365 App user details and counts CSV reports concurrently."""
        reports = [
            ("https://graph.microsoft.com/v1.0/reports/getM365AppUserDetail(period='D180')", "M365AppUserDetail(180d).csv"),
            ("https://graph.microsoft.com/v1.0/reports/getM365AppUserCounts(period='D180')", "getM365AppUserCounts(180d).csv")
        ]
        self.download_reports_batch(reports, output_dir)

    def download_sharepoint_onedrive_details(self, output_dir: str) -> None:
        """Downloads SharePoint site usage, OneDrive account usage, OneDrive activity, and M365 App details CSV reports concurrently."""
        reports = [
            ("https://graph.microsoft.com/v1.0/reports/getSharePointSiteUsageDetail(period='D180')", "SharePointSiteUsageDetail(180d).csv"),
            ("https://graph.microsoft.com/v1.0/reports/getOneDriveUsageAccountDetail(period='D180')", "OneDriveUsageAccountDetail(180d).csv"),
            ("https://graph.microsoft.com/v1.0/reports/getOneDriveActivityUserDetail(period='D180')", "OneDriveActivityUserDetail(180d).csv"),
            ("https://graph.microsoft.com/v1.0/reports/getM365AppUserDetail(period='D180')", "M365AppUserDetail_sp_od(180d).csv")
        ]
        self.download_reports_batch(reports, output_dir)
