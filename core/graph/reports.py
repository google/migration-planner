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

import re

def _sanitize_string(s: str) -> str:
    if not s:
        return ""
    # Mask JWT and access tokens in query parameters or headers
    s = re.sub(r'token=[^&"\')\s]+', 'token=[MASKED]', s)
    s = re.sub(r'Bearer\s+[^&"\')\s]+', 'Bearer [MASKED]', s, flags=re.IGNORECASE)
    return s

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
                            logger.warning("[Attempt %d/%d] Failed stream download. Retrying in %ds... (Error: %s)", attempt, max_retries, retry_interval, _sanitize_string(str(error)))
                            time.sleep(retry_interval)
                        else:
                            logger.error("Failed downloading %s after %d attempts. Error: %s", output_filename, max_retries, _sanitize_string(str(error)))
                            raise ConnectionError(f"Failed downloading report after {max_retries} attempts.")
                
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
                logger.error("Graph report request failed with status code %d: %s", resp.status_code, _sanitize_string(resp.text))
                raise ConnectionError(f"Microsoft Graph API request failed with status code {resp.status_code}")
        finally:
            self.client.release_token(token_slot)

    def download_reports_batch(self, reports: List[Tuple[str, str]], output_dir: str, progress_callback=None) -> None:
        """Downloads a list of reports concurrently using thread executors."""
        logger.info("Starting parallel batch download for %d reports...", len(reports))
        
        completed_count = 0
        total_reports = len(reports)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(reports)) as executor:
            futures = [
                executor.submit(self.download_report, url, filename, output_dir)
                for url, filename in reports
            ]
            # Gather all futures, raising exceptions if any thread failed
            for future in concurrent.futures.as_completed(futures):
                future.result()
                completed_count += 1
                if progress_callback:
                    try:
                        progress_callback(completed_count / total_reports)
                    except Exception as e:
                        logger.warning("Failed to invoke progress callback: %s", e)

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

    def download_sharepoint_details(self, output_dir: str) -> None:
        """Downloads only SharePoint site usage detail report."""
        self.download_report("https://graph.microsoft.com/v1.0/reports/getSharePointSiteUsageDetail(period='D180')", "SharePointSiteUsageDetail(180d).csv", output_dir)

    def download_onedrive_details(self, output_dir: str) -> None:
        """Downloads OneDrive usage account, activity, and app user detail reports concurrently."""
        reports = [
            ("https://graph.microsoft.com/v1.0/reports/getOneDriveUsageAccountDetail(period='D180')", "OneDriveUsageAccountDetail(180d).csv"),
            ("https://graph.microsoft.com/v1.0/reports/getOneDriveActivityUserDetail(period='D180')", "OneDriveActivityUserDetail(180d).csv"),
            ("https://graph.microsoft.com/v1.0/reports/getM365AppUserDetail(period='D180')", "M365AppUserDetail_sp_od(180d).csv")
        ]
        self.download_reports_batch(reports, output_dir)

    def download_mailbox_usage_detail(self, output_dir: str) -> None:
        """Downloads Exchange mailbox usage detail CSV report (180 days)."""
        self.download_report("https://graph.microsoft.com/v1.0/reports/getMailboxUsageDetail(period='D180')", "MailboxUsageDetail(180d).csv", output_dir)

    def download_email_app_usage_detail(self, output_dir: str) -> None:
        """Downloads Exchange email app usage detail CSV report (180 days)."""
        self.download_report("https://graph.microsoft.com/v1.0/reports/getEmailAppUsageUserDetail(period='D180')", "EmailAppUsageUserDetail(180d).csv", output_dir)

    def search_cloud_pst_files(self) -> dict:
        """Queries Microsoft Graph Search API to locate cloud-stored PST archive files across all active regions in a paginated fashion, up to a total of 2000 files."""
        url = "https://graph.microsoft.com/v1.0/search/query"
        token_slot = self.client.get_active_token()
        session = self.client.get_session()
        
        headers = {
            "Authorization": f"Bearer {token_slot['token']}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        total_hits = []
        regions = ["NAM", "EUR", "APC"]
        page_size = 500
        max_total_limit = 2000
        
        try:
            for region in regions:
                # Calculate how many hits we have already fetched in total
                current_fetched = 0
                for r_resp in total_hits:
                    for hc in r_resp.get("hitsContainers", []):
                        current_fetched += len(hc.get("hits", []))
                
                remaining_limit = max_total_limit - current_fetched
                if remaining_limit <= 0:
                    logger.info(f"Reached total limit of {max_total_limit} files. Stopping search across regions.")
                    break
                
                offset = 0
                has_more = True
                region_response = None
                region_hits_container = None
                
                while has_more:
                    # Request only up to the remaining allowed limit
                    current_page_size = min(page_size, remaining_limit)
                    
                    payload = {
                        "requests": [
                            {
                                "entityTypes": ["driveItem"],
                                "query": {"queryString": "fileextension:pst"},
                                "from": offset,
                                "size": current_page_size,
                                "region": region
                            }
                        ]
                    }
                    logger.info(f"Executing Graph Search query for cloud PST archives in region: {region} (offset: {offset}, limit: {current_page_size})...")
                    resp = session.post(url, json=payload, headers=headers)
                    if resp.status_code == 200:
                        data = resp.json()
                        page_response_list = data.get("value", [])
                        
                        if not page_response_list:
                            has_more = False
                            continue
                            
                        page_response = page_response_list[0]
                        page_containers = page_response.get("hitsContainers", [])
                        
                        if not page_containers:
                            has_more = False
                            continue
                            
                        container = page_containers[0]
                        hits = container.get("hits", [])
                        total_count = container.get("total", 0)
                        more_results = container.get("moreResultsAvailable", False)
                        
                        if region_response is None:
                            region_response = {
                                "@odata.type": page_response.get("@odata.type"),
                                "searchTerms": page_response.get("searchTerms", []),
                                "hitsContainers": [
                                    {
                                        "@odata.type": container.get("@odata.type"),
                                        "total": total_count,
                                        "moreResultsAvailable": False,
                                        "hits": []
                                    }
                                ]
                            }
                            region_hits_container = region_response["hitsContainers"][0]
                        
                        region_hits_container["hits"].extend(hits)
                        
                        region_fetched = len(region_hits_container["hits"])
                        if region_fetched >= remaining_limit:
                            logger.info(f"Reached remaining limit of {remaining_limit} files in region {region}. Stopping paginated fetch.")
                            region_hits_container["hits"] = region_hits_container["hits"][:remaining_limit]
                            has_more = False
                        elif more_results and len(hits) > 0:
                            offset += page_size
                        else:
                            has_more = False
                            
                    elif resp.status_code == 400 and "Only valid regions are" in resp.text:
                        logger.info(f"Region {region} is not active for this tenant. Skipping.")
                        has_more = False
                    else:
                        raise ConnectionError(f"Graph Search failed for region {region} (HTTP {resp.status_code}): {resp.text}")
                
                if region_response is not None:
                    total_hits.append(region_response)
            
            return {"value": total_hits}
        finally:
            self.client.release_token(token_slot)

    def download_email_app_usage_apps_user_counts(self, output_dir: str) -> None:
        """Downloads Exchange email app usage apps user counts CSV report (180 days)."""
        self.download_report("https://graph.microsoft.com/v1.0/reports/getEmailAppUsageAppsUserCounts(period='D180')", "EmailAppUsageAppsUserCounts(180d).csv", output_dir)


