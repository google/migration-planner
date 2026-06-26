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
import csv
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
            resp = session.get(api_url, headers=headers, allow_redirects=False, stream=True, timeout=60.0)
            
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
                        with requests.get(location_url, stream=True, timeout=120.0) as csv_response:
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

    def fetch_app_signin_summary(self, csv_path: str, max_rows: int = 5000, on_page_callback=None, is_cancelled_callback=None) -> None:
        """Fetches Azure AD application sign-in summary for the last 7 days and dumps to a CSV file."""
        token_slot = self.client.get_active_token()
        session = self.client.get_session()
        
        headers = {
            "Authorization": f"Bearer {token_slot['token']}",
            "Accept": "application/json"
        }
        
        url = "https://graph.microsoft.com/beta/reports/getAzureADApplicationSignInSummary(period='D7')"
        
        try:
            logger.info("Fetching Azure AD Application Sign-in Summary...")
            retries_left = 4
            resp = None
            value_list = []
            
            while retries_left > 0:
                if is_cancelled_callback and is_cancelled_callback():
                    return
                    
                try:
                    resp = session.get(url, headers=headers, timeout=120.0)
                    logger.info("App Sign-ins HTTP status: %d", resp.status_code)
                    if resp.status_code == 200:
                        data = resp.json()
                        value_list = data.get("value", [])
                        if value_list:
                            break
                        else:
                            logger.warning("Received empty value collection for App Sign-ins summary, retrying...")
                    else:
                        break
                except Exception as get_err:
                    logger.warning("Query attempt failed: %s", get_err)
                    
                retries_left -= 1
                if retries_left > 0:
                    time.sleep(2)
                    
            if resp is not None and resp.status_code == 200 and value_list:
                logger.info("App Sign-ins value collection length: %d", len(value_list))
                logger.info("App Sign-ins first 200 chars: %s", str(resp.json())[:200])
                
                with open(csv_path, 'a', encoding='utf-8', newline='') as f:
                    writer = csv.writer(f)
                    rows_written = 0
                    for item in value_list:
                        if rows_written >= max_rows:
                            break
                        app_name = item.get("appDisplayName") or ""
                        success_count = item.get("successfulSignInCount") or 0
                        writer.writerow([app_name, success_count])
                        rows_written += 1
                        
                if on_page_callback:
                    on_page_callback(value_list)
                    
                logger.info("Successfully fetched %d app sign-in summary records", rows_written)
            else:
                if resp is not None and resp.status_code in [401, 403]:
                    logger.error("App Sign-ins Summary endpoint access denied: %d %s", resp.status_code, resp.text)
                    raise PermissionError("Reports.Read.All permission required.")
                else:
                    status_str = f"status {resp.status_code}" if resp is not None else "connection/timeout error"
                    logger.warning("App Sign-ins Summary query failed (%s) or remained empty.", status_str)
        finally:
            self.client.release_token(token_slot)

    def fetch_auth_methods_summary(self, csv_path: str, period: str = "D7", max_rows: int = 5000, on_page_callback=None, is_cancelled_callback=None) -> None:
        """Fetches User Sign-in by Authentication Method Summary for the specified period and dumps to a CSV file."""
        token_slot = self.client.get_active_token()
        session = self.client.get_session()
        
        headers = {
            "Authorization": f"Bearer {token_slot['token']}",
            "Accept": "application/json"
        }
        
        url = f"https://graph.microsoft.com/beta/reports/authenticationMethods/userSignInsByAuthMethodSummary(period='{period}')"
        
        try:
            logger.info("Fetching User Sign-ins by Authentication Method Summary...")
            if is_cancelled_callback and is_cancelled_callback():
                return
                
            resp = session.get(url, headers=headers, timeout=120.0)
            if resp.status_code == 200:
                data = resp.json()
                value_list = data.get("value", [])
                
                with open(csv_path, 'a', encoding='utf-8', newline='') as f:
                    writer = csv.writer(f)
                    rows_written = 0
                    for item in value_list:
                        if rows_written >= max_rows:
                            break
                        method = item.get("authenticationMethod") or ""
                        success_count = item.get("successActivityCount") or 0
                        writer.writerow([method, success_count])
                        rows_written += 1
                        
                if on_page_callback:
                    on_page_callback(value_list)
                    
                logger.info("Successfully fetched %d authentication method summary records", rows_written)
            else:
                if resp.status_code in [401, 403]:
                    logger.error("Auth Methods Summary endpoint access denied: %d %s", resp.status_code, resp.text)
                    raise PermissionError("AuditLog.Read.All permission required.")
                else:
                    logger.warning("Auth Methods Summary query failed (HTTP %d).", resp.status_code)
        finally:
            self.client.release_token(token_slot)

    def fetch_user_signins(self, csv_path: str, max_rows: int = 20000, on_page_callback=None, is_cancelled_callback=None) -> None:
        """Fetches Microsoft Entra user sign-in logs from the v1.0 auditLogs/signIns endpoint,
        filters for successful sign-ins, flattens, and appends to CSV.
        """
        token_slot = self.client.get_active_token()
        session = self.client.get_session()
        
        headers = {
            "Authorization": f"Bearer {token_slot['token']}",
            "Accept": "application/json"
        }
        
        # Start URL with select query parameter to limit payload size
        next_url = "https://graph.microsoft.com/v1.0/auditLogs/signIns?$select=appDisplayName,status,deviceDetail,isInteractive"
        rows_written = 0
        page_number = 1
        import csv
        
        try:
            logger.info("Starting User Sign-ins fetch...")
            with open(csv_path, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                
                while next_url and rows_written < max_rows:
                    if is_cancelled_callback and is_cancelled_callback():
                        logger.info("User Sign-ins fetch cancelled in-flight. Aborting pagination loop.")
                        break
                    
                    logger.info("Querying MSFT Graph user sign-ins (Page: %d, Successful rows so far: %d)...", 
                                page_number, rows_written)
                    try:
                        # Timeout must be 60.0 seconds, no retries on failure, just exit loop and display what was obtained
                        resp = session.get(next_url, headers=headers, timeout=60.0)
                    except Exception as get_err:
                        logger.warning("Query attempt failed with exception: %s. Displaying data obtained till now.", get_err)
                        break
                        
                    if resp is None or resp.status_code != 200:
                        if resp is not None and resp.status_code in [401, 403]:
                            logger.error("User Sign-ins endpoint permission error: %d %s", resp.status_code, resp.text)
                            raise PermissionError("AuditLog.Read.All permission required.")
                        else:
                            status_str = f"status {resp.status_code}" if resp is not None else "connection/timeout error"
                            logger.warning("User Sign-ins query failed (%s). Displaying data obtained till now.", status_str)
                            break
                            
                    page_number += 1
                    data = resp.json()
                    value_list = data.get("value", [])
                    
                    page_filtered_successful = []
                    for log in value_list:
                        status_obj = log.get("status") or {}
                        # Successful sign-in records are those with status errorCode = 0
                        error_code = status_obj.get("errorCode")
                        if error_code == 0 or error_code == "0":
                            app_name = log.get("appDisplayName") or ""
                            device = log.get("deviceDetail") or {}
                            os_name = device.get("operatingSystem") or ""
                            browser_name = device.get("browser") or ""
                            is_interactive = str(log.get("isInteractive", ""))
                            
                            writer.writerow([app_name, os_name, browser_name, is_interactive])
                            rows_written += 1
                            
                            page_filtered_successful.append(log)
                            if rows_written >= max_rows:
                                break
                                
                    if on_page_callback:
                        try:
                            # Invoke callback with the filtered successful records of the page
                            on_page_callback(page_filtered_successful)
                        except Exception as cb_err:
                            logger.warning("Error in User Sign-ins page callback: %s", cb_err)
                            
                    if rows_written >= max_rows:
                        logger.info("Reached maximum rows limit of %d", max_rows)
                        break
                        
                    next_url = data.get("@odata.nextLink")
                    
            logger.info("Successfully fetched and appended %d successful user sign-in records.", rows_written)
        finally:
            self.client.release_token(token_slot)

    def fetch_app_registrations(self, csv_path: str, max_rows: int = 5000, on_page_callback=None, is_cancelled_callback=None) -> None:
        """Fetches Microsoft Entra app registrations (applications) and dumps to CSV."""
        token_slot = self.client.get_active_token()
        session = self.client.get_session()
        
        headers = {
            "Authorization": f"Bearer {token_slot['token']}",
            "Accept": "application/json"
        }
        
        next_url = "https://graph.microsoft.com/v1.0/applications?$select=displayName,appId,createdDateTime,signInAudience,passwordCredentials,keyCredentials"
        rows_written = 0
        page_number = 1
        import csv
        
        try:
            logger.info("Starting App Registrations fetch...")
            with open(csv_path, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                
                while next_url and rows_written < max_rows:
                    if is_cancelled_callback and is_cancelled_callback():
                        logger.info("App Registrations fetch cancelled in-flight.")
                        break
                    
                    logger.info("Querying MSFT Graph applications (Page: %d, rows so far: %d)...", 
                                page_number, rows_written)
                    try:
                        resp = session.get(next_url, headers=headers, timeout=60.0)
                    except Exception as get_err:
                        logger.warning("Query attempt failed with exception: %s.", get_err)
                        break
                        
                    if resp is None or resp.status_code != 200:
                        if resp is not None and resp.status_code in [401, 403]:
                            logger.error("Applications endpoint permission error: %d %s", resp.status_code, resp.text)
                            raise PermissionError("Application.Read.All permission required.")
                        else:
                            status_str = f"status {resp.status_code}" if resp is not None else "connection/timeout error"
                            logger.warning("Applications query failed (%s).", status_str)
                            break
                            
                    page_number += 1
                    data = resp.json()
                    value_list = data.get("value", [])
                    
                    for app in value_list:
                        display_name = app.get("displayName") or ""
                        app_id = app.get("appId") or ""
                        created_dt = app.get("createdDateTime") or ""
                        audience = app.get("signInAudience") or ""
                        
                        secrets_cnt = len(app.get("passwordCredentials", []))
                        certs_cnt = len(app.get("keyCredentials", []))
                        credentials_str = f"{secrets_cnt} Secrets, {certs_cnt} Certs"
                        
                        writer.writerow([display_name, app_id, created_dt, audience, credentials_str])
                        rows_written += 1
                        
                        if rows_written >= max_rows:
                            break
                            
                    if on_page_callback:
                        try:
                            on_page_callback(value_list)
                        except Exception as cb_err:
                            logger.warning("Error in App Registrations page callback: %s", cb_err)
                            
                    if rows_written >= max_rows:
                        break
                        
                    next_url = data.get("@odata.nextLink")
                    
            logger.info("Successfully fetched and appended %d app registrations.", rows_written)
        finally:
            self.client.release_token(token_slot)



