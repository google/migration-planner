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

"""App Usage, Adoption & Collaboration section view implementation for Flet UI."""

import os
import csv
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional
import flet as ft

from core.graph.m365_apps.app_usage import run_m365_pipeline
from core.graph.m365_apps.active_users import run_o365_pipeline
from core.graph.exchange.mailbox import run_mailbox_usage_pipeline, format_bytes
from core.graph.exchange.email_clients import run_email_client_usage_pipeline
from core.graph.exchange.pst_files import run_pst_discovery_pipeline
from core.graph.files.sharepoint import run_sharepoint_pipeline
from core.graph.files.sharepoint_data_types import run_sharepoint_data_types_pipeline
from core.graph.files.onedrive import run_onedrive_pipeline
from core.graph.files.msteams_overview import run_msteams_pipeline
from core.graph.exchange.calendar import run_calendar_telemetry_pipeline
from flet_ui.views.sections.base_section_view import BaseSectionView
from flet_ui.components.telemetry_card import TelemetryCard
from flet_ui.styles import (
    COLOR_PRIMARY,
    COLOR_SURFACE,
    COLOR_TEXT_PRIMARY,
    COLOR_TEXT_SECONDARY,
)

logger = logging.getLogger("M365TelemetryAsyncLogger.AppUsageAdoptionView")


class AppUsageAdoptionView(BaseSectionView):
    """View rendering all App Usage, Adoption & Collaboration telemetry cards with max 2 concurrency."""

    def __init__(
        self,
        page: ft.Page,
        tenant: str = "",
        client: str = "",
        secret: str = "",
        on_status_change: Optional[Callable[[str], None]] = None,
    ):
        super().__init__(
            page=page,
            tenant=tenant,
            client=client,
            secret=secret,
            on_status_change=on_status_change,
        )

        self._sp_ready_event = threading.Event()
        self.cached_data: Dict[str, Any] = {}
        self._cached_sp_data: Optional[Dict[str, Any]] = None
        self._sp_fetch_lock = threading.Lock()

        # Card container with vertical scrolling
        self.cards_column = ft.Column(
            expand=True,
            spacing=20,
            scroll=ft.ScrollMode.ADAPTIVE,
        )

        # 1. M365 Apps Usage (Paginated listing)
        self.m365_apps_card = TelemetryCard(
            title="M365 Apps Usage (180 Days)",
            link_text="Apps Reports API",
            link_url="https://learn.microsoft.com/en-us/graph/api/reportroot-getm365appuserdetail",
            subtitle="Platform & Client breakdown",
            paginate=True,
            page_size=5,
            column_weights=[3, 1],
            on_reload=lambda: self._reload_card(self._fetch_m365_apps_worker),
        )

        # 2. Active Users Trend (Summary)
        self.active_users_card = TelemetryCard(
            title="Active Users Trend",
            link_text="Active Users API",
            link_url="https://learn.microsoft.com/en-us/graph/api/reportroot-getoffice365activeuserdetail",
            subtitle="Activity volume across 30, 90, and 180-day periods",
            paginate=False,
            column_weights=[3, 2, 2, 2],
            on_reload=lambda: self._reload_card(self._fetch_active_users_worker),
        )

        # 3. Exchange Mailboxes & Storage (Summary)
        self.mailbox_card = TelemetryCard(
            title="Exchange Mailboxes & Storage",
            link_text="Mailbox Usage API",
            link_url="https://learn.microsoft.com/en-us/graph/api/reportroot-getmailboxusagedetail",
            subtitle="Mailbox count, storage capacity, shared mailboxes, and public folders",
            paginate=False,
            column_weights=[3, 2],
            on_reload=lambda: self._reload_card(self._fetch_mailbox_worker),
        )

        # 4. Email Client Classification (Summary)
        self.email_clients_card = TelemetryCard(
            title="Email Client Classification",
            link_text="Email Apps API",
            link_url="https://learn.microsoft.com/en-us/graph/api/reportroot-getemailappusagedetail",
            subtitle="Adoption across browser, desktop Outlook, mobile, and legacy mail protocols",
            paginate=False,
            column_weights=[3, 1],
            on_reload=lambda: self._reload_card(self._fetch_email_clients_worker),
        )

        # 5. PST Files (Location-based summary)
        self.pst_card = TelemetryCard(
            title="PST Files",
            link_text="Search API Reference",
            link_url="https://learn.microsoft.com/en-us/graph/api/resources/search-api-overview",
            subtitle="Discovered PST files across SharePoint and OneDrive",
            footnote="* Note: There may be more than 2,000 files in the tenant; this tool only checks up to 2,000 files.",
            paginate=False,
            column_weights=[2, 5],
            on_reload=lambda: self._reload_card(self._fetch_pst_worker),
        )

        # 6. SharePoint Site Storage (Summary)
        self.sharepoint_card = TelemetryCard(
            title="SharePoint Site Storage",
            link_text="SharePoint Usage API",
            link_url="https://learn.microsoft.com/en-us/graph/api/reportroot-getsharepointsiteusagedetail",
            subtitle="Site collections, storage consumption, total files, and data types",
            paginate=False,
            column_weights=[3, 2],
            on_reload=lambda: self._reload_card(self._fetch_sharepoint_worker),
        )

        # 6b. Heavy Sites Inventory (Paginated)
        self.heavy_sites_card = TelemetryCard(
            title="Heavy Sites Inventory",
            link_text="SharePoint Usage API",
            link_url="https://learn.microsoft.com/en-us/graph/api/reportroot-getsharepointsiteusagedetail",
            subtitle="Top storage-consuming SharePoint sites across tenant",
            paginate=True,
            page_size=5,
            column_weights=[4, 4, 2],
            on_reload=lambda: self._reload_card(self._fetch_heavy_sites_worker),
        )

        # 7. OneDrive Accounts & Storage (180D)
        self.onedrive_card = TelemetryCard(
            title="OneDrive Accounts & Storage (180D)",
            link_text="OneDrive Usage API",
            link_url="https://learn.microsoft.com/en-us/graph/api/reportroot-getonedriveusageaccountdetail",
            subtitle="Personal accounts, storage used, active sync clients, and OneNote users",
            paginate=False,
            column_weights=[3, 2],
            on_reload=lambda: self._reload_card(self._fetch_onedrive_worker),
        )

        # 8. Microsoft Teams Activity (Summary)
        self.teams_card = TelemetryCard(
            title="Microsoft Teams Activity",
            link_text="Teams Activity API",
            link_url="https://learn.microsoft.com/en-us/graph/api/reportroot-getmsteamsuseractivityuserdetail",
            subtitle="Channels, messages, active collaboration users, meetings, and guests",
            paginate=False,
            column_weights=[3, 2],
            on_reload=lambda: self._reload_card(self._fetch_teams_worker),
        )

        # 9. Exchange Online Calendar Environment
        self.calendar_card = TelemetryCard(
            title="Exchange Online Calendar Environment",
            link_text="Calendar API Reference",
            link_url="https://learn.microsoft.com/en-us/graph/api/resources/event",
            subtitle="Room mailboxes, equipment resources, and calendar sharing policies",
            paginate=False,
            column_weights=[3, 2],
            on_reload=lambda: self._reload_card(self._fetch_calendar_worker),
        )

        # Register cards with base class for error status tracking
        self.register_cards(
            self.m365_apps_card,
            self.active_users_card,
            self.mailbox_card,
            self.email_clients_card,
            self.pst_card,
            self.sharepoint_card,
            self.heavy_sites_card,
            self.onedrive_card,
            self.teams_card,
            self.calendar_card,
        )

        # Initial Placeholder State
        self.placeholder = self._build_placeholder()

        self.content = ft.Container(
            expand=True,
            content=self.placeholder,
        )

    def _build_placeholder(self) -> ft.Control:
        """Renders initial placeholder with Icon, Description, and Fetch Data button."""
        return ft.Column(
            alignment=ft.MainAxisAlignment.CENTER,
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            spacing=16,
            controls=[
                ft.Container(
                    width=80,
                    height=80,
                    border_radius=40,
                    bgcolor="#E0EDFD",
                    alignment=ft.alignment.Alignment(0, 0),
                    content=ft.Icon(
                        ft.Icons.QUERY_STATS_ROUNDED,
                        size=38,
                        color=COLOR_PRIMARY,
                    ),
                ),
                ft.Text(
                    "App Usage, Adoption & Collaboration",
                    size=22,
                    weight=ft.FontWeight.BOLD,
                    color=COLOR_TEXT_PRIMARY,
                ),
                ft.Container(
                    width=540,
                    content=ft.Text(
                        "Analyze active usage volume, cross-product active user trends over 30/90/180 days, mailbox sizes, email clients, SharePoint & OneDrive storage, and Teams collaboration metrics.",
                        size=14,
                        color=COLOR_TEXT_SECONDARY,
                        text_align=ft.TextAlign.CENTER,
                    ),
                ),
                ft.Container(height=8),
                ft.ElevatedButton(
                    content=ft.Row(
                        tight=True,
                        spacing=6,
                        controls=[
                            ft.Icon(ft.Icons.SYNC_ROUNDED, size=16),
                            ft.Text("Fetch Data", size=14, weight=ft.FontWeight.W_500),
                        ],
                    ),
                    bgcolor="#0b57d0",
                    color=ft.Colors.WHITE,
                    height=42,
                    style=ft.ButtonStyle(
                        shape=ft.RoundedRectangleBorder(radius=8),
                        padding=ft.Padding(20, 10, 20, 10),
                    ),
                    on_click=lambda _: self.fetch_all_data(),
                ),
            ],
        )

    def _ensure_card_position(self, card: ft.Control, after_card: Optional[ft.Control] = None):
        """Ensures a card is present in cards_column in its deterministic order."""
        if card not in self.cards_column.controls:
            if after_card and after_card in self.cards_column.controls:
                idx = self.cards_column.controls.index(after_card) + 1
                self.cards_column.controls.insert(idx, card)
            else:
                self.cards_column.controls.append(card)

    def fetch_all_data(self):
        """Initiates concurrent data fetch with maximum 2 parallel worker threads."""
        if self.is_fetching:
            return

        self.is_fetching = True
        self.is_fetched = True
        self._notify_status("loading")

        self.cards_column.controls.clear()

        self.progress_bar = ft.ProgressBar(
            value=0.0,
            width=float("inf"),
            height=6,
            color="#15803D",
            bgcolor="#DCFCE7",
            border_radius=3,
        )
        self.progress_text = ft.Text(
            "Fetching App Usage, Adoption & Collaboration telemetry (0 of 9 completed)...",
            size=13,
            weight=ft.FontWeight.W_500,
            color="#166534",
        )
        self.progress_banner = ft.Container(
            bgcolor="#F0FDF4",
            border=ft.Border.all(1, "#BBF7D0"),
            border_radius=8,
            padding=ft.Padding(16, 12, 16, 12),
            margin=ft.Margin(0, 0, 18, 0),
            content=ft.Column(
                spacing=8,
                controls=[
                    self.progress_text,
                    self.progress_bar,
                ],
            ),
        )
        self.cards_column.controls.append(self.progress_banner)

        # Set individual cards to loading state
        self.m365_apps_card.set_loading("Fetching M365 app details...")
        self.active_users_card.set_loading("Fetching active users trend...")
        self.mailbox_card.set_loading("Fetching mailbox usage...")
        self.email_clients_card.set_loading("Fetching email client breakdown...")
        self.pst_card.set_loading("Searching PST files in SharePoint and OneDrive...")
        self.sharepoint_card.set_loading("Fetching SharePoint site storage & data types...")
        self.heavy_sites_card.set_loading("Fetching top heavy SharePoint sites...")
        self.onedrive_card.set_loading("Fetching OneDrive usage...")
        self.teams_card.set_loading("Fetching Teams activity...")
        self.calendar_card.set_loading("Fetching calendar telemetry...")

        self.content = self.cards_column
        try:
            self.update()
        except Exception:
            pass

        completed_count = 0
        total_tasks = 9

        def _track_task_wrapper(func):
            def _wrapped():
                nonlocal completed_count
                try:
                    func()
                finally:
                    completed_count += 1
                    pct = min(completed_count / total_tasks, 1.0)

                    def _update_progress():
                        self.progress_bar.value = pct
                        self.progress_text.value = (
                            f"Fetching App Usage, Adoption & Collaboration telemetry ({completed_count} of {total_tasks} completed)..."
                        )
                        try:
                            self.progress_banner.update()
                        except Exception:
                            pass

                    self._safe_run_on_ui(_update_progress)
            return _wrapped

        # Tasks to execute in worker pool (at max 2 concurrent threads)
        tasks = [
            ("M365Apps", _track_task_wrapper(self._fetch_m365_apps_worker)),
            ("ActiveUsers", _track_task_wrapper(self._fetch_active_users_worker)),
            ("Mailbox", _track_task_wrapper(self._fetch_mailbox_worker)),
            ("EmailClients", _track_task_wrapper(self._fetch_email_clients_worker)),
            ("PSTFiles", _track_task_wrapper(self._fetch_pst_worker)),
            ("SharePoint", _track_task_wrapper(self._fetch_sharepoint_worker)),
            ("OneDrive", _track_task_wrapper(self._fetch_onedrive_worker)),
            ("Teams", _track_task_wrapper(self._fetch_teams_worker)),
            ("Calendar", _track_task_wrapper(self._fetch_calendar_worker)),
        ]

        def _orchestrator():
            logger.info("Starting App Usage & Adoption orchestrator with ThreadPoolExecutor(max_workers=2)")
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [executor.submit(func) for _, func in tasks]
                for f in futures:
                    try:
                        f.result()
                    except Exception as e:
                        logger.error(f"Task failed with error: {e}")

            def _on_all_completed():
                self.is_fetching = False
                if self.progress_banner in self.cards_column.controls:
                    self.cards_column.controls.remove(self.progress_banner)
                try:
                    self.update()
                except Exception:
                    pass
                self._notify_status(self._check_completion_status())

            self._safe_run_on_ui(_on_all_completed)

        threading.Thread(target=_orchestrator, daemon=True).start()

    def _fetch_m365_apps_worker(self, is_reload: bool = False):
        """Fetches M365 Apps usage per client platform."""
        start_time = time.time()
        logger.info("Executing M365 Apps usage fetch task...")
        try:
            m365_data = run_m365_pipeline(self.client_id, self.secret, self.tenant)
            self.cached_data["m365_apps"] = m365_data
            columns = ["App / Platform", "Users Count"]
            rows = [[platform, f"{count:,}"] for platform, count in m365_data]
            elapsed = time.time() - start_time

            def _on_success():
                self.m365_apps_card.set_data(columns, rows, execution_time=elapsed)
                if self.m365_apps_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.m365_apps_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching M365 Apps Usage: {e}")
            err_msg = str(e)

            def _on_error():
                self.m365_apps_card.set_error(f"Failed to fetch M365 App Usage: {err_msg}")
                if self.m365_apps_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.m365_apps_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_active_users_worker(self, is_reload: bool = False):
        """Fetches Active Users trend across 30, 90, and 180-day periods."""
        start_time = time.time()
        logger.info("Executing Active Users Trend fetch task...")
        try:
            active_data = run_o365_pipeline(self.client_id, self.secret, self.tenant)
            self.cached_data["o365_usage"] = active_data
            columns = ["Workload / Product", "30-Day Active", "90-Day Active", "180-Day Active"]
            rows = [
                [product, f"{d30:,}", f"{d90:,}", f"{d180:,}"]
                for product, d30, d90, d180 in active_data
            ]
            elapsed = time.time() - start_time

            def _on_success():
                self.active_users_card.set_data(columns, rows, execution_time=elapsed)
                if self.active_users_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.active_users_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Active Users: {e}")
            err_msg = str(e)

            def _on_error():
                self.active_users_card.set_error(f"Failed to fetch Active Users Trend: {err_msg}")
                if self.active_users_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.active_users_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_mailbox_worker(self, is_reload: bool = False):
        """Fetches Mailbox usage summary."""
        start_time = time.time()
        logger.info("Executing Mailbox usage fetch task...")
        try:
            mb_data = run_mailbox_usage_pipeline(self.client_id, self.secret, self.tenant)
            self.cached_data["mailbox"] = mb_data
            
            s_count = mb_data.get("shared_mailboxes_count")
            s_count_str = f"{s_count:,} Shared Mailboxes" if s_count is not None else "Error/Unavailable"
            s_size_str = mb_data.get("shared_mailboxes_total_formatted", "Error/Unavailable")
            
            pf_count = mb_data.get("public_folders_count")
            pf_count_str = f"{pf_count:,} Public Folders" if pf_count is not None else "Error/Unavailable"
            
            mail_pf_count = mb_data.get("mail_public_folders_count")
            mail_pf_count_str = f"{mail_pf_count:,} Public Folders" if mail_pf_count is not None else "Error/Unavailable"
            
            pf_size_str = mb_data.get("public_folders_total_formatted", "Error/Unavailable")

            columns = ["Mailbox Metric Description", "Value / Measurement"]
            rows = [
                ["Total Mailboxes Analyzed", f"{mb_data.get('total_mailboxes', 0):,} Mailboxes"],
                ["Total Size of All Mailboxes", mb_data.get("total_storage_formatted", "0.00 Bytes")],
                ["Average Mailbox Size", mb_data.get("average_mailbox_size_formatted", "0.00 Bytes")],
                ["Total Number of Emails", f"{mb_data.get('total_emails', 0):,} Emails"],
                ["Average Emails per User", f"{mb_data.get('average_emails', 0.0):,.0f} Emails"],
                ["Shared Mailboxes Count", s_count_str],
                ["Total Shared Mailbox Size", s_size_str],
                ["Public Folders Count", pf_count_str],
                ["Mail-enabled Public Folders Count", mail_pf_count_str],
                ["Total Public Folder Size", pf_size_str],
            ]
            elapsed = time.time() - start_time

            def _on_success():
                self.mailbox_card.set_data(columns, rows, execution_time=elapsed)
                if self.mailbox_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.mailbox_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Mailbox usage: {e}")
            err_msg = str(e)

            def _on_error():
                self.mailbox_card.set_error(f"Failed to fetch Mailbox Usage: {err_msg}")
                if self.mailbox_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.mailbox_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_email_clients_worker(self, is_reload: bool = False):
        """Fetches Email Client Classification breakdown."""
        start_time = time.time()
        logger.info("Executing Email Client Classification fetch task...")
        try:
            result = run_email_client_usage_pipeline(self.client_id, self.secret, self.tenant)
            self.cached_data["email_clients"] = result.get("client_stats", {})
            stats = result.get("client_stats", {})
            columns = ["Client Category", "Active Users (180 Days)"]
            rows = [
                ["Outlook for Web (Browser)", f"{stats.get('browser_users', 0):,}"],
                ["Outlook for Windows", f"{stats.get('desktop_win', 0):,}"],
                ["Outlook for Mac", f"{stats.get('desktop_mac', 0):,}"],
                ["Apple Mail (Mac)", f"{stats.get('desktop_mail_mac', 0):,}"],
                ["Outlook for Mobile (iOS/Android)", f"{stats.get('mobile_outlook', 0):,}"],
                ["Other Mobile Email Apps", f"{stats.get('mobile_other', 0):,}"],
                ["POP3 Legacy Protocol", f"{stats.get('protocol_pop3', 0):,}"],
                ["IMAP4 Legacy Protocol", f"{stats.get('protocol_imap4', 0):,}"],
                ["SMTP Legacy Protocol", f"{stats.get('protocol_smtp', 0):,}"],
            ]
            elapsed = time.time() - start_time

            def _on_success():
                self.email_clients_card.set_data(columns, rows, execution_time=elapsed)
                if self.email_clients_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.email_clients_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Email Clients: {e}")
            err_msg = str(e)

            def _on_error():
                self.email_clients_card.set_error(f"Failed to fetch Email Client Classification: {err_msg}")
                if self.email_clients_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.email_clients_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_pst_worker(self, is_reload: bool = False):
        """Fetches PST files discovery in cloud locations."""
        start_time = time.time()
        logger.info("Executing PST discovery task...")
        try:
            result = run_pst_discovery_pipeline(self.client_id, self.secret, self.tenant)
            pst_err = result.get("pst_error")
            cloud_count = 0
            
            if pst_err:
                cloud_str = f"✖ Error: {pst_err}"
            else:
                pst_cloud = result.get("pst_cloud_data", {})
                cloud_bytes = 0
                if pst_cloud and "value" in pst_cloud:
                    for item in pst_cloud.get("value", []):
                        for hc in item.get("hitsContainers", []):
                            cloud_count += hc.get("total", 0)
                            for hit in hc.get("hits", []):
                                cloud_bytes += int(hit.get("resource", {}).get("size", 0))

                cloud_size_str = f" ({format_bytes(cloud_bytes)})" if cloud_bytes > 0 else ""
                cloud_str = f"{cloud_count:,} Files{cloud_size_str}" if cloud_count > 0 else "None Detected"

            columns = ["PST Storage Location", "Discovered File Count & Size"]
            rows = [
                ["Cloud (SharePoint & OneDrive)", cloud_str]
            ]
            elapsed = time.time() - start_time

            def _on_success():
                self.pst_card.set_data(columns, rows, execution_time=elapsed)
                if self.pst_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.pst_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching PST files: {e}")
            err_msg = str(e)

            def _on_error():
                self.pst_card.set_error(f"Failed to fetch PST Files: {err_msg}")
                if self.pst_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.pst_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_sharepoint_worker(self, is_reload: bool = False):
        """Fetches SharePoint site usage and data types summary metrics."""
        start_time = time.time()
        logger.info("Executing SharePoint Site Storage & Data Types fetch task...")
        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                usage_future = executor.submit(run_sharepoint_pipeline, self.client_id, self.secret, self.tenant)
                datatypes_future = executor.submit(run_sharepoint_data_types_pipeline, self.client_id, self.secret, self.tenant)
                
                usage_data = usage_future.result()
                datatypes_data = datatypes_future.result()

            combined_data = {**usage_data, **datatypes_data}
            self.cached_data["sharepoint"] = combined_data

            # Summary Card
            columns_sp = ["SharePoint Metric Description", "Value / Measurement"]
            rows_sp = [
                ["Total Sites Count", f"{combined_data.get('total_sites', 0):,} Sites"],
                ["Total Storage Used", combined_data.get("total_storage_formatted", "0.00 Bytes")],
                ["Total Files Stored", f"{combined_data.get('total_files', 0):,} Files"],
                ["Active Files (180D)", f"{combined_data.get('active_files', 0):,} Files ({combined_data.get('active_files_pct', 0.0):.1f}%)"],
                ["Document Libraries", f"{combined_data.get('Document Libraries', 0):,}"],
                ["Lists", f"{combined_data.get('Lists', 0):,}"],
                ["Web Pages", f"{combined_data.get('Web Pages', 0):,}"]
            ]

            # Heavy Sites Card (only populated during initial batch fetch if not reload)
            columns_heavy = ["URL", "Site ID", "Storage (GB)"]
            rows_heavy: List[List[Any]] = []
            if not is_reload:
                heavy_sites_list = combined_data.get("heavy_sites", [])
                for site in heavy_sites_list:
                    bytes_val = float(site.get("Storage Used (Byte)", 0))
                    gb_val = bytes_val / (1024 ** 3)
                    rows_heavy.append([
                        site.get("Site URL", "Unknown"),
                        site.get("Site Id", "-"),
                        f"{gb_val:.2f} GB"
                    ])

            elapsed = time.time() - start_time

            def _on_success():
                self.sharepoint_card.set_data(columns_sp, rows_sp, execution_time=elapsed)
                if not is_reload:
                    self.heavy_sites_card.set_data(columns_heavy, rows_heavy, execution_time=elapsed)

                if self.sharepoint_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.sharepoint_card)
                if not is_reload and self.heavy_sites_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.heavy_sites_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching SharePoint: {e}")
            err_msg = str(e)

            def _on_error():
                self.sharepoint_card.set_error(f"Failed to fetch SharePoint Site Storage: {err_msg}")
                if not is_reload:
                    self.heavy_sites_card.set_error(f"Failed to fetch Heavy Sites Inventory: {err_msg}")
                if self.sharepoint_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.sharepoint_card)
                if not is_reload and self.heavy_sites_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.heavy_sites_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_heavy_sites_worker(self, is_reload: bool = False):
        """Fetches Heavy Sites Inventory specifically."""
        start_time = time.time()
        logger.info("Executing Heavy Sites Inventory fetch task...")
        try:
            usage_data = run_sharepoint_pipeline(self.client_id, self.secret, self.tenant)
            columns_heavy = ["URL", "Site ID", "Storage (GB)"]
            rows_heavy: List[List[Any]] = []
            heavy_sites_list = usage_data.get("heavy_sites", [])
            for site in heavy_sites_list:
                bytes_val = float(site.get("Storage Used (Byte)", 0))
                gb_val = bytes_val / (1024 ** 3)
                rows_heavy.append([
                    site.get("Site URL", "Unknown"),
                    site.get("Site Id", "-"),
                    f"{gb_val:.2f} GB"
                ])

            elapsed = time.time() - start_time

            def _on_success():
                self.heavy_sites_card.set_data(columns_heavy, rows_heavy, execution_time=elapsed)
                if self.heavy_sites_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.heavy_sites_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Heavy Sites: {e}")
            err_msg = str(e)

            def _on_error():
                self.heavy_sites_card.set_error(f"Failed to fetch Heavy Sites Inventory: {err_msg}")
                if self.heavy_sites_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.heavy_sites_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_onedrive_worker(self, is_reload: bool = False):
        """Fetches OneDrive accounts and storage usage."""
        start_time = time.time()
        logger.info("Executing OneDrive usage fetch task...")
        try:
            od_data = run_onedrive_pipeline(self.client_id, self.secret, self.tenant)
            self.cached_data["onedrive"] = od_data
            columns = ["OneDrive Metric Description", "Value / Measurement"]
            rows = [
                ["Total User Accounts", f"{od_data.get('total_accounts', 0):,} Accounts"],
                ["Total Storage Used", od_data.get("total_storage_formatted", "0.00 Bytes")],
                ["Total Files Stored", f"{od_data.get('total_files', 0):,} Files"],
                ["Active Files (180D)", f"{od_data.get('active_files', 0):,} Files"],
                ["Active Sync Client Users", f"{od_data.get('sync_users', 0):,} ({od_data.get('sync_users_pct', 0.0):.1f}%)"],
                ["OneNote Active Users", f"{od_data.get('onenote_users', 0):,} Users"],
            ]
            elapsed = time.time() - start_time

            def _on_success():
                self.onedrive_card.set_data(columns, rows, execution_time=elapsed)
                if self.onedrive_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.onedrive_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching OneDrive: {e}")
            err_msg = str(e)

            def _on_error():
                self.onedrive_card.set_error(f"Failed to fetch OneDrive Usage: {err_msg}")
                if self.onedrive_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.onedrive_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_teams_worker(self, is_reload: bool = False):
        """Fetches Microsoft Teams activity telemetry."""
        start_time = time.time()
        logger.info("Executing Microsoft Teams fetch task...")
        try:
            csv_path = run_msteams_pipeline(self.client_id, self.secret, self.tenant)
            
            # Read and aggregate metrics from CSV
            total_teams = 0
            active_users = 0
            guests = 0
            active_channels = 0
            channel_messages = 0
            meetings_organized = 0

            if os.path.exists(csv_path):
                with open(csv_path, mode="r", encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        team_name = row.get("Team Name") or row.get("TeamName")
                        if team_name:
                            total_teams += 1
                            active_users += int(float(row.get("Active Users") or row.get("ActiveUsers") or 0))
                            guests += int(float(row.get("Guests") or 0))
                            active_channels += int(float(row.get("Active Channels") or row.get("ActiveChannels") or 0))
                            channel_messages += int(float(row.get("Channel Messages") or row.get("ChannelMessages") or 0))
                            meetings_organized += int(float(row.get("Meetings Organized") or row.get("MeetingsOrganized") or 0))

            avg_users = f"{(active_users / active_channels):.1f}" if active_channels > 0 else "0"

            columns = ["Teams Metric Description", "Value / Measurement"]
            rows = [
                ["Total Teams Count", f"{total_teams:,} Teams"],
                ["Total Active Channels (180 days)", f"{active_channels:,} Channels"],
                ["Total Channel Messages", f"{channel_messages:,} Messages"],
                ["Total Active Users (180 days)", f"{active_users:,} Users"],
                ["Average Users per Channel", avg_users],
                ["Total Meetings Organized", f"{meetings_organized:,} Meetings"],
                ["Total Guests", f"{guests:,} Guests"],
            ]
            elapsed = time.time() - start_time

            def _on_success():
                self.teams_card.set_data(columns, rows, execution_time=elapsed)
                if self.teams_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.teams_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Teams activity: {e}")
            err_msg = str(e)

            def _on_error():
                self.teams_card.set_error(f"Failed to fetch Microsoft Teams Activity: {err_msg}")
                if self.teams_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.teams_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_calendar_worker(self, is_reload: bool = False):
        """Fetches Exchange Online Calendar Environment telemetry."""
        start_time = time.time()
        logger.info("Executing Calendar telemetry fetch task...")
        try:
            cal_data = run_calendar_telemetry_pipeline(self.client_id, self.secret, self.tenant)
            
            rooms_err = cal_data.get("RoomsError")
            devs_err = cal_data.get("DevicesError")
            rooms_count = cal_data.get("RoomsCount", 0)
            equip_count = cal_data.get("EquipmentCount", 0)

            if rooms_err and devs_err:
                res_val = str(rooms_err)
            else:
                r_str = "Error" if rooms_err else str(rooms_count)
                e_str = "Error" if devs_err else str(equip_count)
                tot = "Error" if (rooms_err or devs_err) else str(rooms_count + equip_count)
                res_val = f"Total: {tot} ({r_str} Rooms, {e_str} Equipment)"

            reserve_val = cal_data.get("CanUsersReserveRooms")
            if isinstance(reserve_val, bool):
                reserve_val = "Yes" if reserve_val else "No"
            elif reserve_val is None:
                reserve_val = "No"

            att_val = cal_data.get("CanShareAttachments")
            if isinstance(att_val, bool):
                attachments_val = "Yes" if att_val else "No"
            elif att_val is None:
                attachments_val = "Yes"
            else:
                attachments_val = str(att_val)

            naming = cal_data.get("NamingConvention") or "None found"

            columns = ["Calendar Configuration / Metric", "Value / Configuration"]
            rows = [
                ["Room & Resource Reservation", str(reserve_val)],
                ["Calendar Resources", res_val],
                ["Resource Naming Convention", str(naming)],
                ["Calendar Attachments Enabled", str(attachments_val)],
            ]
            elapsed = time.time() - start_time

            def _on_success():
                self.calendar_card.set_data(columns, rows, execution_time=elapsed)
                if self.calendar_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.calendar_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Calendar telemetry: {e}")
            err_msg = str(e)

            def _on_error():
                self.calendar_card.set_error(f"Failed to fetch Calendar Telemetry: {err_msg}")
                if self.calendar_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.calendar_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)


