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

"""Usage & Adoption Dashboard view and layout skeleton with real-time system performance monitor."""

import time
import socket
import shutil
import psutil
import logging
import threading
from typing import Callable, Dict, List, Optional
import flet as ft
from flet_ui.components import (
    SectionStatus,
    create_section_status_indicator,
)
from flet_ui.styles import (
    COLOR_APP_BG,
    COLOR_BORDER,
    COLOR_ERROR,
    COLOR_HERO_BG,
    COLOR_PRIMARY,
    COLOR_SURFACE,
    COLOR_TEXT_PRIMARY,
    COLOR_TEXT_SECONDARY,
)
from flet_ui.views.sections import (
    IdentityLicensingView,
    AppUsageAdoptionView,
    SecurityComplianceGovernanceView,
    EcosystemIntegrationsAutomationView,
)

logger = logging.getLogger("M365TelemetryAsyncLogger.DashboardView")


class UsageAdoptionDashboardView(ft.Container):
    """Full-screen Usage and Adoption Telemetry Dashboard view with responsive left sidebar."""

    SECTIONS = [
        {
            "id": "identity",
            "title": "Identity & Licensing",
            "icon": ft.Icons.BADGE_OUTLINED,
            "description": "Inspect user licenses, assigned SKUs, active accounts, directory synchronization, and domain configurations across your tenant.",
        },
        {
            "id": "usage",
            "title": "App Usage, Adoption & Collaboration",
            "icon": ft.Icons.QUERY_STATS_ROUNDED,
            "description": "Analyze active usage volume, cross-product active user trends over 30/90/180 days, mailbox sizes, email clients, SharePoint & OneDrive storage, and Teams collaboration metrics.",
        },
        {
            "id": "security",
            "title": "Security, Compliance & Governance",
            "icon": ft.Icons.SECURITY_ROUNDED,
            "description": "Evaluate Purview retention, sensitivity labels, DLP policies, SITs, Intune compliance, and threat governance across your tenant.",
        },
        {
            "id": "ecosystem",
            "title": "Ecosystem, Integrations & Automation",
            "icon": ft.Icons.HUB_OUTLINED,
            "description": "Review Power Automate cloud flows, registered apps, enterprise single sign-on service principals, and mail flow connectors.",
        },
    ]

    def __init__(
        self,
        page: ft.Page,
        tenant: str = "",
        client: str = "",
        secret: str = "",
        on_back_to_hub: Optional[Callable[[], None]] = None,
        on_disconnect: Optional[Callable[[], None]] = None,
    ):
        super().__init__()
        self.page_ref = page
        self.tenant = tenant
        self.client = client
        self.secret = secret
        self.on_back_to_hub = on_back_to_hub
        self.on_disconnect = on_disconnect

        self.expand = True
        self.bgcolor = COLOR_APP_BG
        # Generous breathing space on left and right margins
        self.padding = ft.Padding(36, 18, 36, 22)

        self.selected_index = 0
        self._stop_metrics = False
        self.section_statuses: Dict[int, str] = {
            0: SectionStatus.IDLE,
            1: SectionStatus.IDLE,
            2: SectionStatus.IDLE,
            3: SectionStatus.IDLE,
        }

        # System performance metric value controls
        self.ram_text = ft.Text("Loading...", size=12, weight=ft.FontWeight.W_600, color=COLOR_TEXT_PRIMARY)
        self.cpu_text = ft.Text("Loading...", size=12, weight=ft.FontWeight.W_600, color=COLOR_TEXT_PRIMARY)
        self.disk_text = ft.Text("Loading...", size=12, weight=ft.FontWeight.W_600, color=COLOR_TEXT_PRIMARY)

        # File picker for exporting PDF
        self.save_pdf_dialog = ft.FilePicker(on_result=self._on_save_pdf_result)

        # Section View Instances (Lazy/Persistent)
        self.identity_view = IdentityLicensingView(
            page=self.page_ref,
            tenant=self.tenant,
            client=self.client,
            secret=self.secret,
            on_status_change=lambda status: self._on_section_status_changed(0, status),
        )
        self.usage_view = AppUsageAdoptionView(
            page=self.page_ref,
            tenant=self.tenant,
            client=self.client,
            secret=self.secret,
            on_status_change=lambda status: self._on_section_status_changed(1, status),
        )
        self.security_view = SecurityComplianceGovernanceView(
            page=self.page_ref,
            tenant=self.tenant,
            client=self.client,
            secret=self.secret,
            on_status_change=lambda status: self._on_section_status_changed(2, status),
        )
        self.ecosystem_view = EcosystemIntegrationsAutomationView(
            page=self.page_ref,
            tenant=self.tenant,
            client=self.client,
            secret=self.secret,
            on_status_change=lambda status: self._on_section_status_changed(3, status),
        )

        self.page_ref.overlay.append(self.save_pdf_dialog)

        # Build UI Components
        self.header = self._build_header()
        self.sidebar = self._build_sidebar()
        self.content_area = self._build_content_area()

        self.content = ft.Column(
            expand=True,
            spacing=18,
            controls=[
                self.header,
                ft.Row(
                    expand=True,
                    spacing=24,
                    vertical_alignment=ft.CrossAxisAlignment.STRETCH,
                    controls=[
                        self.sidebar,
                        self.content_area,
                    ],
                ),
            ],
        )

        # Start real-time 5-second system performance monitor
        self._start_metrics_monitor()

    def _safe_run_on_ui(self, callback: Callable):
        """Dispatches UI updates safely on the event loop."""
        try:
            loop = getattr(self.page_ref, "loop", None)
            if loop and callable(getattr(loop, "is_running", None)) and loop.is_running() and not isinstance(loop, ft.Page):
                loop.call_soon_threadsafe(callback)
            else:
                callback()
        except Exception:
            callback()

    def _start_metrics_monitor(self):
        """Starts real-time system metrics monitoring thread updating every 5 seconds."""
        self._stop_metrics = False

        # Prime CPU percent measurement
        psutil.cpu_percent(interval=None)

        def _monitor_loop():
            import os
            root_path = os.path.abspath(os.sep)
            while not self._stop_metrics:
                time.sleep(5)
                try:
                    ram_val = f"{psutil.virtual_memory().percent:.1f}%"
                    cpu_val = f"{psutil.cpu_percent(interval=None):.1f}%"
                    free_gb = shutil.disk_usage(root_path).free / (10**9)
                    disk_val = f"{free_gb:.1f} GB"

                    def _update_ui():
                        self.ram_text.value = ram_val
                        self.cpu_text.value = cpu_val
                        self.disk_text.value = disk_val
                        try:
                            self.system_metrics_box.update()
                        except Exception:
                            pass

                    self._safe_run_on_ui(_update_ui)
                except Exception as e:
                    logger.debug(f"Error reading system metrics: {e}")

        threading.Thread(target=_monitor_loop, daemon=True).start()

    def _build_header(self) -> ft.Container:
        """Constructs the top header bar with Back to Hub and Export data buttons."""
        return ft.Container(
            content=ft.Row(
                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                controls=[
                    # Left: Back button + Title
                    ft.Row(
                        spacing=12,
                        vertical_alignment=ft.CrossAxisAlignment.CENTER,
                        controls=[
                            ft.IconButton(
                                icon=ft.Icons.ARROW_BACK_ROUNDED,
                                icon_size=26,
                                icon_color=COLOR_TEXT_PRIMARY,
                                tooltip="Back to Hub",
                                on_click=lambda _: self._handle_back_to_hub(),
                            ),
                            ft.Text(
                                "Usage and adoption",
                                size=24,
                                weight=ft.FontWeight.W_500,
                                color=COLOR_TEXT_PRIMARY,
                            ),
                        ],
                    ),
                    # Right: Export data button
                    ft.TextButton(
                        content=ft.Row(
                            tight=True,
                            spacing=8,
                            controls=[
                                ft.Icon(ft.Icons.DOWNLOAD_ROUNDED, size=20, color=COLOR_TEXT_PRIMARY),
                                ft.Text("Export data", size=16, weight=ft.FontWeight.W_500, color=COLOR_TEXT_PRIMARY),
                            ],
                        ),
                        on_click=lambda _: self._handle_export_data(),
                        style=ft.ButtonStyle(
                            padding=ft.Padding(16, 8, 16, 8),
                        ),
                    ),
                ],
            ),
            padding=ft.Padding(0, 4, 0, 8),
        )

    def _build_sidebar(self) -> ft.Container:
        """Constructs the non-collapsible left navigation panel with system performance card."""
        self.nav_items_column = ft.Column(
            spacing=6,
            controls=[self._create_nav_item(i, sec) for i, sec in enumerate(self.SECTIONS)],
        )

        # Nav items container with top breathing room from heading
        nav_container = ft.Container(
            content=self.nav_items_column,
            padding=ft.Padding(0, 16, 0, 0),
        )

        def create_metric_row(label: str, value_control: ft.Text) -> ft.Row:
            return ft.Row(
                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                controls=[
                    ft.Text(label, size=12, color=COLOR_TEXT_SECONDARY),
                    value_control,
                ],
            )

        # Shifted upwards with bottom margin for visibility
        self.system_metrics_box = ft.Container(
            bgcolor=COLOR_SURFACE,
            border=ft.Border.all(1, COLOR_BORDER),
            border_radius=12,
            padding=14,
            margin=ft.Margin(0, 0, 0, 16),
            content=ft.Column(
                spacing=10,
                tight=True,
                controls=[
                    ft.Text(
                        "System Performance",
                        size=12,
                        weight=ft.FontWeight.BOLD,
                        color=COLOR_TEXT_PRIMARY,
                    ),
                    ft.Divider(height=1, color=COLOR_BORDER),
                    create_metric_row("RAM utilization", self.ram_text),
                    create_metric_row("CPU utilization", self.cpu_text),
                    create_metric_row("Available Disk Space", self.disk_text),
                ],
            ),
        )

        sidebar_content = ft.Column(
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            controls=[
                nav_container,
                self.system_metrics_box,
            ],
        )

        return ft.Container(
            width=280,
            content=sidebar_content,
        )

    def _on_section_status_changed(self, index: int, status: str):
        """Callback invoked when a section's fetch status changes."""
        self.section_statuses[index] = status

        def _update_nav():
            self.nav_items_column.controls = [
                self._create_nav_item(i, sec) for i, sec in enumerate(self.SECTIONS)
            ]
            try:
                self.nav_items_column.update()
            except Exception:
                try:
                    self.sidebar.update()
                except Exception:
                    try:
                        self.update()
                    except Exception:
                        pass

        self._safe_run_on_ui(_update_nav)

    def _create_nav_item(self, index: int, section: Dict[str, any]) -> ft.Container:
        """Creates an individual navigation item with active / hover highlights and status icon."""
        is_selected = index == self.selected_index
        status = self.section_statuses.get(index, SectionStatus.IDLE)

        bgcolor = COLOR_HERO_BG if is_selected else "transparent"
        text_color = COLOR_PRIMARY if is_selected else COLOR_TEXT_PRIMARY
        icon_color = COLOR_PRIMARY if is_selected else COLOR_TEXT_SECONDARY
        font_weight = ft.FontWeight.BOLD if is_selected else ft.FontWeight.W_500

        # Status indicator control on the right end
        status_control = create_section_status_indicator(status, is_selected=is_selected)

        row_controls = [
            ft.Icon(section["icon"], size=18, color=icon_color),
            ft.Text(
                section["title"],
                size=13,
                weight=font_weight,
                color=text_color,
                expand=True,
            ),
        ]
        if status_control is not None:
            row_controls.append(status_control)

        return ft.Container(
            border_radius=10,
            bgcolor=bgcolor,
            padding=ft.Padding(12, 10, 12, 10),
            ink=True,
            on_click=lambda _: self._select_section(index),
            content=ft.Row(
                spacing=10,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                controls=row_controls,
            ),
        )

    def _build_content_area(self) -> ft.Container:
        """Constructs the right main content card for the selected section."""
        self.main_card = ft.Container(
            bgcolor=COLOR_SURFACE,
            border=ft.Border.all(1, COLOR_BORDER),
            border_radius=16,
            expand=True,
            padding=20,
            alignment=ft.alignment.Alignment(0, 0),
            content=self._render_current_section(),
        )
        return self.main_card

    def _render_current_section(self) -> ft.Control:
        """Renders either the modular section view or a placeholder."""
        if self.selected_index == 0:
            return self.identity_view
        elif self.selected_index == 1:
            return self.usage_view
        elif self.selected_index == 2:
            return self.security_view
        elif self.selected_index == 3:
            return self.ecosystem_view
        return self._render_active_section_placeholder()

    def _render_active_section_placeholder(self) -> ft.Control:
        """Renders the section placeholder with icon, description, and Fetch Data button."""
        current_sec = self.SECTIONS[self.selected_index]

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
                        current_sec["icon"],
                        size=38,
                        color=COLOR_PRIMARY,
                    ),
                ),
                ft.Text(
                    current_sec["title"],
                    size=22,
                    weight=ft.FontWeight.BOLD,
                    color=COLOR_TEXT_PRIMARY,
                ),
                ft.Container(
                    width=500,
                    content=ft.Text(
                        current_sec["description"],
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
                    on_click=lambda _: self._handle_fetch_data(),
                ),
            ],
        )

    def _select_section(self, index: int):
        """Updates the selected section index and refreshes UI."""
        if index != self.selected_index:
            self.selected_index = index
            # Update sidebar active highlights
            self.nav_items_column.controls = [
                self._create_nav_item(i, sec) for i, sec in enumerate(self.SECTIONS)
            ]
            # Update main card content
            self.main_card.content = self._render_current_section()
            try:
                self.update()
            except Exception:
                pass

    def _handle_fetch_data(self):
        """Triggers data fetch for the active section placeholder."""
        if self.selected_index == 0:
            self.identity_view.fetch_all_data()
        elif self.selected_index == 1:
            self.usage_view.fetch_all_data()
        elif self.selected_index == 2:
            self.security_view.fetch_all_data()
        elif self.selected_index == 3:
            self.ecosystem_view.fetch_all_data()

    def stop_metrics(self):
        """Stops the background real-time system metrics monitoring thread."""
        self._stop_metrics = True

    def will_unmount(self):
        """Lifecycle hook invoked when the control is unmounted from the page."""
        self.stop_metrics()

    def _handle_back_to_hub(self):
        """Navigates back to the main Hub."""
        self.stop_metrics()
        if self.on_back_to_hub:
            self.on_back_to_hub()

    def _handle_disconnect(self):
        """Disconnects active tenant and redirects to Auth screen."""
        self.stop_metrics()
        if self.on_disconnect:
            self.on_disconnect()

    def _handle_export_data(self):
        """Exports gathered telemetry data."""
        self.save_pdf_dialog.save_file(
            dialog_title="Save PDF Report",
            file_name="M365_Telemetry_Report.pdf",
            allowed_extensions=["pdf"],
        )

    def _collect_all_telemetry_data(self) -> dict:
        import os
        import csv
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Get path to telemetry/reports/tenant_client
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(script_dir)))
        reports_dir = os.path.join(repo_root, "telemetry", "reports", f"{self.tenant}_{self.client}")
        
        def load_csv(filename):
            path = os.path.join(reports_dir, filename)
            if not os.path.exists(path):
                return []
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    return list(csv.DictReader(f))
            except Exception:
                return []
        
        identity_data = getattr(self.identity_view, "cached_data", {})
        usage_data = getattr(self.usage_view, "cached_data", {})
        security_data = getattr(self.security_view, "cached_data", {})
        eco_data = getattr(self.ecosystem_view, "cached_data", {})
        
        # Build Intune Data
        intune_data = security_data.get("intune", {})
        if not intune_data.get("mobile_apps"):
            intune_data["mobile_apps"] = [r.get("displayName") for r in load_csv("intune_apps.csv") if r.get("displayName")]
        if not intune_data.get("detected_apps"):
            intune_data["detected_apps"] = load_csv("intune_detected_apps.csv")
        if not intune_data.get("managed_devices"):
            intune_data["managed_devices"] = load_csv("intune_managed_devices.csv")
        if not intune_data.get("vc_devices"):
            intune_data["vc_devices"] = load_csv("intune_vc_devices.csv")
        if not intune_data.get("android_compliance"):
            intune_data["android_compliance"] = load_csv("intune_android_compliance.csv")
        if not intune_data.get("ios_compliance"):
            intune_data["ios_compliance"] = load_csv("intune_ios_compliance.csv")
        if not intune_data.get("mdm_policies"):
            intune_data["mdm_policies"] = load_csv("intune_mdm_policies.csv")
        if not intune_data.get("byod_configs"):
            intune_data["byod_configs"] = load_csv("intune_byod_configs.csv")
            
        combined_data = {
            "tenant_id": self.tenant,
            "skus": identity_data.get("skus", []),
            "directory": {
                "organization": identity_data.get("organization", []),
                "domains": identity_data.get("domains", []),
                "user_creation_logs": identity_data.get("user_creation_logs", []),
                "provisioning_logs": identity_data.get("provisioning_logs", []),
                "group_counts": identity_data.get("group_counts", {}),
                "user_counts": identity_data.get("user_counts", {})
            },
            "o365_usage": usage_data.get("o365_usage", []),
            "o365_trend": usage_data.get("o365_usage", []), # Used as trend if formatted right
            "m365_apps": usage_data.get("m365_apps", []),
            "mailbox": usage_data.get("mailbox", {}),
            "calendar": usage_data.get("calendar", {}),
            "mail_security": security_data.get("mail_security", {}),
            "connectors": security_data.get("connectors", []),
            "email_clients": usage_data.get("email_clients", {}),
            "pst_files": usage_data.get("pst_files", {}),
            "exchange_connectors": eco_data.get("exchange_connectors", []),
            "transport_rules": security_data.get("transport_rules", []),
            "sharepoint": usage_data.get("sharepoint", {}),
            "onedrive": usage_data.get("onedrive", {}),
            "devices_apps": usage_data.get("devices_apps", {}),
            "intune": intune_data,
            "network_security": {
                "filtering_policies": load_csv("network_filtering_policies.csv"),
                "conditional_access": load_csv("network_conditional_access.csv"),
                "firewall_policies": load_csv("network_firewall_policies.csv")
            },
            "security_labels": security_data.get("security_labels", []),
            "retention_policies": security_data.get("retention_policies", []),
            "dlp_policies": security_data.get("dlp_policies", []),
            "sensitive_info_types": {
                "standard": security_data.get("sensitive_info_types", []),
                "custom": load_csv("custom_sits.csv"),
                "edm": load_csv("edm_schemas.csv")
            },
            "service_principals_sso": security_data.get("service_principals_sso", []),
            "conditional_access": security_data.get("conditional_access", []),
            "ediscovery_cases": load_csv("ediscovery_cases.csv"),
            "power_automate": eco_data.get("power_automate", {}),
            "msteams_activity": load_csv("msteams_activity.csv")
        }
            
        return combined_data

    def _on_save_pdf_result(self, e: ft.FilePickerResultEvent):
        if not e.path:
            return  # User canceled
            
        snack = ft.SnackBar(
            content=ft.Text("Preparing and exporting telemetry data..."),
            open=True,
        )
        if hasattr(self.page_ref, "overlay"):
            self.page_ref.overlay.append(snack)
        try:
            self.page_ref.update()
        except Exception:
            pass
            
        try:
            from telemetry.pdf_report import generate_pdf_report
            data = self._collect_all_telemetry_data()
            generate_pdf_report(data, e.path)
            
            snack.content = ft.Text(f"PDF successfully exported to {e.path}")
            snack.bgcolor = ft.colors.GREEN_800
        except Exception as ex:
            import logging
            logging.error(f"Failed to generate PDF: {ex}")
            snack.content = ft.Text(f"Failed to generate PDF: {ex}")
            snack.bgcolor = ft.colors.RED_800
            
        try:
            self.page_ref.update()
        except Exception:
            pass
