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

"""Security, Compliance & Governance section view implementation for Flet UI."""

import os
import csv
import time
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional
import flet as ft

from core.graph.security.retention_policies import run_retention_policies_pipeline
from core.graph.security.dlp_policies import run_dlp_policies_pipeline
from core.graph.security.sensitivity_labels import run_sensitivity_labels_pipeline
from core.graph.security.sensitive_info_types import run_sensitive_info_types_pipeline
from core.graph.security.legal_holds import run_legal_holds_pipeline
from core.graph.network_security.conditional_access import run_conditional_access_pipeline
from core.graph.network_security.filtering import run_filtering_pipeline
from core.graph.network_security.firewall import run_firewall_pipeline
from core.graph.exchange.mail_security import run_mail_security_pipeline
from core.graph.exchange.transport_rules import run_transport_rules_pipeline
from core.graph.intune.device_compliance import run_device_compliance_pipeline
from core.graph.intune.byod_configs import run_byod_configs_pipeline
from core.graph.intune.managed_devices import run_managed_devices_pipeline
from core.graph.intune.mobile_apps import run_mobile_apps_pipeline
from core.graph.intune.detected_apps import run_detected_apps_pipeline
from core.graph.intune.device_configs import run_device_configs_pipeline
from core.graph.intune.mdm_policies import run_mdm_policies_pipeline
from flet_ui.views.telemetry.sections.base_section_view import BaseSectionView
from flet_ui.components.telemetry_card import TelemetryCard
from flet_ui.styles import (
    COLOR_PRIMARY,
    COLOR_SURFACE,
    COLOR_TEXT_PRIMARY,
    COLOR_TEXT_SECONDARY,
)

logger = logging.getLogger("M365TelemetryAsyncLogger.SecurityComplianceView")


class SecurityComplianceGovernanceView(BaseSectionView):
    """View rendering all Security, Compliance & Governance telemetry cards with maximum 2 concurrency."""

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

        # Card container with vertical scrolling
        self.cached_data = {}
        self.cards_column = ft.Column(
            expand=True,
            spacing=20,
            scroll=ft.ScrollMode.ADAPTIVE,
        )

        # 1. Sensitivity Labels (Paginated listing - 7 columns)
        self.sensitivity_card = TelemetryCard(
            title="Sensitivity Labels",
            link_text="Open Microsoft Purview ↗",
            link_url="https://purview.microsoft.com/informationprotection/informationprotectionlabels/sensitivitylabels",
            subtitle="Information protection classifications, encryption, and priority",
            paginate=True,
            page_size=5,
            column_weights=[3, 3, 2, 2, 1, 2, 1],
            on_reload=lambda: self._reload_card(self._fetch_sensitivity_worker),
        )

        # 2. Retention Compliance Policies (Paginated listing - 5 columns)
        self.retention_card = TelemetryCard(
            title="Retention Compliance Policies",
            link_text="Open Microsoft Purview ↗",
            link_url="https://purview.microsoft.com/datalifecyclemanagement/retention",
            subtitle="Data retention rules and deletion schedules across Exchange, SharePoint, OneDrive, and Teams",
            paginate=True,
            page_size=5,
            column_weights=[3, 3, 2, 2, 1],
            on_reload=lambda: self._reload_card(self._fetch_retention_worker),
        )

        # 3. Data Loss Prevention (DLP) Policies (Paginated listing - 6 columns)
        self.dlp_card = TelemetryCard(
            title="Data Loss Prevention (DLP) Policies",
            link_text="Open Purview DLP Portal ↗",
            link_url="https://purview.microsoft.com/datalossprevention/policies",
            subtitle="DLP protection rules, enforcement mode, target workloads, and incident actions",
            paginate=True,
            page_size=5,
            column_weights=[3, 1, 3, 1, 1, 2],
            on_reload=lambda: self._reload_card(self._fetch_dlp_worker),
        )

        # 4. Sensitive Information Types (SIT) (Paginated listing - 4 columns)
        self.sit_card = TelemetryCard(
            title="Sensitive Information Types (SIT)",
            link_text="Open Microsoft Purview ↗",
            link_url="https://purview.microsoft.com/datalossprevention/informationprotection/sensitiveinfotypes",
            subtitle="Built-in sensitive data matchers, confidence levels, and descriptions",
            paginate=True,
            page_size=5,
            column_weights=[3, 2, 2, 4],
            on_reload=lambda: self._reload_card(self._fetch_sits_worker),
        )

        # 5. Custom Sensitive Information Types (Paginated listing - 3 columns)
        self.custom_sit_card = TelemetryCard(
            title="Custom Sensitive Information Types",
            link_text="Open Microsoft Purview ↗",
            link_url="https://purview.microsoft.com/datalossprevention/informationprotection/sensitiveinfotypes",
            subtitle="Organization-defined custom rule packages and regex pattern matchers",
            paginate=True,
            page_size=5,
            column_weights=[3, 2, 4],
            on_reload=lambda: self._reload_card(self._fetch_sits_worker),
        )

        # 6. Exact Data Match (EDM) Schemas (Paginated listing - 3 columns)
        self.edm_schemas_card = TelemetryCard(
            title="Exact Data Match (EDM) Schemas",
            link_text="Open Microsoft Purview ↗",
            link_url="https://purview.microsoft.com/datalossprevention/informationprotection/exactdatamatch",
            subtitle="Exact Data Match classification schemas and custom data store references",
            paginate=True,
            page_size=5,
            column_weights=[3, 4, 3],
            on_reload=lambda: self._reload_card(self._fetch_sits_worker),
        )

        # 7. Microsoft Purview eDiscovery Cases (Paginated listing - 4 columns)
        self.ediscovery_card = TelemetryCard(
            title="Microsoft Purview eDiscovery Cases",
            link_text="Open Microsoft Purview ↗",
            link_url="https://purview.microsoft.com/ediscovery/casespage",
            subtitle="Purview eDiscovery investigations, active cases, and legal hold boundaries",
            paginate=True,
            page_size=5,
            column_weights=[3, 2, 2, 2],
            on_reload=lambda: self._reload_card(self._fetch_ediscovery_worker),
        )

        # 8. Mailboxes on Legal Hold (Paginated listing - 3 columns)
        self.legal_holds_card = TelemetryCard(
            title="Mailboxes on Legal Hold",
            link_text="Open Exchange Admin Center ↗",
            link_url="https://admin.exchange.microsoft.com/#/mailboxes",
            subtitle="Exchange Online user and shared mailboxes placed on Litigation Holds, eDiscovery holds, or retention policies",
            paginate=True,
            page_size=5,
            column_weights=[1, 1, 1],
            on_reload=lambda: self._reload_card(self._fetch_legal_holds_worker),
        )

        # 9. Conditional Access Policies (Paginated listing - 5 columns)
        self.ca_card = TelemetryCard(
            title="Conditional Access Policies",
            link_text="Open Azure Portal ↗",
            link_url="https://portal.azure.com/#view/Microsoft_AAD_IAM/ConditionalAccessBlade/~/Policies",
            subtitle="Zero-Trust access policies, target scopes, and enforced grant controls",
            paginate=True,
            page_size=5,
            column_weights=[3, 1, 2, 2, 2],
            on_reload=lambda: self._reload_card(self._fetch_ca_worker),
        )

        # 8. Global Secure Access Filtering Policies (Paginated listing - 5 columns)
        self.filtering_card = TelemetryCard(
            title="Global Secure Access Filtering Policies",
            link_text="Filtering API ↗",
            link_url="https://learn.microsoft.com/en-us/entra/global-secure-access/",
            subtitle="Internet and Private Access web filtering security profiles",
            paginate=True,
            page_size=5,
            column_weights=[3, 3, 1, 1, 1],
            on_reload=lambda: self._reload_card(self._fetch_network_security_worker),
        )

        # 9. Firewall and Proxy Configurations (Paginated listing - 4 columns)
        self.firewall_card = TelemetryCard(
            title="Firewall and Proxy Configurations",
            link_text="Firewall API ↗",
            link_url="https://learn.microsoft.com/en-us/mem/intune/protect/endpoint-security-firewall-policy",
            subtitle="Intune endpoint security firewall rules and network proxy configurations",
            paginate=True,
            page_size=5,
            column_weights=[3, 2, 1, 1],
            on_reload=lambda: self._reload_card(self._fetch_network_security_worker),
        )

        # 10. Exchange Mail Security & SKUs (Summary - 3 columns)
        self.mail_security_card = TelemetryCard(
            title="Exchange Mail Security & SKUs",
            link_text="Defender Portal ↗",
            link_url="https://security.microsoft.com/antispam",
            subtitle="Defender for Office 365, Exchange Online Protection (EOP), and protected seat counts",
            paginate=False,
            column_weights=[3, 3, 2],
            on_reload=lambda: self._reload_card(self._fetch_mail_security_worker),
        )

        # 11. Encryption Key Management (Summary - 3 columns)
        self.encryption_card = TelemetryCard(
            title="Encryption Key Management",
            link_text="Customer Key Overview ↗",
            link_url="https://learn.microsoft.com/en-us/purview/customer-key-overview",
            subtitle="Microsoft 365 Customer Key policies and Exchange data encryption posture",
            paginate=False,
            column_weights=[3, 2, 2],
            on_reload=lambda: self._reload_card(self._fetch_mail_security_worker),
        )

        # 12. Exchange Transport Rules (Paginated listing - 5 columns)
        self.transport_rules_card = TelemetryCard(
            title="Exchange Transport Rules",
            link_text="Exchange Admin Center ↗",
            link_url="https://admin.exchange.microsoft.com/#/transportrules",
            subtitle="Exchange mail flow rules, message encryption, disclaimers, and routing logic",
            paginate=True,
            page_size=5,
            column_weights=[3, 1, 1, 1, 4],
            on_reload=lambda: self._reload_card(self._fetch_transport_rules_worker),
        )

        # 13. Managed Mobile Applications (Paginated listing - 5 columns)
        self.mobile_apps_card = TelemetryCard(
            title="Managed Mobile Applications",
            link_text="Open Intune Admin Center ↗",
            link_url="https://intune.microsoft.com/#view/Microsoft_Intune_DeviceSettings/AppsMenu/~/allApps",
            subtitle="Enterprise application management, platform targets, and license assignment",
            paginate=True,
            page_size=5,
            column_weights=[3, 2, 2, 1, 1],
            on_reload=lambda: self._reload_card(self._fetch_mobile_detected_apps_worker),
        )

        # 14. Detected Applications (Paginated listing - 4 columns)
        self.detected_apps_card = TelemetryCard(
            title="Detected Applications",
            link_text="Open Intune Admin Center ↗",
            link_url="https://intune.microsoft.com/#view/Microsoft_Intune_DeviceSettings/AppsMenu/~/detectedApps",
            subtitle="Discovered software inventory and client versions installed on managed endpoints",
            paginate=True,
            page_size=5,
            column_weights=[3, 2, 2, 2],
            on_reload=lambda: self._reload_card(self._fetch_mobile_detected_apps_worker),
        )

        # 15. Intune Managed Devices (Paginated listing - 5 columns)
        self.managed_devices_card = TelemetryCard(
            title="Intune Managed Devices",
            link_text="Open Intune Admin Center ↗",
            link_url="https://intune.microsoft.com/#view/Microsoft_Intune_DeviceSettings/DevicesMenu/~/allDevices",
            subtitle="Hardware inventory, operating system versions, and compliance status",
            paginate=True,
            page_size=5,
            column_weights=[3, 2, 2, 2, 2],
            on_reload=lambda: self._reload_card(self._fetch_managed_and_vc_devices_worker),
        )

        # 16. Video Conferencing (VC) Devices (Paginated listing - 7 columns)
        self.vc_devices_card = TelemetryCard(
            title="Video Conferencing (VC) Devices",
            link_text="Open Intune Admin Center ↗",
            link_url="https://intune.microsoft.com/#view/Microsoft_Intune_DeviceSettings/DevicesMenu/~/allDevices",
            subtitle="Dedicated meeting room hardware and resource devices filtered from directory",
            footnote="* Devices associated with room mailboxes or matching video conferencing hardware indicators are listed here. This serves as an estimated projection based on directory signals and is not a comprehensive, exact list of physical VC devices.",
            paginate=True,
            page_size=5,
            column_weights=[2, 2, 2, 1, 1, 1, 1],
            on_reload=lambda: self._reload_card(self._fetch_managed_and_vc_devices_worker),
        )

        # 17. Device Configurations Summary (Paginated listing - 3 columns)
        self.device_configs_card = TelemetryCard(
            title="Device Configurations",
            link_text="Open Intune Admin Center ↗",
            link_url="https://intune.microsoft.com/#view/Microsoft_Intune_DeviceSettings/DevicesMenu/~/configuration",
            subtitle="Device configuration profiles and policy types breakdown by platform",
            paginate=True,
            page_size=5,
            column_weights=[2, 3, 1],
            on_reload=lambda: self._reload_card(self._fetch_device_configs_and_byod_worker),
        )

        # 18. Mobile BYOD Configurations (Paginated listing - 7 columns)
        self.byod_configs_card = TelemetryCard(
            title="Mobile BYOD Configurations",
            link_text="Open Intune Admin Center ↗",
            link_url="https://intune.microsoft.com/#view/Microsoft_Intune_Enrollment/EnrollmentMenu/~/deviceEnrollment",
            subtitle="Device enrollment restrictions and personal platform enrollment controls",
            paginate=True,
            page_size=5,
            column_weights=[2, 3, 1, 2, 3, 3, 3],
            on_reload=lambda: self._reload_card(self._fetch_device_configs_and_byod_worker),
        )

        # 19. Android Devices Compliance Policies (Paginated listing - 5 columns)
        self.android_compliance_card = TelemetryCard(
            title="Android Devices Compliance Policies",
            link_text="Open Intune Admin Center ↗",
            link_url="https://intune.microsoft.com/#view/Microsoft_Intune_DeviceSettings/DevicesMenu/~/compliance",
            subtitle="Android Enterprise compliance criteria, passcode rules, and minimum OS constraints",
            paginate=True,
            page_size=5,
            column_weights=[3, 3, 2, 2, 1],
            on_reload=lambda: self._reload_card(self._fetch_compliance_and_mdm_worker),
        )

        # 20. iOS Devices Compliance Policies (Paginated listing - 5 columns)
        self.ios_compliance_card = TelemetryCard(
            title="iOS Devices Compliance Policies",
            link_text="Open Intune Admin Center ↗",
            link_url="https://intune.microsoft.com/#view/Microsoft_Intune_DeviceSettings/DevicesMenu/~/compliance",
            subtitle="iOS/iPadOS device compliance benchmarks, jailbreak detection, and OS version requirements",
            paginate=True,
            page_size=5,
            column_weights=[3, 3, 2, 2, 1],
            on_reload=lambda: self._reload_card(self._fetch_compliance_and_mdm_worker),
        )

        # 21. Mobile Device Management Policies (Paginated listing - 6 columns)
        self.mdm_policies_card = TelemetryCard(
            title="Mobile Device Management Policies",
            link_text="Open Azure Portal ↗",
            link_url="https://portal.azure.com/#view/Microsoft_AAD_IAM/ActiveDirectoryMenuBlade/~/Mobility",
            subtitle="Microsoft Entra automatic MDM enrollment authorities and terms of use scopes",
            paginate=True,
            page_size=5,
            column_weights=[2, 3, 2, 3, 3, 3],
            on_reload=lambda: self._reload_card(self._fetch_compliance_and_mdm_worker),
        )

        # Register cards with base class for error status tracking
        self.register_cards(
            self.sensitivity_card,
            self.retention_card,
            self.dlp_card,
            self.sit_card,
            self.custom_sit_card,
            self.edm_schemas_card,
            self.ediscovery_card,
            self.legal_holds_card,
            self.ca_card,
            self.filtering_card,
            self.firewall_card,
            self.mail_security_card,
            self.encryption_card,
            self.transport_rules_card,
            self.mobile_apps_card,
            self.detected_apps_card,
            self.managed_devices_card,
            self.vc_devices_card,
            self.device_configs_card,
            self.byod_configs_card,
            self.android_compliance_card,
            self.ios_compliance_card,
            self.mdm_policies_card,
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
                        ft.Icons.SECURITY_ROUNDED,
                        size=38,
                        color=COLOR_PRIMARY,
                    ),
                ),
                ft.Text(
                    "Security, Compliance & Governance",
                    size=22,
                    weight=ft.FontWeight.BOLD,
                    color=COLOR_TEXT_PRIMARY,
                ),
                ft.Container(
                    width=650,
                    content=ft.Text(
                        "Audit Microsoft Purview retention rules, sensitivity labels, DLP policies, custom SITs, "
                        "Conditional Access, network security, and Microsoft Intune endpoint governance across your tenant.",
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

    def fetch_all_data(self):
        """Initiates concurrent data fetch with maximum 2 parallel worker threads."""
        if self.is_fetching:
            return

        self.is_fetching = True
        self.is_fetched = True
        self._notify_status("loading")

        self.cards_column.controls.clear()

        total_tasks = 14

        self.progress_bar = ft.ProgressBar(
            value=0.0,
            width=float("inf"),
            height=6,
            color="#15803D",
            bgcolor="#DCFCE7",
            border_radius=3,
        )
        self.progress_text = ft.Text(
            f"Fetching Security, Compliance & Governance telemetry (0 of {total_tasks} completed)...",
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
        self.sensitivity_card.set_loading("Fetching sensitivity labels...")
        self.retention_card.set_loading("Fetching retention policies...")
        self.dlp_card.set_loading("Fetching DLP policies...")
        self.sit_card.set_loading("Fetching sensitive info types...")
        self.custom_sit_card.set_loading("Fetching custom SITs...")
        self.edm_schemas_card.set_loading("Fetching EDM schemas...")
        self.ediscovery_card.set_loading("Fetching eDiscovery cases...")
        self.legal_holds_card.set_loading("Fetching legal holds...")
        self.ca_card.set_loading("Fetching conditional access...")
        self.filtering_card.set_loading("Fetching filtering policies...")
        self.firewall_card.set_loading("Fetching firewall configurations...")
        self.mail_security_card.set_loading("Fetching mail security SKUs...")
        self.encryption_card.set_loading("Fetching encryption posture...")
        self.transport_rules_card.set_loading("Fetching transport rules...")
        self.mobile_apps_card.set_loading("Fetching mobile applications...")
        self.detected_apps_card.set_loading("Fetching detected applications...")
        self.managed_devices_card.set_loading("Fetching managed devices...")
        self.vc_devices_card.set_loading("Filtering VC devices...")
        self.device_configs_card.set_loading("Fetching device configurations...")
        self.byod_configs_card.set_loading("Fetching BYOD configurations...")
        self.android_compliance_card.set_loading("Fetching Android compliance...")
        self.ios_compliance_card.set_loading("Fetching iOS compliance...")
        self.mdm_policies_card.set_loading("Fetching MDM policies...")

        self.content = self.cards_column
        try:
            self.update()
        except Exception:
            pass

        completed_count = 0

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
                            f"Fetching Security, Compliance & Governance telemetry ({completed_count} of {total_tasks} completed)..."
                        )
                        try:
                            self.progress_banner.update()
                        except Exception:
                            pass

                    self._safe_run_on_ui(_update_progress)
            return _wrapped

        # Tasks to execute in worker pool (at max 2 concurrent threads)
        tasks = [
            ("SensitivityLabels", _track_task_wrapper(self._fetch_sensitivity_worker)),
            ("RetentionPolicies", _track_task_wrapper(self._fetch_retention_worker)),
            ("DLPPolicies", _track_task_wrapper(self._fetch_dlp_worker)),
            ("SITsAndEDMs", _track_task_wrapper(self._fetch_sits_worker)),
            ("EDiscoveryCases", _track_task_wrapper(self._fetch_ediscovery_worker)),
            ("LegalHolds", _track_task_wrapper(self._fetch_legal_holds_worker)),
            ("ConditionalAccess", _track_task_wrapper(self._fetch_ca_worker)),
            ("NetworkSecurity", _track_task_wrapper(self._fetch_network_security_worker)),
            ("MailSecurity", _track_task_wrapper(self._fetch_mail_security_worker)),
            ("TransportRules", _track_task_wrapper(self._fetch_transport_rules_worker)),
            ("MobileAndDetectedApps", _track_task_wrapper(self._fetch_mobile_detected_apps_worker)),
            ("ManagedAndVCDevices", _track_task_wrapper(self._fetch_managed_and_vc_devices_worker)),
            ("DeviceConfigsAndBYOD", _track_task_wrapper(self._fetch_device_configs_and_byod_worker)),
            ("ComplianceAndMDM", _track_task_wrapper(self._fetch_compliance_and_mdm_worker)),
        ]

        def _orchestrate():
            logger.info("Starting Security & Governance orchestrator with ThreadPoolExecutor(max_workers=2)")
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

        import threading
        threading.Thread(target=_orchestrate, daemon=True).start()

    # --- Telemetry Caching & Reports Helpers ---

    def _get_reports_dir_and_db(self):
        """Returns the persistent reports directory and SQLite database cache path for this tenant."""
        return self.get_reports_dir_and_db()

    def _cache_to_sqlite_safe(self, csv_path: str, db_path: str, table_name: str):
        """Asynchronously imports raw CSV on disk into local SQLite database cache without blocking UI."""
        if csv_path and os.path.exists(csv_path):
            try:
                import asyncio
                from core.graph.db import import_csv_to_sqlite
                asyncio.run(import_csv_to_sqlite(csv_path, db_path, table_name))
            except Exception as e:
                logger.warning(f"Error caching {table_name} to SQLite: {e}")

    # --- Telemetry Worker Pipelines ---

    def _fetch_sensitivity_worker(self, is_reload: bool = False):
        """1. Sensitivity Labels Worker (7 columns)."""
        start_time = time.time()
        logger.info("Executing Sensitivity Labels fetch task...")
        headers = ["Sensitivity Label", "Description", "Protection", "Mode", "Priority", "Applicable Targets", "Status"]
        reports_dir, db_path = self._get_reports_dir_and_db()
        csv_path = os.path.join(reports_dir, "sensitivity_labels.csv")

        try:
            collected_labels = []
            def _on_page(page_labels):
                collected_labels.extend(page_labels)

            run_sensitivity_labels_pipeline(
                client_id=self.client_id,
                client_secret=self.secret,
                tenant_id=self.tenant,
                csv_path=csv_path,
                on_page_callback=_on_page,
            )
            self.cached_data["security_labels"] = collected_labels

            self._cache_to_sqlite_safe(csv_path, db_path, "sensitivity_labels")

            rows: List[List[Any]] = []
            for lbl in collected_labels:
                name = lbl.get("name") or "N/A"
                desc = lbl.get("description") or lbl.get("toolTip") or "None"
                has_prot = "Encrypted" if lbl.get("hasProtection") else "Classification Only"
                app_mode = lbl.get("applicationMode") or "Standard"
                prio = str(lbl.get("priority", 0))
                applicable = str(lbl.get("applicableTo") or "File, Email")
                status = "Enabled" if lbl.get("isEnabled", True) else "Disabled"
                rows.append([name, desc, has_prot, app_mode, prio, applicable, status])

                for sub in lbl.get("sublabels", []):
                    sub_name = f"  ↳  {sub.get('name', 'N/A')}"
                    sub_desc = sub.get("description") or sub.get("toolTip") or "None"
                    sub_prot = "Encrypted" if sub.get("hasProtection") else "Classification Only"
                    sub_mode = sub.get("applicationMode") or "Standard"
                    sub_prio = str(sub.get("priority", 0))
                    sub_app = str(sub.get("applicableTo") or applicable)
                    sub_stat = "Enabled" if sub.get("isEnabled", True) else "Disabled"
                    rows.append([sub_name, sub_desc, sub_prot, sub_mode, sub_prio, sub_app, sub_stat])

            elapsed = time.time() - start_time

            def _on_success():
                self.sensitivity_card.set_data(headers, rows, execution_time=elapsed)
                if self.sensitivity_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.sensitivity_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Sensitivity Labels: {e}", exc_info=True)
            err_msg = str(e)

            def _on_error():
                self.sensitivity_card.set_error(f"Failed to fetch Sensitivity Labels: {err_msg}")
                if self.sensitivity_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.sensitivity_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_retention_worker(self, is_reload: bool = False):
        """2. Retention Compliance Policies Worker (5 columns)."""
        start_time = time.time()
        logger.info("Executing Purview Retention Compliance Policies fetch task...")
        headers = ["Policy Name", "Workloads", "Duration", "Distribution", "Status"]
        reports_dir, db_path = self._get_reports_dir_and_db()
        csv_path = os.path.join(reports_dir, "retention_policies.csv")

        try:
            policies_raw = run_retention_policies_pipeline(self.client_id, self.secret, self.tenant)
            policies = policies_raw.get("value", []) if isinstance(policies_raw, dict) else policies_raw
            self.cached_data["retention_policies"] = policies
            rows: List[List[Any]] = []
            
            with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["Name", "Workload", "Duration", "DistributionStatus", "Enabled", "Mode"])
                if isinstance(policies, list):
                    for p in policies:
                        if isinstance(p, dict):
                            name = p.get("Name") or p.get("DisplayName") or "N/A"
                            workload = p.get("Workload") or p.get("ExchangeLocation") or "M365 Tenant"
                            duration = str(p.get("Duration") or p.get("RetentionDuration") or "Indefinite")
                            dist = p.get("DistributionStatus") or "Success"
                            enabled = p.get("Enabled")
                            mode_val = p.get("Mode") or "Active"
                            writer.writerow([name, workload, duration, dist, str(enabled), mode_val])

                            if enabled is not None:
                                status_str = "Enabled" if str(enabled).lower() in ["true", "enabled", "1"] else "Disabled"
                            else:
                                status_str = mode_val
                            rows.append([name, workload, duration, dist, status_str])

            self._cache_to_sqlite_safe(csv_path, db_path, "retention_policies")

            elapsed = time.time() - start_time

            def _on_success():
                self.retention_card.set_data(headers, rows, execution_time=elapsed)
                if self.retention_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.retention_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Retention Policies: {e}", exc_info=True)
            err_msg = str(e)

            def _on_error():
                self.retention_card.set_error(f"Failed to fetch Retention Compliance Policies: {err_msg}")
                if self.retention_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.retention_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_dlp_worker(self, is_reload: bool = False):
        """3. Data Loss Prevention (DLP) Policies Worker (6 columns)."""
        start_time = time.time()
        logger.info("Executing DLP Policies fetch task...")
        headers = ["Policy Name", "Mode", "Workload", "State", "Locations", "Actions"]
        reports_dir, db_path = self._get_reports_dir_and_db()
        csv_path = os.path.join(reports_dir, "dlp_policies.csv")

        try:
            policies_raw = run_dlp_policies_pipeline(self.client_id, self.secret, self.tenant)
            
            # Unwrap dictionary response if needed
            if isinstance(policies_raw, dict) and "value" in policies_raw:
                policies_list = policies_raw["value"]
            elif isinstance(policies_raw, list):
                policies_list = policies_raw
            elif isinstance(policies_raw, dict):
                policies_list = [policies_raw]
            else:
                policies_list = []

            if hasattr(self, "cached_data") and isinstance(self.cached_data, dict):
                self.cached_data["dlp_policies"] = policies_list
            rows: List[List[Any]] = []
            
            with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["Name", "Mode", "Workload", "State", "Locations", "Actions"])
                for p in policies_list:
                    if isinstance(p, dict):
                        name = p.get("Name") or p.get("DisplayName") or "N/A"
                        mode = p.get("Mode") or "N/A"
                        workload = p.get("Workload") or "N/A"
                        
                        en_val = str(p.get("Enabled", "")).lower()
                        state = "Enabled" if en_val in ("true", "1", "yes") else "Disabled"
                        
                        exc = "EX" if p.get("ExchangeLocation") else ""
                        spo = "SPO" if p.get("SharePointLocation") else ""
                        od = "OD" if p.get("OneDriveLocation") else ""
                        locations = ", ".join([loc for loc in (exc, spo, od) if loc]) or "N/A"
                        
                        actions = p.get("Actions") or "None"
                        
                        writer.writerow([name, mode, workload, state, locations, actions])
                        rows.append([name, mode, workload, state, locations, actions])

            self._cache_to_sqlite_safe(csv_path, db_path, "dlp_policies")

            elapsed = time.time() - start_time

            def _on_success():
                self.dlp_card.set_data(headers, rows, execution_time=elapsed)
                if self.dlp_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.dlp_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching DLP policies: {e}", exc_info=True)
            err_msg = str(e)

            def _on_error():
                self.dlp_card.set_error(f"Failed to fetch DLP Policies: {err_msg}")
                if self.dlp_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.dlp_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_sits_worker(self, is_reload: bool = False):
        """4, 5, 6. Sensitive Information Types (SIT), Custom SITs & EDM Schemas Worker."""
        start_time = time.time()
        logger.info("Executing Sensitive Information Types & EDMs fetch task...")
        headers_sit = ["SIT Name", "Type", "Confidence", "Description"]
        headers_custom = ["Name", "Publisher", "Description"]
        headers_edm = ["Name", "Description", "DataStoreName"]

        reports_dir, db_path = self._get_reports_dir_and_db()
        csv_path_sit = os.path.join(reports_dir, "sensitive_info_types.csv")
        csv_path_custom = os.path.join(reports_dir, "custom_sits.csv")
        csv_path_edm = os.path.join(reports_dir, "edm_schemas.csv")

        try:
            sit_data = run_sensitive_info_types_pipeline(
                client_id=self.client_id,
                client_secret=self.secret,
                tenant_id=self.tenant
            )

            sit_list = []
            custom_sits = []
            edm_schemas = []

            if isinstance(sit_data, dict):
                sit_list = sit_data.get("SensitiveInformationTypes", []) or []
                if isinstance(sit_list, dict): sit_list = [sit_list]
                custom_sits = sit_data.get("CustomRulePackages", []) or []
                if isinstance(custom_sits, dict): custom_sits = [custom_sits]
                edm_schemas = sit_data.get("EdmSchemas", []) or []
                if isinstance(edm_schemas, dict): edm_schemas = [edm_schemas]
            elif isinstance(sit_data, list):
                sit_list = sit_data

            self.cached_data["sensitive_info_types"] = sit_list
            self.cached_data["custom_sits"] = custom_sits
            self.cached_data["edm_schemas"] = edm_schemas

            standard_sits = []
            for sit in sit_list:
                if str(sit.get("IsOutOfBox", "True")).lower() == "false":
                    if str(sit.get("Type", "")).lower() == "exactmatch":
                        if "DataStoreName" not in sit:
                            sit["DataStoreName"] = sit.get("Publisher", "Unknown")
                        edm_schemas.append(sit)
                    else:
                        if "PublisherName" not in sit or not sit["PublisherName"]:
                            sit["PublisherName"] = sit.get("Publisher", "Unknown")
                        custom_sits.append(sit)
                else:
                    standard_sits.append(sit)

            # Rows & CSV for Standard SITs
            rows_sit: List[List[Any]] = []
            with open(csv_path_sit, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["Name", "Type", "Confidence", "Description"])
                for s in standard_sits:
                    name = s.get("Name") or s.get("DisplayName") or "N/A"
                    sit_type = s.get("Type") or "Standard"
                    conf = str(s.get("RecommendedConfidence") or s.get("ConfidenceLevel") or s.get("DefaultConfidence") or "85%")
                    if conf.isdigit(): conf = f"{conf}%"
                    desc = s.get("Description") or s.get("Comment") or "Built-in Microsoft Purview sensitive information classifier"
                    writer.writerow([name, sit_type, conf, desc])
                    rows_sit.append([name, sit_type, conf, desc])

            # Rows & CSV for Custom SITs
            rows_custom: List[List[Any]] = []
            with open(csv_path_custom, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["Name", "Publisher", "Description"])
                for cs in custom_sits:
                    name = cs.get("Name") or cs.get("DisplayName") or "N/A"
                    pub = cs.get("PublisherName") or cs.get("Publisher") or "Custom"
                    desc = cs.get("Description") or cs.get("Comment") or "Organization custom sensitive information pattern"
                    writer.writerow([name, pub, desc])
                    rows_custom.append([name, pub, desc])

            # Rows & CSV for EDM Schemas
            rows_edm: List[List[Any]] = []
            with open(csv_path_edm, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["Name", "Description", "DataStoreName"])
                for edm in edm_schemas:
                    name = edm.get("Name") or edm.get("DisplayName") or "N/A"
                    desc = edm.get("Description") or edm.get("Comment") or "Exact Data Match classification schema"
                    store = edm.get("DataStoreName") or edm.get("DataStore") or "Primary Data Store"
                    writer.writerow([name, desc, store])
                    rows_edm.append([name, desc, store])

            self._cache_to_sqlite_safe(csv_path_sit, db_path, "sensitive_info_types")
            self._cache_to_sqlite_safe(csv_path_custom, db_path, "custom_sits")
            self._cache_to_sqlite_safe(csv_path_edm, db_path, "edm_schemas")

            elapsed = time.time() - start_time

            def _on_success():
                self.sit_card.set_data(headers_sit, rows_sit, execution_time=elapsed)
                self.custom_sit_card.set_data(headers_custom, rows_custom, execution_time=elapsed)
                self.edm_schemas_card.set_data(headers_edm, rows_edm, execution_time=elapsed)

                for card in [self.sit_card, self.custom_sit_card, self.edm_schemas_card]:
                    if card not in self.cards_column.controls:
                        self.cards_column.controls.append(card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching SITs: {e}", exc_info=True)
            err_msg = str(e)

            def _on_error():
                self.sit_card.set_error(f"Failed to fetch Sensitive Information Types: {err_msg}")
                self.custom_sit_card.set_error(f"Failed to fetch Custom SITs: {err_msg}")
                self.edm_schemas_card.set_error(f"Failed to fetch EDM Schemas: {err_msg}")
                for card in [self.sit_card, self.custom_sit_card, self.edm_schemas_card]:
                    if card not in self.cards_column.controls:
                        self.cards_column.controls.append(card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_ediscovery_worker(self, is_reload: bool = False):
        """7. Microsoft Purview eDiscovery Cases Worker (4 columns)."""
        start_time = time.time()
        logger.info("Executing eDiscovery Cases fetch task...")
        headers = ["Display Name", "Status", "Created DateTime", "Closed By"]
        reports_dir, db_path = self._get_reports_dir_and_db()
        csv_path = os.path.join(reports_dir, "ediscovery_cases.csv")

        try:
            delegated_tok = self.get_delegated_token()
            use_delegated = self.page_ref.session.store.get("use_delegated") if self.page_ref else False

            if not use_delegated and not delegated_tok:
                raise PermissionError("eDiscovery scanning requires delegated authentication. Please enable 'Enable delegated authentication' on the connection screen.")

            if not delegated_tok:
                raise PermissionError("Failed to acquire delegated authentication token.")

            from core.graph.ediscovery import EDiscoveryFetcher
            fetcher = EDiscoveryFetcher(delegated_tok)
            res = fetcher.fetch_cases(csv_path=csv_path)

            if not res.get("success", False):
                err = res.get("error", "Unknown error fetching eDiscovery cases")
                raise Exception(err)

            self.cached_data["ediscovery_cases"] = res.get("data", [])

            rows: List[List[Any]] = []
            for c in res.get("data", []):
                name = c.get("displayName") or "N/A"
                status = (c.get("status") or "active").capitalize()
                created = (c.get("createdDateTime") or "")[:19] or "N/A"
                closed_by = c.get("closedBy") or "-"
                rows.append([name, status, created, closed_by])

            elapsed = time.time() - start_time

            def _on_success():
                self.ediscovery_card.set_data(headers, rows, execution_time=elapsed)
                if self.ediscovery_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.ediscovery_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching eDiscovery Cases: {e}", exc_info=True)
            err_msg = str(e)

            def _on_error():
                self.ediscovery_card.set_error(f"Failed to fetch eDiscovery Cases: {err_msg}")
                if self.ediscovery_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.ediscovery_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_legal_holds_worker(self, is_reload: bool = False):
        """8. Mailboxes on Legal Hold Worker via Exchange Online PowerShell (3 columns)."""
        start_time = time.time()
        logger.info("Executing Mailboxes on Legal Hold fetch task...")
        headers = ["Mailbox Name", "User Principal Name (UPN)", "Applied Hold Policies"]
        reports_dir, db_path = self._get_reports_dir_and_db()
        csv_path = os.path.join(reports_dir, "legal_holds.csv")

        try:
            data = run_legal_holds_pipeline(
                client_id=self.client_id,
                client_secret=self.secret,
                tenant_id=self.tenant,
            )
            lh_list = data.get("value", []) if isinstance(data, dict) else []
            self.cached_data["legal_holds"] = lh_list

            # Export to CSV & SQLite cache
            os.makedirs(reports_dir, exist_ok=True)
            with open(csv_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["DisplayName", "UserPrincipalName", "InPlaceHolds"])
                for lh in lh_list:
                    dname = lh.get("DisplayName") or lh.get("name") or "Unknown"
                    upn = lh.get("UserPrincipalName") or lh.get("PrimarySmtpAddress") or "N/A"
                    holds = lh.get("InPlaceHolds", [])
                    if isinstance(holds, list):
                        holds_str = ", ".join(str(h) for h in holds) if holds else "Litigation Hold"
                    else:
                        holds_str = str(holds) if holds else "Litigation Hold"
                    writer.writerow([dname, upn, holds_str])

            self._cache_to_sqlite_safe(csv_path, db_path, "legal_holds")

            rows: List[List[Any]] = []
            for lh in lh_list:
                dname = lh.get("DisplayName") or lh.get("name") or "Unknown"
                upn = lh.get("UserPrincipalName") or lh.get("PrimarySmtpAddress") or "N/A"
                holds = lh.get("InPlaceHolds", [])
                if isinstance(holds, list):
                    holds_str = ", ".join(str(h) for h in holds) if holds else "Litigation Hold"
                else:
                    holds_str = str(holds) if holds else "Litigation Hold"
                rows.append([dname, upn, holds_str])

            elapsed = time.time() - start_time

            def _on_success():
                self.legal_holds_card.set_data(headers, rows, execution_time=elapsed)
                if self.legal_holds_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.legal_holds_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Legal Holds: {e}", exc_info=True)
            err_msg = str(e)
            if "certificate" in err_msg.lower():
                err_msg = "Certificate authentication missing or expired. Complete hybrid auth flow."
            elif "pwsh" in err_msg.lower() or "powershell" in err_msg.lower():
                err_msg = "PowerShell Core ('pwsh') is not installed or not available in PATH."

            def _on_error():
                self.legal_holds_card.set_error(f"Failed to fetch Mailboxes on Legal Hold: {err_msg}")
                if self.legal_holds_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.legal_holds_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_ca_worker(self, is_reload: bool = False):
        """9. Conditional Access Policies Worker (5 columns)."""
        start_time = time.time()
        logger.info("Executing Conditional Access Policies fetch task...")
        headers = ["Policy Name", "State", "Target Users", "Grant Controls", "Client Apps"]
        reports_dir, db_path = self._get_reports_dir_and_db()
        csv_path = os.path.join(reports_dir, "network_conditional_access.csv")

        try:
            policies_list = []
            def _on_page(p_list):
                policies_list.extend(p_list)

            run_conditional_access_pipeline(
                client_id=self.client_id,
                client_secret=self.secret,
                tenant_id=self.tenant,
                csv_path=csv_path,
                on_page_callback=_on_page,
            )

            rows: List[List[Any]] = []
            for p in policies_list:
                name = p.get("displayName") or "N/A"
                state = p.get("state") or "N/A"
                conds = p.get("conditions") or {}
                users_cond = conds.get("users") or {}
                inc_users = users_cond.get("includeUsers") or []
                user_target = "All Users" if "All" in inc_users else f"Specific ({len(inc_users)} users)"
                grant_controls = p.get("grantControls") or {}
                controls = grant_controls.get("builtInControls") or []
                ctrl_str = ", ".join(controls) if controls else "Block/None"
                apps_cond = conds.get("applications") or {}
                inc_apps = apps_cond.get("includeApplications") or []
                app_str = "All Cloud Apps" if "All" in inc_apps else "Targeted Apps"
                rows.append([name, state, user_target, ctrl_str, app_str])

            elapsed = time.time() - start_time

            if hasattr(self, "cached_data") and isinstance(self.cached_data, dict):
                self.cached_data["conditional_access"] = policies_list

            def _on_success():
                self.ca_card.set_data(headers, rows, execution_time=elapsed)
                if self.ca_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.ca_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Conditional Access: {e}", exc_info=True)
            err_msg = str(e)

            def _on_error():
                self.ca_card.set_error(f"Failed to fetch Conditional Access: {err_msg}")
                if self.ca_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.ca_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_network_security_worker(self, is_reload: bool = False):
        """8, 9. Global Secure Access Filtering & Firewall Configurations Worker."""
        start_time = time.time()
        logger.info("Executing Network Security (Filtering & Firewall) fetch task...")
        headers_filter = ["Policy Name", "Description", "Version", "Action", "Rules Count"]
        headers_fw = ["Configuration Name", "Policy Type", "Firewall Status", "Proxy Status"]

        reports_dir, db_path = self._get_reports_dir_and_db()
        csv_path_filter = os.path.join(reports_dir, "network_filtering_policies.csv")
        csv_path_fw = os.path.join(reports_dir, "network_firewall_policies.csv")

        try:
            filtering_list = []
            filter_err = None
            def _on_filter_page(f_list):
                filtering_list.extend(f_list)

            try:
                run_filtering_pipeline(
                    client_id=self.client_id,
                    client_secret=self.secret,
                    tenant_id=self.tenant,
                    csv_path=csv_path_filter,
                    on_page_callback=_on_filter_page,
                )
            except Exception as e:
                logger.error(f"Filtering policies query error: {e}", exc_info=True)
                filter_err = str(e)

            firewall_list = []
            fw_err = None
            def _on_fw_page(fw_list):
                firewall_list.extend(fw_list)

            try:
                run_firewall_pipeline(
                    client_id=self.client_id,
                    client_secret=self.secret,
                    tenant_id=self.tenant,
                    csv_path=csv_path_fw,
                    on_page_callback=_on_fw_page,
                )
            except Exception as e:
                logger.error(f"Firewall configurations query error: {e}", exc_info=True)
                fw_err = str(e)

            rows_filter: List[List[Any]] = []
            for fp in filtering_list:
                name = fp.get("name") or fp.get("displayName") or "N/A"
                desc = fp.get("description") or "Default web filtering profile"
                ver = str(fp.get("version") or "1.0")
                action = fp.get("action") or "Block"
                rules_cnt = str(len(fp.get("rules") or []) or fp.get("rulesCount") or 0)
                rows_filter.append([name, desc, ver, action, rules_cnt])

            rows_fw: List[List[Any]] = []
            for fw in firewall_list:
                name = fw.get("displayName") or fw.get("name") or "N/A"
                ptype = fw.get("@odata.type", "").replace("#microsoft.graph.", "") or "Endpoint Security"
                fw_stat = "Enabled" if fw.get("firewallEnabled", True) else "Disabled"
                proxy_stat = "Configured" if fw.get("proxyConfigured") else "None"
                rows_fw.append([name, ptype, fw_stat, proxy_stat])

            elapsed = time.time() - start_time

            if hasattr(self, "cached_data") and isinstance(self.cached_data, dict):
                self.cached_data["filtering_policies"] = filtering_list
                self.cached_data["firewall_proxy"] = firewall_list

            def _on_success():
                if filter_err:
                    self.filtering_card.set_error(f"Failed to fetch Filtering Policies: {filter_err}")
                else:
                    self.filtering_card.set_data(headers_filter, rows_filter, execution_time=elapsed)

                if fw_err:
                    self.firewall_card.set_error(f"Failed to fetch Firewall Configurations: {fw_err}")
                else:
                    self.firewall_card.set_data(headers_fw, rows_fw, execution_time=elapsed)

                for card in [self.filtering_card, self.firewall_card]:
                    if card not in self.cards_column.controls:
                        self.cards_column.controls.append(card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Network Security: {e}", exc_info=True)
            err_msg = str(e)

            def _on_error():
                self.filtering_card.set_error(f"Failed to fetch Filtering Policies: {err_msg}")
                self.firewall_card.set_error(f"Failed to fetch Firewall Configurations: {err_msg}")
                for card in [self.filtering_card, self.firewall_card]:
                    if card not in self.cards_column.controls:
                        self.cards_column.controls.append(card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_mail_security_worker(self, is_reload: bool = False):
        """10, 11. Exchange Mail Security SKUs & Encryption Key Management Worker."""
        start_time = time.time()
        logger.info("Executing Exchange Mail Security & Encryption Key Management fetch task...")
        headers_ms = ["Mail Security Configuration", "Detected SKUs", "Covered User Count"]
        headers_enc = ["Key Management Model", "M365DataAtRestEncryptionPolicy", "DataEncryptionPolicy"]

        reports_dir, db_path = self._get_reports_dir_and_db()
        csv_path_ms = os.path.join(reports_dir, "mail_security.csv")
        csv_path_enc = os.path.join(reports_dir, "encryption_posture.csv")

        try:
            sec_data = run_mail_security_pipeline(self.client_id, self.secret, self.tenant)
            self.cached_data["mail_security"] = sec_data
            self.cached_data["encryption_posture"] = sec_data.get("encryption_posture", [])
            defender_skus = sec_data.get("defender", {}).get("skus", [])
            defender_users = sec_data.get("defender", {}).get("users", 0)
            eop_skus = sec_data.get("eop", {}).get("skus", [])
            eop_users = sec_data.get("eop", {}).get("users", 0)

            rows_ms: List[List[Any]] = [
                [
                    "Microsoft Defender for Office 365",
                    ", ".join(defender_skus) if defender_skus else "None",
                    f"{defender_users:,} Users" if defender_users else "0 Users"
                ],
                [
                    "Exchange Online Protection (Baseline)",
                    ", ".join(eop_skus) if eop_skus else "None",
                    f"{eop_users:,} Users" if eop_users else "0 Users"
                ],
            ]

            rows_enc: List[List[Any]] = [
                [
                    "Microsoft-Managed Keys (Default)",
                    "None detected",
                    "None detected"
                ]
            ]

            with open(csv_path_ms, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["Configuration", "SKUs", "Users"])
                for r in rows_ms: writer.writerow(r)

            with open(csv_path_enc, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["KeyManagementModel", "M365DataAtRestEncryptionPolicy", "DataEncryptionPolicy"])
                for r in rows_enc: writer.writerow(r)

            elapsed = time.time() - start_time

            def _on_success():
                self.mail_security_card.set_data(headers_ms, rows_ms, execution_time=elapsed)
                self.encryption_card.set_data(headers_enc, rows_enc, execution_time=elapsed)

                for card in [self.mail_security_card, self.encryption_card]:
                    if card not in self.cards_column.controls:
                        self.cards_column.controls.append(card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Mail Security & Encryption: {e}", exc_info=True)
            err_msg = str(e)

            def _on_error():
                self.mail_security_card.set_error(f"Failed to fetch Mail Security SKUs: {err_msg}")
                self.encryption_card.set_error(f"Failed to fetch Encryption Key Management: {err_msg}")
                for card in [self.mail_security_card, self.encryption_card]:
                    if card not in self.cards_column.controls:
                        self.cards_column.controls.append(card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_transport_rules_worker(self, is_reload: bool = False):
        """12. Exchange Transport Rules Worker (5 columns)."""
        start_time = time.time()
        logger.info("Executing Exchange Transport Rules fetch task...")
        headers = ["Rule Name", "State", "Priority", "Mode", "Rule Logic"]
        reports_dir, db_path = self._get_reports_dir_and_db()

        try:
            res = run_transport_rules_pipeline(
                client_id=self.client_id,
                client_secret=self.secret,
                tenant_id=self.tenant,
            )
            csv_path = res.get("csv_path")

            rows: List[List[Any]] = []
            rules_list = []
            if csv_path and os.path.exists(csv_path):
                with open(csv_path, mode="r", encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f)
                    for r in reader:
                        rules_list.append(r)
                        name = r.get("Name") or "N/A"
                        state = r.get("State") or "Enabled"
                        prio = str(r.get("Priority") if r.get("Priority") is not None else "0")
                        mode = r.get("Mode") or "Enforce"
                        desc = r.get("Description") or r.get("Comments") or ""
                        if not desc:
                            conds = r.get("Conditions") or ""
                            acts = r.get("Actions") or ""
                            if conds or acts:
                                desc = f"Conditions: {conds}; Actions: {acts}"
                            else:
                                desc = "Apply rule conditions and actions across Exchange mail flow"
                        rows.append([name, state, prio, mode, desc])

            elapsed = time.time() - start_time

            if hasattr(self, "cached_data") and isinstance(self.cached_data, dict):
                self.cached_data["transport_rules"] = rules_list

            def _on_success():
                self.transport_rules_card.set_data(headers, rows, execution_time=elapsed)
                if self.transport_rules_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.transport_rules_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Exchange Transport Rules: {e}", exc_info=True)
            err_msg = str(e)

            def _on_error():
                self.transport_rules_card.set_error(f"Failed to fetch Exchange Transport Rules: {err_msg}")
                if self.transport_rules_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.transport_rules_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_mobile_detected_apps_worker(self, is_reload: bool = False):
        """13, 14. Managed Mobile Apps & Detected Apps Worker."""
        start_time = time.time()
        logger.info("Executing Intune Mobile & Detected Apps fetch task...")
        headers_mobile = ["Application Name", "Publisher", "Platform", "Installed Devices", "License Type"]
        headers_detected = ["App Name", "Version", "Publisher", "Platform"]

        reports_dir, db_path = self._get_reports_dir_and_db()
        csv_path_mobile = os.path.join(reports_dir, "intune_mobile_apps.csv")
        temp_csv_path = os.path.join(reports_dir, "temp_intune_mobile_apps.csv")
        csv_path_detected = os.path.join(reports_dir, "intune_detected_apps.csv")

        try:
            mobile_list = []
            mobile_err = None
            def _on_mobile_page(m_list):
                mobile_list.extend(m_list)

            # Initialize temp CSV with header as in legacy Intune Mobile Apps
            with open(temp_csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["displayName"])

            try:
                run_mobile_apps_pipeline(
                    client_id=self.client_id,
                    client_secret=self.secret,
                    tenant_id=self.tenant,
                    csv_path=temp_csv_path,
                    on_page_callback=_on_mobile_page,
                )
                if os.path.exists(temp_csv_path):
                    if os.path.exists(csv_path_mobile):
                        os.remove(csv_path_mobile)
                    os.rename(temp_csv_path, csv_path_mobile)
                self._cache_to_sqlite_safe(csv_path_mobile, db_path, "mobile_apps")
            except Exception as e:
                logger.error(f"Mobile apps query error: {e}", exc_info=True)
                mobile_err = str(e)
            finally:
                if os.path.exists(temp_csv_path):
                    try:
                        os.remove(temp_csv_path)
                    except Exception:
                        pass

            detected_list = []
            detected_err = None
            try:
                detected_list = run_detected_apps_pipeline(
                    client_id=self.client_id,
                    client_secret=self.secret,
                    tenant_id=self.tenant,
                    csv_path=csv_path_detected,
                )
                self._cache_to_sqlite_safe(csv_path_detected, db_path, "detected_apps")
            except Exception as e:
                logger.error(f"Detected apps query error: {e}", exc_info=True)
                detected_err = str(e)

            rows_mobile: List[List[Any]] = []
            for a in mobile_list:
                name = a.get("displayName") or "N/A"
                pub = a.get("publisher") or "Microsoft"
                plat = a.get("@odata.type", "").replace("#microsoft.graph.", "") or "All Platforms"
                installed = str(a.get("installedDeviceCount", 0))
                lic = "Assigned" if a.get("isAssigned", True) else "Available"
                rows_mobile.append([name, pub, plat, installed, lic])

            rows_detected: List[List[Any]] = []
            if isinstance(detected_list, list):
                for da in detected_list:
                    if isinstance(da, dict):
                        name = da.get("displayName") or "N/A"
                        ver = da.get("version") or "-"
                        pub = da.get("publisher") or "Unknown"
                        plat = da.get("platform") or "Windows / Mobile"
                        rows_detected.append([name, ver, pub, plat])

            elapsed = time.time() - start_time

            if hasattr(self, "cached_data") and isinstance(self.cached_data, dict):
                self.cached_data.setdefault("intune", {}).update({
                    "mobile_apps": mobile_list,
                    "detected_apps": detected_list
                })

            def _on_success():
                if mobile_err:
                    self.mobile_apps_card.set_error(f"Failed to fetch Managed Mobile Apps: {mobile_err}")
                else:
                    self.mobile_apps_card.set_data(headers_mobile, rows_mobile, execution_time=elapsed)

                if detected_err:
                    self.detected_apps_card.set_error(f"Failed to fetch Detected Apps: {detected_err}")
                else:
                    self.detected_apps_card.set_data(headers_detected, rows_detected, execution_time=elapsed)

                for card in [self.mobile_apps_card, self.detected_apps_card]:
                    if card not in self.cards_column.controls:
                        self.cards_column.controls.append(card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Mobile & Detected Apps: {e}", exc_info=True)
            err_msg = str(e)

            def _on_error():
                self.mobile_apps_card.set_error(f"Failed to fetch Managed Mobile Apps: {err_msg}")
                self.detected_apps_card.set_error(f"Failed to fetch Detected Apps: {err_msg}")
                for card in [self.mobile_apps_card, self.detected_apps_card]:
                    if card not in self.cards_column.controls:
                        self.cards_column.controls.append(card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_managed_and_vc_devices_worker(self, is_reload: bool = False):
        """15, 16. Intune Managed Devices & VC Devices Worker."""
        start_time = time.time()
        logger.info("Executing Intune Managed Devices & VC Devices fetch task...")
        headers_managed = ["Device Name", "Operating System", "Compliance State", "Ownership", "Last Sync Date"]
        headers_vc = ["User ID", "Device Name", "Operating System", "Management Agent", "Registration State", "Model", "Manufacturer"]

        reports_dir, db_path = self._get_reports_dir_and_db()
        csv_path_managed = os.path.join(reports_dir, "intune_managed_devices.csv")
        csv_path_vc = os.path.join(reports_dir, "intune_vc_devices.csv")

        try:
            devices_list = []
            try:
                devices_list = run_managed_devices_pipeline(
                    client_id=self.client_id,
                    client_secret=self.secret,
                    tenant_id=self.tenant,
                    csv_path=csv_path_managed,
                )
                self._cache_to_sqlite_safe(csv_path_managed, db_path, "managed_devices")
            except Exception as e:
                logger.error(f"Managed devices query error: {e}", exc_info=True)

            rows_managed: List[List[Any]] = []
            rows_vc: List[List[Any]] = []

            if isinstance(devices_list, list):
                for d in devices_list:
                    if isinstance(d, dict):
                        name = d.get("deviceName") or "N/A"
                        os_name = f"{d.get('operatingSystem', '')} {d.get('osVersion', '')}".strip() or "N/A"
                        comp = d.get("complianceState") or "Compliant"
                        owner = d.get("managedDeviceOwnerType") or "Company"
                        sync_dt = (d.get("lastSyncDateTime") or "")[:10] or "N/A"
                        rows_managed.append([name, os_name, comp, owner, sync_dt])

                        # Video conferencing filter: check if device name / model indicates room device
                        model_name = d.get("model") or ""
                        device_lower = name.lower()
                        if any(kw in device_lower for kw in ["room", "teams room", "hub", "surface hub", "poly", "yealink", "logitech", "meeting", "vc"]):
                            uid = d.get("userId") or d.get("id") or "-"
                            agent = d.get("managementAgent") or "mdm"
                            reg_state = d.get("deviceRegistrationState") or "registered"
                            manuf = d.get("manufacturer") or "Unknown"
                            rows_vc.append([uid, name, os_name, agent, reg_state, model_name, manuf])

            if rows_vc:
                with open(csv_path_vc, 'w', encoding='utf-8', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(["userId", "deviceName", "operatingSystem", "managementAgent", "deviceRegistrationState", "model", "manufacturer"])
                    for r in rows_vc: writer.writerow(r)
                self._cache_to_sqlite_safe(csv_path_vc, db_path, "vc_devices")

            elapsed = time.time() - start_time

            if hasattr(self, "cached_data") and isinstance(self.cached_data, dict):
                self.cached_data.setdefault("intune", {}).update({
                    "managed_devices": managed_list,
                    "vc_devices": vc_list
                })

            def _on_success():
                self.managed_devices_card.set_data(headers_managed, rows_managed, execution_time=elapsed)
                self.vc_devices_card.set_data(headers_vc, rows_vc, execution_time=elapsed)

                for card in [self.managed_devices_card, self.vc_devices_card]:
                    if card not in self.cards_column.controls:
                        self.cards_column.controls.append(card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Managed & VC Devices: {e}", exc_info=True)
            err_msg = str(e)

            def _on_error():
                self.managed_devices_card.set_error(f"Failed to fetch Managed Devices: {err_msg}")
                self.vc_devices_card.set_error(f"Failed to fetch VC Devices: {err_msg}")
                for card in [self.managed_devices_card, self.vc_devices_card]:
                    if card not in self.cards_column.controls:
                        self.cards_column.controls.append(card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_device_configs_and_byod_worker(self, is_reload: bool = False):
        """17, 18. Device Configurations Summary & BYOD Configurations Worker."""
        start_time = time.time()
        logger.info("Executing Device Configurations & BYOD Configurations fetch task...")
        headers_configs = ["Platform", "Policy Type", "Number of Policies"]
        headers_byod = [
            "Display Name", "Description", "Priority",
            "Last Modified Date Time", "iOS Restrictions",
            "Windows Mobile Restrictions", "Android Restrictions"
        ]

        reports_dir, db_path = self._get_reports_dir_and_db()
        csv_path_configs = os.path.join(reports_dir, "intune_device_configs.csv")
        csv_path_policies = os.path.join(reports_dir, "intune_config_policies.csv")
        csv_path_byod = os.path.join(reports_dir, "intune_byod_configs.csv")

        try:
            byod_list = []
            byod_err = None
            try:
                byod_list = run_byod_configs_pipeline(
                    client_id=self.client_id,
                    client_secret=self.secret,
                    tenant_id=self.tenant,
                    csv_path=csv_path_byod,
                )
                self._cache_to_sqlite_safe(csv_path_byod, db_path, "byod_configs")
            except Exception as e:
                logger.error(f"BYOD configs query error: {e}", exc_info=True)
                byod_err = str(e)

            configs_err = None
            rows_configs: List[List[Any]] = []
            total_dc = 0
            total_cp = 0
            temp_path_configs = csv_path_configs + ".tmp"
            temp_path_policies = csv_path_policies + ".tmp"

            try:
                with open(temp_path_configs, 'w', encoding='utf-8', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(["displayName", "platform", "policyType"])

                with open(temp_path_policies, 'w', encoding='utf-8', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(["displayName", "platform", "policyType"])

                run_device_configs_pipeline(
                    client_id=self.client_id,
                    client_secret=self.secret,
                    tenant_id=self.tenant,
                    endpoint_name="deviceConfigurations",
                    csv_path=temp_path_configs,
                )
                run_device_configs_pipeline(
                    client_id=self.client_id,
                    client_secret=self.secret,
                    tenant_id=self.tenant,
                    endpoint_name="configurationPolicies",
                    csv_path=temp_path_policies,
                )

                if os.path.exists(temp_path_configs):
                    if os.path.exists(csv_path_configs):
                        os.remove(csv_path_configs)
                    os.rename(temp_path_configs, csv_path_configs)

                if os.path.exists(temp_path_policies):
                    if os.path.exists(csv_path_policies):
                        os.remove(csv_path_policies)
                    os.rename(temp_path_policies, csv_path_policies)

                if os.path.exists(csv_path_configs):
                    self._cache_to_sqlite_safe(csv_path_configs, db_path, "device_configs")
                if os.path.exists(csv_path_policies):
                    self._cache_to_sqlite_safe(csv_path_policies, db_path, "device_policies")

                counts: Dict[tuple, int] = defaultdict(int)
                if os.path.exists(csv_path_configs):
                    with open(csv_path_configs, 'r', encoding='utf-8', errors='ignore') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            plat = (row.get("platform") or "").strip()
                            p_type = (row.get("policyType") or "").strip()
                            if plat and p_type:
                                counts[(plat, p_type)] += 1
                                total_dc += 1

                if os.path.exists(csv_path_policies):
                    with open(csv_path_policies, 'r', encoding='utf-8', errors='ignore') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            plat = (row.get("platform") or "").strip()
                            p_type = (row.get("policyType") or "").strip()
                            if plat and p_type:
                                counts[(plat, p_type)] += 1
                                total_cp += 1

                for (platform, p_type), count in sorted(counts.items()):
                    rows_configs.append([platform, p_type, str(count)])

                self.cached_data.setdefault("intune", {}).update({
                    "table_rows": rows_configs,
                    "total_device_configs": total_dc,
                    "total_config_policies": total_cp,
                })
            except Exception as e:
                logger.error(f"Device configurations pipeline error: {e}", exc_info=True)
                configs_err = str(e)
            finally:
                if os.path.exists(temp_path_configs):
                    try:
                        os.remove(temp_path_configs)
                    except Exception:
                        pass
                if os.path.exists(temp_path_policies):
                    try:
                        os.remove(temp_path_policies)
                    except Exception:
                        pass

            rows_byod: List[List[Any]] = []
            if isinstance(byod_list, list):
                for b in byod_list:
                    if isinstance(b, dict):
                        name = b.get("displayName") or "N/A"
                        desc = b.get("description") or "None"
                        prio = str(b.get("priority", 0))
                        lmod = (b.get("lastModifiedDateTime") or "")[:19] or "N/A"
                        ios_r = str(b.get("iosRestrictionFormatted") or "N/A")
                        win_r = str(b.get("windowsRestrictionFormatted") or "N/A")
                        android_r = str(b.get("androidRestrictionFormatted") or "N/A")
                        rows_byod.append([name, desc, prio, lmod, ios_r, win_r, android_r])

            elapsed = time.time() - start_time

            def _on_success():
                if configs_err:
                    self.device_configs_card.set_error(f"Failed to fetch Device Configurations: {configs_err}")
                else:
                    self.device_configs_card.set_data(
                        headers_configs,
                        rows_configs,
                        execution_time=elapsed,
                    )

                if byod_err:
                    self.byod_configs_card.set_error(f"Failed to fetch BYOD Configurations: {byod_err}")
                else:
                    self.byod_configs_card.set_data(headers_byod, rows_byod, execution_time=elapsed)

                for card in [self.device_configs_card, self.byod_configs_card]:
                    if card not in self.cards_column.controls:
                        self.cards_column.controls.append(card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Device Configs & BYOD: {e}", exc_info=True)
            err_msg = str(e)

            def _on_error():
                self.device_configs_card.set_error(f"Failed to fetch Device Configurations: {err_msg}")
                self.byod_configs_card.set_error(f"Failed to fetch BYOD Configurations: {err_msg}")
                for card in [self.device_configs_card, self.byod_configs_card]:
                    if card not in self.cards_column.controls:
                        self.cards_column.controls.append(card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_compliance_and_mdm_worker(self, is_reload: bool = False):
        """19, 20, 21. Android Compliance, iOS Compliance & MDM Policies Worker."""
        start_time = time.time()
        logger.info("Executing Compliance (Android/iOS) & MDM Policies fetch task...")
        headers_comp = ["Display Name", "Description", "Created Time", "Last Modified", "Version"]
        headers_mdm = ["Display Name", "Description", "Applies To", "Discovery URL", "Terms of Use URL", "Compliance URL"]

        reports_dir, db_path = self._get_reports_dir_and_db()
        csv_path_android = os.path.join(reports_dir, "intune_android_compliance.csv")
        csv_path_ios = os.path.join(reports_dir, "intune_ios_compliance.csv")
        csv_path_mdm = os.path.join(reports_dir, "intune_mdm_policies.csv")

        try:
            android_list = []
            android_err = None
            try:
                android_list = run_device_compliance_pipeline(
                    client_id=self.client_id,
                    client_secret=self.secret,
                    tenant_id=self.tenant,
                    csv_path=csv_path_android,
                    filter_type="microsoft.graph.androidCompliancePolicy"
                )
                self._cache_to_sqlite_safe(csv_path_android, db_path, "android_compliance")
            except Exception as e:
                logger.error(f"Android compliance query error: {e}", exc_info=True)
                android_err = str(e)

            ios_list = []
            ios_err = None
            try:
                ios_list = run_device_compliance_pipeline(
                    client_id=self.client_id,
                    client_secret=self.secret,
                    tenant_id=self.tenant,
                    csv_path=csv_path_ios,
                    filter_type="microsoft.graph.iosCompliancePolicy"
                )
                self._cache_to_sqlite_safe(csv_path_ios, db_path, "ios_compliance")
            except Exception as e:
                logger.error(f"iOS compliance query error: {e}", exc_info=True)
                ios_err = str(e)

            mdm_list = []
            mdm_err = None
            try:
                delegated_tok = self.get_delegated_token()
                use_delegated = self.page_ref.session.store.get("use_delegated") if self.page_ref else False
                if not use_delegated and not delegated_tok:
                    mdm_err = "Delegated authentication required. Please enable 'Enable delegated authentication' on the connection screen."
                else:
                    mdm_list = run_mdm_policies_pipeline(
                        client_id=self.client_id,
                        client_secret=self.secret,
                        tenant_id=self.tenant,
                        csv_path=csv_path_mdm,
                        delegated_token=delegated_tok,
                    )
                    self._cache_to_sqlite_safe(csv_path_mdm, db_path, "mdm_policies")
            except Exception as e:
                logger.error(f"MDM policies query error: {e}", exc_info=True)
                mdm_err = str(e)

            rows_android: List[List[Any]] = []
            if isinstance(android_list, list):
                for a in android_list:
                    if isinstance(a, dict):
                        name = a.get("displayName") or "N/A"
                        desc = a.get("description") or "Android Device Compliance Benchmark"
                        ctime = (a.get("createdDateTime") or "")[:19] or "N/A"
                        mtime = (a.get("lastModifiedDateTime") or "")[:19] or "N/A"
                        ver = str(a.get("version", 1))
                        rows_android.append([name, desc, ctime, mtime, ver])

            rows_ios: List[List[Any]] = []
            if isinstance(ios_list, list):
                for i in ios_list:
                    if isinstance(i, dict):
                        name = i.get("displayName") or "N/A"
                        desc = i.get("description") or "iOS Device Compliance Benchmark"
                        ctime = (i.get("createdDateTime") or "")[:19] or "N/A"
                        mtime = (i.get("lastModifiedDateTime") or "")[:19] or "N/A"
                        ver = str(i.get("version", 1))
                        rows_ios.append([name, desc, ctime, mtime, ver])

            rows_mdm: List[List[Any]] = []
            if isinstance(mdm_list, list):
                for m in mdm_list:
                    if isinstance(m, dict):
                        name = m.get("displayName") or "N/A"
                        desc = m.get("description") or "None"
                        applies = m.get("appliesTo") or "All Users"
                        d_url = m.get("discoveryUrl") or "https://enrollment.manage.microsoft.com"
                        t_url = m.get("termsOfUseUrl") or "-"
                        c_url = m.get("complianceUrl") or "-"
                        rows_mdm.append([name, desc, applies, d_url, t_url, c_url])

            elapsed = time.time() - start_time

            if hasattr(self, "cached_data") and isinstance(self.cached_data, dict):
                self.cached_data.setdefault("intune", {}).update({
                    "android_compliance": android_list,
                    "ios_compliance": ios_list,
                    "mdm_policies": mdm_list
                })

            def _on_success():
                if android_err:
                    self.android_compliance_card.set_error(f"Failed to fetch Android Compliance: {android_err}")
                else:
                    self.android_compliance_card.set_data(headers_comp, rows_android, execution_time=elapsed)

                if ios_err:
                    self.ios_compliance_card.set_error(f"Failed to fetch iOS Compliance: {ios_err}")
                else:
                    self.ios_compliance_card.set_data(headers_comp, rows_ios, execution_time=elapsed)

                if mdm_err:
                    self.mdm_policies_card.set_error(f"Failed to fetch MDM Policies: {mdm_err}")
                else:
                    self.mdm_policies_card.set_data(headers_mdm, rows_mdm, execution_time=elapsed)

                for card in [self.android_compliance_card, self.ios_compliance_card, self.mdm_policies_card]:
                    if card not in self.cards_column.controls:
                        self.cards_column.controls.append(card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Compliance & MDM: {e}", exc_info=True)
            err_msg = str(e)

            def _on_error():
                self.android_compliance_card.set_error(f"Failed to fetch Android Compliance: {err_msg}")
                self.ios_compliance_card.set_error(f"Failed to fetch iOS Compliance: {err_msg}")
                self.mdm_policies_card.set_error(f"Failed to fetch MDM Policies: {err_msg}")
                for card in [self.android_compliance_card, self.ios_compliance_card, self.mdm_policies_card]:
                    if card not in self.cards_column.controls:
                        self.cards_column.controls.append(card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

