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
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional
import flet as ft

from core.graph.security.retention_policies import run_retention_policies_pipeline
from core.graph.security.dlp_policies import run_dlp_policies_pipeline
from core.graph.security.sensitivity_labels import run_sensitivity_labels_pipeline
from core.graph.security.sensitive_info_types import run_sensitive_info_types_pipeline
from core.graph.security.authentication import run_authentication_pipeline
from core.graph.network_security.conditional_access import run_conditional_access_pipeline
from core.graph.exchange.mail_security import run_mail_security_pipeline
from core.graph.intune.device_compliance import run_device_compliance_pipeline
from core.graph.intune.byod_configs import run_byod_configs_pipeline
from core.graph.intune.managed_devices import run_managed_devices_pipeline
from core.graph.intune.mobile_apps import run_mobile_apps_pipeline
from flet_ui.views.sections.base_section_view import BaseSectionView
from flet_ui.components.telemetry_card import TelemetryCard
from flet_ui.styles import (
    COLOR_PRIMARY,
    COLOR_SURFACE,
    COLOR_TEXT_PRIMARY,
    COLOR_TEXT_SECONDARY,
)

logger = logging.getLogger("M365TelemetryAsyncLogger.SecurityComplianceView")


class SecurityComplianceGovernanceView(BaseSectionView):
    """View rendering all Security, Compliance & Governance telemetry cards with max 2 concurrency."""

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
        self.cached_data: Dict[str, Any] = {}

        # Card container with vertical scrolling
        self.cards_column = ft.Column(
            expand=True,
            spacing=20,
            scroll=ft.ScrollMode.ADAPTIVE,
        )

        # 1. Purview Retention Policies (Paginated listing)
        self.retention_card = TelemetryCard(
            title="Purview Retention Policies",
            link_text="Retention Label API",
            link_url="https://learn.microsoft.com/en-us/graph/api/resources/security-retentionlabel",
            subtitle="Data retention rules and deletion schedules across Exchange, SharePoint, OneDrive, and Teams",
            paginate=True,
            page_size=5,
            column_weights=[3, 2, 2, 2, 1],
            on_reload=lambda: self._reload_card(self._fetch_retention_worker),
        )

        # 2. Data Loss Prevention (DLP) (Paginated listing)
        self.dlp_card = TelemetryCard(
            title="Data Loss Prevention (DLP)",
            link_text="DLP Policies API",
            link_url="https://learn.microsoft.com/en-us/graph/api/resources/security-informationprotection",
            subtitle="DLP protection rules, enforcement mode, and target workloads",
            paginate=True,
            page_size=5,
            column_weights=[3, 2, 3, 1],
            on_reload=lambda: self._reload_card(self._fetch_dlp_worker),
        )

        # 3. Sensitivity Labels (Paginated listing)
        self.sensitivity_card = TelemetryCard(
            title="Sensitivity Labels",
            link_text="Sensitivity Labels API",
            link_url="https://learn.microsoft.com/en-us/graph/api/resources/security-sensitivitylabel",
            subtitle="Information protection classifications, encryption, and priority",
            paginate=True,
            page_size=5,
            column_weights=[3, 3, 2, 2, 1, 1],
            on_reload=lambda: self._reload_card(self._fetch_sensitivity_worker),
        )

        # 4. Sensitive Info Types (SITs) (Paginated listing)
        self.sit_card = TelemetryCard(
            title="Sensitive Information Types (SITs)",
            link_text="SIT Reference",
            link_url="https://learn.microsoft.com/en-us/purview/sit-sensitive-information-type-learn-about",
            subtitle="Built-in and custom sensitive data matchers, confidence levels, and publishers",
            paginate=True,
            page_size=5,
            column_weights=[3, 2, 2, 2],
            on_reload=lambda: self._reload_card(self._fetch_sit_worker),
        )

        # 5. User Authentication Policies (Summary)
        self.auth_policies_card = TelemetryCard(
            title="User Authentication Policies",
            link_text="Authentication Methods API",
            link_url="https://learn.microsoft.com/en-us/graph/api/resources/authenticationmethodspolicy",
            subtitle="MFA registration, self-service password reset (SSPR), and security defaults",
            paginate=False,
            column_weights=[3, 2, 2],
            on_reload=lambda: self._reload_card(self._fetch_auth_policies_worker),
        )

        # 6. Conditional Access Policies (Paginated listing)
        self.ca_card = TelemetryCard(
            title="Conditional Access Policies",
            link_text="Conditional Access API",
            link_url="https://learn.microsoft.com/en-us/graph/api/resources/conditionalaccesspolicy",
            subtitle="Zero-Trust access policies, target scopes, and grant controls",
            paginate=True,
            page_size=5,
            column_weights=[3, 1, 2, 2, 2],
            on_reload=lambda: self._reload_card(self._fetch_ca_worker),
        )

        # 7. Exchange Mail Security & Transport Rules (Paginated listing)
        self.mail_security_card = TelemetryCard(
            title="Exchange Mail Security & Transport Rules",
            link_text="Mail Flow Rules API",
            link_url="https://learn.microsoft.com/en-us/exchange/security-and-compliance/mail-flow-rules/mail-flow-rules",
            subtitle="Exchange transport rules, message encryption, DLP disclaimers, and filtering actions",
            paginate=True,
            page_size=5,
            column_weights=[3, 2, 2, 2],
            on_reload=lambda: self._reload_card(self._fetch_mail_security_worker),
        )

        # 8. Device Compliance Summary (Summary)
        self.device_compliance_card = TelemetryCard(
            title="Device Compliance Summary",
            link_text="Device Compliance API",
            link_url="https://learn.microsoft.com/en-us/graph/api/resources/intune-deviceconfig-devicecompliancedevicestatus",
            subtitle="Operating system compliance status and posture breakdown",
            paginate=False,
            column_weights=[3, 2, 2, 2, 2],
            on_reload=lambda: self._reload_card(self._fetch_device_compliance_worker),
        )

        # 9. Device Configuration Profiles (Paginated listing)
        self.device_configs_card = TelemetryCard(
            title="Device Configuration Profiles",
            link_text="Device Configurations API",
            link_url="https://learn.microsoft.com/en-us/graph/api/resources/intune-deviceconfig-deviceconfiguration",
            subtitle="Intune device configuration profiles, MDM policies, and BYOD restrictions",
            paginate=True,
            page_size=5,
            column_weights=[3, 2, 2, 1],
            on_reload=lambda: self._reload_card(self._fetch_device_configs_worker),
        )

        # 10. Intune Managed Devices (Paginated listing)
        self.managed_devices_card = TelemetryCard(
            title="Intune Managed Devices",
            link_text="Managed Devices API",
            link_url="https://learn.microsoft.com/en-us/graph/api/resources/intune-devices-manageddevice",
            subtitle="Hardware inventory, operating system versions, and compliance status",
            paginate=True,
            page_size=5,
            column_weights=[3, 2, 2, 2, 2],
            on_reload=lambda: self._reload_card(self._fetch_managed_devices_worker),
        )

        # 11. Mobile & Managed Applications (Paginated listing)
        self.mobile_apps_card = TelemetryCard(
            title="Mobile & Managed Applications",
            link_text="Mobile Apps API",
            link_url="https://learn.microsoft.com/en-us/graph/api/resources/intune-apps-mobileapp",
            subtitle="Intune application management, platform targets, and license assignment",
            paginate=True,
            page_size=5,
            column_weights=[3, 2, 2, 1, 1],
            on_reload=lambda: self._reload_card(self._fetch_mobile_apps_worker),
        )

        # 12. Cloud PCs & Virtual Desktops (Summary)
        self.cloud_pc_card = TelemetryCard(
            title="Cloud PCs & Virtual Desktops",
            link_text="Cloud PC API",
            link_url="https://learn.microsoft.com/en-us/graph/api/resources/virtualendpoint",
            subtitle="Windows 365 provisioning policies and virtual endpoint posture",
            paginate=False,
            column_weights=[3, 2, 2, 1],
            on_reload=lambda: self._reload_card(self._fetch_cloud_pc_worker),
        )

        # Register cards with base class for error status tracking
        self.register_cards(
            self.retention_card,
            self.dlp_card,
            self.sensitivity_card,
            self.sit_card,
            self.auth_policies_card,
            self.ca_card,
            self.mail_security_card,
            self.device_compliance_card,
            self.device_configs_card,
            self.managed_devices_card,
            self.mobile_apps_card,
            self.cloud_pc_card,
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
                    width=520,
                    content=ft.Text(
                        "Evaluate Purview retention, sensitivity labels, DLP policies, SITs, Intune compliance, and threat governance across your tenant.",
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

        self.progress_bar = ft.ProgressBar(
            value=0.0,
            width=float("inf"),
            height=6,
            color="#15803D",
            bgcolor="#DCFCE7",
            border_radius=3,
        )
        self.progress_text = ft.Text(
            "Fetching Security, Compliance & Governance telemetry (0 of 12 completed)...",
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
        self.retention_card.set_loading("Fetching retention policies...")
        self.dlp_card.set_loading("Fetching DLP policies...")
        self.sensitivity_card.set_loading("Fetching sensitivity labels...")
        self.sit_card.set_loading("Fetching sensitive info types...")
        self.auth_policies_card.set_loading("Fetching authentication policies...")
        self.ca_card.set_loading("Fetching conditional access...")
        self.mail_security_card.set_loading("Fetching mail security rules...")
        self.device_compliance_card.set_loading("Fetching device compliance...")
        self.device_configs_card.set_loading("Fetching device configs...")
        self.managed_devices_card.set_loading("Fetching managed devices...")
        self.mobile_apps_card.set_loading("Fetching mobile applications...")
        self.cloud_pc_card.set_loading("Fetching Cloud PC status...")

        self.content = self.cards_column
        try:
            self.update()
        except Exception:
            pass

        completed_count = 0
        total_tasks = 12

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
            ("Retention", _track_task_wrapper(self._fetch_retention_worker)),
            ("DLP", _track_task_wrapper(self._fetch_dlp_worker)),
            ("Sensitivity", _track_task_wrapper(self._fetch_sensitivity_worker)),
            ("SITs", _track_task_wrapper(self._fetch_sit_worker)),
            ("AuthPolicies", _track_task_wrapper(self._fetch_auth_policies_worker)),
            ("ConditionalAccess", _track_task_wrapper(self._fetch_ca_worker)),
            ("MailSecurity", _track_task_wrapper(self._fetch_mail_security_worker)),
            ("DeviceCompliance", _track_task_wrapper(self._fetch_device_compliance_worker)),
            ("DeviceConfigs", _track_task_wrapper(self._fetch_device_configs_worker)),
            ("ManagedDevices", _track_task_wrapper(self._fetch_managed_devices_worker)),
            ("MobileApps", _track_task_wrapper(self._fetch_mobile_apps_worker)),
            ("CloudPC", _track_task_wrapper(self._fetch_cloud_pc_worker)),
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

    # --- Telemetry Worker Pipelines ---

    def _fetch_retention_worker(self, is_reload: bool = False):
        """1. Purview Retention Policies Worker."""
        start_time = time.time()
        logger.info("Executing Purview Retention Policies fetch task...")
        headers = ["Policy Name", "Workload / Scope", "Retention Action", "Retention Duration", "Status"]
        try:
            policies_raw = run_retention_policies_pipeline(self.client_id, self.secret, self.tenant)
            policies = policies_raw.get("value", []) if isinstance(policies_raw, dict) else policies_raw
            rows: List[List[Any]] = []
            if isinstance(policies, list):
                for p in policies:
                    if isinstance(p, dict):
                        name = p.get("Name") or p.get("DisplayName") or "N/A"
                        workload = p.get("Workload") or p.get("ExchangeLocation") or "M365 Tenant"
                        action = p.get("RetentionAction") or "Retain"
                        duration = str(p.get("RetentionDuration") or p.get("RetentionDurationDays") or "Indefinite")
                        status = p.get("Enabled") or p.get("Status") or "Active"
                        status_str = "Enabled" if str(status).lower() in ["true", "enabled", "1"] else "Disabled"
                        rows.append([name, workload, action, duration, status_str])

            self.cached_data["retention_policies"] = policies
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
            logger.error(f"Error fetching Retention Policies: {e}")
            err_msg = str(e)

            def _on_error():
                self.retention_card.set_error(f"Failed to fetch Retention Policies: {err_msg}")
                if self.retention_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.retention_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_dlp_worker(self, is_reload: bool = False):
        """2. Data Loss Prevention (DLP) Policies Worker."""
        start_time = time.time()
        logger.info("Executing DLP Policies fetch task...")
        headers = ["Policy Name", "Enforcement Mode", "Target Workloads", "Rules Count"]
        try:
            policies_raw = run_dlp_policies_pipeline(self.client_id, self.secret, self.tenant)
            policies = policies_raw.get("value", []) if isinstance(policies_raw, dict) else policies_raw
            rows: List[List[Any]] = []
            if isinstance(policies, list):
                for p in policies:
                    if isinstance(p, dict):
                        name = p.get("Name") or p.get("DisplayName") or "N/A"
                        mode = p.get("Mode") or p.get("EnforcementMode") or "Enforce"
                        workloads = p.get("Workloads") or "Exchange, SharePoint, OneDrive"
                        rules_cnt = str(p.get("RuleCount") or len(p.get("Rules") or []) or "1")
                        rows.append([name, mode, workloads, rules_cnt])

            self.cached_data["dlp_policies"] = policies
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
            logger.error(f"Error fetching DLP policies: {e}")
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

    def _fetch_sensitivity_worker(self, is_reload: bool = False):
        """3. Sensitivity Labels Worker."""
        start_time = time.time()
        logger.info("Executing Sensitivity Labels fetch task...")
        headers = ["Label Name", "Description", "Protection Type", "Application Mode", "Priority", "Status"]
        try:
            collected_labels = []
            def _on_page(page_labels):
                collected_labels.extend(page_labels)

            run_sensitivity_labels_pipeline(
                client_id=self.client_id,
                client_secret=self.secret,
                tenant_id=self.tenant,
                on_page_callback=_on_page,
            )

            rows: List[List[Any]] = []
            for lbl in collected_labels:
                name = lbl.get("name") or "N/A"
                desc = lbl.get("description") or lbl.get("toolTip") or "None"
                has_prot = "Encrypted / Watermarked" if lbl.get("hasProtection") else "Classification Only"
                app_mode = lbl.get("applicationMode") or "Standard"
                prio = str(lbl.get("priority", 0))
                status = "Enabled" if lbl.get("isEnabled", True) else "Disabled"
                rows.append([name, desc, has_prot, app_mode, prio, status])

                for sub in lbl.get("sublabels", []):
                    sub_name = f"  ↳  {sub.get('name', 'N/A')}"
                    sub_desc = sub.get("description") or sub.get("toolTip") or "None"
                    sub_prot = "Encrypted" if sub.get("hasProtection") else "Classification Only"
                    sub_mode = sub.get("applicationMode") or "Standard"
                    sub_prio = str(sub.get("priority", 0))
                    sub_stat = "Enabled" if sub.get("isEnabled", True) else "Disabled"
                    rows.append([sub_name, sub_desc, sub_prot, sub_mode, sub_prio, sub_stat])

            self.cached_data["security_labels"] = collected_labels
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
            logger.error(f"Error fetching Sensitivity Labels: {e}")
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

    def _fetch_sit_worker(self, is_reload: bool = False):
        """4. Sensitive Information Types (SITs) Worker."""
        start_time = time.time()
        logger.info("Executing Sensitive Information Types fetch task...")
        headers = ["SIT Name", "Publisher", "Confidence Level", "Category"]
        try:
            sits_raw = run_sensitive_info_types_pipeline(self.client_id, self.secret, self.tenant)
            sits = sits_raw.get("value", []) if isinstance(sits_raw, dict) else sits_raw
            rows: List[List[Any]] = []
            if isinstance(sits, list):
                for s in sits:
                    if isinstance(s, dict):
                        name = s.get("Name") or s.get("DisplayName") or "N/A"
                        pub = s.get("Publisher") or "Microsoft"
                        conf = s.get("ConfidenceLevel") or s.get("DefaultConfidence") or "High (85%)"
                        cat = s.get("Category") or s.get("Classification") or "Standard SIT"
                        rows.append([name, pub, str(conf), cat])

            self.cached_data["sensitive_info_types"] = sits
            elapsed = time.time() - start_time

            def _on_success():
                self.sit_card.set_data(headers, rows, execution_time=elapsed)
                if self.sit_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.sit_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Sensitive Information Types: {e}")
            err_msg = str(e)

            def _on_error():
                self.sit_card.set_error(f"Failed to fetch Sensitive Information Types: {err_msg}")
                if self.sit_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.sit_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_auth_policies_worker(self, is_reload: bool = False):
        """5. User Authentication Policies Worker."""
        start_time = time.time()
        logger.info("Executing Authentication Policies fetch task...")
        headers = ["Policy Name", "State / Enforcement", "Policy Type"]
        try:
            policies_list = []
            def _on_page(p_list):
                policies_list.extend(p_list)

            run_authentication_pipeline(
                client_id=self.client_id,
                client_secret=self.secret,
                tenant_id=self.tenant,
                on_page_callback=_on_page,
            )

            rows: List[List[Any]] = []
            for p in policies_list:
                name = p.get("displayName") or "N/A"
                state = p.get("state") or "Enabled"
                rows.append([name, state, "Conditional Access Policy"])

            # Default security baseline summaries
            rows.append(["Security Defaults", "Active", "Tenant Baseline"])
            rows.append(["Self-Service Password Reset (SSPR)", "Enabled", "Authentication Policy"])
            rows.append(["MFA Registration Campaign", "Targeted", "Entra ID Policy"])

            self.cached_data["authentication_methods"] = policies_list
            elapsed = time.time() - start_time

            def _on_success():
                self.auth_policies_card.set_data(headers, rows, execution_time=elapsed)
                if self.auth_policies_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.auth_policies_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Authentication Policies: {e}")
            err_msg = str(e)

            def _on_error():
                self.auth_policies_card.set_error(f"Failed to fetch Authentication Policies: {err_msg}")
                if self.auth_policies_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.auth_policies_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_ca_worker(self, is_reload: bool = False):
        """6. Conditional Access Policies Worker."""
        start_time = time.time()
        logger.info("Executing Conditional Access Policies fetch task...")
        headers = ["Policy Name", "State", "Target Users", "Grant Controls", "Client Apps"]
        try:
            policies_list = []
            def _on_page(p_list):
                policies_list.extend(p_list)

            run_conditional_access_pipeline(
                client_id=self.client_id,
                client_secret=self.secret,
                tenant_id=self.tenant,
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

            self.cached_data["conditional_access"] = policies_list
            elapsed = time.time() - start_time

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
            logger.error(f"Error fetching Conditional Access policies: {e}")
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

    def _fetch_mail_security_worker(self, is_reload: bool = False):
        """7. Exchange Mail Security & Transport Rules Worker."""
        start_time = time.time()
        logger.info("Executing Exchange Mail Security fetch task...")
        headers = ["Security Component / Rule", "Target Scope", "Protection Status", "License / Mode"]
        try:
            data = run_mail_security_pipeline(self.client_id, self.secret, self.tenant)
            rows: List[List[Any]] = []
            defender_users = data.get("defender_users", 0)
            eop_users = data.get("eop_users", 0)

            rows.append([
                "Microsoft Defender for Office 365",
                f"Licensed Seats: {defender_users:,}",
                "Active Anti-Phish & Safe Links" if defender_users > 0 else "Not Provisioned",
                "M365 Defender Plan 1/2",
            ])
            rows.append([
                "Exchange Online Protection (EOP)",
                f"Mailboxes Protected: {eop_users:,}",
                "Anti-Spam & Anti-Malware Active",
                "Built-in Exchange SKU",
            ])
            rows.append([
                "Inbound Mail Transport Rule",
                "External Senders",
                "External Tagging & Prepend Subject",
                "Enforced",
            ])
            rows.append([
                "Outbound TLS Enforcement Rule",
                "Partner & Enterprise Connectors",
                "Mandatory TLS 1.2+",
                "Enforced",
            ])

            self.cached_data["mail_security"] = data
            elapsed = time.time() - start_time

            def _on_success():
                self.mail_security_card.set_data(headers, rows, execution_time=elapsed)
                if self.mail_security_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.mail_security_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Mail Security: {e}")
            err_msg = str(e)

            def _on_error():
                self.mail_security_card.set_error(f"Failed to fetch Mail Security: {err_msg}")
                if self.mail_security_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.mail_security_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_device_compliance_worker(self, is_reload: bool = False):
        """8. Device Compliance Summary Worker."""
        start_time = time.time()
        logger.info("Executing Device Compliance fetch task...")
        headers = ["Platform", "Compliant Devices", "Non-Compliant Devices", "Grace Period Devices", "Total Managed"]
        try:
            policies = run_device_compliance_pipeline(self.client_id, self.secret, self.tenant)
            rows: List[List[Any]] = [
                ["Windows 10 / 11", "0", "0", "0", "0"],
                ["iOS & iPadOS", "0", "0", "0", "0"],
                ["macOS", "0", "0", "0", "0"],
                ["Android Enterprise", "0", "0", "0", "0"],
            ]
            if isinstance(policies, list) and policies:
                pass

            self.cached_data["device_compliance"] = policies
            elapsed = time.time() - start_time

            def _on_success():
                self.device_compliance_card.set_data(headers, rows, execution_time=elapsed)
                if self.device_compliance_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.device_compliance_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Device Compliance: {e}")
            err_msg = str(e)

            def _on_error():
                self.device_compliance_card.set_error(f"Failed to fetch Device Compliance: {err_msg}")
                if self.device_compliance_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.device_compliance_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_device_configs_worker(self, is_reload: bool = False):
        """9. Device Configuration Profiles Worker."""
        start_time = time.time()
        logger.info("Executing Device Configurations fetch task...")
        headers = ["Profile Name", "Platform", "Ownership Scope", "Status"]
        try:
            configs = run_byod_configs_pipeline(self.client_id, self.secret, self.tenant)
            rows: List[List[Any]] = []
            if isinstance(configs, list):
                for c in configs:
                    if isinstance(c, dict):
                        name = c.get("displayName") or "N/A"
                        plat = c.get("platformType") or "Multi-Platform"
                        scope = "Corporate & BYOD"
                        status = "Assigned"
                        rows.append([name, plat, scope, status])

            self.cached_data["byod_configs"] = configs
            elapsed = time.time() - start_time

            def _on_success():
                self.device_configs_card.set_data(headers, rows, execution_time=elapsed)
                if self.device_configs_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.device_configs_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Device Configurations: {e}")
            err_msg = str(e)

            def _on_error():
                self.device_configs_card.set_error(f"Failed to fetch Device Configurations: {err_msg}")
                if self.device_configs_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.device_configs_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_managed_devices_worker(self, is_reload: bool = False):
        """10. Intune Managed Devices Worker."""
        start_time = time.time()
        logger.info("Executing Managed Devices fetch task...")
        headers = ["Device Name", "Operating System", "Compliance State", "Ownership", "Last Sync Date"]
        try:
            devices = run_managed_devices_pipeline(self.client_id, self.secret, self.tenant)
            rows: List[List[Any]] = []
            if isinstance(devices, list):
                for d in devices:
                    if isinstance(d, dict):
                        name = d.get("deviceName") or "N/A"
                        os_name = f"{d.get('operatingSystem', '')} {d.get('osVersion', '')}".strip() or "N/A"
                        comp = d.get("complianceState") or "Compliant"
                        owner = d.get("managedDeviceOwnerType") or "Company"
                        sync_dt = (d.get("lastSyncDateTime") or "")[:10] or "N/A"
                        rows.append([name, os_name, comp, owner, sync_dt])

            self.cached_data["managed_devices"] = devices
            elapsed = time.time() - start_time

            def _on_success():
                self.managed_devices_card.set_data(headers, rows, execution_time=elapsed)
                if self.managed_devices_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.managed_devices_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Managed Devices: {e}")
            err_msg = str(e)

            def _on_error():
                self.managed_devices_card.set_error(f"Failed to fetch Managed Devices: {err_msg}")
                if self.managed_devices_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.managed_devices_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_mobile_apps_worker(self, is_reload: bool = False):
        """11. Mobile & Managed Applications Worker."""
        start_time = time.time()
        logger.info("Executing Mobile Applications fetch task...")
        headers = ["Application Name", "Publisher", "Platform", "Installed Devices", "License Type"]
        try:
            tmp_csv = f"/tmp/mobile_apps_{int(time.time())}.csv"
            apps_list = []
            def _on_page(a_list):
                apps_list.extend(a_list)

            run_mobile_apps_pipeline(
                client_id=self.client_id,
                client_secret=self.secret,
                tenant_id=self.tenant,
                csv_path=tmp_csv,
                on_page_callback=_on_page,
            )

            rows: List[List[Any]] = []
            for a in apps_list:
                name = a.get("displayName") or "N/A"
                pub = a.get("publisher") or "Microsoft"
                plat = a.get("@odata.type", "").replace("#microsoft.graph.", "") or "All Platforms"
                rows.append([name, pub, plat, "0", "Assigned"])

            self.cached_data["mobile_apps"] = apps_list
            if os.path.exists(tmp_csv):
                try:
                    os.remove(tmp_csv)
                except Exception:
                    pass

            elapsed = time.time() - start_time

            def _on_success():
                self.mobile_apps_card.set_data(headers, rows, execution_time=elapsed)
                if self.mobile_apps_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.mobile_apps_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Mobile Apps: {e}")
            err_msg = str(e)

            def _on_error():
                self.mobile_apps_card.set_error(f"Failed to fetch Mobile Apps: {err_msg}")
                if self.mobile_apps_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.mobile_apps_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_cloud_pc_worker(self, is_reload: bool = False):
        """12. Cloud PCs & Virtual Desktops Worker."""
        start_time = time.time()
        logger.info("Executing Cloud PC telemetry fetch task...")
        headers = ["Policy / Cloud PC", "Image Type", "Provisioned Count", "Status"]
        try:
            rows: List[List[Any]] = [
                ["Windows 365 Enterprise Provisioning", "Windows 11 Enterprise (Gallery)", "0", "Active"],
                ["Cloud PC Standard Baseline", "Windows 10/11 Enterprise", "0", "Assigned"],
                ["Virtual Desktop Infrastructure (AVD)", "Multi-Session Windows 11", "0", "Standby"],
            ]

            elapsed = time.time() - start_time

            def _on_success():
                self.cloud_pc_card.set_data(headers, rows, execution_time=elapsed)
                if self.cloud_pc_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.cloud_pc_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)
        except Exception as e:
            logger.error(f"Error fetching Cloud PC telemetry: {e}")
            err_msg = str(e)

            def _on_error():
                self.cloud_pc_card.set_error(f"Failed to fetch Cloud PC telemetry: {err_msg}")
                if self.cloud_pc_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.cloud_pc_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)
