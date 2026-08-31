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

"""Identity & Licensing Section view implementation for Flet UI."""

import os
import csv
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional
import flet as ft

from core.graph.client import GraphClient
from core.graph.directory.domains import DomainsService
from core.graph.directory.organization import OrganizationService
from core.graph.directory.provisioning_logs import ProvisioningLogsService
from core.graph.directory.subscribed_skus import SubscribedSKUsService
from core.graph.directory.user_logs import UserLogsService
from core.graph.directory.users_groups import UsersGroupsService
from flet_ui.views.telemetry.sections.base_section_view import BaseSectionView
from flet_ui.components.telemetry_card import TelemetryCard
from flet_ui.styles import (
    COLOR_BORDER,
    COLOR_PRIMARY,
    COLOR_SURFACE,
    COLOR_TEXT_PRIMARY,
    COLOR_TEXT_SECONDARY,
)

logger = logging.getLogger("M365TelemetryAsyncLogger.IdentityLicensingView")


class IdentityLicensingView(BaseSectionView):
    """View rendering all Identity & Licensing telemetry cards with full-width layout and max 2 concurrency."""

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

        # Card instances configured with full-width column weights
        self.cached_data: Dict[str, Any] = {}
        self.cards_column = ft.Column(
            expand=True,
            spacing=20,
            scroll=ft.ScrollMode.ADAPTIVE,
        )

        # 1. Directory Summary (Organization)
        self.org_card = TelemetryCard(
            title="Directory Summary",
            link_text="Organization API Reference",
            link_url="https://learn.microsoft.com/en-us/graph/api/resources/organization?view=graph-rest-1.0#properties",
            subtitle="Organization",
            footnote="* If OnPremisesSyncEnabled returns True, on-premises Active Directory is a primary source of truth.",
            paginate=False,
            column_weights=[1, 3],
            on_reload=lambda: self._reload_card(self._fetch_org_worker),
        )

        # 2. Subscribed SKUs
        self.sku_card = TelemetryCard(
            title="Subscribed SKUs",
            link_text="Service Plan Reference",
            link_url="https://learn.microsoft.com/en-us/entra/identity/users/licensing-service-plan-reference",
            subtitle="* To view specific services offered, export the spreadsheet.",
            paginate=True,
            page_size=5,
            column_weights=[4, 3, 2],
            on_reload=lambda: self._reload_card(self._fetch_skus_worker),
        )

        # 3. Domains
        self.domains_card = TelemetryCard(
            title="Domains",
            link_text="Domains API Reference",
            link_url="https://learn.microsoft.com/en-us/graph/api/resources/domain?view=graph-rest-1.0#properties",
            subtitle="Tenant domain names, authentication types, and federation configuration",
            footnote="* AuthenticationType=Managed indicates a cloud managed domain where Microsoft Entra ID performs user authentication. Federated indicates authentication is federated with an identity provider (eg. AD FS, Okta etc.)",
            paginate=True,
            page_size=5,
            column_weights=[3, 2, 1, 1, 1, 2, 2, 3],
            on_reload=lambda: self._reload_card(self._fetch_domains_worker),
        )

        # 4. Users & Groups Breakdown
        self.users_groups_card = TelemetryCard(
            title="Users & Groups Breakdown",
            link_text="Users & Groups API Reference",
            link_url="https://learn.microsoft.com/en-us/graph/api/resources/user",
            subtitle="Directory object counts across account states and group types",
            paginate=False,
            column_weights=[3, 1],
            on_reload=lambda: self._reload_card(self._fetch_users_groups_worker),
        )

        # 5. User Creation & Deletion Logs
        self.user_logs_card = TelemetryCard(
            title="User Creation & Deletion Logs",
            link_text="Directory Audit API Reference",
            link_url="https://learn.microsoft.com/en-us/graph/api/resources/directoryaudit?view=graph-rest-1.0",
            subtitle="Recent user creation and deletion events from directory audit logs",
            paginate=True,
            page_size=5,
            column_weights=[2, 5],
            on_reload=lambda: self._reload_card(self._fetch_user_logs_worker),
        )

        # 6. Directory Provisioning Logs
        self.provisioning_logs_card = TelemetryCard(
            title="Directory Provisioning Logs",
            link_text="Provisioning Logs API Reference",
            link_url="https://learn.microsoft.com/en-us/graph/api/resources/provisioningobjectsummary",
            subtitle="SCIM and inbound/outbound directory identity synchronization logs",
            paginate=True,
            page_size=5,
            column_weights=[2, 3, 2, 2, 2],
            on_reload=lambda: self._reload_card(self._fetch_provisioning_logs_worker),
        )

        # Register cards with base class for error status tracking
        self.register_cards(
            self.org_card,
            self.sku_card,
            self.domains_card,
            self.users_groups_card,
            self.user_logs_card,
            self.provisioning_logs_card,
        )

        # Placeholder / Initial state
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
                        ft.Icons.BADGE_OUTLINED,
                        size=38,
                        color=COLOR_PRIMARY,
                    ),
                ),
                ft.Text(
                    "Identity & Licensing",
                    size=22,
                    weight=ft.FontWeight.BOLD,
                    color=COLOR_TEXT_PRIMARY,
                ),
                ft.Container(
                    width=520,
                    content=ft.Text(
                        "Inspect user licenses, assigned SKUs, active accounts, directory synchronization, and domain configurations across your tenant.",
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

        # Switch to cards layout with active fetching progress banner
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
            "Fetching Identity & Licensing telemetry (0 of 6 completed)...",
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
        self.org_card.set_loading("Fetching organization details...")
        self.sku_card.set_loading("Fetching subscribed SKUs...")
        self.domains_card.set_loading("Fetching domains...")
        self.users_groups_card.set_loading("Fetching user & group counts...")
        self.user_logs_card.set_loading("Fetching user creation & deletion logs...")
        self.provisioning_logs_card.set_loading("Fetching provisioning logs...")

        self.content = self.cards_column
        try:
            self.update()
        except Exception:
            pass

        completed_count = 0
        total_tasks = 6

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
                        self.progress_text.value = f"Fetching Identity & Licensing telemetry ({completed_count} of {total_tasks} completed)..."
                        try:
                            self.progress_banner.update()
                        except Exception:
                            pass

                    self._safe_run_on_ui(_update_progress)
            return _wrapped

        # Tasks to execute in worker pool (at max 2 concurrent threads)
        tasks = [
            ("Organization", _track_task_wrapper(self._fetch_org_worker)),
            ("SKUs", _track_task_wrapper(self._fetch_skus_worker)),
            ("Domains", _track_task_wrapper(self._fetch_domains_worker)),
            ("UsersGroups", _track_task_wrapper(self._fetch_users_groups_worker)),
            ("UserLogs", _track_task_wrapper(self._fetch_user_logs_worker)),
            ("ProvisioningLogs", _track_task_wrapper(self._fetch_provisioning_logs_worker)),
        ]

        def _orchestrator():
            logger.info("Starting Identity & Licensing fetch orchestrator with ThreadPoolExecutor(max_workers=2)")
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
        threading.Thread(target=_orchestrator, daemon=True).start()

    def _get_client(self, scopes: Optional[List[str]] = None) -> GraphClient:
        """Creates authenticated GraphClient instance."""
        client = GraphClient(
            tenant_id=self.tenant,
            client_ids=self.client_id,
            client_secrets=self.secret,
            concurrency=1,
            retries=5,
            backoff=2,
        )
        client.authenticate(required_scopes=scopes or ["Organization.Read.All", "Directory.Read.All"])
        return client

    def _format_initiated_by(self, raw_val: Any) -> str:
        """Helper to format initiatedBy json/dict into human-readable text."""
        if not raw_val:
            return "-"
        if isinstance(raw_val, str):
            try:
                raw_val = json.loads(raw_val)
            except Exception:
                return raw_val
        if isinstance(raw_val, dict):
            user_info = raw_val.get("user") or {}
            if user_info:
                upn = user_info.get("userPrincipalName")
                disp = user_info.get("displayName")
                if upn and disp and upn != disp:
                    return f"{disp} ({upn})"
                return disp or upn or "User"
            app_info = raw_val.get("app") or {}
            if app_info:
                return f"App: {app_info.get('displayName') or app_info.get('servicePrincipalName') or 'Application'}"
        return str(raw_val)

    def _fetch_org_worker(self, is_reload: bool = False):
        """Fetches Organization properties from Graph API."""
        start_time = time.time()
        logger.info("Executing Organization summary fetch task...")
        try:
            client = self._get_client(["Organization.Read.All", "Directory.Read.All"])
            org_service = OrganizationService(client)
            org_list = org_service.get_organization_info()
            self.cached_data["organization"] = org_list
            client.close()

            columns = ["Property", "Value"]
            rows: List[List[Any]] = []

            if org_list:
                org = org_list[0]
                rows.append(["displayName", org.get("displayName", "null")])
                rows.append([
                    "isMultipleDataLocationsForServicesEnabled",
                    org.get("isMultipleDataLocationsForServicesEnabled", "null"),
                ])
                rows.append(["onPremisesSyncEnabled", org.get("onPremisesSyncEnabled", "null")])
                rows.append(["onPremisesLastSyncDateTime", org.get("onPremisesLastSyncDateTime", "null")])
                rows.append(["partnerTenantType", org.get("partnerTenantType", "null")])
                rows.append(["tenantType", org.get("tenantType", "AAD")])

                # Deduplicate provisioned plans for active/warning services
                plans = org.get("provisionedPlans", [])
                plan_services = sorted(list(set(
                    plan.get("service") for plan in plans 
                    if plan.get("service") and str(plan.get("capabilityStatus", "")).lower() in ["enabled", "warning"]
                )))
                plans_str = ", ".join(plan_services) if plan_services else "null"
                rows.append(["provisionedPlans", plans_str])

            elapsed = time.time() - start_time

            def _on_success():
                self.org_card.set_data(columns, rows, execution_time=elapsed)
                if self.org_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.org_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)

        except Exception as e:
            logger.error(f"Error fetching Organization summary: {e}")
            err_msg = str(e)

            def _on_error():
                self.org_card.set_error(f"Failed to fetch organization: {err_msg}")
                if self.org_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.org_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_skus_worker(self, is_reload: bool = False):
        """Fetches Subscribed SKUs from Graph API."""
        start_time = time.time()
        logger.info("Executing Subscribed SKUs fetch task...")
        try:
            client = self._get_client(["Organization.Read.All", "Directory.Read.All"])
            sku_service = SubscribedSKUsService(client)
            sku_data = sku_service.get_subscribed_skus()
            if isinstance(sku_data, dict):
                self.cached_data["skus"] = sku_data.get("value", [])
            elif isinstance(sku_data, list):
                self.cached_data["skus"] = sku_data
            else:
                self.cached_data["skus"] = []
            client.close()

            columns = ["SKU Part Number", "Units", "Consumed Units"]
            rows: List[List[Any]] = []

            items = sku_data.get("value", [])
            for item in items:
                sku_name = item.get("skuPartNumber", "UNKNOWN_SKU")
                prepaid = item.get("prepaidUnits", {})
                enabled_units = prepaid.get("enabled", 0)
                warn_units = prepaid.get("warning", 0)
                susp_units = prepaid.get("suspended", 0)

                units_parts = [f"Enabled: {enabled_units:,}"]
                if warn_units > 0:
                    units_parts.append(f"Warning: {warn_units:,}")
                if susp_units > 0:
                    units_parts.append(f"Suspended: {susp_units:,}")
                units_str = " | ".join(units_parts)

                consumed_str = f"{item.get('consumedUnits', 0):,}"
                rows.append([sku_name, units_str, consumed_str])

            elapsed = time.time() - start_time

            def _on_success():
                self.sku_card.set_data(columns, rows, execution_time=elapsed)
                if self.sku_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.sku_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)

        except Exception as e:
            logger.error(f"Error fetching Subscribed SKUs: {e}")
            err_msg = str(e)

            def _on_error():
                self.sku_card.set_error(f"Failed to fetch SKUs: {err_msg}")
                if self.sku_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.sku_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_domains_worker(self, is_reload: bool = False):
        """Fetches Domains from Graph API."""
        start_time = time.time()
        logger.info("Executing Domains fetch task...")
        try:
            client = self._get_client(["Organization.Read.All", "Directory.Read.All"])
            domains_service = DomainsService(client)
            domains_list = domains_service.get_domains()
            self.cached_data["domains"] = domains_list
            client.close()

            columns = [
                "Domain Name",
                "Authentication Type",
                "Default",
                "Initial",
                "Verified",
                "Supported Services",
                "Federation Display Name",
                "Federation Issuer URI",
            ]
            rows: List[List[Any]] = []

            for d in domains_list:
                services = d.get("supportedServices", [])
                services_str = ", ".join(services) if services else "-"
                fed_disp = d.get("federationDisplayName")
                if not fed_disp or fed_disp == "-":
                    fed_disp = "N/A"
                fed_issuer = d.get("federationIssuerUri")
                if not fed_issuer or fed_issuer == "-":
                    fed_issuer = "N/A"

                rows.append([
                    d.get("id", "Unknown"),
                    d.get("authenticationType", "Managed"),
                    "Yes" if d.get("isDefault") else "No",
                    "Yes" if d.get("isInitial") else "No",
                    "Yes" if d.get("isVerified", True) else "No",
                    services_str,
                    fed_disp,
                    fed_issuer,
                ])

            elapsed = time.time() - start_time

            def _on_success():
                self.domains_card.set_data(columns, rows, execution_time=elapsed)
                if self.domains_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.domains_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)

        except Exception as e:
            logger.error(f"Error fetching Domains: {e}")
            err_msg = str(e)

            def _on_error():
                self.domains_card.set_error(f"Failed to fetch domains: {err_msg}")
                if self.domains_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.domains_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_users_groups_worker(self, is_reload: bool = False):
        """Fetches Users & Groups summary from Graph API."""
        start_time = time.time()
        logger.info("Executing Users & Groups summary fetch task...")
        try:
            client = self._get_client(["Directory.Read.All", "User.Read.All", "Group.Read.All"])
            ug_service = UsersGroupsService(client)
            counts = ug_service.get_users_groups_counts()
            self.cached_data["user_groups"] = counts
            client.close()

            user_c = counts.get("user_counts", {})
            group_c = counts.get("group_counts", {})

            columns = ["Category", "Count"]
            rows: List[List[Any]] = [
                ["Total Users", f"{user_c.get('total', 0):,}"],
                ["Enabled Users", f"{user_c.get('enabled', 0):,}"],
                ["Disabled Users", f"{user_c.get('disabled', 0):,}"],
                ["Member Users", f"{user_c.get('member', 0):,}"],
                ["Guest Users", f"{user_c.get('guest', 0):,}"],
                ["Total Groups", f"{group_c.get('total', 0):,}"],
                ["Microsoft 365 Groups (Unified)", f"{group_c.get('m365', 0):,}"],
                ["Security Groups (Static, non-mail-enabled)", f"{group_c.get('security', 0):,}"],
                ["Mail-enabled Security Groups", f"{group_c.get('mail_enabled_security', 0):,}"],
                ["Distribution Groups", f"{group_c.get('distribution', 0):,}"],
                ["Dynamic Groups (Dynamic Membership)", f"{group_c.get('dynamic', 0):,}"],
            ]

            elapsed = time.time() - start_time

            def _on_success():
                self.users_groups_card.set_data(columns, rows, execution_time=elapsed)
                if self.users_groups_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.users_groups_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)

        except Exception as e:
            logger.error(f"Error fetching Users & Groups: {e}")
            err_msg = str(e)

            def _on_error():
                self.users_groups_card.set_error(f"Failed to fetch users and groups: {err_msg}")
                if self.users_groups_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.users_groups_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_user_logs_worker(self, is_reload: bool = False):
        """Fetches User Creation & Deletion audit logs from Graph API."""
        start_time = time.time()
        logger.info("Executing User Creation & Deletion Logs fetch task...")
        try:
            client = self._get_client(["AuditLog.Read.All", "Directory.Read.All"])
            reports_dir = os.path.join(os.getcwd(), "reports", f"{self.tenant}_{self.client_id}")
            os.makedirs(reports_dir, exist_ok=True)
            csv_path = os.path.join(reports_dir, "directory_user_creation_logs.csv")

            user_logs_service = UserLogsService(client)
            self.cached_data["user_creation_logs"] = []
            collected_rows: List[Dict[str, Any]] = []

            def _on_page(page_items):
                self.cached_data["user_creation_logs"].extend(page_items)
                collected_rows.extend(page_items)

            user_logs_service.fetch_user_creation_logs(
                csv_path=csv_path,
                max_rows=50,
                on_page_callback=_on_page,
            )
            client.close()

            columns = ["Activity", "Initiated By"]
            rows: List[List[Any]] = []

            for item in collected_rows:
                activity = item.get("activity") or ""
                raw_init = item.get("initiatedBy") or ""
                if activity == "ERROR":
                    raise Exception(str(raw_init))
                initiated_by = self._format_initiated_by(raw_init)
                rows.append([activity, initiated_by])

            elapsed = time.time() - start_time

            def _on_success():
                self.user_logs_card.set_data(columns, rows, execution_time=elapsed)
                if self.user_logs_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.user_logs_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)

        except Exception as e:
            logger.error(f"Error fetching User Creation logs: {e}")
            err_msg = str(e)
            if "401" in err_msg or "403" in err_msg or "AuditLog.Read.All" in err_msg:
                err_msg = "AuditLog.Read.All permission required. Please grant 'AuditLog.Read.All' in Microsoft Entra ID App Registration."

            def _on_error():
                self.user_logs_card.set_error(f"Failed to fetch user logs: {err_msg}")
                if self.user_logs_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.user_logs_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_provisioning_logs_worker(self, is_reload: bool = False):
        """Fetches Directory Provisioning Logs from Graph API."""
        start_time = time.time()
        logger.info("Executing Directory Provisioning Logs fetch task...")
        try:
            client = self._get_client(["AuditLog.Read.All", "Directory.Read.All"])
            reports_dir = os.path.join(os.getcwd(), "reports", f"{self.tenant}_{self.client_id}")
            os.makedirs(reports_dir, exist_ok=True)
            csv_path = os.path.join(reports_dir, "directory_provisioning_logs.csv")

            prov_service = ProvisioningLogsService(client)
            self.cached_data["provisioning_logs"] = []
            collected_rows: List[Dict[str, Any]] = []

            def _on_page(page_items):
                self.cached_data["provisioning_logs"].extend(page_items)
                collected_rows.extend(page_items)

            prov_service.fetch_provisioning_logs(
                csv_path=csv_path,
                max_rows=50,
                on_page_callback=_on_page,
            )
            client.close()

            columns = ["Action", "Initiated By", "Target System", "Source System", "Status"]
            rows: List[List[Any]] = []

            for item in collected_rows:
                raw_init = item.get("initiatedBy") or ""
                action = item.get("provisioningAction") or "-"
                if raw_init == "ERROR":
                    raise Exception(str(action))
                
                initiated_by = self._format_initiated_by(raw_init)
                target_sys = item.get("targetSystem") or "-"
                source_sys = item.get("sourceSystem") or "-"
                
                status_raw = item.get("provisioningStatusInfo") or "-"
                status_str = "-"
                if status_raw:
                    try:
                        status_obj = json.loads(status_raw) if isinstance(status_raw, str) else status_raw
                        if isinstance(status_obj, dict):
                            status_str = status_obj.get("status") or status_obj.get("result") or str(status_raw)
                        else:
                            status_str = str(status_raw)
                    except Exception:
                        status_str = str(status_raw)

                rows.append([action, initiated_by, target_sys, source_sys, status_str])

            elapsed = time.time() - start_time

            def _on_success():
                self.provisioning_logs_card.set_data(columns, rows, execution_time=elapsed)
                if self.provisioning_logs_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.provisioning_logs_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)

        except Exception as e:
            logger.error(f"Error fetching Provisioning logs: {e}")
            err_msg = str(e)
            if "401" in err_msg or "403" in err_msg or "AuditLog.Read.All" in err_msg:
                err_msg = "AuditLog.Read.All permission required. Please grant 'AuditLog.Read.All' in Microsoft Entra ID App Registration."

            def _on_error():
                self.provisioning_logs_card.set_error(f"Failed to fetch provisioning logs: {err_msg}")
                if self.provisioning_logs_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.provisioning_logs_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

