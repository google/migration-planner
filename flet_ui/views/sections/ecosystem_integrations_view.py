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

"""Ecosystem, Integrations & Automation section view implementation for Flet UI."""

import time
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional
import flet as ft

from core.graph.client import GraphClient
from core.graph.directory.service_principals import ServicePrincipalsService
from core.graph.exchange.connectors import fetch_exchange_connectors_data
from core.graph.exchange.integrated_apps import run_exchange_apps_pipeline
from telemetry.power_automate import run_power_automate_pipeline
from flet_ui.views.sections.base_section_view import BaseSectionView
from flet_ui.components.telemetry_card import TelemetryCard
from flet_ui.styles import (
    COLOR_PRIMARY,
    COLOR_SURFACE,
    COLOR_TEXT_PRIMARY,
    COLOR_TEXT_SECONDARY,
)

logger = logging.getLogger("M365TelemetryAsyncLogger.EcosystemIntegrationsView")


class EcosystemIntegrationsAutomationView(BaseSectionView):
    """View rendering all Ecosystem, Integrations & Automation telemetry cards with max 2 concurrency."""

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

        # 1. Power Automate Cloud Flows (Paginated listing)
        self.power_automate_card = TelemetryCard(
            title="Power Automate Cloud Flows",
            link_text="Power Automate API",
            link_url="https://learn.microsoft.com/en-us/power-automate/api-overview",
            subtitle="Cloud flow inventory, execution activity, runtime tiers, and connector usage",
            paginate=True,
            page_size=5,
            column_weights=[3, 2, 2, 2, 3],
            on_reload=lambda: self._reload_card(self._fetch_power_automate_worker),
        )

        # 2. Third-Party Apps & OAuth Scopes (Paginated listing)
        self.integrated_apps_card = TelemetryCard(
            title="Third-Party Apps & OAuth Scopes",
            link_text="Integrated Apps API",
            link_url="https://learn.microsoft.com/en-us/graph/api/resources/serviceprincipal",
            subtitle="Enterprise applications, organization-wide add-ins, and delegated consent permissions",
            paginate=True,
            page_size=5,
            column_weights=[3, 2, 2, 2],
            on_reload=lambda: self._reload_card(self._fetch_integrated_apps_worker),
        )

        # 3. Enterprise Service Principals & SSO (Paginated listing)
        self.service_principals_card = TelemetryCard(
            title="Enterprise Service Principals & SSO",
            link_text="Service Principals API",
            link_url="https://learn.microsoft.com/en-us/graph/api/resources/serviceprincipal",
            subtitle="Enterprise single sign-on (SSO) service principals, SAML/OIDC configuration, and tenant apps",
            paginate=True,
            page_size=5,
            column_weights=[3, 3, 2, 2],
            on_reload=lambda: self._reload_card(self._fetch_service_principals_worker),
        )

        # 4. Exchange Connectors & Mail Flow Routing (Paginated listing)
        self.connectors_card = TelemetryCard(
            title="Exchange Connectors & Mail Flow Routing",
            link_text="Exchange Connectors API",
            link_url="https://learn.microsoft.com/en-us/exchange/mail-flow-best-practices/use-connectors-to-configure-mail-flow",
            subtitle="Inbound and outbound hybrid email connectors, smart hosts, and TLS enforcement policies",
            paginate=True,
            page_size=5,
            column_weights=[2, 3, 2, 3, 3],
            on_reload=lambda: self._reload_card(self._fetch_connectors_worker),
        )

        # Register cards with base class for error status tracking
        self.register_cards(
            self.power_automate_card,
            self.integrated_apps_card,
            self.service_principals_card,
            self.connectors_card,
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
                        ft.Icons.HUB_OUTLINED,
                        size=38,
                        color=COLOR_PRIMARY,
                    ),
                ),
                ft.Text(
                    "Ecosystem, Integrations & Automation",
                    size=22,
                    weight=ft.FontWeight.BOLD,
                    color=COLOR_TEXT_PRIMARY,
                ),
                ft.Container(
                    width=520,
                    content=ft.Text(
                        "Review Power Automate cloud flows, registered apps, enterprise single sign-on service principals, and mail flow connectors.",
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
            "Fetching Ecosystem, Integrations & Automation telemetry (0 of 4 completed)...",
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
        self.power_automate_card.set_loading("Fetching Power Automate flows...")
        self.integrated_apps_card.set_loading("Fetching third-party apps & add-ins...")
        self.service_principals_card.set_loading("Fetching enterprise service principals & SSO...")
        self.connectors_card.set_loading("Fetching Exchange connectors...")

        self.content = self.cards_column
        try:
            self.update()
        except Exception:
            pass

        completed_count = 0
        total_tasks = 4

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
                            f"Fetching Ecosystem, Integrations & Automation telemetry ({completed_count} of {total_tasks} completed)..."
                        )
                        try:
                            self.progress_banner.update()
                        except Exception:
                            pass

                    self._safe_run_on_ui(_update_progress)
            return _wrapped

        # Tasks to execute in worker pool (at max 2 concurrent threads)
        tasks = [
            ("PowerAutomate", _track_task_wrapper(self._fetch_power_automate_worker)),
            ("IntegratedApps", _track_task_wrapper(self._fetch_integrated_apps_worker)),
            ("ServicePrincipals", _track_task_wrapper(self._fetch_service_principals_worker)),
            ("Connectors", _track_task_wrapper(self._fetch_connectors_worker)),
        ]

        def _orchestrator():
            logger.info("Starting Ecosystem, Integrations & Automation fetch orchestrator with ThreadPoolExecutor(max_workers=2)")
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
            retries=3,
            backoff=2,
        )
        client.authenticate(required_scopes=scopes or ["Directory.Read.All", "Application.Read.All"])
        return client

    # --- Telemetry Worker Pipelines ---

    def _fetch_power_automate_worker(self, is_reload: bool = False):
        """Fetches Power Automate Cloud and Desktop flow counts and tier distribution."""
        start_time = time.time()
        logger.info("Executing Power Automate telemetry fetch task...")
        try:
            results = run_power_automate_pipeline(self.client_id, self.secret, self.tenant)
            if not isinstance(results, dict):
                raise Exception("Failed to retrieve Power Automate flow data from tenant.")

            errs = results.get("errors", [])
            if errs:
                raise Exception("; ".join(errs))

            columns = ["Flow Category / Name", "Type", "Tier", "Active Status", "Complexity / Connectors"]
            rows: List[List[Any]] = []

            counts = results.get("counts", {})
            active_counts = results.get("active_counts", {})
            tier_counts = results.get("tier_counts", {})
            premium_conns = results.get("premium_connectors", [])
            custom_conns = results.get("custom_connectors", [])

            cloud_total = counts.get("Cloud Flows", 0)
            cloud_active = active_counts.get("Cloud Flows", 0)
            desktop_total = counts.get("Desktop Flows", 0)
            desktop_active = active_counts.get("Desktop Flows", 0)

            if cloud_total > 0 or desktop_total > 0 or tier_counts.get("Personal Productivity", 0) > 0 or tier_counts.get("Enterprise/Departmental", 0) > 0:
                rows.append([
                    "Automated Cloud Flows",
                    "Cloud Flow",
                    "Personal / Enterprise",
                    f"{cloud_active:,} Active / {cloud_total:,} Total",
                    f"Premium: {len(premium_conns)} ({', '.join(premium_conns[:2]) or 'None'})",
                ])
                rows.append([
                    "Desktop & RPA Flows",
                    "Desktop Flow",
                    "Enterprise",
                    f"{desktop_active:,} Active / {desktop_total:,} Total",
                    f"Custom: {len(custom_conns)} ({', '.join(custom_conns[:2]) or 'None'})",
                ])
                rows.append([
                    "Personal Productivity Flows",
                    "Cloud Flow",
                    "Personal",
                    f"{tier_counts.get('Personal Productivity', 0):,} Configured",
                    "Standard M365 Connectors",
                ])
                rows.append([
                    "Enterprise Departmental Flows",
                    "Cloud / Solution",
                    "Enterprise",
                    f"{tier_counts.get('Enterprise/Departmental', 0):,} Configured",
                    "Dataverse / Multi-Connector",
                ])

            elapsed = time.time() - start_time

            def _on_success():
                self.power_automate_card.set_data(columns, rows, execution_time=elapsed)
                if self.power_automate_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.power_automate_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)

        except Exception as e:
            logger.error(f"Error fetching Power Automate telemetry: {e}")
            err_msg = str(e)

            def _on_error():
                self.power_automate_card.set_error(f"Failed to fetch Power Automate: {err_msg}")
                if self.power_automate_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.power_automate_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_integrated_apps_worker(self, is_reload: bool = False):
        """Fetches third-party integrated apps and organization add-ins."""
        start_time = time.time()
        logger.info("Executing Third-Party Integrated Apps fetch task...")
        try:
            data = run_exchange_apps_pipeline(self.client_id, self.secret, self.tenant)
            if not isinstance(data, dict):
                raise Exception("Failed to retrieve integrated apps data.")

            if data.get("powershell_error"):
                raise Exception(f"Exchange PowerShell query failed: {data['powershell_error']}")
            if data.get("AppsError"):
                raise Exception(f"Apps query error: {data['AppsError']}")
            if data.get("error"):
                raise Exception(str(data["error"]))

            columns = ["Application / Add-in Name", "Publisher / Type", "Organization Scope", "Status"]
            rows: List[List[Any]] = []

            org_apps = data.get("OrganizationApps", [])
            if isinstance(org_apps, list):
                for a in org_apps:
                    if isinstance(a, dict):
                        name = a.get("DisplayName") or a.get("Name") or "N/A"
                        pub = a.get("Publisher") or a.get("AppType") or "Microsoft / Third-Party"
                        scope = a.get("Scope") or "Tenant-Wide"
                        stat = "Enabled" if str(a.get("Enabled", True)).lower() in ["true", "enabled", "1"] else "Disabled"
                        rows.append([name, pub, scope, stat])

            elapsed = time.time() - start_time

            def _on_success():
                self.integrated_apps_card.set_data(columns, rows, execution_time=elapsed)
                if self.integrated_apps_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.integrated_apps_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)

        except Exception as e:
            logger.error(f"Error fetching Third-Party Integrated Apps: {e}")
            err_msg = str(e)

            def _on_error():
                self.integrated_apps_card.set_error(f"Failed to fetch Third-Party Apps: {err_msg}")
                if self.integrated_apps_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.integrated_apps_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_service_principals_worker(self, is_reload: bool = False):
        """Fetches enterprise service principals and Single Sign-On configurations."""
        start_time = time.time()
        logger.info("Executing Enterprise Service Principals & SSO fetch task...")
        try:
            client = self._get_client(["Directory.Read.All", "Application.Read.All"])
            sp_service = ServicePrincipalsService(client)

            collected_sps = []
            def _on_page(sps):
                collected_sps.extend(sps)

            sp_service.fetch_service_principals_sso(on_page_callback=_on_page)
            client.close()
            
            self.cached_data["service_principals_sso"] = collected_sps

            columns = ["Display Name", "App ID", "Single Sign-On Mode", "Account State"]
            rows: List[List[Any]] = []

            for sp in collected_sps:
                name = sp.get("displayName") or "N/A"
                app_id = sp.get("appId") or "N/A"
                sso_mode = sp.get("preferredSingleSignOnMode") or "OIDC / OAuth"
                account_state = "Active" if sp.get("accountEnabled", True) else "Disabled"
                rows.append([name, app_id, sso_mode, account_state])

            elapsed = time.time() - start_time

            def _on_success():
                self.service_principals_card.set_data(columns, rows, execution_time=elapsed)
                if self.service_principals_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.service_principals_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)

        except Exception as e:
            logger.error(f"Error fetching Enterprise Service Principals & SSO: {e}")
            err_msg = str(e)

            def _on_error():
                self.service_principals_card.set_error(f"Failed to fetch Service Principals: {err_msg}")
                if self.service_principals_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.service_principals_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_connectors_worker(self, is_reload: bool = False):
        """Fetches Exchange Online inbound and outbound mail flow connectors."""
        start_time = time.time()
        logger.info("Executing Exchange Connectors & Mail Flow fetch task...")
        try:
            data = fetch_exchange_connectors_data(self.client_id, self.secret, self.tenant)
            if not isinstance(data, dict):
                raise Exception("Failed to retrieve Exchange connectors data.")

            if data.get("error"):
                raise Exception(f"Exchange Connectors retrieval failed: {data['error']}")

            conns_dict = data.get("connectors") or {}
            ps_errs = conns_dict.get("Errors") or {}
            if ps_errs:
                raise Exception("PowerShell Execution Error: " + "; ".join(f"{k}: {v}" for k, v in ps_errs.items()))

            columns = ["Direction", "Connector Name", "Status", "Target Domains", "Routing & Security"]
            rows: List[List[Any]] = []

            inbound = conns_dict.get("InboundConnectors", [])
            outbound = conns_dict.get("OutboundConnectors", [])

            for conn in inbound:
                name = conn.get("Name", "N/A")
                status = "Enabled" if conn.get("Enabled") else "Disabled"
                domains = str(conn.get("SenderDomains") or "All External Domains")
                routing = f"Type: {conn.get('ConnectorType', 'Partner')} | TLS: {'Required' if conn.get('RequireTls') else 'Optional'}"
                rows.append(["📥 Inbound", name, status, domains, routing])

            for conn in outbound:
                name = conn.get("Name", "N/A")
                status = "Enabled" if conn.get("Enabled") else "Disabled"
                domains = str(conn.get("RecipientDomains") or "All External Domains")
                routing = f"SmartHosts: {conn.get('SmartHosts') or 'MX Routing'} | TLS: {'Enforced' if conn.get('TlsDomain') or conn.get('UseMxRecord') else 'Standard'}"
                rows.append(["📤 Outbound", name, status, domains, routing])

            elapsed = time.time() - start_time

            def _on_success():
                self.connectors_card.set_data(columns, rows, execution_time=elapsed)
                if self.connectors_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.connectors_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)

        except Exception as e:
            logger.error(f"Error fetching Exchange Connectors: {e}")
            err_msg = str(e)

            def _on_error():
                self.connectors_card.set_error(f"Failed to fetch Exchange Connectors: {err_msg}")
                if self.connectors_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.connectors_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)
