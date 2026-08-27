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

import os
import csv
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional
import flet as ft

from core.graph.client import GraphClient
from core.graph.directory.service_principals import ServicePrincipalsService
from core.graph.exchange.connectors import fetch_exchange_connectors_data
from core.graph.exchange.integrated_apps import run_exchange_apps_pipeline
from core.graph.entra.app_registrations import run_app_registrations_pipeline
from core.graph.entra.app_signins import run_app_signins_pipeline
from core.graph.entra.user_signins import run_user_signins_pipeline
from core.graph.entra.auth_methods import run_auth_methods_pipeline
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
    """View rendering all 7 Ecosystem, Integrations & Automation telemetry cards matching deal_assistant.py."""

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

        # 4.1. Power Automate (Metric / Value summary)
        self.power_automate_card = TelemetryCard(
            title="Power Automate",
            link_text="Open Power Platform Admin Center ↗",
            link_url="https://admin.powerplatform.microsoft.com",
            subtitle="Tenant environments, automated cloud flow counts, and custom connector utilization",
            paginate=False,
            column_weights=[1, 1],
            on_reload=lambda: self._reload_card(self._fetch_power_automate_worker),
        )

        # 4.2. Third-Party Apps & OAuth Scopes (DisplayName / Enabled)
        self.integrated_apps_card = TelemetryCard(
            title="Integrated Apps",
            link_text="Open Microsoft 365 Admin Center ↗",
            link_url="https://admin.microsoft.com/#/Settings/IntegratedApps",
            subtitle="Organization-wide add-ins and integrated applications",
            paginate=True,
            page_size=5,
            column_weights=[3, 1],
            on_reload=lambda: self._reload_card(self._fetch_integrated_apps_worker),
        )

        # 4.3. Enterprise Service Principals & SSO (SSO Mode / Application Count)
        self.service_principals_card = TelemetryCard(
            title="Enterprise Service Principals & SSO",
            link_text="Open Enterprise Applications ↗",
            link_url="https://entra.microsoft.com/#view/Microsoft_AAD_IAM/StartboardApplicationsMenuBlade/~/AppAppsPreview",
            subtitle="Enterprise Single Sign-On (SSO) configuration distribution",
            paginate=False,
            column_weights=[1, 1],
            on_reload=lambda: self._reload_card(self._fetch_service_principals_worker),
        )

        # 4.4. Exchange Connectors & Mail Flow Routing (Direction / Connector Name / Status / Target Domains / Routing & Security)
        self.connectors_card = TelemetryCard(
            title="Exchange Connectors & Mail Flow Routing",
            link_text="Open Exchange Admin Center ↗",
            link_url="https://admin.cloud.microsoft/exchange?#/connectors",
            subtitle="Inbound and outbound hybrid email connectors, smart hosts, and TLS enforcement policies",
            paginate=True,
            page_size=5,
            column_weights=[2, 3, 2, 3, 3],
            on_reload=lambda: self._reload_card(self._fetch_connectors_worker),
        )

        # 4.5. App Registrations (App Name / Application ID / Created Date / Sign In Audience / Credentials)
        self.app_registrations_card = TelemetryCard(
            title="App Registrations",
            link_text="Open App Registrations ↗",
            link_url="https://entra.microsoft.com/#view/Microsoft_AAD_IAM/ActiveDirectoryMenuBlade/~/RegisteredApps",
            subtitle="Registered applications, API permissions, and credential security counts",
            paginate=True,
            page_size=5,
            column_weights=[3, 3, 2, 2, 2],
            on_reload=lambda: self._reload_card(self._fetch_app_registrations_worker),
        )

        # 4.6. App Sign-in Activity (App Name / Successful Sign Ins)
        self.app_signins_card = TelemetryCard(
            title="App Sign-in Activity",
            link_text="Open Sign-in Logs ↗",
            link_url="https://entra.microsoft.com/#view/Microsoft_AAD_IAM/SignInsMenuBlade/~/ServicePrincipalSignIns",
            subtitle="Azure AD application sign-in summary for the past 7 days",
            paginate=True,
            page_size=5,
            column_weights=[3, 1],
            on_reload=lambda: self._reload_card(self._fetch_app_signins_worker),
        )

        # 4.7. User Sign-in Activity (Sign-in Attribute / Successful Unique Values)
        self.user_signins_card = TelemetryCard(
            title="User Sign-in Activity",
            link_text="Open User Sign-in Logs ↗",
            link_url="https://entra.microsoft.com/#view/Microsoft_AAD_IAM/SignInsMenuBlade/~/UserSignIns",
            subtitle="Unique applications, operating systems, and browsers seen in recent sign-in telemetry",
            paginate=False,
            column_weights=[1, 3],
            on_reload=lambda: self._reload_card(self._fetch_user_signins_worker),
        )

        # 4.8. Authentication Methods (Authentication Method / Success Activity Count (7 days))
        self.auth_methods_card = TelemetryCard(
            title="Authentication Methods",
            link_text="Open Authentication Methods ↗",
            link_url="https://entra.microsoft.com/#view/Microsoft_AAD_IAM/AuthenticationMethodsMenuBlade/~/AuthMethods",
            subtitle="User sign-in activity breakdown by authentication method for the past 7 days",
            paginate=True,
            page_size=5,
            column_weights=[3, 1],
            on_reload=lambda: self._reload_card(self._fetch_auth_methods_worker),
        )

        # Register all 8 cards with base class for status tracking
        self.register_cards(
            self.power_automate_card,
            self.integrated_apps_card,
            self.service_principals_card,
            self.connectors_card,
            self.app_registrations_card,
            self.app_signins_card,
            self.user_signins_card,
            self.auth_methods_card,
        )

        # Initial Placeholder State
        self.placeholder = self._build_placeholder()

        self.content = ft.Container(
            expand=True,
            content=self.placeholder,
        )

    def _get_reports_dir_and_db(self) -> tuple[str, str]:
        """Returns reports directory path and sqlite database path."""
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        reports_dir = os.path.join(base_dir, "telemetry", "reports", f"{self.tenant}_{self.client_id}")
        os.makedirs(reports_dir, exist_ok=True)
        db_path = os.path.join(reports_dir, "telemetry_cache.db")
        return reports_dir, db_path

    def _cache_to_sqlite_safe(self, csv_path: str, db_path: str, table_name: str):
        """Asynchronously imports CSV into SQLite without blocking UI."""
        if not csv_path or not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
            return
        try:
            from core.graph.db import import_csv_to_sqlite
            import asyncio
            asyncio.run(import_csv_to_sqlite(csv_path, db_path, table_name))
        except Exception as e:
            logger.debug(f"Non-fatal error caching {table_name} to SQLite: {e}")

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
                        "Review Power Automate cloud flows, registered apps, enterprise single sign-on service principals, and sign-in activity logs.",
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
        """Initiates concurrent data fetch across all 8 cards with maximum 2 parallel worker threads."""
        if self.is_fetching:
            return

        self.is_fetching = True
        self.is_fetched = True
        self._notify_status("loading")

        self.cards_column.controls.clear()

        total_tasks = 8

        self.progress_bar = ft.ProgressBar(
            value=0.0,
            width=float("inf"),
            height=6,
            color="#15803D",
            bgcolor="#DCFCE7",
            border_radius=3,
        )
        self.progress_text = ft.Text(
            f"Fetching Ecosystem, Integrations & Automation telemetry (0 of {total_tasks} completed)...",
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
        self.integrated_apps_card.set_loading("Fetching integrated apps & add-ins...")
        self.service_principals_card.set_loading("Fetching enterprise service principals & SSO...")
        self.connectors_card.set_loading("Fetching Exchange connectors...")
        self.app_registrations_card.set_loading("Fetching app registrations...")
        self.app_signins_card.set_loading("Fetching app sign-in summary...")
        self.user_signins_card.set_loading("Fetching user sign-in activity...")
        self.auth_methods_card.set_loading("Fetching authentication methods...")

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
                            f"Fetching Ecosystem, Integrations & Automation telemetry ({completed_count} of {total_tasks} completed)..."
                        )
                        try:
                            self.progress_banner.update()
                        except Exception:
                            pass

                    self._safe_run_on_ui(_update_progress)
            return _wrapped

        # 8 Tasks to execute in worker pool (at max 2 concurrent threads)
        tasks = [
            ("PowerAutomate", _track_task_wrapper(self._fetch_power_automate_worker)),
            ("IntegratedApps", _track_task_wrapper(self._fetch_integrated_apps_worker)),
            ("ServicePrincipals", _track_task_wrapper(self._fetch_service_principals_worker)),
            ("Connectors", _track_task_wrapper(self._fetch_connectors_worker)),
            ("AppRegistrations", _track_task_wrapper(self._fetch_app_registrations_worker)),
            ("AppSignins", _track_task_wrapper(self._fetch_app_signins_worker)),
            ("UserSignins", _track_task_wrapper(self._fetch_user_signins_worker)),
            ("AuthMethods", _track_task_wrapper(self._fetch_auth_methods_worker)),
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

    def _generate_power_automate_chart(
        self,
        cloud_active: int,
        cloud_inactive: int,
        desktop_active: int,
        desktop_inactive: int,
        personal_active: int,
        personal_inactive: int,
        enterprise_active: int,
        enterprise_inactive: int,
        complex_active: int,
        complex_inactive: int,
    ) -> Optional[str]:
        """Generates high-resolution base64 PNG for Power Automate flows breakdown chart."""
        try:
            import io
            import base64
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt

            categories = ['Cloud Flows', 'Desktop Flows', 'Personal Flows', 'Enterprise Flows', 'Complex Flows']
            actives = [cloud_active, desktop_active, personal_active, enterprise_active, complex_active]
            inactives = [cloud_inactive, desktop_inactive, personal_inactive, enterprise_inactive, complex_inactive]

            fig, ax = plt.subplots(figsize=(10.5, 3.8), dpi=150)
            fig.patch.set_facecolor('#FFFFFF')
            ax.set_facecolor('#FFFFFF')

            x = list(range(len(categories)))
            width = 0.24
            shift = 0.13

            color_active = "#0B57D0"      # Google Primary Blue
            color_inactive = "#D3E3FD"    # Tonal Light Blue
            color_text = "#0F172A"        # Slate 900
            color_sub = "#64748B"         # Slate 500
            color_grid = "#F1F5F9"        # Slate 100

            rects1 = ax.bar([i - shift for i in x], actives, width, label='Active', color=color_active, edgecolor='none', zorder=3)
            rects2 = ax.bar([i + shift for i in x], inactives, width, label='Inactive', color=color_inactive, edgecolor='none', zorder=3)

            ax.set_title('Power Automate Flows Breakdown', fontsize=12, fontweight='bold', color=color_text, pad=16)
            ax.set_ylabel('Count', fontsize=10, fontweight='bold', color=color_text, labelpad=8)
            ax.set_xticks(x)
            ax.set_xticklabels(categories, fontsize=9.5, fontweight='bold', color=color_text)

            ax.yaxis.grid(True, linestyle='-', linewidth=0.8, color=color_grid, zorder=0)
            ax.xaxis.grid(False)

            legend = ax.legend(
                loc='upper right',
                frameon=True,
                facecolor='#FFFFFF',
                edgecolor='#E2E8F0',
                fontsize=8.5,
                framealpha=0.95
            )
            for text in legend.get_texts():
                text.set_color(color_text)
                text.set_fontweight('bold')

            for rect in rects1:
                height = rect.get_height()
                ax.annotate(
                    f"{int(height)}",
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom',
                    fontsize=8.5,
                    fontweight='bold',
                    color=color_text
                )

            for rect in rects2:
                height = rect.get_height()
                ax.annotate(
                    f"{int(height)}",
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom',
                    fontsize=8.5,
                    fontweight='bold',
                    color=color_text
                )

            for spine in ax.spines.values():
                spine.set_color('#E2E8F0')
                spine.set_linewidth(1.0)
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)

            max_val = max(max(actives, default=0), max(inactives, default=0))
            top_limit = max(max_val + 2, int(max_val * 1.25)) if max_val > 0 else 5
            ax.set_ylim(0, top_limit)
            ax.tick_params(axis='y', colors=color_sub, labelsize=8.5)
            ax.tick_params(axis='x', colors=color_text, length=0)

            fig.tight_layout()

            buf = io.BytesIO()
            fig.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            plt.close(fig)
            buf.seek(0)
            return base64.b64encode(buf.read()).decode('utf-8')
        except Exception as e:
            logger.error(f"Error generating Power Automate chart: {e}", exc_info=True)
            return None

    def _fetch_power_automate_worker(self, is_reload: bool = False):
        """4.1. Power Automate (Metric / Value summary matching deal_assistant.py)."""
        start_time = time.time()
        logger.info("Executing Power Automate telemetry fetch task...")
        reports_dir, db_path = self._get_reports_dir_and_db()
        csv_path = os.path.join(reports_dir, "power_automate_summary.csv")

        try:
            results = run_power_automate_pipeline(self.client_id, self.secret, self.tenant)
            if not isinstance(results, dict):
                raise Exception("Failed to retrieve Power Automate flow data from tenant.")

            self.cached_data["power_automate"] = results

            errs = results.get("errors", [])
            if errs:
                raise Exception("; ".join(errs))

            columns = ["Metric", "Value"]
            total_envs = results.get("total_environments", 0)
            counts = results.get("counts", {})
            active_counts = results.get("active_counts", {})
            tier_counts = results.get("tier_counts", {})
            active_tier_counts = results.get("active_tier_counts", {})
            premium_conns = results.get("premium_connectors", [])
            custom_conns = results.get("custom_connectors", [])
            total_flows = counts.get("Cloud Flows", 0) + counts.get("Desktop Flows", 0)

            prem_str = ", ".join(premium_conns) if premium_conns else "0"
            cust_str = ", ".join(custom_conns) if custom_conns else "0"

            rows = [
                ["Total Environments Scanned", str(total_envs)],
                ["Total Flows (Active + Inactive)", str(total_flows)],
                ["Premium Connectors In Use", prem_str],
                ["Custom Connectors In Use", cust_str],
            ]

            try:
                with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(columns)
                    for r in rows:
                        writer.writerow(r)
                self._cache_to_sqlite_safe(csv_path, db_path, "power_automate")
            except Exception as ce:
                logger.warning(f"Error caching Power Automate CSV: {ce}")

            # Flow Category Counts for Breakdown Chart
            c_total = counts.get("Cloud Flows", 0)
            c_active = active_counts.get("Cloud Flows", 0)
            c_inactive = max(0, c_total - c_active)

            d_total = counts.get("Desktop Flows", 0)
            d_active = active_counts.get("Desktop Flows", 0)
            d_inactive = max(0, d_total - d_active)

            p_total = tier_counts.get("Personal Productivity", 0)
            p_active = active_tier_counts.get("Personal Productivity", 0)
            p_inactive = max(0, p_total - p_active)

            e_total = tier_counts.get("Enterprise/Departmental", 0)
            e_active = active_tier_counts.get("Enterprise/Departmental", 0)
            e_inactive = max(0, e_total - e_active)

            complex_active = results.get("complex_active_count", 0)
            complex_inactive = results.get("complex_inactive_count", 0)

            chart_base64 = self._generate_power_automate_chart(
                cloud_active=c_active,
                cloud_inactive=c_inactive,
                desktop_active=d_active,
                desktop_inactive=d_inactive,
                personal_active=p_active,
                personal_inactive=p_inactive,
                enterprise_active=e_active,
                enterprise_inactive=e_inactive,
                complex_active=complex_active,
                complex_inactive=complex_inactive,
            )

            footnotes = ft.Column(
                spacing=4,
                controls=[
                    ft.Text(
                        "* Premium Connectors: Identified when the API tier is flagged as 'Premium' by the Power Platform API or the connector identifier contains known enterprise keywords (shared_sql, shared_httpaction, shared_salesforce, shared_oracle, shared_sap). Displays the count of distinct premium connectors across all scanned flows.",
                        size=11,
                        italic=True,
                        color=COLOR_TEXT_SECONDARY,
                    ),
                    ft.Text(
                        "* Custom Connectors: Identified when the API resource ID contains 'custom' or the API entity type is 'Microsoft.PowerApps/apis/custom'. Displays the count of distinct custom connectors across all scanned flows.",
                        size=11,
                        italic=True,
                        color=COLOR_TEXT_SECONDARY,
                    ),
                ],
            )

            extra_controls: List[ft.Control] = [
                ft.Container(
                    padding=ft.Padding(0, 4, 0, 0),
                    content=footnotes,
                )
            ]

            if chart_base64:
                chart_img = ft.Image(
                    src=f"data:image/png;base64,{chart_base64}",
                    fit=ft.BoxFit.CONTAIN,
                    width=950,
                    height=320,
                )
                chart_container = ft.Container(
                    bgcolor=COLOR_SURFACE,
                    border=ft.Border.all(1, "#E2E8F0"),
                    border_radius=8,
                    padding=ft.Padding(12, 12, 12, 12),
                    alignment=ft.alignment.Alignment(0, 0),
                    content=chart_img,
                )
                extra_controls.append(chart_container)

            extra_content = ft.Column(
                spacing=12,
                controls=extra_controls,
            )

            elapsed = time.time() - start_time

            def _on_success():
                self.power_automate_card.set_data(columns, rows, execution_time=elapsed)
                self.power_automate_card.set_extra_content(extra_content)
                if self.power_automate_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.power_automate_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)

        except Exception as e:
            logger.error(f"Error fetching Power Automate telemetry: {e}", exc_info=True)
            err_msg = str(e)
            if "401" in err_msg or "403" in err_msg or "unauthorized" in err_msg.lower() or "forbidden" in err_msg.lower():
                err_msg = (
                    "Power Platform Admin / Dataverse permissions required.\n"
                    "Register the App Registration via PowerShell:\n"
                    f"New-PowerAppManagementApp -ApplicationId \"{self.client_id}\"\n"
                    "and assign the 'System Administrator' security role in target environments."
                )

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
        """4.2. Third-Party Apps & OAuth Scopes (DisplayName / Enabled matching deal_assistant.py)."""
        start_time = time.time()
        logger.info("Executing Third-Party Integrated Apps fetch task...")
        reports_dir, db_path = self._get_reports_dir_and_db()
        csv_path = os.path.join(reports_dir, "exchange_organization_apps.csv")

        try:
            data = run_exchange_apps_pipeline(self.client_id, self.secret, self.tenant)
            if not isinstance(data, dict):
                raise Exception("Failed to retrieve integrated apps data.")

            self.cached_data["integrated_apps"] = data

            if data.get("powershell_error"):
                raise Exception(f"Exchange PowerShell query failed: {data['powershell_error']}")
            if data.get("AppsError"):
                raise Exception(f"Apps query error: {data['AppsError']}")
            if data.get("error"):
                raise Exception(str(data["error"]))

            columns = ["DisplayName", "Enabled"]
            rows: List[List[Any]] = []

            org_apps = data.get("OrganizationApps", [])
            if isinstance(org_apps, list):
                for a in org_apps:
                    if isinstance(a, dict):
                        name = a.get("DisplayName") or a.get("Name") or "-"
                        stat = "Enabled" if str(a.get("Enabled", True)).lower() in ["true", "enabled", "1"] else "Disabled"
                        rows.append([name, stat])

            try:
                with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(["DisplayName", "Enabled"])
                    for r in rows:
                        writer.writerow(r)
                self._cache_to_sqlite_safe(csv_path, db_path, "exchange_organization_apps")
            except Exception as ce:
                logger.warning(f"Error caching integrated apps to CSV: {ce}")

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
            logger.error(f"Error fetching Third-Party Integrated Apps: {e}", exc_info=True)
            err_msg = str(e)
            if "integrated_apps" not in self.cached_data:
                self.cached_data["integrated_apps"] = {"AppsError": err_msg}
            if "pwsh" in err_msg.lower() or "powershell" in err_msg.lower():
                err_msg = "PowerShell Core ('pwsh') is not installed or not available in PATH."
            elif "module" in err_msg.lower():
                err_msg = "ExchangeOnlineManagement PowerShell module is missing."

            def _on_error():
                self.integrated_apps_card.set_error(f"Failed to fetch Integrated Apps: {err_msg}")
                if self.integrated_apps_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.integrated_apps_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_service_principals_worker(self, is_reload: bool = False):
        """4.3. Enterprise Service Principals & SSO (SSO Mode / Application Count matching deal_assistant.py)."""
        start_time = time.time()
        logger.info("Executing Enterprise Service Principals & SSO fetch task...")
        reports_dir, db_path = self._get_reports_dir_and_db()
        csv_path = os.path.join(reports_dir, "service_principals_sso.csv")

        try:
            client = self._get_client(["Directory.Read.All", "Application.Read.All"])
            sp_service = ServicePrincipalsService(client)

            collected_sps = []
            def _on_page(sps):
                collected_sps.extend(sps)

            sp_service.fetch_service_principals_sso(csv_path=csv_path, on_page_callback=_on_page)
            client.close()

            self._cache_to_sqlite_safe(csv_path, db_path, "service_principals_sso")
            self.cached_data["service_principals_sso"] = collected_sps

            saml = 0
            oidc = 0
            password = 0
            none_count = 0

            for sp in collected_sps:
                m = str(sp.get("preferredSingleSignOnMode") or "").lower()
                if m == "saml":
                    saml += 1
                elif m == "oidc":
                    oidc += 1
                elif m == "password":
                    password += 1
                else:
                    none_count += 1

            columns = ["SSO Mode", "Application Count"]
            rows = [
                ["SAML", str(saml)],
                ["OIDC", str(oidc)],
                ["Password", str(password)],
                ["Null / Not Supported", str(none_count)],
            ]

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
            logger.error(f"Error fetching Enterprise Service Principals & SSO: {e}", exc_info=True)
            err_msg = str(e)
            if "401" in err_msg or "403" in err_msg or "permission" in err_msg.lower() or "unauthorized" in err_msg.lower():
                err_msg = "Application.Read.All application permission required in Microsoft Entra."

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
        """4.4. Exchange Connectors & Mail Flow Routing (Direction / Name / Status / Domains / Routing matching deal_assistant.py)."""
        start_time = time.time()
        logger.info("Executing Exchange Connectors & Mail Flow fetch task...")
        reports_dir, db_path = self._get_reports_dir_and_db()
        inbound_path = os.path.join(reports_dir, "exchange_inbound_connectors.csv")
        outbound_path = os.path.join(reports_dir, "exchange_outbound_connectors.csv")

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

            flat_connectors = []
            if isinstance(inbound, list):
                for c in inbound:
                    c_copy = dict(c)
                    c_copy["Direction"] = "Inbound"
                    flat_connectors.append(c_copy)
            if isinstance(outbound, list):
                for c in outbound:
                    c_copy = dict(c)
                    c_copy["Direction"] = "Outbound"
                    flat_connectors.append(c_copy)
            self.cached_data["exchange_connectors"] = flat_connectors

            for conn in inbound:
                name = conn.get("Name", "N/A")
                status = "Enabled" if conn.get("Enabled") else "Disabled"
                domains = str(conn.get("SenderDomains") or "All External Domains")
                routing = f"Type: {conn.get('ConnectorType', 'N/A')}\nRequire TLS: {'Yes' if conn.get('RequireTls') else 'No'}"
                rows.append(["Inbound", name, status, domains, routing])

            for conn in outbound:
                name = conn.get("Name", "N/A")
                status = "Enabled" if conn.get("Enabled") else "Disabled"
                domains = str(conn.get("RecipientDomains") or "All External Domains")
                routing = f"SmartHosts: {conn.get('SmartHosts', 'N/A')}\nUse MX: {'Yes' if conn.get('UseMxRecord') else 'No'}"
                rows.append(["Outbound", name, status, domains, routing])

            try:
                with open(inbound_path, 'w', encoding='utf-8', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(["Name", "Enabled", "SenderDomains", "ConnectorType", "RequireTls"])
                    for c in inbound:
                        writer.writerow([c.get("Name"), c.get("Enabled"), c.get("SenderDomains"), c.get("ConnectorType"), c.get("RequireTls")])
                self._cache_to_sqlite_safe(inbound_path, db_path, "inbound_connectors")

                with open(outbound_path, 'w', encoding='utf-8', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(["Name", "Enabled", "RecipientDomains", "SmartHosts", "UseMxRecord"])
                    for c in outbound:
                        writer.writerow([c.get("Name"), c.get("Enabled"), c.get("RecipientDomains"), c.get("SmartHosts"), c.get("UseMxRecord")])
                self._cache_to_sqlite_safe(outbound_path, db_path, "outbound_connectors")
            except Exception as ce:
                logger.warning(f"Error caching Exchange Connectors to CSV/SQLite: {ce}")

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
            logger.error(f"Error fetching Exchange Connectors: {e}", exc_info=True)
            err_msg = str(e)
            if "pwsh" in err_msg.lower() or "powershell" in err_msg.lower():
                err_msg = "PowerShell Core ('pwsh') is not installed or not available in PATH."
            elif "exchangeonlinemanagement" in err_msg.lower() or "module" in err_msg.lower():
                err_msg = "ExchangeOnlineManagement PowerShell module is missing."

            def _on_error():
                self.connectors_card.set_error(f"Failed to fetch Exchange Connectors: {err_msg}")
                if self.connectors_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.connectors_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_app_registrations_worker(self, is_reload: bool = False):
        """4.5. App Registrations (App Name / Application ID / Created Date / Sign In Audience / Credentials matching deal_assistant.py)."""
        start_time = time.time()
        logger.info("Executing App Registrations fetch task...")
        reports_dir, db_path = self._get_reports_dir_and_db()
        csv_path = os.path.join(reports_dir, "entra_app_registrations.csv")

        try:
            columns = ["App Name", "Application ID", "Created Date", "Sign In Audience", "Credentials"]
            rows: List[List[Any]] = []
            collected_apps: List[Dict[str, Any]] = []

            def _on_page(page_items):
                collected_apps.extend(page_items)

            with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["displayName", "appId", "createdDateTime", "signInAudience", "credentials"])

            run_app_registrations_pipeline(
                client_id=self.client_id,
                client_secret=self.secret,
                tenant_id=self.tenant,
                csv_path=csv_path,
                max_rows=5000,
                on_page_callback=_on_page,
            )

            self._cache_to_sqlite_safe(csv_path, db_path, "app_registrations")
            self.cached_data["app_registrations"] = collected_apps

            for app in collected_apps:
                name = app.get("displayName") or ""
                app_id = app.get("appId") or ""
                created = (app.get("createdDateTime") or "")[:10]
                audience = app.get("signInAudience") or ""
                secrets_cnt = len(app.get("passwordCredentials", []))
                certs_cnt = len(app.get("keyCredentials", []))
                creds_str = f"{secrets_cnt} Secrets, {certs_cnt} Certs"
                rows.append([name, app_id, created, audience, creds_str])

            elapsed = time.time() - start_time

            def _on_success():
                self.app_registrations_card.set_data(columns, rows, execution_time=elapsed)
                if self.app_registrations_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.app_registrations_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)

        except Exception as e:
            logger.error(f"Error fetching App Registrations: {e}", exc_info=True)
            err_msg = str(e)
            if "401" in err_msg or "403" in err_msg or "permission" in err_msg.lower() or "unauthorized" in err_msg.lower():
                err_msg = "Application.Read.All application permission required in Microsoft Entra."

            def _on_error():
                self.app_registrations_card.set_error(f"Failed to fetch App Registrations: {err_msg}")
                if self.app_registrations_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.app_registrations_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_app_signins_worker(self, is_reload: bool = False):
        """4.6. App Sign-in Activity (App Name / Successful Sign Ins matching deal_assistant.py)."""
        start_time = time.time()
        logger.info("Executing App Sign-in Activity fetch task...")
        reports_dir, db_path = self._get_reports_dir_and_db()
        csv_path = os.path.join(reports_dir, "entra_app_signins.csv")

        try:
            columns = ["App Name", "Successful Sign Ins"]
            rows: List[List[Any]] = []
            collected_signins: List[Dict[str, Any]] = []

            def _on_page(page_items):
                collected_signins.extend(page_items)

            with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["appDisplayName", "successfulSignInCount"])

            run_app_signins_pipeline(
                client_id=self.client_id,
                client_secret=self.secret,
                tenant_id=self.tenant,
                csv_path=csv_path,
                max_rows=5000,
                on_page_callback=_on_page,
            )

            self._cache_to_sqlite_safe(csv_path, db_path, "app_signins")
            self.cached_data["app_signins"] = collected_signins

            for item in collected_signins:
                app_name = item.get("appDisplayName") or "Enterprise App"
                success_count = str(item.get("successfulSignInCount") or 0)
                rows.append([app_name, success_count])

            elapsed = time.time() - start_time

            def _on_success():
                self.app_signins_card.set_data(columns, rows, execution_time=elapsed)
                if self.app_signins_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.app_signins_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)

        except Exception as e:
            logger.error(f"Error fetching App Sign-in logs: {e}", exc_info=True)
            err_msg = str(e)
            if "401" in err_msg or "403" in err_msg or "permission" in err_msg.lower() or "unauthorized" in err_msg.lower():
                err_msg = "AuditLog.Read.All or Reports.Read.All permission required in Microsoft Entra."

            def _on_error():
                self.app_signins_card.set_error(f"Failed to fetch App Sign-ins: {err_msg}")
                if self.app_signins_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.app_signins_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_user_signins_worker(self, is_reload: bool = False):
        """4.7. User Sign-in Activity (Sign-in Attribute / Successful Unique Values matching deal_assistant.py)."""
        start_time = time.time()
        logger.info("Executing User Sign-in Activity fetch task...")
        reports_dir, db_path = self._get_reports_dir_and_db()
        csv_path = os.path.join(reports_dir, "entra_user_signins.csv")

        try:
            columns = ["Sign-in Attribute", "Successful Unique Values"]
            unique_apps = set()
            unique_os = set()
            unique_browsers = set()

            def _on_page(page_items):
                for log in page_items:
                    app_name = log.get("appDisplayName") or ""
                    device = log.get("deviceDetail") or {}
                    os_name = device.get("operatingSystem") or ""
                    browser_name = device.get("browser") or ""
                    if app_name:
                        unique_apps.add(app_name)
                    if os_name:
                        unique_os.add(os_name)
                    if browser_name:
                        unique_browsers.add(browser_name)

            with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["appDisplayName", "operatingSystem", "browser", "isInteractive"])

            run_user_signins_pipeline(
                client_id=self.client_id,
                client_secret=self.secret,
                tenant_id=self.tenant,
                csv_path=csv_path,
                max_rows=20000,
                on_page_callback=_on_page,
            )

            self._cache_to_sqlite_safe(csv_path, db_path, "user_signins")
            self.cached_data["user_signins"] = {
                "apps": sorted(list(unique_apps)),
                "os": sorted(list(unique_os)),
                "browsers": sorted(list(unique_browsers)),
            }

            apps_str = ", ".join(sorted(list(unique_apps))) or "None"
            os_str = ", ".join(sorted(list(unique_os))) or "None"
            browsers_str = ", ".join(sorted(list(unique_browsers))) or "None"

            rows = [
                ["Successful App Sign-ins", apps_str],
                ["Successful Client OS", os_str],
                ["Successful Browsers", browsers_str],
            ]

            elapsed = time.time() - start_time

            def _on_success():
                self.user_signins_card.set_data(columns, rows, execution_time=elapsed)
                if self.user_signins_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.user_signins_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)

        except Exception as e:
            logger.error(f"Error fetching User Sign-ins: {e}", exc_info=True)
            err_msg = str(e)
            if "401" in err_msg or "403" in err_msg or "permission" in err_msg.lower() or "unauthorized" in err_msg.lower():
                err_msg = "AuditLog.Read.All permission required in Microsoft Entra."

            def _on_error():
                self.user_signins_card.set_error(f"Failed to fetch User Sign-ins: {err_msg}")
                if self.user_signins_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.user_signins_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)

    def _fetch_auth_methods_worker(self, is_reload: bool = False):
        """4.8. Authentication Methods (Authentication Method / Success Activity Count (7 days) matching deal_assistant.py)."""
        start_time = time.time()
        logger.info("Executing Authentication Methods fetch task...")
        reports_dir, db_path = self._get_reports_dir_and_db()
        csv_path = os.path.join(reports_dir, "entra_auth_methods.csv")

        try:
            columns = ["Authentication Method", "Success Activity Count (7 days)"]
            rows: List[List[Any]] = []
            collected_methods: List[Dict[str, Any]] = []

            def _on_page(page_items):
                collected_methods.extend(page_items)

            with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["authenticationMethod", "successActivityCount"])

            run_auth_methods_pipeline(
                client_id=self.client_id,
                client_secret=self.secret,
                tenant_id=self.tenant,
                csv_path=csv_path,
                period="D7",
                max_rows=5000,
                on_page_callback=_on_page,
            )

            self._cache_to_sqlite_safe(csv_path, db_path, "entra_auth_methods")
            self.cached_data["auth_methods"] = collected_methods

            for item in collected_methods:
                method = item.get("authenticationMethod") or "Unknown"
                success_count = str(item.get("successActivityCount") or 0)
                rows.append([method, success_count])

            elapsed = time.time() - start_time

            def _on_success():
                self.auth_methods_card.set_data(columns, rows, execution_time=elapsed)
                if self.auth_methods_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.auth_methods_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_success)

        except Exception as e:
            logger.error(f"Error fetching Authentication Methods: {e}", exc_info=True)
            err_msg = str(e)
            if "401" in err_msg or "403" in err_msg or "permission" in err_msg.lower() or "unauthorized" in err_msg.lower():
                err_msg = "AuditLog.Read.All permission required in Microsoft Entra."

            def _on_error():
                self.auth_methods_card.set_error(f"Failed to fetch Authentication Methods: {err_msg}")
                if self.auth_methods_card not in self.cards_column.controls:
                    self.cards_column.controls.append(self.auth_methods_card)
                try:
                    self.update()
                except Exception:
                    pass

            self._safe_run_on_ui(_on_error)



