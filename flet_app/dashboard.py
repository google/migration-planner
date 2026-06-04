import flet as ft
from flet_app.styles import *
from flet_app.sidebar import Sidebar
from flet_app.custom_chart import CustomLineChart
import threading
import sys
import os
import datetime
import csv

# Import existing backend modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.graph.client import GraphClient
from core.graph.directory import DirectoryService
from telemetry import active_users_usage as usage
from telemetry.power_automate import PowerAutomateScanner
from telemetry.mailbox_usage import run_mailbox_usage_pipeline
from telemetry.sharepoint_onedrive_usage import run_sharepoint_pipeline, run_onedrive_pipeline
from telemetry.data_security_governance import run_security_governance_pipeline

class DashboardView(ft.Container):
    def __init__(self, tenant, client, secret, on_disconnect):
        super().__init__()
        self.tenant = tenant
        self.client = client
        self.secret = secret
        self.on_disconnect = on_disconnect
        
        self.expand = True
        
        self.sidebar = Sidebar(on_disconnect=self.on_disconnect)
        
        # Saved data for CSV exports
        self.last_licenses_items = []
        self.last_complex_flows = []
        
        # Saved data for Sensitivity Labels pagination
        self.flattened_labels = []
        self.current_labels_page = 0
        self.labels_per_page = 8
        
        # Track states of parallel fetches
        self.fetch_statuses = {}
        
        # Header
        self.fetch_btn = ft.ElevatedButton(
            content=ft.Text("Fetch Report", weight=ft.FontWeight.BOLD),
            bgcolor=COLOR_PRIMARY,
            color=ft.Colors.WHITE,
            height=40,
            style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
            on_click=self.handle_fetch
        )
        
        self.header = ft.Container(
            content=ft.Row(
                controls=[
                    ft.Text("Usage Report", size=20, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_MAIN),
                    self.fetch_btn
                ],
                alignment=ft.MainAxisAlignment.SPACE_BETWEEN
            ),
            bgcolor=COLOR_SURFACE,
            border_radius=12,
            border=ft.Border.all(1, COLOR_OUTLINE_LIGHT),
            padding=ft.Padding.symmetric(horizontal=20, vertical=15),
            margin=ft.Margin.only(bottom=20)
        )
        
        # 1. SKUs Card (with Export Button)
        self.export_sku_btn = ft.IconButton(
            icon=ft.Icons.DOWNLOAD,
            icon_color=COLOR_PRIMARY,
            tooltip="Export Spreadsheet",
            disabled=True,
            on_click=self.handle_export_skus
        )
        self.sku_link = ft.TextButton(
            content=ft.Text("Service Plan Reference ↗", color=COLOR_PRIMARY, weight=ft.FontWeight.BOLD),
            on_click=lambda e: e.page.launch_url("https://learn.microsoft.com/en-us/entra/identity/users/licensing-service-plan-reference")
        )
        sku_actions = ft.Row(
            controls=[
                self.sku_link,
                self.export_sku_btn
            ],
            spacing=5
        )
        self.sku_section = self.create_card("Subscribed SKUs Inventory Summary", action_control=sku_actions, on_retry=self.handle_retry_skus)
        
        # 2. O365 Usage Card
        self.o365_section = self.create_card("O365 Active Users Usage", on_retry=self.handle_retry_o365)
        
        # 3. O365 Trend Chart Card
        self.trend_section = self.create_card("O365 30-Day Active User Trend", on_retry=self.handle_retry_trend)
        
        # 4. M365 App Usage Card
        self.m365_section = self.create_card("M365 App Usage (180 Days)", on_retry=self.handle_retry_m365)
        
        # 5. Mailbox Card
        self.mailbox_section = self.create_card("Exchange Online Mailbox Usage Telemetry", on_retry=self.handle_retry_mailbox)
        
        # 6. SharePoint Card
        self.sharepoint_section = self.create_card("SharePoint Site Usage Telemetry (180 Days)", on_retry=self.handle_retry_sharepoint)
        
        # 7. OneDrive Card
        self.onedrive_section = self.create_card("OneDrive Usage Telemetry (180 Days)", on_retry=self.handle_retry_onedrive)
        
        # 8. Sensitivity Labels Card (with Pagination Controls)
        self.labels_pagination_info = ft.Text("Page 1 of 1", size=13, color=COLOR_TEXT_MAIN)
        self.labels_prev_btn = ft.IconButton(ft.Icons.ARROW_BACK, on_click=self.handle_labels_prev, disabled=True)
        self.labels_next_btn = ft.IconButton(ft.Icons.ARROW_FORWARD, on_click=self.handle_labels_next, disabled=True)
        self.labels_pagination_row = ft.Row(
            controls=[self.labels_prev_btn, self.labels_pagination_info, self.labels_next_btn],
            alignment=ft.MainAxisAlignment.CENTER,
            visible=False
        )
        self.labels_section = self.create_card(
            "Sensitivity Labels", 
            bottom_control=self.labels_pagination_row,
            on_retry=self.handle_retry_security_gov
        )
        
        # 9. Retention Policies Card (with Purview Link)
        self.purview_btn = ft.TextButton(
            content=ft.Text("Open Microsoft Purview Portal ↗", color=COLOR_PRIMARY, weight=ft.FontWeight.BOLD),
            on_click=lambda e: e.page.launch_url("https://purview.microsoft.com/datalifecyclemanagement/retention")
        )
        self.retention_section = self.create_card("Retention Compliance Policies", action_control=self.purview_btn, on_retry=self.handle_retry_security_gov)
        
        # 10. Power Automate Card (with Export Button)
        self.export_pa_btn = ft.IconButton(
            icon=ft.Icons.DOWNLOAD,
            icon_color=COLOR_PRIMARY,
            tooltip="Export Complex Flows",
            disabled=True,
            on_click=self.handle_export_pa
        )
        self.pa_section = self.create_card("Power Automate", action_control=self.export_pa_btn, on_retry=self.handle_retry_pa)
        
        self.content_area = ft.Column(
            controls=[
                self.sku_section,
                self.o365_section,
                self.trend_section,
                self.m365_section,
                self.mailbox_section,
                self.sharepoint_section,
                self.onedrive_section,
                self.labels_section,
                self.retention_section,
                self.pa_section
            ],
            scroll=ft.ScrollMode.AUTO,
            expand=True,
            spacing=20
        )
        
        self.content = ft.Row(
            controls=[
                self.sidebar,
                ft.Container(
                    content=ft.Column(
                        controls=[self.header, self.content_area],
                        expand=True
                    ),
                    expand=True,
                    padding=ft.Padding.only(left=20)
                )
            ],
            expand=True
        )

    def create_card(self, title, action_control=None, bottom_control=None, on_retry=None):
        content_container = ft.Container(content=ft.Text("No data yet.", color=COLOR_TEXT_SUB))
        header_controls = [ft.Text(title, size=16, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_MAIN)]
        
        actions = []
        if action_control:
            actions.append(action_control)
            
        retry_btn = None
        if on_retry:
            retry_btn = ft.IconButton(
                icon=ft.Icons.REFRESH,
                icon_color=COLOR_PRIMARY,
                icon_size=20,
                tooltip="Refresh this section",
                on_click=on_retry
            )
            actions.append(retry_btn)
            
        if actions:
            header_controls.append(ft.Row(controls=actions, spacing=10))
            
        header_row = ft.Row(
            controls=header_controls,
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN
        )
        
        column_controls = [
            header_row,
            ft.Divider(height=20, color="transparent"),
            content_container
        ]
        if bottom_control:
            column_controls.append(bottom_control)
            
        card = ft.Container(
            content=ft.Column(column_controls),
            bgcolor=COLOR_SURFACE,
            border_radius=12,
            border=ft.Border.all(1, COLOR_OUTLINE_LIGHT),
            padding=20,
            width=float('inf')
        )
        card.content_container = content_container
        card.retry_btn = retry_btn
        return card

    def set_loading(self, card, message):
        card.content_container.content = ft.Column([
            ft.ProgressRing(),
            ft.Text(message, color=COLOR_TEXT_SUB)
        ], alignment=ft.MainAxisAlignment.CENTER, horizontal_alignment=ft.CrossAxisAlignment.CENTER)
        if hasattr(card, "retry_btn") and card.retry_btn:
            card.retry_btn.disabled = True
            try:
                card.retry_btn.update()
            except Exception:
                pass
        
    def set_error(self, card, message):
        card.content_container.content = ft.Text(f"Error: {message}", color=COLOR_ERROR)
        if hasattr(card, "retry_btn") and card.retry_btn:
            card.retry_btn.disabled = False
            try:
                card.retry_btn.update()
            except Exception:
                pass

    def clear_loading(self, card):
        if hasattr(card, "retry_btn") and card.retry_btn:
            card.retry_btn.disabled = False
            try:
                card.retry_btn.update()
            except Exception:
                pass

    def start_individual_fetch(self, key, section, message, target):
        self.fetch_btn.disabled = True
        self.fetch_btn.update()
        self.fetch_statuses[key] = "pending"
        self.set_loading(section, message)
        section.update()
        threading.Thread(target=target, daemon=True).start()

    def handle_retry_skus(self, e):
        self.start_individual_fetch("sku", self.sku_section, "Fetching SKU inventories...", self.fetch_skus)

    def handle_retry_o365(self, e):
        self.start_individual_fetch("o365", self.o365_section, "Downloading O365 Active User reports...", self.fetch_o365)

    def handle_retry_trend(self, e):
        self.start_individual_fetch("trend", self.trend_section, "Downloading O365 Trend report...", self.fetch_trend)

    def handle_retry_m365(self, e):
        self.start_individual_fetch("m365", self.m365_section, "Downloading M365 App reports...", self.fetch_m365)

    def handle_retry_mailbox(self, e):
        self.start_individual_fetch("mailbox", self.mailbox_section, "Downloading Mailbox reports...", self.fetch_mailbox)

    def handle_retry_sharepoint(self, e):
        self.start_individual_fetch("sharepoint", self.sharepoint_section, "Downloading SharePoint reports...", self.fetch_sharepoint)

    def handle_retry_onedrive(self, e):
        self.start_individual_fetch("onedrive", self.onedrive_section, "Downloading OneDrive reports...", self.fetch_onedrive)

    def handle_retry_security_gov(self, e):
        self.fetch_btn.disabled = True
        self.fetch_btn.update()
        self.fetch_statuses["security_gov"] = "pending"
        
        self.set_loading(self.labels_section, "Retrieving Sensitivity labels...")
        self.labels_section.update()
        self.set_loading(self.retention_section, "Retrieving Retention policies...")
        self.retention_section.update()
        
        threading.Thread(target=self.fetch_security_gov, daemon=True).start()

    def handle_retry_pa(self, e):
        self.start_individual_fetch("pa", self.pa_section, "Scanning Power Automate flows...", self.fetch_pa)

    def handle_fetch(self, e):
        self.fetch_btn.disabled = True
        self.fetch_btn.content = ft.Text("Fetching...", color=ft.Colors.WHITE)
        self.fetch_btn.update()
        
        # Initialize fetch statuses
        self.fetch_statuses = {
            "sku": "pending",
            "o365": "pending",
            "trend": "pending",
            "m365": "pending",
            "mailbox": "pending",
            "sharepoint": "pending",
            "onedrive": "pending",
            "security_gov": "pending",
            "pa": "pending"
        }
        
        # 1. SKUs
        self.set_loading(self.sku_section, "Fetching SKU inventories...")
        self.sku_section.update()
        threading.Thread(target=self.fetch_skus, daemon=True).start()
        
        # 2. O365 Usage
        self.set_loading(self.o365_section, "Downloading O365 Active User reports...")
        self.o365_section.update()
        threading.Thread(target=self.fetch_o365, daemon=True).start()
        
        # 3. O365 Trend
        self.set_loading(self.trend_section, "Downloading O365 Trend report...")
        self.trend_section.update()
        threading.Thread(target=self.fetch_trend, daemon=True).start()
        
        # 4. M365 App Usage
        self.set_loading(self.m365_section, "Downloading M365 App reports...")
        self.m365_section.update()
        threading.Thread(target=self.fetch_m365, daemon=True).start()
        
        # 5. Mailbox
        self.set_loading(self.mailbox_section, "Downloading Mailbox reports...")
        self.mailbox_section.update()
        threading.Thread(target=self.fetch_mailbox, daemon=True).start()
        
        # 6. SharePoint
        self.set_loading(self.sharepoint_section, "Downloading SharePoint reports...")
        self.sharepoint_section.update()
        threading.Thread(target=self.fetch_sharepoint, daemon=True).start()
        
        # 7. OneDrive
        self.set_loading(self.onedrive_section, "Downloading OneDrive reports...")
        self.onedrive_section.update()
        threading.Thread(target=self.fetch_onedrive, daemon=True).start()
        
        # 8 & 9. Sensitivity Labels & Retention Policies
        self.set_loading(self.labels_section, "Retrieving Sensitivity labels...")
        self.labels_section.update()
        self.set_loading(self.retention_section, "Retrieving Retention policies...")
        self.retention_section.update()
        threading.Thread(target=self.fetch_security_gov, daemon=True).start()
        
        # 10. Power Automate
        self.set_loading(self.pa_section, "Scanning Power Automate flows...")
        self.pa_section.update()
        threading.Thread(target=self.fetch_pa, daemon=True).start()

    def mark_complete(self, key, status):
        self.fetch_statuses[key] = status
        self.check_all_done()

    def check_all_done(self):
        if not self.fetch_statuses:
            return
        if "pending" not in self.fetch_statuses.values():
            self.fetch_btn.disabled = False
            self.fetch_btn.content = ft.Text("Fetch Report", weight=ft.FontWeight.BOLD)
            if self.page:
                self.fetch_btn.update()

    # --- Fetching Logic ---
    
    def fetch_skus(self):
        try:
            client = GraphClient(tenant_id=self.tenant, client_ids=self.client, client_secrets=self.secret, concurrency=1, retries=30, backoff=2)
            client.authenticate(required_scopes=["Organization.Read.All", "Directory.Read.All"])
            dir_service = DirectoryService(client)
            sku_data = dir_service.get_subscribed_skus()
            client.close()
            
            items = sku_data.get("value", [])
            self.last_licenses_items = items
            
            if not items:
                self.sku_section.content_container.content = ft.Text("No subscribed product configurations found.", color=COLOR_TEXT_SUB)
                self.export_sku_btn.disabled = True
            else:
                items.sort(key=lambda x: len(x.get("servicePlans", [])), reverse=True)
                rows = []
                for item in items:
                    prepaid = item.get("prepaidUnits", {})
                    p_str = f"Enabled: {prepaid.get('enabled', 0):,}"
                    if prepaid.get('warning', 0) > 0: p_str += f"\nWarn: {prepaid.get('warning'):,}"
                    if prepaid.get('suspended', 0) > 0: p_str += f"\nSusp: {prepaid.get('suspended'):,}"
                    rows.append(ft.DataRow(cells=[
                        ft.DataCell(ft.Text(item.get("skuPartNumber", "UNKNOWN_SKU"), weight=ft.FontWeight.BOLD)),
                        ft.DataCell(ft.Text(p_str)),
                        ft.DataCell(ft.Text(f"{item.get('consumedUnits', 0):,}"))
                    ]))
                
                table = ft.DataTable(
                    columns=[
                        ft.DataColumn(ft.Text("SKU Part Number", weight=ft.FontWeight.BOLD)),
                        ft.DataColumn(ft.Text("Units", weight=ft.FontWeight.BOLD)),
                        ft.DataColumn(ft.Text("Consumed Units", weight=ft.FontWeight.BOLD)),
                    ],
                    rows=rows,
                    border=ft.Border.all(1, COLOR_OUTLINE_LIGHT),
                    border_radius=8,
                    heading_row_color=COLOR_TONAL_BG,
                )
                self.sku_section.content_container.content = ft.Column([table], scroll=ft.ScrollMode.AUTO, height=300)
                self.export_sku_btn.disabled = False
            
            self.mark_complete("sku", "success")
        except Exception as e:
            self.set_error(self.sku_section, str(e))
            self.export_sku_btn.disabled = True
            self.mark_complete("sku", "error")
        finally:
            self.clear_loading(self.sku_section)
            if self.page:
                self.sku_section.update()
                self.export_sku_btn.update()

    def fetch_o365(self):
        try:
            try:
                o365_data = usage.run_o365_pipeline(self.client, self.secret, self.tenant)
            except Exception as o365_err:
                script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                local_file = os.path.join(script_dir, "telemetry", "reports", f"{self.tenant}_{self.client}", "Office365ActiveUserDetail(180d).csv")
                if not os.path.exists(local_file):
                    local_file = os.path.join(script_dir, "telemetry", "reports", "Office365ActiveUserDetail(180d).csv")
                if os.path.exists(local_file):
                    print(f"Falling back to local O365 file: {local_file}")
                    o365_data = usage.process_active_user_detail(local_file)
                else:
                    raise o365_err
            if not o365_data:
                self.o365_section.content_container.content = ft.Text("No O365 usage data found.", color=COLOR_TEXT_SUB)
            else:
                rows = []
                for row_data in o365_data:
                    rows.append(ft.DataRow(cells=[
                        ft.DataCell(ft.Text(str(row_data[0]), weight=ft.FontWeight.BOLD)),
                        ft.DataCell(ft.Text(f"{row_data[1]:,}")),
                        ft.DataCell(ft.Text(f"{row_data[2]:,}")),
                        ft.DataCell(ft.Text(f"{row_data[3]:,}"))
                    ]))
                
                table = ft.DataTable(
                    columns=[
                        ft.DataColumn(ft.Text("Service", weight=ft.FontWeight.BOLD)),
                        ft.DataColumn(ft.Text("30 Days", weight=ft.FontWeight.BOLD)),
                        ft.DataColumn(ft.Text("90 Days", weight=ft.FontWeight.BOLD)),
                        ft.DataColumn(ft.Text("180 Days", weight=ft.FontWeight.BOLD)),
                    ],
                    rows=rows,
                    border=ft.Border.all(1, COLOR_OUTLINE_LIGHT),
                    border_radius=8,
                    heading_row_color=COLOR_TONAL_BG,
                )
                self.o365_section.content_container.content = table
            
            self.mark_complete("o365", "success")
        except Exception as e:
            self.set_error(self.o365_section, str(e))
            self.mark_complete("o365", "error")
        finally:
            self.clear_loading(self.o365_section)
            if self.page:
                self.o365_section.update()

    def fetch_trend(self):
        try:
            try:
                trend_data = usage.run_o365_trend_pipeline(self.client, self.secret, self.tenant)
            except Exception as trend_err:
                script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                local_file = os.path.join(script_dir, "telemetry", "reports", f"{self.tenant}_{self.client}", "Office365ActiveUserCounts(30d).csv")
                if not os.path.exists(local_file):
                    local_file = os.path.join(script_dir, "telemetry", "reports", "Office365ActiveUserCounts(30d).csv")
                if os.path.exists(local_file):
                    print(f"Falling back to local trend file: {local_file}")
                    trend_data = usage.process_active_user_counts(local_file)
                else:
                    raise trend_err
            if not trend_data or not trend_data.get("dates"):
                self.trend_section.content_container.content = ft.Text("No O365 trend data found.", color=COLOR_TEXT_SUB)
            else:
                dates = trend_data["dates"]
                datasets = {
                    "Office 365": trend_data["office365"],
                    "Exchange": trend_data["exchange"],
                    "OneDrive": trend_data["onedrive"],
                    "SharePoint": trend_data["sharepoint"],
                    "Teams": trend_data["teams"]
                }
                colors = {
                    "Office 365": COLOR_PRIMARY,
                    "Exchange": "#C2410C",
                    "OneDrive": "#3B82F6",
                    "SharePoint": "#15803D",
                    "Teams": "#9333EA"
                }
                
                chart = CustomLineChart(dates=dates, datasets=datasets, colors=colors, height=300)
                
                # Legend container
                def legend_item(label, color):
                    return ft.Row([
                        ft.Container(width=12, height=12, bgcolor=color, border_radius=3),
                        ft.Text(label, size=12, weight=ft.FontWeight.W_500, color=COLOR_TEXT_MAIN)
                    ], spacing=5)
                
                legend = ft.Row([
                    legend_item("Office 365", colors["Office 365"]),
                    legend_item("Exchange", colors["Exchange"]),
                    legend_item("OneDrive", colors["OneDrive"]),
                    legend_item("SharePoint", colors["SharePoint"]),
                    legend_item("Teams", colors["Teams"]),
                ], alignment=ft.MainAxisAlignment.CENTER, spacing=20)
                
                self.trend_section.content_container.content = ft.Column([
                    chart,
                    ft.Divider(height=10, color="transparent"),
                    legend
                ])
                
            self.mark_complete("trend", "success")
        except Exception as e:
            self.set_error(self.trend_section, str(e))
            self.mark_complete("trend", "error")
        finally:
            self.clear_loading(self.trend_section)
            if self.page:
                self.trend_section.update()

    def fetch_m365(self):
        try:
            try:
                m365_data = usage.run_m365_pipeline(self.client, self.secret, self.tenant)
            except Exception as m365_err:
                script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                local_file = os.path.join(script_dir, "telemetry", "reports", f"{self.tenant}_{self.client}", "M365AppUserDetail(180d).csv")
                if not os.path.exists(local_file):
                    local_file = os.path.join(script_dir, "telemetry", "reports", "M365AppUserDetail(180d).csv")
                if os.path.exists(local_file):
                    print(f"Falling back to local M365 file: {local_file}")
                    m365_data = usage.process_m365_app_user_detail(local_file)
                else:
                    raise m365_err
            if not m365_data:
                self.m365_section.content_container.content = ft.Text("No M365 App usage data found.", color=COLOR_TEXT_SUB)
            else:
                rows = []
                for row in m365_data:
                    rows.append(ft.DataRow(cells=[
                        ft.DataCell(ft.Text(str(row[0]), weight=ft.FontWeight.BOLD)),
                        ft.DataCell(ft.Text(f"{row[1]:,}"))
                    ]))
                
                table = ft.DataTable(
                    columns=[
                        ft.DataColumn(ft.Text("App / Platform", weight=ft.FontWeight.BOLD)),
                        ft.DataColumn(ft.Text("Users Count", weight=ft.FontWeight.BOLD)),
                    ],
                    rows=rows,
                    border=ft.Border.all(1, COLOR_OUTLINE_LIGHT),
                    border_radius=8,
                    heading_row_color=COLOR_TONAL_BG,
                )
                self.m365_section.content_container.content = ft.Column([table], scroll=ft.ScrollMode.AUTO, height=300)
            
            self.mark_complete("m365", "success")
        except Exception as e:
            self.set_error(self.m365_section, str(e))
            self.mark_complete("m365", "error")
        finally:
            self.clear_loading(self.m365_section)
            if self.page:
                self.m365_section.update()

    def fetch_mailbox(self):
        try:
            data = run_mailbox_usage_pipeline(self.client, self.secret, self.tenant)
            rows_data = [
                ("Total Mailboxes Analyzed", f"{data.get('total_mailboxes', 0):,} Mailboxes"),
                ("Total Size of All Mailboxes", data.get("total_storage_formatted", "0.00 Bytes")),
                ("Average Mailbox Size", data.get("average_mailbox_size_formatted", "0.00 Bytes")),
                ("Total Number of Emails", f"{data.get('total_emails', 0):,} Emails"),
                ("Average Emails per User", f"{data.get('average_emails', 0.0):,.0f} Emails")
            ]
            rows = [ft.DataRow(cells=[
                ft.DataCell(ft.Text(str(r[0]), weight=ft.FontWeight.BOLD)), 
                ft.DataCell(ft.Text(str(r[1])))
            ]) for r in rows_data]
            
            table = ft.DataTable(
                columns=[
                    ft.DataColumn(ft.Text("Mailbox Metric Description", weight=ft.FontWeight.BOLD)),
                    ft.DataColumn(ft.Text("Value / Measurement", weight=ft.FontWeight.BOLD)),
                ],
                rows=rows,
                border=ft.Border.all(1, COLOR_OUTLINE_LIGHT),
                border_radius=8,
                heading_row_color=COLOR_TONAL_BG,
            )
            self.mailbox_section.content_container.content = table
            self.mark_complete("mailbox", "success")
        except Exception as e:
            self.set_error(self.mailbox_section, str(e))
            self.mark_complete("mailbox", "error")
        finally:
            self.clear_loading(self.mailbox_section)
            if self.page:
                self.mailbox_section.update()

    def fetch_sharepoint(self):
        try:
            data = run_sharepoint_pipeline(self.client, self.secret, self.tenant)
            rows_data = [
                ("Total Sites Count", f"{data.get('total_sites', 0):,} Sites"),
                ("Total Storage Used", data.get("total_storage_formatted", "0.00 Bytes")),
                ("Total Files Stored", f"{data.get('total_files', 0):,} Files"),
                ("Active Files Count", f"{data.get('active_files', 0):,} Files ({data.get('active_files_pct', 0.0):.1f}%)")
            ]
            rows = [ft.DataRow(cells=[
                ft.DataCell(ft.Text(str(r[0]), weight=ft.FontWeight.BOLD)), 
                ft.DataCell(ft.Text(str(r[1])))
            ]) for r in rows_data]
            
            table = ft.DataTable(
                columns=[
                    ft.DataColumn(ft.Text("SharePoint Site Metric Description", weight=ft.FontWeight.BOLD)),
                    ft.DataColumn(ft.Text("Value / Measurement", weight=ft.FontWeight.BOLD)),
                ],
                rows=rows,
                border=ft.Border.all(1, COLOR_OUTLINE_LIGHT),
                border_radius=8,
                heading_row_color=COLOR_TONAL_BG,
            )
            self.sharepoint_section.content_container.content = table
            self.mark_complete("sharepoint", "success")
        except Exception as e:
            self.set_error(self.sharepoint_section, str(e))
            self.mark_complete("sharepoint", "error")
        finally:
            self.clear_loading(self.sharepoint_section)
            if self.page:
                self.sharepoint_section.update()

    def fetch_onedrive(self):
        try:
            data = run_onedrive_pipeline(self.client, self.secret, self.tenant)
            rows_data = [
                ("Total Accounts Count", f"{data.get('total_accounts', 0):,} Accounts"),
                ("Total Storage Used", data.get("total_storage_formatted", "0.00 Bytes")),
                ("Total Files Stored", f"{data.get('total_files', 0):,} Files"),
                ("Active Files Count", f"{data.get('active_files', 0):,} Files ({data.get('active_files_pct', 0.0):.1f}%)"),
                ("Users with Synced Files", f"{data.get('sync_users', 0):,} Users ({data.get('sync_users_pct', 0.0):.1f}%)"),
                ("OneNote Active Users", f"{data.get('onenote_users', 0):,} Users")
            ]
            rows = [ft.DataRow(cells=[
                ft.DataCell(ft.Text(str(r[0]), weight=ft.FontWeight.BOLD)), 
                ft.DataCell(ft.Text(str(r[1])))
            ]) for r in rows_data]
            
            table = ft.DataTable(
                columns=[
                    ft.DataColumn(ft.Text("OneDrive Metric Description", weight=ft.FontWeight.BOLD)),
                    ft.DataColumn(ft.Text("Value / Measurement", weight=ft.FontWeight.BOLD)),
                ],
                rows=rows,
                border=ft.Border.all(1, COLOR_OUTLINE_LIGHT),
                border_radius=8,
                heading_row_color=COLOR_TONAL_BG,
            )
            self.onedrive_section.content_container.content = table
            self.mark_complete("onedrive", "success")
        except Exception as e:
            self.set_error(self.onedrive_section, str(e))
            self.mark_complete("onedrive", "error")
        finally:
            self.clear_loading(self.onedrive_section)
            if self.page:
                self.onedrive_section.update()

    def fetch_security_gov(self):
        try:
            data = run_security_governance_pipeline(self.client, self.secret, self.tenant)
            
            labels = data.get("labels")
            labels_error = data.get("labels_error")
            policies = data.get("policies")
            policies_error = data.get("policies_error")
            
            # 1. Populate Sensitivity Labels pagination data
            self.flattened_labels = []
            if labels_error:
                self.labels_section.content_container.content = ft.Text(f"Error loading labels: {labels_error}", color=COLOR_ERROR)
                self.labels_pagination_row.visible = False
            elif not labels:
                self.labels_section.content_container.content = ft.Text("No Sensitivity Labels configured in this tenant.", color=COLOR_TEXT_SUB)
                self.labels_pagination_row.visible = False
            else:
                for parent in labels:
                    self.flattened_labels.append({
                        "name": parent.get("name", "N/A"),
                        "description": parent.get("description", "") or parent.get("toolTip", "") or "N/A",
                        "hasProtection": parent.get("hasProtection", False),
                        "applicationMode": parent.get("applicationMode", "N/A") or "N/A",
                        "priority": parent.get("priority", 0),
                        "applicableTo": parent.get("applicableTo", ""),
                        "isEnabled": parent.get("isEnabled", True),
                        "is_sublabel": False
                    })
                    sublabels = parent.get("sublabels", [])
                    if sublabels:
                        sublabels_sorted = sorted(sublabels, key=lambda x: x.get("priority", 0), reverse=True)
                        for sub in sublabels_sorted:
                            self.flattened_labels.append({
                                "name": f"    ↳  {sub.get('name', 'N/A')}",
                                "description": sub.get("description", "") or sub.get("toolTip", "") or "N/A",
                                "hasProtection": sub.get("hasProtection", False),
                                "applicationMode": sub.get("applicationMode", "N/A") or "N/A",
                                "priority": sub.get("priority", 0),
                                "applicableTo": sub.get("applicableTo", ""),
                                "isEnabled": sub.get("isEnabled", True),
                                "is_sublabel": True
                            })
                self.current_labels_page = 0
                self.render_labels_page()
            
            # 2. Populate Retention Policies
            if policies_error:
                msg = policies_error
                if "powershell" in policies_error.lower() or "pwsh" in policies_error.lower():
                    msg = "PowerShell Core ('pwsh') is not installed or configured on this machine."
                elif "exchangeonlinemanagement" in policies_error.lower():
                    msg = "ExchangeOnlineManagement PowerShell module is missing."
                self.retention_section.content_container.content = ft.Text(f"Error loading policies: {msg}", color=COLOR_ERROR)
            elif not policies:
                self.retention_section.content_container.content = ft.Text("No Retention Compliance Policies found.", color=COLOR_TEXT_SUB)
            else:
                policies_list = policies if isinstance(policies, list) else [policies]
                rows = []
                for policy in policies_list:
                    duration_val = str(policy.get("Duration", "N/A"))
                    duration_str = duration_val
                    if duration_val.lower() == "unlimited":
                        duration_str = "Keep Forever"
                    elif duration_val.isdigit():
                        days = int(duration_val)
                        if days >= 365:
                            years = days / 365.0
                            duration_str = f"{int(years)} Years ({days} days)" if years.is_integer() else f"{years:.1f} Years ({days} days)"
                        else:
                            duration_str = f"{days} days"
                    
                    trigger_val = policy.get("RetentionTrigger", "N/A")
                    if trigger_val and trigger_val != "N/A":
                        trigger_map = {"DateCreated": "created date", "DateModified": "last modified date", "DateLabeled": "labeled date"}
                        duration_str += f"\n(from {trigger_map.get(trigger_val, trigger_val)})"
                    
                    enabled_val = policy.get("Enabled", True)
                    is_enabled = enabled_val.lower() == "true" if isinstance(enabled_val, str) else bool(enabled_val)
                    status_str = "🟢 Enabled" if is_enabled else "🔴 Disabled"
                    
                    rows.append(ft.DataRow(cells=[
                        ft.DataCell(ft.Column([
                            ft.Text(policy.get("Name", "N/A"), weight=ft.FontWeight.BOLD),
                            ft.Text(policy.get("Comment", ""), size=11, color=COLOR_TEXT_SUB) if policy.get("Comment") else ft.Container()
                        ], alignment=ft.MainAxisAlignment.CENTER)),
                        ft.DataCell(ft.Text(policy.get("Workload", "N/A"))),
                        ft.DataCell(ft.Text(duration_str)),
                        ft.DataCell(ft.Text(policy.get("Mode", "Enforce"))),
                        ft.DataCell(ft.Text(policy.get("DistributionStatus", "Success"))),
                        ft.DataCell(ft.Text(status_str))
                    ]))
                
                table = ft.DataTable(
                    columns=[
                        ft.DataColumn(ft.Text("Policy Name", weight=ft.FontWeight.BOLD)),
                        ft.DataColumn(ft.Text("Workloads", weight=ft.FontWeight.BOLD)),
                        ft.DataColumn(ft.Text("Duration", weight=ft.FontWeight.BOLD)),
                        ft.DataColumn(ft.Text("Mode", weight=ft.FontWeight.BOLD)),
                        ft.DataColumn(ft.Text("Distribution", weight=ft.FontWeight.BOLD)),
                        ft.DataColumn(ft.Text("Status", weight=ft.FontWeight.BOLD)),
                    ],
                    rows=rows,
                    border=ft.Border.all(1, COLOR_OUTLINE_LIGHT),
                    border_radius=8,
                    heading_row_color=COLOR_TONAL_BG,
                )
                self.retention_section.content_container.content = ft.Column([table], scroll=ft.ScrollMode.AUTO, height=300)
            
            self.mark_complete("security_gov", "success")
        except Exception as e:
            self.set_error(self.labels_section, str(e))
            self.set_error(self.retention_section, str(e))
            self.labels_pagination_row.visible = False
            self.mark_complete("security_gov", "error")
        finally:
            self.clear_loading(self.labels_section)
            self.clear_loading(self.retention_section)
            if self.page:
                self.labels_section.update()
                self.retention_section.update()

    def render_labels_page(self):
        total_items = len(self.flattened_labels)
        total_pages = (total_items + self.labels_per_page - 1) // self.labels_per_page
        if total_pages < 1:
            total_pages = 1
            
        start_idx = self.current_labels_page * self.labels_per_page
        end_idx = min(start_idx + self.labels_per_page, total_items)
        page_items = self.flattened_labels[start_idx:end_idx]
        
        rows = []
        for item in page_items:
            protection = "🛡️ Yes" if item["hasProtection"] else "🔓 No"
            status_str = "🟢 Enabled" if item["isEnabled"] else "🔴 Disabled"
            
            name_color = COLOR_TEXT_MAIN if not item["is_sublabel"] else COLOR_TEXT_SUB
            name_weight = ft.FontWeight.BOLD if not item["is_sublabel"] else ft.FontWeight.NORMAL
            
            rows.append(ft.DataRow(cells=[
                ft.DataCell(ft.Text(item["name"], weight=name_weight, color=name_color)),
                ft.DataCell(ft.Text(item["description"])),
                ft.DataCell(ft.Text(protection)),
                ft.DataCell(ft.Text(str(item["applicationMode"]).capitalize())),
                ft.DataCell(ft.Text(str(item["priority"]))),
                ft.DataCell(ft.Text(", ".join([x.capitalize() for x in item["applicableTo"].split(",") if x.strip()]) or "N/A")),
                ft.DataCell(ft.Text(status_str))
            ]))
            
        table = ft.DataTable(
            columns=[
                ft.DataColumn(ft.Text("Sensitivity Label", weight=ft.FontWeight.BOLD)),
                ft.DataColumn(ft.Text("Description", weight=ft.FontWeight.BOLD)),
                ft.DataColumn(ft.Text("Protection", weight=ft.FontWeight.BOLD)),
                ft.DataColumn(ft.Text("Mode", weight=ft.FontWeight.BOLD)),
                ft.DataColumn(ft.Text("Priority", weight=ft.FontWeight.BOLD)),
                ft.DataColumn(ft.Text("Applicable Targets", weight=ft.FontWeight.BOLD)),
                ft.DataColumn(ft.Text("Status", weight=ft.FontWeight.BOLD)),
            ],
            rows=rows,
            border=ft.Border.all(1, COLOR_OUTLINE_LIGHT),
            border_radius=8,
            heading_row_color=COLOR_TONAL_BG,
        )
        self.labels_section.content_container.content = ft.Column([table], scroll=ft.ScrollMode.AUTO, height=350)
        
        # Update Pagination Control status
        self.labels_pagination_info.value = f"Page {self.current_labels_page + 1} of {total_pages}"
        self.labels_prev_btn.disabled = (self.current_labels_page <= 0)
        self.labels_next_btn.disabled = (self.current_labels_page >= total_pages - 1)
        self.labels_pagination_row.visible = (total_items > self.labels_per_page)
        
        if self.page:
            self.labels_section.update()
            self.labels_pagination_row.update()

    def handle_labels_prev(self, e):
        if self.current_labels_page > 0:
            self.current_labels_page -= 1
            self.render_labels_page()

    def handle_labels_next(self, e):
        total_items = len(self.flattened_labels)
        total_pages = (total_items + self.labels_per_page - 1) // self.labels_per_page
        if self.current_labels_page < total_pages - 1:
            self.current_labels_page += 1
            self.render_labels_page()

    def fetch_pa(self):
        try:
            scanner = PowerAutomateScanner(self.tenant, self.client, self.secret)
            results = scanner.scan_flows()
            if not results:
                self.pa_section.content_container.content = ft.Text("No Power Automate data found.", color=COLOR_TEXT_SUB)
                self.export_pa_btn.disabled = True
            else:
                total_envs = results.get("total_environments", 0)
                counts = results.get("counts", {})
                total_flows = counts.get("Cloud Flows", 0) + counts.get("Desktop Flows", 0)
                premium_conns = results.get("premium_connectors", [])
                custom_conns = results.get("custom_connectors", [])
                self.last_complex_flows = results.get("complex_logic_flows", [])
                
                prem_str = ", ".join(premium_conns) if premium_conns else "0"
                cust_str = ", ".join(custom_conns) if custom_conns else "0"
                
                rows_data = [
                    ("Total Environments Scanned", str(total_envs)),
                    ("Total Flows (Active + Inactive)", str(total_flows)),
                    ("Premium Connectors In Use", prem_str),
                    ("Custom Connectors In Use", cust_str),
                ]
                rows = [ft.DataRow(cells=[
                    ft.DataCell(ft.Text(str(r[0]), weight=ft.FontWeight.BOLD)), 
                    ft.DataCell(ft.Text(str(r[1])))
                ]) for r in rows_data]
                
                table = ft.DataTable(
                    columns=[
                        ft.DataColumn(ft.Text("Metric", weight=ft.FontWeight.BOLD)),
                        ft.DataColumn(ft.Text("Value", weight=ft.FontWeight.BOLD)),
                    ],
                    rows=rows,
                    border=ft.Border.all(1, COLOR_OUTLINE_LIGHT),
                    border_radius=8,
                    heading_row_color=COLOR_TONAL_BG,
                )
                self.pa_section.content_container.content = table
                self.export_pa_btn.disabled = (len(self.last_complex_flows) == 0)
            
            self.mark_complete("pa", "success")
        except Exception as e:
            self.set_error(self.pa_section, str(e))
            self.export_pa_btn.disabled = True
            self.mark_complete("pa", "error")
        finally:
            self.clear_loading(self.pa_section)
            if self.page:
                self.pa_section.update()
                self.export_pa_btn.update()

    # --- CSV Export Handlers ---
    
    def handle_export_skus(self, e):
        def on_save_result(save_event: ft.FilePickerResultEvent):
            if save_event.path:
                try:
                    headers = ["SKU Part Number", "Units", "Consumed Units", "Included Service Plans", "Applies To"]
                    rows = []
                    for item in self.last_licenses_items:
                        sku_name = item.get("skuPartNumber", "UNKNOWN_SKU")
                        prepaid = item.get("prepaidUnits", {})
                        enabled_units = prepaid.get("enabled", 0)
                        warn_units = prepaid.get("warning", 0)
                        susp_units = prepaid.get("suspended", 0)

                        prepaid_str = f"Enabled: {enabled_units:,}"
                        if warn_units > 0: prepaid_str += f"\nWarn: {warn_units:,}"
                        if susp_units > 0: prepaid_str += f"\nSusp: {susp_units:,}"
                        consumed_str = f"{item.get('consumedUnits', 0):,}"

                        plans = item.get("servicePlans", [])

                        if not plans:
                            rows.append([sku_name, prepaid_str, consumed_str, "None designated.", "-"])
                        else:
                            for idx, p in enumerate(plans):
                                p_name = p.get("servicePlanName", "UnnamedPlan")
                                p_scope = p.get("appliesTo", "Unknown")
                                if idx == 0:
                                    rows.append([sku_name, prepaid_str, consumed_str, p_name, p_scope])
                                else:
                                    rows.append(["", "", "", p_name, p_scope])

                    with open(save_event.path, 'w', newline='', encoding='utf-8') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerow(headers)
                        writer.writerows(rows)
                        
                except Exception as ex:
                    print(f"Failed to export SKUs: {ex}")
        
        picker = ft.FilePicker(on_result=on_save_result)
        e.page.overlay.append(picker)
        e.page.update()
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        picker.save_file(file_name=f"licenses_inventory_{ts}.csv")

    def handle_export_pa(self, e):
        def on_save_result(save_event: ft.FilePickerResultEvent):
            if save_event.path:
                try:
                    headers = ["Environment", "Name", "Type", "Tier", "Active", "Reason"]
                    rows = []
                    for flow in self.last_complex_flows:
                        rows.append([
                            flow.get("Environment"),
                            flow.get("Name"),
                            flow.get("Type"),
                            flow.get("Tier"),
                            flow.get("Active"),
                            flow.get("Reason")
                        ])

                    with open(save_event.path, 'w', newline='', encoding='utf-8') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerow(headers)
                        writer.writerows(rows)
                except Exception as ex:
                    print(f"Failed to export complex flows: {ex}")
        
        picker = ft.FilePicker(on_result=on_save_result)
        e.page.overlay.append(picker)
        e.page.update()
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        picker.save_file(file_name=f"complex_flows_{ts}.csv")
