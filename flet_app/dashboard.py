import flet as ft
from flet_app.styles import *
from flet_app.sidebar import Sidebar
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
from telemetry.sharepoint_onedrive_usage import run_sharepoint_pipeline, run_onedrive_pipeline

class DashboardView(ft.Container):
    def __init__(self, tenant, client, secret, on_disconnect, page: ft.Page = None):
        super().__init__()
        self.tenant = tenant
        self.client = client
        self.secret = secret
        self.on_disconnect = on_disconnect
        self._page_ref = page
        
        self.expand = True
        
        self.sidebar = Sidebar(on_disconnect=self.on_disconnect)
        
        # Saved data for CSV exports
        self.last_licenses_items = []
        
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
                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                vertical_alignment=ft.CrossAxisAlignment.CENTER
            ),
            bgcolor=COLOR_SURFACE,
            border_radius=12,
            border=ft.Border.all(1, COLOR_OUTLINE_LIGHT),
            padding=ft.Padding.symmetric(horizontal=20, vertical=12),
            margin=ft.Margin.only(bottom=16),
            width=float('inf')
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
        self.sku_section = self.create_card("Subscribed SKUs", action_control=sku_actions, on_retry=self.handle_retry_skus)
        
        # 2. O365 Usage Card
        self.o365_section = self.create_card("O365 Active Users Usage", on_retry=self.handle_retry_o365)
        
        # 3. M365 App Usage Card
        self.m365_section = self.create_card("M365 App Usage (180 Days)", on_retry=self.handle_retry_m365)
        
        # 4. Files Card (Combined SharePoint and OneDrive Sections)
        self.sharepoint_container = ft.Container(content=ft.Text("No data yet.", color=COLOR_TEXT_SUB, size=13), width=float('inf'))
        self.onedrive_container = ft.Container(content=ft.Text("No data yet.", color=COLOR_TEXT_SUB, size=13), width=float('inf'))
        self.files_layout = ft.Column(
            controls=[
                ft.Text("SharePoint Site Usage (180 Days)", size=14, weight=ft.FontWeight.BOLD, color=COLOR_PRIMARY),
                self.sharepoint_container,
                ft.Container(height=8),
                ft.Text("OneDrive Usage (180 Days)", size=14, weight=ft.FontWeight.BOLD, color=COLOR_PRIMARY),
                self.onedrive_container
            ],
            spacing=6,
            horizontal_alignment=ft.CrossAxisAlignment.STRETCH
        )
        self.files_section = self.create_card("Files", on_retry=self.handle_retry_files)
        self.files_section.content_container.content = self.files_layout
        
        self.content_area = ft.Column(
            controls=[
                self.sku_section,
                self.o365_section,
                self.m365_section,
                self.files_section,
            ],
            scroll=ft.ScrollMode.AUTO,
            expand=True,
            spacing=16,
            horizontal_alignment=ft.CrossAxisAlignment.STRETCH
        )
        
        self.content = ft.Row(
            controls=[
                self.sidebar,
                ft.Container(
                    content=ft.Column(
                        controls=[self.header, self.content_area],
                        expand=True,
                        spacing=0,
                        horizontal_alignment=ft.CrossAxisAlignment.STRETCH
                    ),
                    expand=True,
                    padding=ft.Padding.only(left=20, right=20, top=10, bottom=20)
                )
            ],
            expand=True
        )

    def _get_page(self) -> ft.Page:
        target = self._page_ref
        if not target:
            try:
                target = self.page
            except Exception:
                target = None
        return target

    def safe_update(self, *controls):
        """
        Thread-safe UI update that schedules execution on the page asyncio event loop.
        This wakes up the event loop immediately so updates are pushed to the client without delay.
        """
        try:
            page = self._get_page()
            if not page:
                return

            loop = getattr(page, "loop", None)
            
            def _do_update():
                try:
                    if controls:
                        for c in controls:
                            if c:
                                try:
                                    c.update()
                                except Exception:
                                    pass
                    page.update()
                except Exception as ex:
                    print(f"Error in _do_update: {ex}")

            if loop and loop.is_running():
                loop.call_soon_threadsafe(_do_update)
            else:
                _do_update()
        except Exception as e:
            print(f"Error in safe_update: {e}")

    def create_card(self, title, action_control=None, bottom_control=None, on_retry=None):
        content_container = ft.Container(
            content=ft.Text("No data yet.", color=COLOR_TEXT_SUB, size=13),
            width=float('inf')
        )
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
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            vertical_alignment=ft.CrossAxisAlignment.CENTER
        )
        
        column_controls = [
            header_row,
            content_container
        ]
        if bottom_control:
            column_controls.append(bottom_control)
            
        card = ft.Container(
            content=ft.Column(
                column_controls,
                spacing=8,
                horizontal_alignment=ft.CrossAxisAlignment.STRETCH
            ),
            bgcolor=COLOR_SURFACE,
            border_radius=12,
            border=ft.Border.all(1, COLOR_OUTLINE_LIGHT),
            padding=ft.Padding.only(left=16, right=16, top=14, bottom=14),
            width=float('inf')
        )
        card.content_container = content_container
        card.retry_btn = retry_btn
        return card

    def create_table(self, columns: list[ft.DataColumn], rows: list[ft.DataRow]) -> ft.DataTable:
        return ft.DataTable(
            columns=columns,
            rows=rows,
            border=ft.Border.all(1, COLOR_OUTLINE_LIGHT),
            border_radius=8,
            heading_row_color=COLOR_TONAL_BG,
            heading_row_height=42,
            data_row_min_height=38,
            data_row_max_height=float('inf'),
            column_spacing=24,
            horizontal_margin=12,
            width=float('inf'),
            expand=True,
        )

    def set_loading(self, card, message):
        card.content_container.content = ft.Column([
            ft.ProgressRing(width=28, height=28),
            ft.Text(message, color=COLOR_TEXT_SUB, size=13)
        ], alignment=ft.MainAxisAlignment.CENTER, horizontal_alignment=ft.CrossAxisAlignment.CENTER, spacing=10)
        if hasattr(card, "retry_btn") and card.retry_btn:
            card.retry_btn.disabled = True

    def set_tab_loading(self, tab_container, message):
        tab_container.content = ft.Column([
            ft.ProgressRing(width=24, height=24),
            ft.Text(message, color=COLOR_TEXT_SUB, size=13)
        ], alignment=ft.MainAxisAlignment.CENTER, horizontal_alignment=ft.CrossAxisAlignment.CENTER, spacing=8)
        
    def set_error(self, card, message):
        card.content_container.content = ft.Text(f"Error: {message}", color=COLOR_ERROR, size=13)
        if hasattr(card, "retry_btn") and card.retry_btn:
            card.retry_btn.disabled = False

    def clear_loading(self, card):
        if hasattr(card, "retry_btn") and card.retry_btn:
            card.retry_btn.disabled = False

    def start_individual_fetch(self, key, section, message, target):
        self.fetch_btn.disabled = True
        self.fetch_statuses[key] = "pending"
        self.set_loading(section, message)
        self.safe_update(self.fetch_btn, section)
        threading.Thread(target=target, daemon=True).start()

    def handle_retry_skus(self, e):
        self.start_individual_fetch("sku", self.sku_section, "Fetching SKU inventories...", self.fetch_skus)

    def handle_retry_o365(self, e):
        self.start_individual_fetch("o365", self.o365_section, "Downloading O365 Active User reports...", self.fetch_o365)

    def handle_retry_m365(self, e):
        self.start_individual_fetch("m365", self.m365_section, "Downloading M365 App reports...", self.fetch_m365)

    def handle_retry_files(self, e):
        self.fetch_btn.disabled = True
        self.fetch_statuses["sharepoint"] = "pending"
        self.fetch_statuses["onedrive"] = "pending"
        self.set_tab_loading(self.sharepoint_container, "Downloading SharePoint reports...")
        self.set_tab_loading(self.onedrive_container, "Downloading OneDrive reports...")
        self.safe_update(self.fetch_btn, self.files_section)
        threading.Thread(target=self.fetch_sharepoint, daemon=True).start()
        threading.Thread(target=self.fetch_onedrive, daemon=True).start()

    def handle_fetch(self, e):
        self.fetch_btn.disabled = True
        self.fetch_btn.content = ft.Text("Fetching...", color=ft.Colors.WHITE)
        
        # Initialize fetch statuses
        self.fetch_statuses = {
            "sku": "pending",
            "o365": "pending",
            "m365": "pending",
            "sharepoint": "pending",
            "onedrive": "pending"
        }
        
        # 1. SKUs
        self.set_loading(self.sku_section, "Fetching SKU inventories...")
        
        # 2. O365 Usage
        self.set_loading(self.o365_section, "Downloading O365 Active User reports...")
        
        # 3. M365 App Usage
        self.set_loading(self.m365_section, "Downloading M365 App reports...")
        
        # 4. Files (SharePoint & OneDrive)
        self.set_tab_loading(self.sharepoint_container, "Downloading SharePoint reports...")
        self.set_tab_loading(self.onedrive_container, "Downloading OneDrive reports...")
        
        self.safe_update(self.fetch_btn, self.sku_section, self.o365_section, self.m365_section, self.files_section)
        
        threading.Thread(target=self.fetch_skus, daemon=True).start()
        threading.Thread(target=self.fetch_o365, daemon=True).start()
        threading.Thread(target=self.fetch_m365, daemon=True).start()
        threading.Thread(target=self.fetch_sharepoint, daemon=True).start()
        threading.Thread(target=self.fetch_onedrive, daemon=True).start()

    def mark_complete(self, key, status):
        self.fetch_statuses[key] = status
        if key in ["sharepoint", "onedrive"]:
            if self.fetch_statuses.get("sharepoint") != "pending" and self.fetch_statuses.get("onedrive") != "pending":
                self.clear_loading(self.files_section)
                self.safe_update(self.files_section)
        self.check_all_done()

    def check_all_done(self):
        if not self.fetch_statuses:
            return
        if "pending" not in self.fetch_statuses.values():
            self.fetch_btn.disabled = False
            self.fetch_btn.content = ft.Text("Fetch Report", weight=ft.FontWeight.BOLD)
            self.safe_update(self.fetch_btn)

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
                self.sku_section.content_container.content = ft.Text("No subscribed product configurations found.", color=COLOR_TEXT_SUB, size=13)
                self.export_sku_btn.disabled = True
            else:
                items.sort(key=lambda x: len(x.get("servicePlans", [])), reverse=True)
                rows = []
                for item in items:
                    prepaid = item.get("prepaidUnits", {})
                    p_str = f"Enabled: {prepaid.get('enabled', 0):,}"
                    if prepaid.get('warning', 0) > 0: p_str += f", Warn: {prepaid.get('warning'):,}"
                    if prepaid.get('suspended', 0) > 0: p_str += f", Susp: {prepaid.get('suspended'):,}"
                    rows.append(ft.DataRow(cells=[
                        ft.DataCell(ft.Text(item.get("skuPartNumber", "UNKNOWN_SKU"), weight=ft.FontWeight.BOLD)),
                        ft.DataCell(ft.Text(p_str)),
                        ft.DataCell(ft.Text(f"{item.get('consumedUnits', 0):,}"))
                    ]))
                
                table = self.create_table(
                    columns=[
                        ft.DataColumn(ft.Text("SKU Part Number", weight=ft.FontWeight.BOLD)),
                        ft.DataColumn(ft.Text("Units", weight=ft.FontWeight.BOLD)),
                        ft.DataColumn(ft.Text("Consumed Units", weight=ft.FontWeight.BOLD)),
                    ],
                    rows=rows
                )
                self.sku_section.content_container.content = table
                self.export_sku_btn.disabled = False
            
            self.mark_complete("sku", "success")
        except Exception as e:
            self.set_error(self.sku_section, str(e))
            self.export_sku_btn.disabled = True
            self.mark_complete("sku", "error")
        finally:
            self.clear_loading(self.sku_section)
            self.safe_update(self.sku_section, self.export_sku_btn)

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
                self.o365_section.content_container.content = ft.Text("No O365 usage data found.", color=COLOR_TEXT_SUB, size=13)
            else:
                rows = []
                for row_data in o365_data:
                    rows.append(ft.DataRow(cells=[
                        ft.DataCell(ft.Text(str(row_data[0]), weight=ft.FontWeight.BOLD)),
                        ft.DataCell(ft.Text(f"{row_data[1]:,}")),
                        ft.DataCell(ft.Text(f"{row_data[2]:,}")),
                        ft.DataCell(ft.Text(f"{row_data[3]:,}"))
                    ]))
                
                table = self.create_table(
                    columns=[
                        ft.DataColumn(ft.Text("Service", weight=ft.FontWeight.BOLD)),
                        ft.DataColumn(ft.Text("30 Days", weight=ft.FontWeight.BOLD)),
                        ft.DataColumn(ft.Text("90 Days", weight=ft.FontWeight.BOLD)),
                        ft.DataColumn(ft.Text("180 Days", weight=ft.FontWeight.BOLD)),
                    ],
                    rows=rows
                )
                self.o365_section.content_container.content = table
            
            self.mark_complete("o365", "success")
        except Exception as e:
            self.set_error(self.o365_section, str(e))
            self.mark_complete("o365", "error")
        finally:
            self.clear_loading(self.o365_section)
            self.safe_update(self.o365_section)

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
                self.m365_section.content_container.content = ft.Text("No M365 App usage data found.", color=COLOR_TEXT_SUB, size=13)
            else:
                rows = []
                for row in m365_data:
                    rows.append(ft.DataRow(cells=[
                        ft.DataCell(ft.Text(str(row[0]), weight=ft.FontWeight.BOLD)),
                        ft.DataCell(ft.Text(f"{row[1]:,}"))
                    ]))
                
                table = self.create_table(
                    columns=[
                        ft.DataColumn(ft.Text("App / Platform", weight=ft.FontWeight.BOLD)),
                        ft.DataColumn(ft.Text("Users Count", weight=ft.FontWeight.BOLD)),
                    ],
                    rows=rows
                )
                self.m365_section.content_container.content = table
            
            self.mark_complete("m365", "success")
        except Exception as e:
            self.set_error(self.m365_section, str(e))
            self.mark_complete("m365", "error")
        finally:
            self.clear_loading(self.m365_section)
            self.safe_update(self.m365_section)

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
            
            table = self.create_table(
                columns=[
                    ft.DataColumn(ft.Text("SharePoint Site Metric Description", weight=ft.FontWeight.BOLD)),
                    ft.DataColumn(ft.Text("Value / Measurement", weight=ft.FontWeight.BOLD)),
                ],
                rows=rows
            )
            self.sharepoint_container.content = table
            self.mark_complete("sharepoint", "success")
        except Exception as e:
            self.sharepoint_container.content = ft.Text(f"Error: {e}", color=COLOR_ERROR, size=13)
            self.mark_complete("sharepoint", "error")
        finally:
            self.safe_update(self.sharepoint_container, self.files_section)

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
            
            table = self.create_table(
                columns=[
                    ft.DataColumn(ft.Text("OneDrive Metric Description", weight=ft.FontWeight.BOLD)),
                    ft.DataColumn(ft.Text("Value / Measurement", weight=ft.FontWeight.BOLD)),
                ],
                rows=rows
            )
            self.onedrive_container.content = table
            self.mark_complete("onedrive", "success")
        except Exception as e:
            self.onedrive_container.content = ft.Text(f"Error: {e}", color=COLOR_ERROR, size=13)
            self.mark_complete("onedrive", "error")
        finally:
            self.safe_update(self.onedrive_container, self.files_section)

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
                        if warn_units > 0: prepaid_str += f", Warn: {warn_units:,}"
                        if susp_units > 0: prepaid_str += f", Susp: {susp_units:,}"
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
        page = self._get_page()
        if page:
            page.overlay.append(picker)
            self.safe_update()
            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            picker.save_file(file_name=f"licenses_inventory_{ts}.csv")
