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

"""Usage & Adoption Dashboard view and layout skeleton."""

from typing import Callable, Dict, List, Optional
import flet as ft
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
from flet_ui.views.sections.identity_licensing_view import IdentityLicensingView


class UsageAdoptionDashboardView(ft.Container):
    """Telemetry and adoption reports dashboard skeleton with sidebar navigation and system metrics."""

    SECTIONS: List[Dict[str, any]] = [
        {
            "id": "identity",
            "title": "Identity & Licensing",
            "icon": ft.Icons.BADGE_OUTLINED,
            "description": "Inspect user licenses, assigned SKUs, active accounts, directory synchronization, and domain configurations across your tenant.",
        },
        {
            "id": "usage",
            "title": "App Usage, Adoption & Collaboration",
            "icon": ft.Icons.ANALYTICS_OUTLINED,
            "description": "Analyze Microsoft Teams, Exchange, SharePoint, OneDrive, and M365 Apps activity and collaboration trends.",
        },
        {
            "id": "security",
            "title": "Security, Compliance & Governance",
            "icon": ft.Icons.SHIELD_OUTLINED,
            "description": "Monitor compliance policies, DLP rules, retention policies, and potential threat posture across your tenant.",
        },
        {
            "id": "ecosystem",
            "title": "Ecosystem, Integrations & Automation",
            "icon": ft.Icons.HUB_OUTLINED,
            "description": "Discover Power Automate flows, third-party app permissions, API connections, and automation integrations.",
        },
    ]

    def __init__(
        self,
        page: ft.Page,
        on_back_to_hub: Optional[Callable[[], None]] = None,
        on_disconnect: Optional[Callable[[], None]] = None,
        tenant: str = "",
        client: str = "",
        secret: str = "",
    ):
        super().__init__()
        self.page_ref = page
        self.on_back_to_hub = on_back_to_hub
        self.on_disconnect = on_disconnect
        self.tenant = tenant
        self.client = client
        self.secret = secret

        self.expand = True
        self.bgcolor = COLOR_APP_BG
        # Generous breathing space on left and right margins
        self.padding = ft.Padding(36, 18, 36, 22)

        self.selected_index = 0

        # Section View Instances (Lazy/Persistent)
        self.identity_view = IdentityLicensingView(
            page=self.page_ref,
            tenant=self.tenant,
            client=self.client,
            secret=self.secret,
        )

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

    def _build_header(self) -> ft.Container:
        """Constructs the top header bar with Back to Hub and Export data buttons."""
        return ft.Container(
            content=ft.Row(
                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                controls=[
                    # Left: Back button + Title
                    ft.Row(
                        spacing=8,
                        vertical_alignment=ft.CrossAxisAlignment.CENTER,
                        controls=[
                            ft.IconButton(
                                icon=ft.Icons.ARROW_BACK_ROUNDED,
                                icon_size=20,
                                icon_color=COLOR_TEXT_PRIMARY,
                                tooltip="Back to Hub",
                                on_click=lambda _: self._handle_back_to_hub(),
                            ),
                            ft.Text(
                                "Usage and adoption",
                                size=16,
                                weight=ft.FontWeight.W_600,
                                color=COLOR_TEXT_PRIMARY,
                            ),
                        ],
                    ),
                    # Right: Export data button (Read me removed)
                    ft.TextButton(
                        content=ft.Row(
                            tight=True,
                            spacing=6,
                            controls=[
                                ft.Icon(ft.Icons.DOWNLOAD_ROUNDED, size=16, color=COLOR_TEXT_PRIMARY),
                                ft.Text("Export data", size=13, weight=ft.FontWeight.W_500, color=COLOR_TEXT_PRIMARY),
                            ],
                        ),
                        on_click=lambda _: self._handle_export_data(),
                        style=ft.ButtonStyle(
                            padding=ft.Padding(12, 8, 12, 8),
                        ),
                    ),
                ],
            ),
            padding=ft.Padding(0, 2, 0, 4),
        )

    def _build_sidebar(self) -> ft.Container:
        """Constructs the non-collapsible left navigation panel with system metrics card."""
        self.nav_items_column = ft.Column(
            spacing=6,
            controls=[self._create_nav_item(i, sec) for i, sec in enumerate(self.SECTIONS)],
        )

        # Nav items container with top breathing room from heading
        nav_container = ft.Container(
            content=self.nav_items_column,
            padding=ft.Padding(0, 16, 0, 0),
        )

        # Bottom System Info Box with 3-dots popup menu
        disconnect_menu = ft.PopupMenuButton(
            icon=ft.Icons.MORE_VERT_ROUNDED,
            icon_size=18,
            icon_color=COLOR_TEXT_SECONDARY,
            tooltip="Options",
            items=[
                ft.PopupMenuItem(
                    content=ft.Row(
                        tight=True,
                        spacing=8,
                        controls=[
                            ft.Icon(ft.Icons.LOGOUT_ROUNDED, color=COLOR_ERROR, size=16),
                            ft.Text("Disconnect", color=COLOR_ERROR, weight=ft.FontWeight.W_600, size=13),
                        ],
                    ),
                    on_click=lambda _: self._handle_disconnect(),
                ),
            ],
        )

        def create_metric_row(label: str, value: str) -> ft.Row:
            return ft.Row(
                alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                controls=[
                    ft.Text(label, size=12, color=COLOR_TEXT_SECONDARY),
                    ft.Text(value, size=12, weight=ft.FontWeight.W_600, color=COLOR_TEXT_PRIMARY),
                ],
            )

        # Shifted upwards with bottom margin for visibility
        system_metrics_box = ft.Container(
            bgcolor=COLOR_SURFACE,
            border=ft.Border.all(1, COLOR_BORDER),
            border_radius=12,
            padding=14,
            margin=ft.Margin(0, 0, 0, 16),
            content=ft.Column(
                spacing=10,
                tight=True,
                controls=[
                    # Active connection badge + 3 dots menu
                    ft.Row(
                        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                        vertical_alignment=ft.CrossAxisAlignment.CENTER,
                        controls=[
                            ft.Container(
                                content=ft.Row(
                                    tight=True,
                                    spacing=6,
                                    controls=[
                                        ft.Container(
                                            width=8,
                                            height=8,
                                            border_radius=4,
                                            bgcolor="#10B981",
                                        ),
                                        ft.Text(
                                            "Active connection",
                                            size=11,
                                            weight=ft.FontWeight.W_600,
                                            color="#15803D",
                                        ),
                                    ],
                                ),
                                bgcolor="#DCFCE7",
                                border_radius=12,
                                padding=ft.Padding(8, 4, 8, 4),
                            ),
                            disconnect_menu,
                        ],
                    ),
                    ft.Divider(height=1, color=COLOR_BORDER),
                    # System Info parameters (hard-coded N/A)
                    create_metric_row("RAM used", "N/A"),
                    create_metric_row("Network Strength", "N/A"),
                    create_metric_row("Available Disk Space", "N/A"),
                ],
            ),
        )

        sidebar_content = ft.Column(
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            controls=[
                nav_container,
                system_metrics_box,
            ],
        )

        return ft.Container(
            width=280,
            content=sidebar_content,
        )

    def _create_nav_item(self, index: int, section: Dict[str, any]) -> ft.Container:
        """Creates an individual navigation item with active / hover highlights."""
        is_selected = index == self.selected_index

        bgcolor = COLOR_HERO_BG if is_selected else "transparent"
        text_color = COLOR_PRIMARY if is_selected else COLOR_TEXT_PRIMARY
        icon_color = COLOR_PRIMARY if is_selected else COLOR_TEXT_SECONDARY
        font_weight = ft.FontWeight.BOLD if is_selected else ft.FontWeight.W_500

        return ft.Container(
            border_radius=10,
            bgcolor=bgcolor,
            padding=ft.Padding(12, 10, 12, 10),
            ink=True,
            on_click=lambda _: self._select_section(index),
            content=ft.Row(
                spacing=12,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                controls=[
                    ft.Icon(section["icon"], size=18, color=icon_color),
                    ft.Text(
                        section["title"],
                        size=13,
                        weight=font_weight,
                        color=text_color,
                        expand=True,
                    ),
                ],
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
        return self._render_active_section_placeholder()

    def _render_active_section_placeholder(self) -> ft.Control:
        """Renders the section placeholder with icon, description, and Fetch Data button."""
        current_sec = self.SECTIONS[self.selected_index]

        return ft.Column(
            alignment=ft.MainAxisAlignment.CENTER,
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            spacing=16,
            controls=[
                # Section Icon Badge
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
                # Section Title
                ft.Text(
                    current_sec["title"],
                    size=22,
                    weight=ft.FontWeight.BOLD,
                    color=COLOR_TEXT_PRIMARY,
                ),
                # Section Description
                ft.Container(
                    width=520,
                    content=ft.Text(
                        current_sec["description"],
                        size=14,
                        color=COLOR_TEXT_SECONDARY,
                        text_align=ft.TextAlign.CENTER,
                    ),
                ),
                ft.Container(height=8),
                # Fetch Data Action Button
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
        """Switches active section and updates sidebar and main content area."""
        if self.selected_index != index:
            self.selected_index = index
            # Rebuild nav items
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
        sec_name = self.SECTIONS[self.selected_index]["title"]
        snack = ft.SnackBar(
            content=ft.Text(f"Fetching telemetry data for {sec_name}..."),
            bgcolor=ft.Colors.BLUE_GREY_800,
        )
        self.page_ref.overlay.append(snack)
        snack.open = True
        self.page_ref.update()

    def _handle_export_data(self):
        """Handles export data trigger."""
        snack = ft.SnackBar(
            content=ft.Text("Exporting tenant telemetry report..."),
            bgcolor=ft.Colors.BLUE_GREY_800,
        )
        self.page_ref.overlay.append(snack)
        snack.open = True
        self.page_ref.update()

    def _handle_back_to_hub(self):
        """Redirects back to the Hub landing page."""
        if self.on_back_to_hub:
            self.on_back_to_hub()

    def _handle_disconnect(self):
        """Redirects back to the Usage & Adoption authentication view."""
        if self.on_disconnect:
            self.on_disconnect()
