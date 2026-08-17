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

"""Home Screen view for Deal Assistant Platform."""

from typing import Callable, Optional
import flet as ft

from flet_ui.components.app_card import AppCard
from flet_ui.components.dialogs import show_help_dialog, show_readme_dialog
from flet_ui.styles import (
    COLOR_BORDER,
    COLOR_HERO_BG,
    COLOR_PRIMARY,
    COLOR_SURFACE,
    COLOR_TEXT_HERO_BADGE,
    COLOR_TEXT_HERO_SUB,
    COLOR_TEXT_PRIMARY,
    COLOR_TEXT_SECONDARY,
)


class HomeView(ft.Container):
    """Modern, responsive Home Screen matching the Deal Assistant Platform design."""

    def __init__(
        self,
        page: ft.Page,
        on_open_usage_adoption: Optional[Callable] = None,
        on_open_migration_planner: Optional[Callable] = None,
    ):
        super().__init__()
        self.page_ref = page
        self.on_open_usage_adoption = on_open_usage_adoption
        self.on_open_migration_planner = on_open_migration_planner
        self.expand = True
        self.alignment = ft.alignment.Alignment(0, 0)

        # ---------------- Left Hero Pane ----------------
        # Top Icon Badge: Analytics / Chart Icon
        hero_icon_badge = ft.Container(
            width=54,
            height=54,
            border_radius=14,
            bgcolor=COLOR_SURFACE,
            alignment=ft.alignment.Alignment(0, 0),
            shadow=ft.BoxShadow(
                spread_radius=0,
                blur_radius=10,
                color="#00000010",
                offset=ft.Offset(0, 2),
            ),
            content=ft.Icon(
                ft.Icons.QUERY_STATS_ROUNDED,
                size=28,
                color=COLOR_PRIMARY,
            ),
        )

        # Security Badge Capsule
        security_badge = ft.Container(
            bgcolor=COLOR_SURFACE,
            border_radius=20,
            padding=ft.Padding(14, 7, 16, 7),
            shadow=ft.BoxShadow(
                spread_radius=0,
                blur_radius=6,
                color="#0000000A",
                offset=ft.Offset(0, 1),
            ),
            content=ft.Row(
                tight=True,
                spacing=7,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                controls=[
                    ft.Icon(
                        ft.Icons.LOCK_OUTLINE_ROUNDED,
                        size=15,
                        color=COLOR_TEXT_HERO_BADGE,
                    ),
                    ft.Text(
                        "Data stays secure locally",
                        size=13,
                        weight=ft.FontWeight.W_600,
                        color=COLOR_TEXT_HERO_BADGE,
                    ),
                ],
            ),
        )

        # Left bottom action buttons: Read me and Help
        readme_button = ft.TextButton(
            content=ft.Row(
                tight=True,
                spacing=6,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                controls=[
                    ft.Icon(
                        ft.Icons.MENU_BOOK_ROUNDED,
                        size=18,
                        color=COLOR_TEXT_PRIMARY,
                    ),
                    ft.Text(
                        "Read me",
                        size=14,
                        weight=ft.FontWeight.W_500,
                        color=COLOR_TEXT_PRIMARY,
                    ),
                ],
            ),
            on_click=lambda e: show_readme_dialog(self.page_ref),
            style=ft.ButtonStyle(
                padding=ft.Padding(10, 6, 10, 6),
            ),
        )

        help_button = ft.TextButton(
            content=ft.Row(
                tight=True,
                spacing=6,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                controls=[
                    ft.Icon(
                        ft.Icons.HELP_OUTLINE_ROUNDED,
                        size=18,
                        color=COLOR_TEXT_PRIMARY,
                    ),
                    ft.Text(
                        "Help",
                        size=14,
                        weight=ft.FontWeight.W_500,
                        color=COLOR_TEXT_PRIMARY,
                    ),
                ],
            ),
            on_click=lambda e: show_help_dialog(self.page_ref),
            style=ft.ButtonStyle(
                padding=ft.Padding(10, 6, 10, 6),
            ),
        )

        left_pane = ft.Container(
            width=400,
            bgcolor=COLOR_HERO_BG,
            border_radius=ft.BorderRadius(top_left=20, bottom_left=20, top_right=0, bottom_right=0),
            padding=ft.Padding(36, 36, 36, 30),
            content=ft.Column(
                spacing=0,
                horizontal_alignment=ft.CrossAxisAlignment.START,
                controls=[
                    hero_icon_badge,
                    ft.Container(height=24),
                    ft.Text(
                        "Deal Assistant\nPlatform",
                        size=32,
                        weight=ft.FontWeight.BOLD,
                        color=COLOR_TEXT_PRIMARY,
                    ),
                    ft.Container(height=14),
                    security_badge,
                    ft.Container(height=16),
                    ft.Text(
                        "Your localized, secure environment for Microsoft 365 analytics and migration planning.",
                        size=14,
                        color=COLOR_TEXT_HERO_SUB,
                    ),
                    ft.Container(expand=True),
                    ft.Row(
                        spacing=16,
                        controls=[readme_button, help_button],
                    ),
                ],
            ),
        )

        # ---------------- Right Applications Pane ----------------
        right_header = ft.Column(
            spacing=4,
            controls=[
                ft.Text(
                    "Available Applications",
                    size=24,
                    weight=ft.FontWeight.BOLD,
                    color=COLOR_TEXT_PRIMARY,
                ),
                ft.Text(
                    "Select an app to begin your secure session.",
                    size=14,
                    color=COLOR_TEXT_SECONDARY,
                ),
            ],
        )

        # List of Available Applications (Usage & Adoption, Migration Planner)
        apps_container = ft.Container(
            border=ft.Border.all(1, COLOR_BORDER),
            border_radius=16,
            bgcolor=COLOR_SURFACE,
            clip_behavior=ft.ClipBehavior.ANTI_ALIAS,
            content=ft.Column(
                spacing=0,
                controls=[
                    AppCard(
                        title="Usage & Adoption",
                        description="Uncover how your teams use provisioned tools.",
                        icon=ft.Icons.BAR_CHART_ROUNDED,
                        on_click=self._handle_usage_adoption_click,
                    ),
                    ft.Divider(height=1, thickness=1, color=COLOR_BORDER),
                    AppCard(
                        title="Migration Planner",
                        description="Analyze current Microsoft 365 readiness.",
                        icon=ft.Icons.SEARCH_ROUNDED,
                        on_click=self._handle_migration_planner_click,
                    ),
                ],
            ),
        )

        right_pane = ft.Container(
            expand=True,
            bgcolor=COLOR_SURFACE,
            border_radius=ft.BorderRadius(top_left=0, bottom_left=0, top_right=20, bottom_right=20),
            padding=ft.Padding(44, 36, 44, 36),
            content=ft.Column(
                spacing=0,
                alignment=ft.MainAxisAlignment.CENTER,
                horizontal_alignment=ft.CrossAxisAlignment.START,
                controls=[
                    right_header,
                    ft.Container(height=24),
                    apps_container,
                ],
            ),
        )

        # Main Card Layout - Larger screen footprint with balanced vertical height
        main_card = ft.Container(
            width=980,
            height=470,
            bgcolor=COLOR_SURFACE,
            border_radius=20,
            border=ft.Border.all(1, COLOR_BORDER),
            clip_behavior=ft.ClipBehavior.ANTI_ALIAS,
            shadow=ft.BoxShadow(
                spread_radius=0,
                blur_radius=24,
                color="#00000012",
                offset=ft.Offset(0, 6),
            ),
            content=ft.Row(
                spacing=0,
                expand=True,
                controls=[left_pane, right_pane],
            ),
        )

        self.content = ft.Column(
            alignment=ft.MainAxisAlignment.CENTER,
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            controls=[main_card],
        )

    def _handle_usage_adoption_click(self, e: ft.ControlEvent):
        if self.on_open_usage_adoption:
            self.on_open_usage_adoption()

    def _handle_migration_planner_click(self, e: ft.ControlEvent):
        if self.on_open_migration_planner:
            self.on_open_migration_planner()
