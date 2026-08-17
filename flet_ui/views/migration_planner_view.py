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

"""Placeholder / Transition view for Migration Planner module."""

from typing import Callable, Optional
import flet as ft
from flet_ui.styles import (
    COLOR_BORDER,
    COLOR_PRIMARY,
    COLOR_SURFACE,
    COLOR_TEXT_PRIMARY,
    COLOR_TEXT_SECONDARY,
)


class MigrationPlannerPlaceholderView(ft.Container):
    """Placeholder view for the Migration Planner module."""

    def __init__(self, page: ft.Page, on_back: Optional[Callable] = None):
        super().__init__()
        self.page_ref = page
        self.on_back = on_back
        self.expand = True
        self.alignment = ft.alignment.Alignment(0, 0)

        card = ft.Container(
            width=600,
            padding=40,
            bgcolor=COLOR_SURFACE,
            border_radius=16,
            border=ft.Border.all(1, COLOR_BORDER),
            shadow=ft.BoxShadow(
                spread_radius=0,
                blur_radius=16,
                color="#00000010",
                offset=ft.Offset(0, 4),
            ),
            content=ft.Column(
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                spacing=16,
                controls=[
                    ft.Container(
                        width=64,
                        height=64,
                        border_radius=32,
                        bgcolor="#E0EDFD",
                        alignment=ft.alignment.Alignment(0, 0),
                        content=ft.Icon(
                            ft.Icons.SEARCH_ROUNDED,
                            size=32,
                            color=COLOR_PRIMARY,
                        ),
                    ),
                    ft.Text(
                        "Migration Planner",
                        size=24,
                        weight=ft.FontWeight.BOLD,
                        color=COLOR_TEXT_PRIMARY,
                    ),
                    ft.Text(
                        "Analyze Microsoft 365 readiness and plan migration batches for Exchange Online, Files, and Teams.",
                        size=14,
                        color=COLOR_TEXT_SECONDARY,
                        text_align=ft.TextAlign.CENTER,
                    ),
                    ft.Container(height=12),
                    ft.OutlinedButton(
                        content=ft.Row(
                            tight=True,
                            spacing=8,
                            controls=[
                                ft.Icon(ft.Icons.ARROW_BACK_ROUNDED, size=18),
                                ft.Text("Back to Home", size=14, weight=ft.FontWeight.W_600),
                            ],
                        ),
                        on_click=lambda e: self.on_back() if self.on_back else None,
                        style=ft.ButtonStyle(
                            shape=ft.RoundedRectangleBorder(radius=8),
                            padding=ft.Padding(20, 12, 20, 12),
                        ),
                    ),
                ],
            ),
        )

        self.content = ft.Column(
            alignment=ft.MainAxisAlignment.CENTER,
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            controls=[card],
        )
