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

"""Reusable application selection card component."""

from typing import Callable, Optional
import flet as ft
from flet_ui.styles import (
    COLOR_BORDER,
    COLOR_HOVER_BG,
    COLOR_ICON_BG,
    COLOR_SURFACE,
    COLOR_TEXT_MUTED,
    COLOR_TEXT_PRIMARY,
    COLOR_TEXT_SECONDARY,
)


class AppCard(ft.Container):
    """Clickable application card with icon, title, description, and hover animation."""

    def __init__(
        self,
        title: str,
        description: str,
        icon: ft.IconData,
        on_click: Optional[Callable] = None,
        is_locked: bool = False,
    ):
        self.card_title = title
        self.card_description = description
        self.custom_on_click = on_click
        self.is_locked = is_locked

        icon_control = ft.Container(
            width=50,
            height=50,
            border_radius=25,
            bgcolor=COLOR_ICON_BG,
            alignment=ft.alignment.Alignment(0, 0),
            content=ft.Icon(
                icon,
                size=24,
                color=COLOR_TEXT_PRIMARY if not is_locked else COLOR_TEXT_MUTED,
            ),
        )

        text_content = ft.Column(
            spacing=4,
            alignment=ft.MainAxisAlignment.CENTER,
            controls=[
                ft.Text(
                    title,
                    size=16,
                    weight=ft.FontWeight.W_600,
                    color=COLOR_TEXT_PRIMARY if not is_locked else COLOR_TEXT_MUTED,
                ),
                ft.Text(
                    description,
                    size=14,
                    weight=ft.FontWeight.W_400,
                    color=COLOR_TEXT_SECONDARY if not is_locked else COLOR_TEXT_MUTED,
                ),
            ],
        )

        trailing_control = ft.Icon(
            ft.Icons.LOCK_OUTLINE_ROUNDED if is_locked else ft.Icons.CHEVRON_RIGHT_ROUNDED,
            size=22,
            color=COLOR_TEXT_MUTED,
        )

        row_content = ft.Row(
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
            controls=[
                ft.Row(
                    spacing=16,
                    vertical_alignment=ft.CrossAxisAlignment.CENTER,
                    controls=[icon_control, text_content],
                ),
                trailing_control,
            ],
        )

        super().__init__(
            content=row_content,
            padding=ft.Padding(20, 18, 20, 18),
            bgcolor=COLOR_SURFACE,
            ink=True,
            border_radius=12,
            on_click=self._handle_click if not is_locked else None,
            on_hover=self._handle_hover if not is_locked else None,
            animate=ft.Animation(150, ft.AnimationCurve.EASE_OUT),
        )

    def _handle_hover(self, e: ft.ControlEvent):
        is_hovered = e.data == "true"
        self.bgcolor = COLOR_HOVER_BG if is_hovered else COLOR_SURFACE
        self.update()

    def _handle_click(self, e: ft.ControlEvent):
        if self.custom_on_click:
            self.custom_on_click(e)
