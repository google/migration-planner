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

"""Authentication and API Connection View for Usage & Adoption."""

from typing import Callable, Optional
import flet as ft
from flet_ui.components.dialogs import show_delegated_auth_learn_more_dialog
from flet_ui.styles import (
    COLOR_BORDER,
    COLOR_ERROR,
    COLOR_PRIMARY,
    COLOR_SURFACE,
    COLOR_TEXT_PRIMARY,
    COLOR_TEXT_SECONDARY,
)


class AuthView(ft.Container):
    """Authentication view matching the Deal Assistant login interface with Delegated Auth & Cancellation support."""

    def __init__(
        self,
        on_connect_clicked: Callable[[str, str, str, bool], None],
        on_back_to_hub: Optional[Callable[[], None]] = None,
        on_cancel_clicked: Optional[Callable[[], None]] = None,
        page: Optional[ft.Page] = None,
    ):
        super().__init__()
        self.on_connect_clicked = on_connect_clicked
        self.on_back_to_hub = on_back_to_hub
        self.on_cancel_clicked = on_cancel_clicked
        self._page_ref = page
        self.expand = True

        # Stretched text fields matching the full width of the card's right section
        self.tenant_input = ft.TextField(
            border_color="#BDBDBD",
            focused_border_color="#0b57d0",
            text_size=14,
            content_padding=ft.Padding(12, 10, 12, 10),
            border_radius=6,
            width=float("inf"),
        )
        self.client_input = ft.TextField(
            border_color="#BDBDBD",
            focused_border_color="#0b57d0",
            text_size=14,
            content_padding=ft.Padding(12, 10, 12, 10),
            border_radius=6,
            width=float("inf"),
        )
        self.secret_input = ft.TextField(
            password=True,
            can_reveal_password=False,
            border_color="#BDBDBD",
            focused_border_color="#0b57d0",
            text_size=14,
            content_padding=ft.Padding(12, 10, 12, 10),
            border_radius=6,
            width=float("inf"),
        )

        # Delegated Auth Checkbox and Learn More link
        self.delegated_checkbox = ft.Checkbox(
            label="Enable delegated authentication",
            value=False,
            active_color="#0b57d0",
            check_color=ft.Colors.WHITE,
            label_style=ft.TextStyle(size=13, color=COLOR_TEXT_PRIMARY, weight=ft.FontWeight.W_500),
        )

        self.learn_more_btn = ft.TextButton(
            content=ft.Row(
                tight=True,
                spacing=4,
                controls=[
                    ft.Icon(ft.Icons.HELP_OUTLINE_ROUNDED, size=14, color=COLOR_PRIMARY),
                    ft.Text("Learn More", size=12, weight=ft.FontWeight.W_600, color=COLOR_PRIMARY),
                ],
            ),
            on_click=lambda _: show_delegated_auth_learn_more_dialog(self._get_page()),
            style=ft.ButtonStyle(
                padding=ft.Padding(4, 2, 4, 2),
            ),
        )

        self.status_text = ft.Text(value="", color=COLOR_ERROR, size=13)

        # Back to Hub button placed at top left with generous breathing space
        back_to_hub_btn = ft.Container(
            content=ft.OutlinedButton(
                content=ft.Row(
                    tight=True,
                    spacing=6,
                    vertical_alignment=ft.CrossAxisAlignment.CENTER,
                    controls=[
                        ft.Icon(ft.Icons.ARROW_BACK_ROUNDED, size=16, color=COLOR_TEXT_PRIMARY),
                        ft.Text("Back to Hub", size=13, weight=ft.FontWeight.W_500, color=COLOR_TEXT_PRIMARY),
                    ],
                ),
                on_click=lambda _: self._handle_back(),
                style=ft.ButtonStyle(
                    shape=ft.RoundedRectangleBorder(radius=20),
                    padding=ft.Padding(16, 10, 18, 10),
                ),
            ),
            padding=ft.Padding(40, 32, 0, 0),
        )

        left_pane = ft.Container(
            height=510,
            width=460,
            bgcolor="#F9FAFA",
            border_radius=ft.BorderRadius(top_left=16, bottom_left=16, top_right=0, bottom_right=0),
            padding=44,
            content=ft.Column(
                alignment=ft.MainAxisAlignment.CENTER,
                horizontal_alignment=ft.CrossAxisAlignment.START,
                spacing=0,
                controls=[
                    ft.Container(
                        bgcolor="#E8EAF6",
                        border_radius=12,
                        padding=12,
                        content=ft.Icon(ft.Icons.INSERT_CHART_OUTLINED, color="#3F51B5", size=28),
                    ),
                    ft.Container(height=20),
                    ft.Text(
                        "Usage & adoption insights",
                        size=24,
                        weight=ft.FontWeight.BOLD,
                        color=COLOR_TEXT_PRIMARY,
                        text_align=ft.TextAlign.LEFT,
                    ),
                    ft.Container(height=12),
                    ft.Text(
                        "Uncover how your teams use provisioned tools. Identify shelfware and consolidation opportunities by analyzing login frequency and collaboration metrics. All data is processed securely and locally.",
                        size=14,
                        color=COLOR_TEXT_SECONDARY,
                        text_align=ft.TextAlign.LEFT,
                    ),
                ],
            ),
        )

        delegated_row = ft.Row(
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
            controls=[
                self.delegated_checkbox,
                self.learn_more_btn,
            ],
            width=float("inf"),
        )

        self.connect_btn = ft.ElevatedButton(
            content=ft.Text("Authorize connection", size=14, weight=ft.FontWeight.W_500),
            bgcolor="#0b57d0",
            color=ft.Colors.WHITE,
            height=44,
            style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
            on_click=self.handle_connect,
            width=float("inf"),
        )

        right_pane = ft.Container(
            height=510,
            width=500,
            bgcolor=ft.Colors.WHITE,
            border_radius=ft.BorderRadius(top_left=0, bottom_left=0, top_right=16, bottom_right=16),
            padding=44,
            content=ft.Column(
                alignment=ft.MainAxisAlignment.CENTER,
                horizontal_alignment=ft.CrossAxisAlignment.START,
                spacing=0,
                controls=[
                    ft.Row(
                        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                        controls=[
                            ft.Text("Log in to Microsoft", size=20, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_PRIMARY),
                            ft.Container(
                                border=ft.Border.all(1, "#E0E0E0"),
                                border_radius=6,
                                padding=6,
                                content=ft.Icon(ft.Icons.WINDOW, size=18, color="#00A4EF"),
                            ),
                        ],
                        width=float("inf"),
                    ),
                    ft.Container(height=20),
                    ft.Text("Tenant ID", size=13, color=COLOR_TEXT_PRIMARY),
                    ft.Container(height=2),
                    self.tenant_input,
                    ft.Container(height=10),
                    ft.Text("App client ID", size=13, color=COLOR_TEXT_PRIMARY),
                    ft.Container(height=2),
                    self.client_input,
                    ft.Container(height=10),
                    ft.Text("Client secret", size=13, color=COLOR_TEXT_PRIMARY),
                    ft.Container(height=2),
                    self.secret_input,
                    ft.Container(height=8),
                    delegated_row,
                    ft.Container(height=4),
                    self.status_text,
                    ft.Container(height=8),
                    self.connect_btn,
                ],
            ),
        )

        card = ft.Container(
            width=960,
            height=510,
            border_radius=16,
            border=ft.Border.all(1, COLOR_BORDER),
            shadow=ft.BoxShadow(
                spread_radius=0,
                blur_radius=20,
                color="#00000010",
                offset=ft.Offset(0, 4),
            ),
            content=ft.Row(
                spacing=0,
                controls=[left_pane, right_pane],
            ),
        )

        self.content = ft.Column(
            alignment=ft.MainAxisAlignment.START,
            horizontal_alignment=ft.CrossAxisAlignment.START,
            expand=True,
            controls=[
                back_to_hub_btn,
                ft.Container(
                    content=ft.Column(
                        alignment=ft.MainAxisAlignment.CENTER,
                        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                        controls=[card],
                    ),
                    alignment=ft.alignment.Alignment(0, 0),
                    expand=True,
                ),
            ],
        )

    def _get_page(self) -> Optional[ft.Page]:
        target = self._page_ref
        if not target:
            try:
                target = self.page
            except Exception:
                target = None
        return target

    def set_loading(self, is_loading: bool, message: str = "", is_error: bool = False):
        """Updates button into an active Cancel button and disables fields during browser authentication."""
        if is_loading:
            self.tenant_input.disabled = True
            self.client_input.disabled = True
            self.secret_input.disabled = True
            self.delegated_checkbox.disabled = True

            self.connect_btn.disabled = False
            self.connect_btn.bgcolor = "#DC2626"
            self.connect_btn.content = ft.Row(
                tight=True,
                spacing=8,
                alignment=ft.MainAxisAlignment.CENTER,
                controls=[
                    ft.ProgressRing(width=16, height=16, stroke_width=2, color=ft.Colors.WHITE),
                    ft.Text("Cancel Authentication", size=14, color=ft.Colors.WHITE, weight=ft.FontWeight.W_500),
                ],
            )
            self.connect_btn.on_click = self.handle_cancel
            self.status_text.value = ""
        else:
            self.tenant_input.disabled = False
            self.client_input.disabled = False
            self.secret_input.disabled = False
            self.delegated_checkbox.disabled = False

            self.connect_btn.disabled = False
            self.connect_btn.bgcolor = "#0b57d0"
            self.connect_btn.content = ft.Text("Authorize connection", size=14, weight=ft.FontWeight.W_500)
            self.connect_btn.on_click = self.handle_connect

            if message:
                self.status_text.value = message
                self.status_text.color = COLOR_ERROR if is_error else COLOR_PRIMARY
            else:
                self.status_text.value = ""

        try:
            self.update()
        except Exception:
            pass

    def handle_cancel(self, e):
        """Aborts active browser authentication and resets the view immediately."""
        if self.on_cancel_clicked:
            self.on_cancel_clicked()
        self.set_loading(False, "Authentication cancelled.")

    def handle_connect(self, e):
        tenant = self.tenant_input.value.strip() if self.tenant_input.value else ""
        client = self.client_input.value.strip() if self.client_input.value else ""
        secret = self.secret_input.value.strip() if self.secret_input.value else ""
        use_delegated = bool(self.delegated_checkbox.value)

        if not tenant or not client or not secret:
            self.status_text.value = "Error: Tenant ID, Client ID, and Client Secret are required."
            self.status_text.color = COLOR_ERROR
            try:
                self.update()
            except Exception:
                pass
            return

        self.status_text.value = ""
        try:
            self.update()
        except Exception:
            pass

        # Pass the credentials and delegated auth state to the parent callback
        self.on_connect_clicked(tenant, client, secret, use_delegated)

    def _handle_back(self):
        # Cancel any active auth if user clicks Back to Hub
        if self.on_cancel_clicked:
            self.on_cancel_clicked()
        if self.on_back_to_hub:
            self.on_back_to_hub()
