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

"""Dialog components for Read Me and Help documentation."""

import flet as ft
from flet_ui.styles import COLOR_PRIMARY, COLOR_TEXT_PRIMARY, COLOR_TEXT_SECONDARY


def show_readme_dialog(page: ft.Page):
    """Displays the Read Me documentation modal."""

    def close_dialog(e):
        dialog.open = False
        page.update()

    content = ft.Container(
        width=500,
        content=ft.Column(
            spacing=12,
            tight=True,
            controls=[
                ft.Text(
                    "About Deal Assistant Platform",
                    size=16,
                    weight=ft.FontWeight.W_600,
                    color=COLOR_TEXT_PRIMARY,
                ),
                ft.Text(
                    "Deal Assistant is a comprehensive desktop application designed to help deployment partners and IT administrators assess Microsoft 365 environments.",
                    size=13,
                    color=COLOR_TEXT_SECONDARY,
                ),
                ft.Text(
                    "Key Capabilities:",
                    size=13,
                    weight=ft.FontWeight.W_600,
                    color=COLOR_TEXT_PRIMARY,
                ),
                ft.Text(
                    "• Usage & Adoption: Deep telemetry into licenses, active user trends, SharePoint, OneDrive, Retention policies, and Power Automate flows.\n"
                    "• Migration Planner: Assess Exchange Online mailboxes, OneDrive/SharePoint files, and Teams chats to generate migration batch plans.",
                    size=13,
                    color=COLOR_TEXT_SECONDARY,
                ),
                ft.Container(height=4),
                ft.Text(
                    "Security Notice: All tenant telemetry and logs are processed locally on your workstation. No tenant credentials or customer data leave your machine.",
                    size=12,
                    weight=ft.FontWeight.W_500,
                    color=COLOR_PRIMARY,
                ),
            ],
        ),
    )

    dialog = ft.AlertDialog(
        title=ft.Row(
            spacing=10,
            controls=[
                ft.Icon(ft.Icons.MENU_BOOK_ROUNDED, color=COLOR_PRIMARY, size=22),
                ft.Text("Platform Documentation", weight=ft.FontWeight.BOLD, size=18),
            ],
        ),
        content=content,
        actions=[
            ft.TextButton("Close", on_click=close_dialog),
        ],
        actions_alignment=ft.MainAxisAlignment.END,
    )
    page.overlay.append(dialog)
    dialog.open = True
    page.update()


def show_help_dialog(page: ft.Page):
    """Displays the Help and support modal."""

    def close_dialog(e):
        dialog.open = False
        page.update()

    content = ft.Container(
        width=500,
        content=ft.Column(
            spacing=12,
            tight=True,
            controls=[
                ft.Text(
                    "Need Assistance?",
                    size=16,
                    weight=ft.FontWeight.W_600,
                    color=COLOR_TEXT_PRIMARY,
                ),
                ft.Text(
                    "Prerequisites & Connection Requirements:",
                    size=13,
                    weight=ft.FontWeight.W_600,
                    color=COLOR_TEXT_PRIMARY,
                ),
                ft.Text(
                    "1. Azure App Registration with appropriate Microsoft Graph API permissions.\n"
                    "2. Tenant ID, Client ID, and Client Secret or Certificate for authentication.\n"
                    "3. PowerShell Core (`pwsh`) with ExchangeOnlineManagement module installed for retention policy scans.",
                    size=13,
                    color=COLOR_TEXT_SECONDARY,
                ),
                ft.Container(height=4),
                ft.Text(
                    "For additional documentation and troubleshooting guides, refer to the project repository docs.",
                    size=13,
                    color=COLOR_TEXT_SECONDARY,
                ),
            ],
        ),
    )

    dialog = ft.AlertDialog(
        title=ft.Row(
            spacing=10,
            controls=[
                ft.Icon(ft.Icons.HELP_OUTLINE_ROUNDED, color=COLOR_PRIMARY, size=22),
                ft.Text("Help & Support", weight=ft.FontWeight.BOLD, size=18),
            ],
        ),
        content=content,
        actions=[
            ft.TextButton("Close", on_click=close_dialog),
        ],
        actions_alignment=ft.MainAxisAlignment.END,
    )
    page.overlay.append(dialog)
    dialog.open = True
    page.update()
