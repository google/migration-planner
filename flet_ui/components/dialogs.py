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

"""Dialog components for documentation, help, certificate workflows, and authentication guidance."""

from typing import Callable, Optional
import flet as ft
from flet_ui.styles import (
    COLOR_BORDER,
    COLOR_ERROR,
    COLOR_PRIMARY,
    COLOR_TEXT_PRIMARY,
    COLOR_TEXT_SECONDARY,
)


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
                    "• Usage & Adoption: Telemetry into licenses, active user trends, SharePoint, OneDrive, Retention policies, and Power Automate flows.\n"
                    "• Migration Planner: Assess Exchange Online mailboxes, OneDrive/SharePoint files, and Teams chats.",
                    size=13,
                    color=COLOR_TEXT_SECONDARY,
                ),
                ft.Container(height=4),
                ft.Text(
                    "Security Notice: All tenant telemetry and logs are processed locally on your workstation.",
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


def show_delegated_auth_learn_more_dialog(page: ft.Page):
    """Displays the Delegated Authentication guidance and prerequisite modal popup."""

    def close_dialog(e):
        dialog.open = False
        page.update()

    content = ft.Container(
        width=520,
        content=ft.Column(
            spacing=14,
            tight=True,
            controls=[
                ft.Column(
                    spacing=4,
                    controls=[
                        ft.Row(
                            tight=True,
                            spacing=6,
                            controls=[
                                ft.Icon(ft.Icons.POLICY_ROUNDED, color=COLOR_PRIMARY, size=18),
                                ft.Text(
                                    "Required for eDiscovery & MDM Policies",
                                    size=14,
                                    weight=ft.FontWeight.BOLD,
                                    color=COLOR_TEXT_PRIMARY,
                                ),
                            ],
                        ),
                        ft.Text(
                            "Enabling delegated authentication is required for fetching Microsoft Purview eDiscovery searches and Intune / MDM device management policies data.",
                            size=13,
                            color=COLOR_TEXT_SECONDARY,
                        ),
                    ],
                ),
                ft.Divider(height=1, color=COLOR_BORDER),
                ft.Column(
                    spacing=4,
                    controls=[
                        ft.Row(
                            tight=True,
                            spacing=6,
                            controls=[
                                ft.Icon(ft.Icons.ADMIN_PANEL_SETTINGS_ROUNDED, color=COLOR_PRIMARY, size=18),
                                ft.Text(
                                    "Global Administrator Account Required",
                                    size=14,
                                    weight=ft.FontWeight.BOLD,
                                    color=COLOR_TEXT_PRIMARY,
                                ),
                            ],
                        ),
                        ft.Text(
                            "The user signing in during the interactive browser authentication prompt must have Global Administrator privileges in the target Microsoft 365 tenant.",
                            size=13,
                            color=COLOR_TEXT_SECONDARY,
                        ),
                    ],
                ),
                ft.Divider(height=1, color=COLOR_BORDER),
                ft.Column(
                    spacing=4,
                    controls=[
                        ft.Row(
                            tight=True,
                            spacing=6,
                            controls=[
                                ft.Icon(ft.Icons.SETTINGS_APPLICATIONS_ROUNDED, color=COLOR_PRIMARY, size=18),
                                ft.Text(
                                    "Azure App Registration Setup",
                                    size=14,
                                    weight=ft.FontWeight.BOLD,
                                    color=COLOR_TEXT_PRIMARY,
                                ),
                            ],
                        ),
                        ft.Text(
                            "Ensure your App Registration in Microsoft Entra ID has 'Allow public client flows' enabled (Yes) and a redirect URI (such as http://localhost) configured.",
                            size=13,
                            color=COLOR_TEXT_SECONDARY,
                        ),
                    ],
                ),
            ],
        ),
    )

    dialog = ft.AlertDialog(
        title=ft.Row(
            spacing=10,
            controls=[
                ft.Icon(ft.Icons.VERIFIED_USER_ROUNDED, color=COLOR_PRIMARY, size=22),
                ft.Text("Delegated Authentication", weight=ft.FontWeight.BOLD, size=18, color=COLOR_TEXT_PRIMARY),
            ],
        ),
        content=content,
        shape=ft.RoundedRectangleBorder(radius=16),
        actions=[
            ft.TextButton("Close", on_click=close_dialog),
        ],
        actions_alignment=ft.MainAxisAlignment.END,
    )
    page.overlay.append(dialog)
    dialog.open = True
    page.update()


def show_upload_certificate_dialog(
    page: ft.Page,
    pem_path: str,
    on_uploaded: Optional[Callable[[], None]] = None,
    on_cancel: Optional[Callable[[], None]] = None,
):
    """Displays the Upload Security Certificate modal popup matching the design mockup."""

    def handle_cancel(e):
        dialog.open = False
        page.update()
        if on_cancel:
            on_cancel()

    def handle_uploaded(e):
        dialog.open = False
        page.update()
        if on_uploaded:
            on_uploaded()

    content = ft.Container(
        width=540,
        content=ft.Column(
            spacing=14,
            tight=True,
            controls=[
                ft.Text(
                    "A new security certificate has been generated for hybrid authentication.",
                    size=13,
                    color=COLOR_TEXT_PRIMARY,
                ),
                ft.Text(
                    "1. Locate the certificate file at:",
                    size=13,
                    weight=ft.FontWeight.W_500,
                    color=COLOR_TEXT_PRIMARY,
                ),
                ft.Container(
                    bgcolor="#F1F5F9",
                    border=ft.Border.all(1, COLOR_BORDER),
                    border_radius=8,
                    padding=ft.Padding(12, 10, 12, 10),
                    width=float("inf"),
                    content=ft.Text(
                        pem_path,
                        size=12,
                        font_family="Courier New",
                        selectable=True,
                        color="#0F172A",
                    ),
                ),
                ft.Text(
                    "2. Upload this certificate.pem file to your App Registration in the Microsoft Entra ID portal.\n"
                    "   (App Registration → Certificates & secrets → Certificates → Upload certificate)",
                    size=13,
                    color=COLOR_TEXT_PRIMARY,
                ),
                ft.Text(
                    "3. Once you have successfully uploaded the certificate, click \"I have uploaded the certificate\" below.",
                    size=13,
                    color=COLOR_TEXT_PRIMARY,
                ),
            ],
        ),
    )

    dialog = ft.AlertDialog(
        title=ft.Text("Upload security certificate", weight=ft.FontWeight.BOLD, size=18, color=COLOR_TEXT_PRIMARY),
        content=content,
        shape=ft.RoundedRectangleBorder(radius=16),
        actions=[
            ft.TextButton(
                "Cancel",
                style=ft.ButtonStyle(color=COLOR_PRIMARY),
                on_click=handle_cancel,
            ),
            ft.ElevatedButton(
                "I have uploaded the certificate",
                bgcolor="#0b57d0",
                color=ft.Colors.WHITE,
                style=ft.ButtonStyle(
                    shape=ft.RoundedRectangleBorder(radius=20),
                    padding=ft.Padding(16, 10, 16, 10),
                ),
                on_click=handle_uploaded,
            ),
        ],
        actions_alignment=ft.MainAxisAlignment.END,
    )
    page.overlay.append(dialog)
    dialog.open = True
    page.update()


def show_cert_decryption_error_dialog(
    page: ft.Page,
    error_message: str,
    on_retry: Optional[Callable[[], None]] = None,
    on_generate_new: Optional[Callable[[], None]] = None,
):
    """Displays the Certificate Decryption Error modal selection dialog."""

    def handle_retry(e):
        dialog.open = False
        page.update()
        if on_retry:
            on_retry()

    def handle_generate(e):
        dialog.open = False
        page.update()
        if on_generate_new:
            on_generate_new()

    content = ft.Container(
        width=540,
        content=ft.Column(
            spacing=12,
            tight=True,
            controls=[
                ft.Text(
                    "Unable to decrypt existing certificate passkey using the provided Client Secret. How would you like to proceed?",
                    size=13,
                    color=COLOR_TEXT_PRIMARY,
                ),
                ft.Container(
                    bgcolor="#FEF2F2",
                    border=ft.Border.all(1, "#FECACA"),
                    border_radius=8,
                    padding=ft.Padding(12, 10, 12, 10),
                    width=float("inf"),
                    content=ft.Text(
                        f"Error details: {error_message}",
                        size=12,
                        color=COLOR_ERROR,
                        selectable=True,
                    ),
                ),
            ],
        ),
    )

    dialog = ft.AlertDialog(
        title=ft.Row(
            spacing=10,
            controls=[
                ft.Icon(ft.Icons.WARNING_AMBER_ROUNDED, color=COLOR_ERROR, size=22),
                ft.Text("Certificate Decryption Error", weight=ft.FontWeight.BOLD, size=18, color=COLOR_TEXT_PRIMARY),
            ],
        ),
        content=content,
        shape=ft.RoundedRectangleBorder(radius=16),
        actions=[
            ft.OutlinedButton(
                "Retry with existing secret",
                style=ft.ButtonStyle(
                    shape=ft.RoundedRectangleBorder(radius=20),
                    padding=ft.Padding(14, 8, 14, 8),
                ),
                on_click=handle_retry,
            ),
            ft.ElevatedButton(
                "Generate new certificate",
                bgcolor="#0b57d0",
                color=ft.Colors.WHITE,
                style=ft.ButtonStyle(
                    shape=ft.RoundedRectangleBorder(radius=20),
                    padding=ft.Padding(16, 8, 16, 8),
                ),
                on_click=handle_generate,
            ),
        ],
        actions_alignment=ft.MainAxisAlignment.END,
    )
    page.overlay.append(dialog)
    dialog.open = True
    page.update()
