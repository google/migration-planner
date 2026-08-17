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

"""
Deal Assistant Platform - Main Application Entry Point (Flet UI)

Run using:
    python home.py
"""

import os
import sys
import ssl
import threading

# Ensure project root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Workaround for macOS Python SSL Certificate errors when initializing Flet
ssl._create_default_https_context = ssl._create_unverified_context

import flet as ft
from core.cert_auth import check_certificate_exists, generate_certificate, load_certificate
from flet_ui.components.dialogs import (
    show_cert_decryption_error_dialog,
    show_upload_certificate_dialog,
)
from flet_ui.styles import COLOR_APP_BG, get_app_theme
from flet_ui.views import (
    AuthView,
    HomeView,
    MigrationPlannerPlaceholderView,
    UsageAdoptionDashboardPlaceholderView,
)


def main(page: ft.Page):
    """Main application setup and navigation controller."""
    page.title = "Deal Assistant Platform"
    page.theme_mode = ft.ThemeMode.LIGHT
    page.theme = get_app_theme()
    page.bgcolor = COLOR_APP_BG
    page.padding = 0
    page.vertical_alignment = ft.MainAxisAlignment.CENTER
    page.horizontal_alignment = ft.CrossAxisAlignment.CENTER

    # Configure desktop window properties
    try:
        if hasattr(page, "window"):
            page.window.width = 1120
            page.window.height = 720
            page.window.min_width = 880
            page.window.min_height = 560
            page.window.alignment = ft.Alignment(0, 0)
    except Exception:
        pass

    # Session storage for active authentication credentials
    page.session.store.set("tenant", "")
    page.session.store.set("client", "")
    page.session.store.set("secret", "")
    page.session.store.set("use_delegated", False)

    current_auth_view: ft.Control = None
    active_auth_client = None
    is_auth_cancelled = False

    def safe_run_on_ui(callback):
        """Thread-safe UI callback execution."""
        try:
            loop = getattr(page, "loop", None)
            if loop and callable(getattr(loop, "is_running", None)) and loop.is_running() and not isinstance(loop, ft.Page):
                loop.call_soon_threadsafe(callback)
            else:
                callback()
        except Exception:
            callback()

    def cancel_delegated_auth():
        """Aborts active background browser authentication."""
        nonlocal is_auth_cancelled, active_auth_client
        is_auth_cancelled = True
        if active_auth_client:
            try:
                active_auth_client.cancel()
            except Exception:
                pass

    def show_error_dialog(title: str, message: str):
        def close_dialog(e):
            dialog.open = False
            page.update()

        dialog = ft.AlertDialog(
            title=ft.Text(title, weight=ft.FontWeight.BOLD),
            content=ft.Text(message),
            actions=[ft.TextButton("Close", on_click=close_dialog)],
            actions_alignment=ft.MainAxisAlignment.END,
        )
        page.overlay.append(dialog)
        dialog.open = True
        page.update()

    def show_snack(message: str, is_error: bool = False):
        snack = ft.SnackBar(
            content=ft.Text(message),
            bgcolor=ft.Colors.RED_700 if is_error else ft.Colors.GREEN_700,
        )
        page.overlay.append(snack)
        snack.open = True
        page.update()

    def clear_session_credentials():
        """Strictly purge all session credentials from memory on navigation."""
        cancel_delegated_auth()
        page.session.store.set("tenant", "")
        page.session.store.set("client", "")
        page.session.store.set("secret", "")
        page.session.store.set("use_delegated", False)

    def show_home():
        """Navigate back to the main Hub (Home Screen) and clear credentials."""
        clear_session_credentials()
        page.controls.clear()
        page.add(
            HomeView(
                page=page,
                on_open_usage_adoption=show_usage_adoption,
                on_open_migration_planner=show_migration_planner,
            )
        )
        page.update()

    def show_auth():
        """Navigate to the Usage & Adoption Auth Connection screen."""
        nonlocal current_auth_view
        current_auth_view = AuthView(
            on_connect_clicked=handle_connect,
            on_back_to_hub=show_home,
            on_cancel_clicked=cancel_delegated_auth,
            page=page,
        )
        page.controls.clear()
        page.add(current_auth_view)
        page.update()

    def show_usage_adoption_dashboard():
        """Navigate to the blank Usage & Adoption dashboard view."""
        page.controls.clear()
        page.add(
            UsageAdoptionDashboardPlaceholderView(
                page=page,
                on_back=show_home,
            )
        )
        page.update()

    def generate_and_show_cert_dialog(tenant: str, client: str, secret: str):
        """Generates a new certificate and displays the upload instructions modal popup."""
        try:
            pem_path, _ = generate_certificate(secret, tenant_id=tenant, client_id=client)
            show_upload_certificate_dialog(
                page=page,
                pem_path=pem_path,
                on_uploaded=lambda: handle_cert_uploaded(tenant, client, secret),
                on_cancel=lambda: None,
            )
        except Exception as e:
            show_error_dialog(
                "Certificate Generation Error",
                f"Unable to generate certificate: {e}",
            )

    def handle_cert_uploaded(tenant: str, client: str, secret: str):
        """Validates certificate loading once the user confirms upload and moves to dashboard."""
        try:
            load_certificate(secret, tenant_id=tenant, client_id=client)
            show_snack("Security certificate verified and connection established successfully!")
            show_usage_adoption_dashboard()
        except Exception as e:
            show_error_dialog(
                "Certificate Verification Error",
                f"Unable to unlock certificate with Client Secret: {e}",
            )

    def _proceed_with_cert_check(tenant: str, client: str, secret: str):
        """Verifies existing certificate or prompts to generate a new one, then moves to dashboard."""
        if check_certificate_exists(tenant_id=tenant, client_id=client):
            try:
                load_certificate(secret, tenant_id=tenant, client_id=client)
                show_snack("Authentication and security certificate validated successfully!")
                show_usage_adoption_dashboard()
            except Exception as decrypt_err:
                # Decryption failed: Display choice dialog matching deal_assistant.py
                show_cert_decryption_error_dialog(
                    page=page,
                    error_message=str(decrypt_err),
                    on_retry=lambda: None,  # User stays on auth screen to re-type secret
                    on_generate_new=lambda: generate_and_show_cert_dialog(tenant, client, secret),
                )
        else:
            # Certificate not detected: Generate new and show popup instructions
            generate_and_show_cert_dialog(tenant, client, secret)

    def handle_connect(tenant: str, client: str, secret: str, use_delegated: bool = False):
        """Handles connection, delegated auth flow, certificate check, and popup triggers."""
        nonlocal is_auth_cancelled, active_auth_client
        page.session.store.set("tenant", tenant)
        page.session.store.set("client", client)
        page.session.store.set("secret", secret)
        page.session.store.set("use_delegated", use_delegated)

        if use_delegated:
            is_auth_cancelled = False
            if isinstance(current_auth_view, AuthView):
                current_auth_view.set_loading(True)

            def _auth_worker():
                nonlocal active_auth_client
                try:
                    from core.graph.delegated_auth import DelegatedAuthClient

                    auth_client = DelegatedAuthClient(tenant, client, secret)
                    active_auth_client = auth_client

                    token = auth_client.get_token(
                        scopes=["https://graph.microsoft.com/.default"],
                        force_interactive=True,
                        timeout=60,
                    )

                    if is_auth_cancelled or getattr(auth_client, "is_cancelled", False):
                        return

                    if not token:
                        def on_failed():
                            if isinstance(current_auth_view, AuthView):
                                current_auth_view.set_loading(False, "Authentication timed out or browser was closed.", is_error=True)
                        safe_run_on_ui(on_failed)
                        return

                    def on_success():
                        if isinstance(current_auth_view, AuthView):
                            current_auth_view.set_loading(False)
                        _proceed_with_cert_check(tenant, client, secret)

                    safe_run_on_ui(on_success)

                except Exception as e:
                    if is_auth_cancelled or "Cancelled" in str(e):
                        def on_cancelled():
                            if isinstance(current_auth_view, AuthView):
                                current_auth_view.set_loading(False, "Authentication cancelled.")
                        safe_run_on_ui(on_cancelled)
                    else:
                        err_msg = str(e)
                        def on_error():
                            if isinstance(current_auth_view, AuthView):
                                current_auth_view.set_loading(False, f"Authentication error: {err_msg}", is_error=True)
                            show_error_dialog("Delegated Auth Error", err_msg)
                        safe_run_on_ui(on_error)
                finally:
                    active_auth_client = None

            threading.Thread(target=_auth_worker, daemon=True).start()
        else:
            _proceed_with_cert_check(tenant, client, secret)

    def show_usage_adoption():
        """Entry point for Usage & Adoption module."""
        show_auth()

    def show_migration_planner():
        """Entry point for Migration Planner module."""
        clear_session_credentials()
        page.controls.clear()
        page.add(
            MigrationPlannerPlaceholderView(
                page=page,
                on_back=show_home,
            )
        )
        page.update()

    # Initial view load
    show_home()


if __name__ == "__main__":
    ft.run(main)
