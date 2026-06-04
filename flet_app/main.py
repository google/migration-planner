import flet as ft
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from flet_app.styles import get_theme, COLOR_BACKGROUND
from flet_app.auth_view import AuthView
from flet_app.cert_instructions_view import CertInstructionsView
from flet_app.dashboard import DashboardView
from core.cert_auth import check_certificate_exists, generate_certificate, load_certificate

def main(page: ft.Page):
    page.title = "Deal Assistant (Flet)"
    page.theme_mode = ft.ThemeMode.LIGHT
    page.theme = get_theme()
    page.bgcolor = COLOR_BACKGROUND
    page.padding = 20
    
    # Store session variables
    page.session.store.set("tenant", "")
    page.session.store.set("client", "")
    page.session.store.set("secret", "")

    def show_error_dialog(title, message):
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

    def show_auth():
        page.controls.clear()
        page.add(AuthView(on_connect_clicked=handle_connect))
        page.update()

    def show_dashboard():
        tenant = page.session.store.get("tenant")
        client = page.session.store.get("client")
        secret = page.session.store.get("secret")
        page.controls.clear()
        page.add(DashboardView(tenant, client, secret, on_disconnect=handle_disconnect))
        page.update()

    def show_cert_instructions(pem_path):
        page.controls.clear()
        page.add(CertInstructionsView(pem_path=pem_path, on_continue=handle_cert_continue))
        page.update()

    def handle_connect(tenant, client, secret):
        page.session.store.set("tenant", tenant)
        page.session.store.set("client", client)
        page.session.store.set("secret", secret)
        
        if check_certificate_exists(tenant_id=tenant, client_id=client):
            try:
                # Decrypt the PFX certificate using the client secret
                load_certificate(secret, tenant_id=tenant, client_id=client)
                show_dashboard()
            except Exception as e:
                show_error_dialog(
                    "Certificate Decryption Error",
                    f"Unable to unlock certificate with Client Secret. Proceeding with standard Client Secret authentication fallback.\n\nError: {e}"
                )
                show_dashboard()
        else:
            try:
                # Generate new certificate and pfx encrypted with the client secret
                pem_path, _ = generate_certificate(secret, tenant_id=tenant, client_id=client)
                # Show instructions UI
                show_cert_instructions(pem_path)
            except Exception as e:
                show_error_dialog(
                    "Certificate Generation Error",
                    f"Unable to generate certificate. Proceeding with standard Client Secret authentication fallback.\n\nError: {e}"
                )
                show_dashboard()

    def handle_cert_continue():
        tenant = page.session.store.get("tenant")
        client = page.session.store.get("client")
        secret = page.session.store.get("secret")
        try:
            load_certificate(secret, tenant_id=tenant, client_id=client)
        except Exception as e:
            show_error_dialog(
                "Certificate Verification Error",
                f"Unable to verify certificate. Proceeding with standard Client Secret authentication fallback.\n\nError: {e}"
            )
        show_dashboard()

    def handle_disconnect():
        page.session.store.set("tenant", "")
        page.session.store.set("client", "")
        page.session.store.set("secret", "")
        show_auth()

    # Start app on auth page
    show_auth()

if __name__ == "__main__":
    # Ensure matplotlib backend doesn't crash Flet if graph operations are pulled
    import matplotlib
    matplotlib.use("Agg")
    
    ft.run(main)
