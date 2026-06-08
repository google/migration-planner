import flet as ft
from flet_app.styles import *

class CertInstructionsView(ft.Container):
    def __init__(self, pem_path, client_id, on_continue):
        super().__init__()
        self.pem_path = pem_path
        self.client_id = client_id
        self.on_continue = on_continue
        
        self.expand = True
        
        self.content = ft.Column(
            alignment=ft.MainAxisAlignment.CENTER,
            horizontal_alignment=ft.CrossAxisAlignment.STRETCH,
            controls=[
                ft.Container(
                    bgcolor=COLOR_SURFACE,
                    border_radius=12,
                    border=ft.Border.all(1, COLOR_OUTLINE_LIGHT),
                    padding=40,
                    margin=ft.margin.symmetric(horizontal=40),
                    content=ft.Column(
                        controls=[
                            ft.Text("Certificate Upload", size=24, weight=ft.FontWeight.BOLD, color=COLOR_PRIMARY),
                            ft.Text("A new security certificate has been generated for hybrid authentication.\n\nUploading this certificate is highly recommended, but optional:", size=14, color=COLOR_TEXT_MAIN),
                            ft.Text("• If you UPLOAD the certificate:\n  All report sections will be fully functional.", size=14, color=COLOR_SUCCESS, weight=ft.FontWeight.BOLD),
                            ft.Text("• If you SKIP uploading the certificate:\n  You can still run the reports. However, sections relying on certificate-based authentication (such as detailed Calendar settings, Shared/Public mailbox statistics, Retention Policies etc.) will be skipped and show as unavailable.", size=14, color=COLOR_ERROR, weight=ft.FontWeight.BOLD),
                            ft.Divider(height=20, color="transparent"),
                            
                            ft.Container(
                                content=ft.Column([
                                    ft.Text("Upload Instructions", size=14, weight=ft.FontWeight.BOLD, color=COLOR_TONAL_TEXT),
                                    ft.Text("1. Locate the certificate file generated at:", size=13, color=COLOR_TEXT_MAIN),
                                    ft.Container(
                                        content=ft.Text(self.pem_path, size=12, color=COLOR_TEXT_MAIN, selectable=True, font_family="Courier New"),
                                        bgcolor=COLOR_SURFACE,
                                        padding=10,
                                        border_radius=6,
                                        width=float('inf')
                                    ),
                                    ft.Text("2. Log in to the Microsoft Azure portal and navigate to the App Registration with Client ID:", size=13, color=COLOR_TEXT_MAIN),
                                    ft.Container(
                                        content=ft.Text(self.client_id, size=12, color=COLOR_TEXT_MAIN, selectable=True, font_family="Courier New"),
                                        bgcolor=COLOR_SURFACE,
                                        padding=10,
                                        border_radius=6,
                                        width=float('inf')
                                    ),
                                    ft.Text("3. Upload the certificate under:", size=13, color=COLOR_TEXT_MAIN),
                                    ft.Text("   Certificates & secrets -> Certificates -> Upload certificate", size=13, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_MAIN),
                                ]),
                                border=ft.Border.all(1, COLOR_OUTLINE),
                                border_radius=8,
                                padding=20,
                                bgcolor=COLOR_TONAL_BG
                            ),
                            
                            ft.Divider(height=30, color="transparent"),
                            ft.ElevatedButton(
                                content=ft.Text("Continue", color=ft.Colors.WHITE, weight=ft.FontWeight.BOLD),
                                bgcolor=COLOR_PRIMARY,
                                height=40,
                                style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=20)),
                                on_click=self.handle_continue,
                                width=float('inf')
                            )
                        ]
                    )
                )
            ]
        )
        
    def handle_continue(self, e):
        self.on_continue()
