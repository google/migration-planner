import flet as ft
from flet_app.styles import *

class CertInstructionsView(ft.Container):
    def __init__(self, pem_path, on_continue):
        super().__init__()
        self.pem_path = pem_path
        self.on_continue = on_continue
        
        self.expand = True
        
        self.content = ft.Column(
            alignment=ft.MainAxisAlignment.CENTER,
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            controls=[
                ft.Container(
                    width=600,
                    bgcolor=COLOR_SURFACE,
                    border_radius=12,
                    border=ft.Border.all(1, COLOR_OUTLINE_LIGHT),
                    padding=40,
                    content=ft.Column(
                        controls=[
                            ft.Text("Certificate Upload Required", size=24, weight=ft.FontWeight.BOLD, color=COLOR_PRIMARY),
                            ft.Text("A new security certificate has been generated for hybrid authentication.", size=14, color=COLOR_TEXT_MAIN),
                            ft.Divider(height=20, color="transparent"),
                            
                            ft.Container(
                                content=ft.Column([
                                    ft.Text("1. Locate the certificate file at:", size=13, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_MAIN),
                                    ft.Container(
                                        content=ft.Text(self.pem_path, size=12, color=COLOR_TEXT_MAIN, selectable=True, font_family="Courier New"),
                                        bgcolor=COLOR_SURFACE_VARIANT,
                                        padding=10,
                                        border_radius=6,
                                        width=float('inf')
                                    ),
                                    ft.Divider(height=10, color="transparent"),
                                    ft.Text("2. Upload this 'certificate.pem' file to your App Registration in the Microsoft Entra ID portal.", size=13, color=COLOR_TEXT_MAIN),
                                    ft.Text("   (App Registration -> Certificates & secrets -> Certificates -> Upload certificate)", size=12, color=COLOR_TEXT_SUB, italic=True),
                                    ft.Divider(height=10, color="transparent"),
                                    ft.Text("3. Once you have successfully uploaded the certificate, click the 'Continue' button below.", size=13, color=COLOR_TEXT_MAIN),
                                ]),
                                border=ft.Border.all(1, COLOR_OUTLINE_LIGHT),
                                border_radius=8,
                                padding=15,
                                bgcolor=COLOR_BACKGROUND
                            ),
                            
                            ft.Divider(height=30, color="transparent"),
                            ft.ElevatedButton(
                                content="I have uploaded the certificate. Continue",
                                bgcolor=COLOR_PRIMARY,
                                color=ft.Colors.WHITE,
                                height=45,
                                style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
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
