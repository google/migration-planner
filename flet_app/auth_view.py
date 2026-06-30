import flet as ft
from flet_app.styles import *

class AuthView(ft.Container):
    def __init__(self, on_connect_clicked):
        super().__init__()
        self.on_connect_clicked = on_connect_clicked
        
        self.expand = True
        
        self.tenant_input = ft.TextField(
            label="Tenant ID", 
            border_color=COLOR_OUTLINE, 
            focused_border_color=COLOR_PRIMARY,
            text_size=14,
        )
        self.client_input = ft.TextField(
            label="Client ID", 
            border_color=COLOR_OUTLINE, 
            focused_border_color=COLOR_PRIMARY,
            text_size=14,
        )
        self.secret_input = ft.TextField(
            label="Client Secret", 
            password=True, 
            can_reveal_password=True,
            border_color=COLOR_OUTLINE, 
            focused_border_color=COLOR_PRIMARY,
            text_size=14,
        )
        
        self.status_text = ft.Text(value="", color=COLOR_ERROR, size=14)

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
                            ft.Text("Deal Assistant", size=28, weight=ft.FontWeight.BOLD, color=COLOR_PRIMARY),
                            ft.Text("Connect your Azure App Credentials to begin auditing your tenant.", size=16, color=COLOR_TEXT_SUB),
                            ft.Divider(height=40, color="transparent"),
                            self.tenant_input,
                            ft.Divider(height=10, color="transparent"),
                            self.client_input,
                            ft.Divider(height=10, color="transparent"),
                            self.secret_input,
                            ft.Divider(height=10, color="transparent"),
                            self.status_text,
                            ft.Divider(height=20, color="transparent"),
                            ft.ElevatedButton(
                                content="Connect & Continue",
                                bgcolor=COLOR_PRIMARY,
                                color=ft.Colors.WHITE,
                                height=45,
                                style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
                                on_click=self.handle_connect,
                                width=float('inf')
                            )
                        ]
                    )
                )
            ]
        )
        
    def handle_connect(self, e):
        tenant = self.tenant_input.value.strip() if self.tenant_input.value else ""
        client = self.client_input.value.strip() if self.client_input.value else ""
        secret = self.secret_input.value.strip() if self.secret_input.value else ""
        
        if not tenant or not client or not secret:
            self.status_text.value = "Error: Tenant ID, Client ID, and Client Secret are required."
            self.update()
            return
            
        self.status_text.value = ""
        self.update()
        
        # Pass the credentials to the parent callback
        self.on_connect_clicked(tenant, client, secret)
