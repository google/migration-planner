import flet as ft
from flet_app.styles import *

class AuthView(ft.Container):
    def __init__(self, on_connect_clicked):
        super().__init__()
        self.on_connect_clicked = on_connect_clicked
        self.expand = True
        self.alignment = ft.alignment.Alignment.CENTER
        
        self.tenant_input = ft.TextField(
            border_color="#BDBDBD",
            focused_border_color="#0b57d0",
            text_size=14,
            content_padding=ft.padding.Padding(left=12, right=12, top=10, bottom=10),
            border_radius=6,
        )
        self.client_input = ft.TextField(
            border_color="#BDBDBD",
            focused_border_color="#0b57d0",
            text_size=14,
            content_padding=ft.padding.Padding(left=12, right=12, top=10, bottom=10),
            border_radius=6,
        )
        self.secret_input = ft.TextField(
            password=True, 
            can_reveal_password=False,
            border_color="#BDBDBD",
            focused_border_color="#0b57d0",
            text_size=14,
            content_padding=ft.padding.Padding(left=12, right=12, top=10, bottom=10),
            border_radius=6,
        )
        
        self.status_text = ft.Text(value="", color=COLOR_ERROR, size=13)

        left_pane = ft.Container(
            height=500,
            width=420,
            bgcolor="#F9FAFA",
            border_radius=ft.border_radius.BorderRadius.only(top_left=12, bottom_left=12),
            padding=50,
            content=ft.Column(
                horizontal_alignment=ft.CrossAxisAlignment.START,
                spacing=0,
                controls=[
                    ft.Container(
                        bgcolor="#E8EAF6",
                        border_radius=12,
                        padding=12,
                        content=ft.Icon(ft.Icons.INSERT_CHART_OUTLINED, color="#3F51B5", size=28)
                    ),
                    ft.Container(height=20),
                    ft.Text("Usage & adoption insights", size=26, weight=ft.FontWeight.W_600, color="#1D1D1D"),
                    ft.Container(height=10),
                    ft.Text(
                        "Uncover how your teams use provisioned tools. Identify shelfware and consolidation opportunities by analyzing login frequency and collaboration metrics. All data is processed securely and locally.",
                        size=14, color="#4A4A4A",
                    )
                ]
            )
        )

        right_pane = ft.Container(
            height=500,
            width=420,
            bgcolor=ft.Colors.WHITE,
            border_radius=ft.border_radius.BorderRadius.only(top_right=12, bottom_right=12),
            padding=50,
            content=ft.Column(
                horizontal_alignment=ft.CrossAxisAlignment.START,
                spacing=0,
                controls=[
                    ft.Row(
                        alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
                        controls=[
                            ft.Text("Log in to APIs", size=20, weight=ft.FontWeight.W_600, color="#1D1D1D"),
                            ft.Row(
                                spacing=8,
                                controls=[
                                    ft.Container(
                                        border=ft.border.Border.all(1, "#E0E0E0"),
                                        border_radius=6,
                                        padding=4,
                                        content=ft.Icon(ft.Icons.WINDOW, size=16, color="#00A4EF")
                                    ),
                                    ft.Icon(ft.Icons.ARROW_FORWARD, size=14, color="#9E9E9E"),
                                    ft.Container(
                                        border=ft.border.Border.all(1, "#E0E0E0"),
                                        border_radius=6,
                                        padding=4,
                                        content=ft.Icon(ft.Icons.G_TRANSLATE, size=16, color="#EA4335")
                                    ),
                                ]
                            )
                        ]
                    ),
                    ft.Container(height=30),
                    
                    ft.Text("Tenant ID", size=13, color="#4A4A4A"),
                    ft.Container(height=2),
                    self.tenant_input,
                    
                    ft.Container(height=15),
                    ft.Text("App client ID", size=13, color="#4A4A4A"),
                    ft.Container(height=2),
                    self.client_input,
                    
                    ft.Container(height=15),
                    ft.Text("Client secret", size=13, color="#4A4A4A"),
                    ft.Container(height=2),
                    self.secret_input,
                    
                    ft.Container(height=5),
                    self.status_text,
                    ft.Container(height=15),
                    
                    ft.ElevatedButton(
                        content=ft.Text("Authorize connection", size=14, weight=ft.FontWeight.W_500),
                        bgcolor="#0b57d0",
                        color=ft.Colors.WHITE,
                        height=45,
                        style=ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=8)),
                        on_click=self.handle_connect,
                        width=float('inf')
                    )
                ]
            )
        )

        card = ft.Container(
            width=840,
            height=500,
            border_radius=12,
            border=ft.border.Border.all(1, "#E0E0E0"),
            shadow=ft.BoxShadow(
                spread_radius=1,
                blur_radius=15,
                color=ft.Colors.with_opacity(0.1, ft.Colors.BLACK),
                offset=ft.Offset(0, 4)
            ),
            content=ft.Row(
                spacing=0,
                controls=[left_pane, right_pane]
            )
        )

        self.content = ft.Column(
            alignment=ft.MainAxisAlignment.CENTER,
            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
            controls=[card]
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
