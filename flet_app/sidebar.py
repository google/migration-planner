import flet as ft
from flet_app.styles import *

class Sidebar(ft.Container):
    def __init__(self, on_disconnect):
        super().__init__()
        self.on_disconnect = on_disconnect
        
        self.width = 300
        self.bgcolor = COLOR_SURFACE
        self.border_radius = 12
        self.border = ft.Border.all(1, COLOR_OUTLINE_LIGHT)
        self.padding = 20
        
        menu_items = [
            ("Usage and adoption", ft.Icons.BAR_CHART, True),
            ("Workforce analysis", ft.Icons.PEOPLE, False),
            ("Cost savings plan", ft.Icons.ATTACH_MONEY, False),
            ("Migration planner", ft.Icons.ROCKET_LAUNCH, False)
        ]
        
        self.menu_column = ft.Column(spacing=10)
        
        for label, icon, is_active in menu_items:
            bg = COLOR_TONAL_BG if is_active else "transparent"
            text_col = COLOR_PRIMARY if is_active else COLOR_TEXT_SUB
            weight = ft.FontWeight.BOLD if is_active else ft.FontWeight.NORMAL
            
            btn = ft.Container(
                content=ft.Row([
                    ft.Icon(icon, color=text_col, size=20),
                    ft.Text(label, color=text_col, weight=weight, size=14)
                ]),
                bgcolor=bg,
                padding=ft.Padding.symmetric(horizontal=15, vertical=12),
                border_radius=8,
                ink=True if not is_active else False,
            )
            self.menu_column.controls.append(btn)
            
        self.content = ft.Column(
            controls=[
                ft.Row([
                    ft.Text("🤝", size=24),
                    ft.Text("Deal Assistant", size=18, weight=ft.FontWeight.BOLD, color=COLOR_TEXT_MAIN)
                ], alignment=ft.MainAxisAlignment.START),
                ft.Divider(height=30, color="transparent"),
                self.menu_column,
                ft.Container(expand=True), # Spacer
                ft.Container(
                    content=ft.Row([
                        ft.Icon(ft.Icons.LOGOUT, color=COLOR_ERROR, size=20),
                        ft.Text("Disconnect", color=COLOR_ERROR, weight=ft.FontWeight.W_500, size=14)
                    ]),
                    padding=ft.Padding.symmetric(horizontal=15, vertical=12),
                    border_radius=8,
                    ink=True,
                    on_click=lambda _: self.on_disconnect()
                )
            ]
        )
