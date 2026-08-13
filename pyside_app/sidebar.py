from PySide6.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QFrame, QSpacerItem, QSizePolicy
from PySide6.QtCore import Qt
from PySide6.QtGui import QCursor

class Sidebar(QFrame):
    def __init__(self, on_disconnect):
        super().__init__()
        self.on_disconnect = on_disconnect
        self.setFixedWidth(300)
        self.setObjectName("sidebar")
        self.setStyleSheet("""
            #sidebar {
                background-color: white;
                border: 1px solid #E0E0E0;
                border-radius: 12px;
            }
        """)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(10)

        # Title
        title_layout = QHBoxLayout()
        icon = QLabel("🤝")
        icon.setStyleSheet("font-size: 24px;")
        title = QLabel("Deal Assistant")
        title.setStyleSheet("font-size: 18px; font-weight: bold; color: #1D1D1D;")
        title_layout.addWidget(icon)
        title_layout.addWidget(title)
        title_layout.addStretch()
        layout.addLayout(title_layout)
        
        layout.addSpacing(30)

        # Menu items
        menu_items = [
            ("Usage and adoption", "📊", True),
            ("Workforce analysis", "👥", False),
            ("Cost savings plan", "💵", False),
            ("Migration planner", "🚀", False)
        ]

        for label, ic, is_active in menu_items:
            btn = QPushButton(f"{ic}  {label}")
            btn.setCursor(Qt.PointingHandCursor)
            if is_active:
                btn.setStyleSheet("""
                    QPushButton {
                        background-color: #E8EAF6;
                        color: #3F51B5;
                        font-weight: bold;
                        border: none;
                        border-radius: 8px;
                        padding: 12px 15px;
                        text-align: left;
                    }
                """)
            else:
                btn.setStyleSheet("""
                    QPushButton {
                        background-color: transparent;
                        color: #757575;
                        border: none;
                        border-radius: 8px;
                        padding: 12px 15px;
                        text-align: left;
                    }
                    QPushButton:hover {
                        background-color: #F5F5F5;
                    }
                """)
            layout.addWidget(btn)

        layout.addStretch()

        # Disconnect button
        disconnect_btn = QPushButton("🚪  Disconnect")
        disconnect_btn.setCursor(Qt.PointingHandCursor)
        disconnect_btn.setStyleSheet("""
            QPushButton {
                background-color: transparent;
                color: #D32F2F;
                font-weight: 500;
                border: none;
                border-radius: 8px;
                padding: 12px 15px;
                text-align: left;
            }
            QPushButton:hover {
                background-color: #FFEBEE;
            }
        """)
        disconnect_btn.clicked.connect(self.on_disconnect)
        layout.addWidget(disconnect_btn)
