from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                               QLineEdit, QPushButton, QFrame, QGraphicsDropShadowEffect)
from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QFont, QIcon

class AuthView(QWidget):
    def __init__(self, on_connect_clicked):
        super().__init__()
        self.on_connect_clicked = on_connect_clicked
        self.init_ui()

    def init_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setAlignment(Qt.AlignCenter)
        
        # Card Container
        card = QFrame()
        card.setFixedSize(840, 500)
        card.setObjectName("card")
        card.setStyleSheet("""
            #card {
                background-color: white;
                border: 1px solid #E0E0E0;
                border-radius: 12px;
            }
            QLabel {
                background-color: transparent;
            }
        """)
        
        # Shadow
        shadow = QGraphicsDropShadowEffect(self)
        shadow.setBlurRadius(15)
        shadow.setColor(QColor(0, 0, 0, 25)) # 0.1 opacity black
        shadow.setOffset(0, 4)
        card.setGraphicsEffect(shadow)

        card_layout = QHBoxLayout(card)
        card_layout.setContentsMargins(0, 0, 0, 0)
        card_layout.setSpacing(0)

        # Left Pane
        left_pane = QFrame()
        left_pane.setFixedSize(420, 500)
        left_pane.setObjectName("leftPane")
        left_pane.setStyleSheet("""
            #leftPane {
                background-color: #F9FAFA;
                border-top-left-radius: 12px;
                border-bottom-left-radius: 12px;
            }
            QLabel {
                background-color: transparent;
            }
        """)
        
        left_layout = QVBoxLayout(left_pane)
        left_layout.setContentsMargins(50, 50, 50, 50)
        left_layout.setAlignment(Qt.AlignTop)

        # Left Pane Content
        icon_container = QLabel()
        icon_container.setFixedSize(52, 52)
        icon_container.setStyleSheet("background-color: #E8EAF6; border-radius: 12px;")
        # Placeholder for icon
        icon_container.setText("📊")
        icon_container.setAlignment(Qt.AlignCenter)
        font = icon_container.font()
        font.setPointSize(24)
        icon_container.setFont(font)
        left_layout.addWidget(icon_container)
        
        left_layout.addSpacing(20)
        
        title = QLabel("Usage & adoption insights")
        title.setStyleSheet("color: #1D1D1D; font-size: 26px; font-weight: 600;")
        title.setWordWrap(True)
        left_layout.addWidget(title)
        
        left_layout.addSpacing(10)
        
        desc = QLabel("Uncover how your teams use provisioned tools. Identify shelfware and consolidation opportunities by analyzing login frequency and collaboration metrics. All data is processed securely and locally.")
        desc.setStyleSheet("color: #4A4A4A; font-size: 14px;")
        desc.setWordWrap(True)
        left_layout.addWidget(desc)

        # Right Pane
        right_pane = QFrame()
        right_pane.setFixedSize(420, 500)
        right_pane.setObjectName("rightPane")
        right_pane.setStyleSheet("""
            #rightPane {
                background-color: white;
                border-top-right-radius: 12px;
                border-bottom-right-radius: 12px;
            }
            QLabel {
                background-color: transparent;
            }
        """)
        
        right_layout = QVBoxLayout(right_pane)
        right_layout.setContentsMargins(50, 50, 50, 50)
        right_layout.setAlignment(Qt.AlignTop)

        # Right Pane Content
        header_layout = QHBoxLayout()
        header_title = QLabel("Log in to APIs")
        header_title.setStyleSheet("color: #1D1D1D; font-size: 20px; font-weight: 600;")
        header_layout.addWidget(header_title)
        header_layout.addStretch()
        # Badges placeholder
        badges = QLabel("🪟 ➔ 🌐")
        header_layout.addWidget(badges)
        
        right_layout.addLayout(header_layout)
        right_layout.addSpacing(30)

        # Inputs
        self.tenant_input = self.create_input("Tenant ID")
        right_layout.addWidget(self.tenant_input[0])
        right_layout.addWidget(self.tenant_input[1])
        right_layout.addSpacing(15)

        self.client_input = self.create_input("App client ID")
        right_layout.addWidget(self.client_input[0])
        right_layout.addWidget(self.client_input[1])
        right_layout.addSpacing(15)

        self.secret_input = self.create_input("Client secret", is_password=True)
        right_layout.addWidget(self.secret_input[0])
        right_layout.addWidget(self.secret_input[1])
        right_layout.addSpacing(5)

        self.status_text = QLabel("")
        self.status_text.setStyleSheet("color: #D32F2F; font-size: 13px;")
        right_layout.addWidget(self.status_text)
        right_layout.addSpacing(15)

        auth_btn = QPushButton("Authorize connection")
        auth_btn.setFixedHeight(45)
        auth_btn.setCursor(Qt.PointingHandCursor)
        auth_btn.setStyleSheet("""
            QPushButton {
                background-color: #0b57d0;
                color: white;
                border-radius: 8px;
                font-size: 14px;
                font-weight: 500;
            }
            QPushButton:hover {
                background-color: #0842a0;
            }
        """)
        auth_btn.clicked.connect(self.handle_connect)
        right_layout.addWidget(auth_btn)

        # Add panes to card
        card_layout.addWidget(left_pane)
        card_layout.addWidget(right_pane)

        main_layout.addWidget(card)

    def create_input(self, label_text, is_password=False):
        label = QLabel(label_text)
        label.setStyleSheet("color: #4A4A4A; font-size: 13px;")
        
        line_edit = QLineEdit()
        line_edit.setFixedHeight(40)
        line_edit.setStyleSheet("""
            QLineEdit {
                border: 1px solid #BDBDBD;
                border-radius: 6px;
                padding: 0 12px;
                font-size: 14px;
                background-color: white;
                color: #1D1D1D;
            }
            QLineEdit:focus {
                border: 2px solid #0b57d0;
            }
        """)
        if is_password:
            line_edit.setEchoMode(QLineEdit.Password)
            
        return (label, line_edit)

    def handle_connect(self):
        tenant = self.tenant_input[1].text().strip()
        client = self.client_input[1].text().strip()
        secret = self.secret_input[1].text().strip()

        if not tenant or not client or not secret:
            self.status_text.setText("Error: Tenant ID, Client ID, and Client Secret are required.")
            return

        self.status_text.setText("")
        self.on_connect_clicked(tenant, client, secret)
