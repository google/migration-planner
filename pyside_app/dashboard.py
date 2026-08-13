
import sys
import os
import threading
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                               QPushButton, QFrame, QScrollArea, QTableWidget, QTableWidgetItem, QHeaderView)
from PySide6.QtCore import Qt, QThread, Signal
from PySide6.QtGui import QColor, QFont, QIcon, QBrush

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pyside_app.sidebar import Sidebar
from core.graph.client import GraphClient
from core.graph.directory import DirectoryService
from telemetry import active_users_usage as usage
from telemetry.mailbox_usage import run_mailbox_usage_pipeline
from telemetry.calendar_telemetry import run_calendar_telemetry_pipeline
from telemetry.sharepoint_onedrive_usage import run_sharepoint_pipeline, run_onedrive_pipeline

class FetchWorker(QThread):
    finished = Signal(str, object)
    error = Signal(str, str)
    
    def __init__(self, section_id, func, *args, **kwargs):
        super().__init__()
        self.section_id = section_id
        self.func = func
        self.args = args
        self.kwargs = kwargs
        
    def run(self):
        try:
            res = self.func(*self.args, **self.kwargs)
            self.finished.emit(self.section_id, res)
        except Exception as e:
            self.error.emit(self.section_id, str(e))

class DashboardView(QWidget):
    def __init__(self, tenant, client, secret, on_disconnect):
        super().__init__()
        self.tenant = tenant
        self.client = client
        self.secret = secret
        self.on_disconnect = on_disconnect
        
        self.init_ui()

    def init_ui(self):
        main_layout = QHBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)
        
        # Sidebar
        self.sidebar = Sidebar(self.on_disconnect)
        main_layout.addWidget(self.sidebar)
        
        # Main content
        content_widget = QWidget()
        content_widget.setStyleSheet("background-color: #F8FAFC;")
        content_layout = QVBoxLayout(content_widget)
        content_layout.setContentsMargins(30, 30, 30, 30)
        content_layout.setSpacing(20)
        
        # Header
        header = QFrame()
        header.setStyleSheet("background-color: white; border: 1px solid #E0E0E0; border-radius: 12px;")
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(20, 15, 20, 15)
        
        title = QLabel("Usage Report")
        title.setStyleSheet("font-size: 20px; font-weight: bold; color: #1D1D1D; border: none;")
        
        self.fetch_btn = QPushButton("Fetch Report")
        self.fetch_btn.setFixedSize(120, 40)
        self.fetch_btn.setCursor(Qt.PointingHandCursor)
        self.fetch_btn.setStyleSheet("""
            QPushButton {
                background-color: #0b57d0; color: white; font-weight: bold; border-radius: 8px;
            }
        """)
        self.fetch_btn.clicked.connect(self.handle_fetch)
        
        header_layout.addWidget(title)
        header_layout.addStretch()
        header_layout.addWidget(self.fetch_btn)
        content_layout.addWidget(header)
        
        # Scroll Area for Cards
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("border: none; background-color: transparent;")
        
        cards_widget = QWidget()
        cards_widget.setStyleSheet("background-color: transparent;")
        self.cards_layout = QVBoxLayout(cards_widget)
        self.cards_layout.setContentsMargins(0, 0, 0, 0)
        self.cards_layout.setSpacing(20)
        
        # Create Cards
        self.cards = {}
        self.create_card("o365", "O365 Active Users Usage")
        self.create_card("m365", "M365 App Usage (180 Days)")
        self.create_card("mailbox", "Mailbox Usage")
        self.create_card("calendar", "Calendar Usage")
        self.create_card("sharepoint", "SharePoint Usage")
        self.create_card("onedrive", "OneDrive Usage")
        
        self.cards_layout.addStretch()
        scroll.setWidget(cards_widget)
        content_layout.addWidget(scroll)
        
        main_layout.addWidget(content_widget)

    def create_card(self, card_id, title_text):
        card = QFrame()
        card.setStyleSheet("background-color: white; border: 1px solid #E0E0E0; border-radius: 12px;")
        layout = QVBoxLayout(card)
        layout.setContentsMargins(20, 20, 20, 20)
        
        title = QLabel(title_text)
        title.setStyleSheet("font-size: 16px; font-weight: bold; color: #1D1D1D; border: none; background-color: transparent;")
        layout.addWidget(title)
        
        status = QLabel("Ready")
        status.setStyleSheet("color: #757575; border: none; background-color: transparent;")
        layout.addWidget(status)
        
        content = QWidget()
        content_layout = QVBoxLayout(content)
        content_layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(content)
        
        self.cards_layout.addWidget(card)
        self.cards[card_id] = {"status": status, "content": content, "layout": content_layout}

    def set_loading(self, card_id):
        self.cards[card_id]["status"].setText("Loading...")
        self.cards[card_id]["status"].setStyleSheet("color: #F57C00; border: none; background-color: transparent;")
        
    def set_error(self, card_id, err):
        self.cards[card_id]["status"].setText(f"Error: {err}")
        self.cards[card_id]["status"].setStyleSheet("color: #D32F2F; border: none; background-color: transparent;")

    def set_done(self, card_id):
        self.cards[card_id]["status"].setText("Done")
        self.cards[card_id]["status"].setStyleSheet("color: #388E3C; border: none; background-color: transparent;")

    def get_graph_client(self):
        return GraphClient(tenant_id=self.tenant, client_ids=self.client, client_secrets=self.secret, concurrency=1, retries=1, backoff=2)
        
    def handle_fetch(self):
        self.fetch_btn.setDisabled(True)
        self.fetch_btn.setText("Fetching...")
        
        tasks = {
            "o365": lambda: usage.run_o365_pipeline(self.client, self.secret, self.tenant),
            "m365": lambda: usage.run_m365_pipeline(self.client, self.secret, self.tenant),
            "mailbox": lambda: run_mailbox_usage_pipeline(self.client, self.secret, self.tenant),
            "calendar": lambda: run_calendar_telemetry_pipeline(self.client, self.secret, self.tenant),
            "sharepoint": lambda: run_sharepoint_pipeline(self.client, self.secret, self.tenant),
            "onedrive": lambda: run_onedrive_pipeline(self.client, self.secret, self.tenant),
        }
        
        for card_id, func in tasks.items():
            self.set_loading(card_id)
            worker = FetchWorker(card_id, func)
            worker.finished.connect(self.on_fetch_success)
            worker.error.connect(self.on_fetch_error)
            worker.start()
            setattr(self, f"worker_{card_id}", worker)

    def generate_table(self, data):
        if not data:
            lbl = QLabel("No data found")
            lbl.setStyleSheet("border: none; background-color: transparent; color: #1D1D1D;")
            return lbl
            
        if isinstance(data, dict):
            # Flatten dict to list of key-value pairs
            data = [{"Property": k, "Value": str(v)} for k, v in data.items()]
            
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], tuple):
            new_data = []
            for item in data:
                if len(item) == 4:
                    new_data.append({"Product": item[0], "30 Days": item[1], "90 Days": item[2], "180 Days": item[3]})
                elif len(item) == 2:
                    new_data.append({"Platform": item[0], "Usage": item[1]})
                else:
                    d = {"Category": item[0]}
                    for idx, val in enumerate(item[1:]):
                        d[f"Value {idx+1}"] = val
                    new_data.append(d)
            data = new_data
            
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
            keys = list(data[0].keys())
            table = QTableWidget(len(data), len(keys))
            table.setHorizontalHeaderLabels([str(k) for k in keys])
            table.verticalHeader().setVisible(False)
            table.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
            
            # Styling the table
            table.setStyleSheet("""
                QTableWidget {
                    border: 1px solid #E0E0E0; 
                    background-color: white; 
                    color: black;
                }
                QHeaderView::section {
                    background-color: #E8F0FE;
                    color: #0b57d0;
                    font-weight: bold;
                    padding: 8px;
                    border: none;
                    border-bottom: 2px solid #E0E0E0;
                    border-right: 1px solid #E0E0E0;
                }
                QTableWidget::item {
                    color: black;
                    padding: 4px;
                    border-bottom: 1px solid #F0F0F0;
                }
            """)
            
            table.setAlternatingRowColors(True)
            
            for i, row in enumerate(data):
                for j, key in enumerate(keys):
                    item = QTableWidgetItem(str(row.get(key, '')))
                    item.setForeground(QBrush(QColor("black")))
                    item.setFlags(item.flags() & ~Qt.ItemIsEditable) # Make uneditable
                    table.setItem(i, j, item)
            
            table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
            
            # Adjust height to fit contents perfectly
            table.resizeRowsToContents()
            h = table.horizontalHeader().height()
            for i in range(table.rowCount()):
                h += table.rowHeight(i)
            # Add a small buffer for borders
            table.setFixedHeight(h + 2)
            
            return table
            
        lbl = QLabel(str(data))
        lbl.setStyleSheet("border: none; background-color: transparent; color: #1D1D1D;")
        lbl.setWordWrap(True)
        return lbl

    def on_fetch_success(self, card_id, data):
        self.set_done(card_id)
        layout = self.cards[card_id]["layout"]
        while layout.count():
            child = layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()
        
        widget = self.generate_table(data)
        layout.addWidget(widget)
        self.check_all_done()

    def on_fetch_error(self, card_id, err):
        self.set_error(card_id, err)
        self.check_all_done()
        
    def check_all_done(self):
        all_done = all(self.cards[c]["status"].text() in ("Done",) or "Error" in self.cards[c]["status"].text() for c in self.cards)
        if all_done:
            self.fetch_btn.setDisabled(False)
            self.fetch_btn.setText("Fetch Report")
