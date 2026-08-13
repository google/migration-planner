import sys
import os
import ssl

ssl._create_default_https_context = ssl._create_unverified_context
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from PySide6.QtWidgets import QApplication, QMainWindow, QStackedWidget, QMessageBox
from pyside_app.auth_view import AuthView
from pyside_app.dashboard import DashboardView
from core.cert_auth import check_certificate_exists, generate_certificate, load_certificate

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Deal Assistant (PySide6)")
        self.resize(1024, 768)
        self.setStyleSheet("QMainWindow { background-color: #F0F2F5; } QWidget#centralWidget { background-color: #F0F2F5; }")
        
        self.stack = QStackedWidget()
        self.stack.setObjectName("centralWidget")
        self.setCentralWidget(self.stack)
        
        self.tenant = ""
        self.client = ""
        self.secret = ""
        
        self.show_auth()
        
    def show_error_dialog(self, title, message):
        QMessageBox.critical(self, title, message)

    def show_auth(self):
        auth_view = AuthView(self.handle_connect)
        self.stack.addWidget(auth_view)
        self.stack.setCurrentWidget(auth_view)

    def show_dashboard(self):
        dash = DashboardView(self.tenant, self.client, self.secret, self.show_auth)
        self.stack.addWidget(dash)
        self.stack.setCurrentWidget(dash)

    def handle_connect(self, tenant, client, secret):
        self.tenant = tenant
        self.client = client
        self.secret = secret
        
        if check_certificate_exists(tenant_id=tenant, client_id=client):
            try:
                load_certificate(secret, tenant_id=tenant, client_id=client)
                self.show_dashboard()
            except Exception as e:
                self.show_error_dialog("Certificate Error", f"Error: {e}")
                self.show_dashboard()
        else:
            try:
                pem_path, _ = generate_certificate(secret, tenant_id=tenant, client_id=client)
                # For now, just bypass cert instructions and show dashboard
                self.show_dashboard()
            except Exception as e:
                self.show_error_dialog("Certificate Generation Error", f"Error: {e}")
                self.show_dashboard()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
