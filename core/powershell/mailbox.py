import json
from core.powershell.client import PowerShellClient

class MailboxStatsService:
    def __init__(self, client: PowerShellClient):
        self.client = client

    def fetch_mailbox_and_folder_stats(self) -> dict:
        """Locates certificate, triggers powershell execution, and parses shared mailbox and public folder stats."""
        try:
            cert_path = self.client.locate_certificate()
        except Exception as e:
            raise RuntimeError(f"Failed to locate certificate for authentication: {str(e)}")

        args = [
            "-AppId", self.client.client_id,
            "-Organization", self.client.tenant_id,
            "-CertificatePath", cert_path
        ]
        if self.client.cert_password:
            args += ["-CertificatePassword", self.client.cert_password]

        raw_output = self.client.execute_script("scripts/get_mailbox_and_folder_stats.ps1", args)
        
        if not raw_output or not raw_output.strip():
            return {}

        try:
            return json.loads(raw_output)
        except json.JSONDecodeError:
            # Parse JSON block in case of warning/header lines outputted by powershell environment
            lines = raw_output.strip().split('\n')
            json_str = ""
            for line in lines:
                if line.startswith("[") or line.startswith("{") or json_str:
                    json_str += line
            if json_str:
                return json.loads(json_str)
            raise RuntimeError(f"PowerShell returned non-JSON format: {raw_output}")

    def fetch_legal_holds(self) -> dict:
        """Locates certificate, triggers powershell execution, and fetches mailboxes with legal hold."""
        try:
            cert_path = self.client.locate_certificate()
        except Exception as e:
            raise RuntimeError(f"Failed to locate certificate for authentication: {str(e)}")

        args = [
            "-AppId", self.client.client_id,
            "-Organization", self.client.tenant_id,
            "-CertificatePath", cert_path
        ]
        if self.client.cert_password:
            args += ["-CertificatePassword", self.client.cert_password]

        raw_output = self.client.execute_script("scripts/get_legal_holds.ps1", args)
        
        if not raw_output or not raw_output.strip():
            return {"value": []}

        try:
            return json.loads(raw_output)
        except json.JSONDecodeError:
            lines = raw_output.strip().split('\n')
            json_str = ""
            for line in lines:
                if line.startswith("[") or line.startswith("{") or json_str:
                    json_str += line
            if json_str:
                return json.loads(json_str)
            raise RuntimeError(f"PowerShell returned non-JSON format: {raw_output}")

    def export_to_csv(self, data_list: list, filename: str, output_dir: str = "scratch"):
        import os, csv
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, filename)
        if not data_list:
            open(filepath, 'a').close()
            return
        
        keys = set()
        for row in data_list:
            if isinstance(row, dict):
                keys.update(row.keys())
                
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(keys))
            writer.writeheader()
            for row in data_list:
                writer.writerow(row)
