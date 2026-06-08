import json
from core.powershell.client import PowerShellClient

class CalendarStatsService:
    def __init__(self, client: PowerShellClient):
        self.client = client

    def fetch_calendar_attachments_policy(self) -> dict:
        """Locates certificate, triggers powershell execution, and parses Exchange calendar configurations."""
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

        raw_output = self.client.execute_script("scripts/exchange_calendar_metadata.ps1", args)
        
        if not raw_output or not raw_output.strip():
            return {
                "RoomsCount": 0,
                "RoomsError": None,
                "RoomsNaming": None,
                "EquipmentCount": 0,
                "EquipmentError": None,
                "CanShareAttachments": True,
                "OwaPolicyError": None,
                "OrganizationApps": [],
                "AppsError": None
            }

        # Parse JSON block in case of warning/header lines outputted by powershell environment
        lines = raw_output.strip().split('\n')
        json_str = ""
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("[") or stripped.startswith("{") or json_str:
                json_str += stripped

        if json_str:
            try:
                return json.loads(json_str)
            except json.JSONDecodeError as e:
                raise RuntimeError(f"Failed to decode JSON from script output: {str(e)}. Output: {json_str}")
        
        raise RuntimeError(f"PowerShell returned non-JSON format: {raw_output}")
