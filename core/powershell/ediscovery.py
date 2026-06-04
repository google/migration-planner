import json
import logging
from core.powershell.client import PowerShellClient

logger = logging.getLogger("EDiscoveryService")

class EDiscoveryService:
    def __init__(self, client: PowerShellClient):
        self.client = client

    def fetch_ediscovery_cases(self) -> list:
        """Locates certificate, triggers powershell execution, and parses eDiscovery cases."""
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

        raw_output = self.client.execute_script("scripts/get_compliance_cases.ps1", args)
        
        if not raw_output or not raw_output.strip():
            return []

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
