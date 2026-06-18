import os
import json
import logging
from core.powershell.client import PowerShellClient

logger = logging.getLogger("PowerShell.TransportRules")

class TransportRulesFetcher:
    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.runner = PowerShellClient(tenant_id, client_id, client_secret)
        self.script_path = "scripts/get_transport_rules.ps1"

    def fetch_rules(self, csv_path: str) -> dict:
        """
        Executes the get_transport_rules.ps1 script, exporting data to csv_path.
        Returns a dict: {"success": bool, "errors": dict}
        """
        args = [
            "-AppId", self.client_id,
            "-Organization", self.tenant_id,
            "-ClientSecret", self.client_secret,
            "-CsvPath", csv_path
        ]

        try:
            logger.info("Executing Exchange Transport Rules PowerShell script...")
            stdout = self.runner.execute_script(self.script_path, args)
            
            if not stdout.strip():
                return {"success": False, "errors": {"ParseError": "Empty response from script"}}

            data = json.loads(stdout)
            return {
                "success": data.get("Success", False),
                "errors": data.get("Errors", {})
            }

        except Exception as e:
            logger.error(f"Failed to fetch transport rules: {e}", exc_info=True)
            return {"success": False, "errors": {"ExecutionError": str(e)}}
