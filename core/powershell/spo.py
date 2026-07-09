import json
import logging
from core.powershell.client import PowerShellClient

logger = logging.getLogger(__name__)

class SPOService:
    """Service for interacting with SharePoint Online PowerShell."""
    
    def __init__(self, client: PowerShellClient, spo_admin_url: str):
        self.client = client
        self.spo_admin_url = spo_admin_url

    def fetch_spo_sites(self) -> dict:
        """Executes the PowerShell script to retrieve SPO sites via Get-SPOSite."""
        args = [
            "-SharePointAdminUrl", self.spo_admin_url
        ]

        logger.info("Executing fetch_spo_sites script")
        
        try:
            raw_output = self.client.execute_script("scripts/get_spo_sites.ps1", args)
            if not raw_output or not raw_output.strip():
                return {"value": []}
            
            try:
                data = json.loads(raw_output)
                if isinstance(data, dict):
                    return data
                elif isinstance(data, list):
                    return {"value": data}
                else:
                    return {"value": [data]}
            except json.JSONDecodeError:
                lines = raw_output.strip().split('\n')
                json_str = ""
                for line in lines:
                    if line.startswith("[") or line.startswith("{") or json_str:
                        json_str += line
                if json_str:
                    data = json.loads(json_str)
                    if isinstance(data, dict):
                        return data
                    elif isinstance(data, list):
                        return {"value": data}
                    else:
                        return {"value": [data]}
                raise RuntimeError(f"PowerShell returned non-JSON format: {raw_output}")
                
        except Exception as e:
            logger.error("Error executing fetch_spo_sites", exc_info=True)
            raise Exception(f"PowerShell script execution failed: {str(e)}")

    def export_to_csv(self, data_list: list, filename: str, output_dir: str = "scratch"):
        import os, csv
        if not data_list:
            return
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, filename)
        
        keys = set()
        for row in data_list:
            if isinstance(row, dict):
                keys.update(row.keys())
                
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(keys))
            writer.writeheader()
            for row in data_list:
                writer.writerow(row)
