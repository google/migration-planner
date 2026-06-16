import os
import json
import logging

logger = logging.getLogger(__name__)

class DLPService:
    """Service for interacting with Exchange Online / Purview PowerShell for DLP policies."""
    
    def __init__(self, ps_client):
        self.ps_client = ps_client
        self.script_dir = os.path.join(os.path.dirname(__file__), 'scripts')

    def fetch_dlp_policies(self) -> dict:
        """
        Executes the PowerShell script to retrieve DLP policies via Get-DlpCompliancePolicy.
        """
        script_path = os.path.join(self.script_dir, 'get_dlp_policies.ps1')
        logger.info("Executing fetch_dlp_policies script")
        
        try:
            raw_output = self.ps_client.execute_script(script_path, [])
            if not raw_output:
                return {"value": []}
            
            try:
                data = json.loads(raw_output)
                if isinstance(data, dict) and "value" in data:
                    return data
                elif isinstance(data, list):
                    return {"value": data}
                else:
                    return {"value": [data]}
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON from get_dlp_policies.ps1: {e}")
                logger.debug(f"Raw output was: {raw_output}")
                raise Exception(f"Invalid JSON returned from PowerShell: {raw_output[:200]}")
                
        except Exception as e:
            logger.error("Error executing fetch_dlp_policies", exc_info=True)
            raise Exception(f"PowerShell script execution failed: {str(e)}")
