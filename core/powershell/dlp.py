import os
import json
import logging

logger = logging.getLogger(__name__)

class DLPService:
    """Service for interacting with Exchange Online / Purview PowerShell for DLP policies."""
    
    def __init__(self, ps_client):
        self.ps_client = ps_client

    def fetch_dlp_policies(self) -> dict:
        """
        Executes the PowerShell script to retrieve DLP policies via Get-DlpCompliancePolicy.
        """
        try:
            cert_path = self.ps_client.locate_certificate()
        except Exception as e:
            raise RuntimeError(f"Failed to locate certificate for authentication: {str(e)}")

        args = [
            "-AppId", self.ps_client.client_id,
            "-Organization", self.ps_client.tenant_id,
            "-CertificatePath", cert_path
        ]
        if self.ps_client.cert_password:
            args += ["-CertificatePassword", self.ps_client.cert_password]

        logger.info("Executing fetch_dlp_policies script")
        
        try:
            raw_output = self.ps_client.execute_script("scripts/get_dlp_policies.ps1", args)
            if not raw_output or not raw_output.strip():
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
                # Parse JSON block in case of warning/header lines outputted by powershell environment
                lines = raw_output.strip().split('\n')
                json_str = ""
                for line in lines:
                    if line.startswith("[") or line.startswith("{") or json_str:
                        json_str += line
                if json_str:
                    data = json.loads(json_str)
                    if isinstance(data, dict) and "value" in data:
                        return data
                    elif isinstance(data, list):
                        return {"value": data}
                    else:
                        return {"value": [data]}
                raise RuntimeError(f"PowerShell returned non-JSON format: {raw_output}")
                
        except Exception as e:
            logger.error("Error executing fetch_dlp_policies", exc_info=True)
            raise Exception(f"PowerShell script execution failed: {str(e)}")
