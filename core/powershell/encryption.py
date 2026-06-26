import json
import logging
from core.powershell.client import PowerShellClient
from core.cert_auth import load_certificate

logger = logging.getLogger(__name__)

def get_encryption_policies(client: PowerShellClient) -> dict:
    """
    Executes the export_encryption_policies.ps1 script using the PowerShellClient.
    Returns a dictionary with 'm365_policies' and 'exchange_deps'.
    """
    logger.info("Starting M365 Data Encryption Policy fetch via PowerShell...")
    try:
        cert_path = client.locate_certificate()

        args = [
            "-TenantId", client.tenant_id,
            "-AppId", client.client_id,
            "-CertificateFilePath", cert_path,
            "-CertificatePassword", client.cert_password
        ]

        stdout = client.execute_script("scripts/export_encryption_policies.ps1", args)
        
        # Parse the JSON output from PowerShell
        if stdout and stdout.strip():
            # Find where the JSON starts in case of warnings
            try:
                json_start = stdout.find('{')
                if json_start != -1:
                    json_str = stdout[json_start:]
                    return json.loads(json_str)
                else:
                    logger.warning("No JSON object found in PowerShell output.")
                    return {"m365_policies": [], "exchange_deps": []}
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON from PowerShell: {e}\nOutput: {stdout}")
                raise RuntimeError("Failed to parse encryption policies from Exchange Online.")
        else:
            return {"m365_policies": [], "exchange_deps": []}

    except Exception as e:
        logger.error(f"Error fetching encryption policies: {e}", exc_info=True)
        raise
