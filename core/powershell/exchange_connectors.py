import json
import logging
from core.powershell.client import PowerShellClient

logger = logging.getLogger("ExchangeConnectorsService")

class ExchangeConnectorsService:
    def __init__(self, client: PowerShellClient):
        self.client = client

    def fetch_exchange_connectors(self) -> dict:
        """Executes get_connectors.ps1 and parses the JSON response."""
        logger.info("Executing Exchange Connectors PowerShell script...")
        try:
            cert_path = self.client.locate_certificate()
        except FileNotFoundError as e:
            logger.error(str(e))
            raise RuntimeError(str(e))
            
        args = [
            "-AppId", self.client.client_id,
            "-Organization", self.client.tenant_id,
            "-CertificatePath", cert_path
        ]
        
        if self.client.cert_password:
            args.extend(["-CertificatePassword", self.client.cert_password])
            
        try:
            output = self.client.execute_script("scripts/get_connectors.ps1", args)
            
            # Extract JSON from output
            json_str = output
            if "{" in output:
                json_str = output[output.find("{"):]
            
            if not json_str.strip():
                return {"InboundConnectors": [], "OutboundConnectors": [], "Errors": {}}
                
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Exchange Connectors JSON: {e}")
            raise RuntimeError(f"Failed to parse Exchange Connectors data: {e}")
        except Exception as e:
            logger.error(f"Exchange Connectors retrieval failed: {e}")
            raise RuntimeError(f"Exchange Connectors retrieval failed: {e}")
