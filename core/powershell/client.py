import os
import subprocess
import logging

logger = logging.getLogger("PowerShellClient")

class PowerShellClient:
    def __init__(self, tenant_id, client_id, client_secret, cert_tenant_id=None):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.cert_password = client_secret
        self.cert_tenant_id = cert_tenant_id or tenant_id

    def locate_certificate(self) -> str:
        """Locates the automated hybrid auth PFX certificate path."""
        from core.cert_auth import get_cert_paths
        _, _, pfx_path = get_cert_paths(tenant_id=self.cert_tenant_id, client_id=self.client_id)
        if not os.path.exists(pfx_path):
            raise FileNotFoundError(f"Hybrid auth PFX certificate not found at {pfx_path}. Please complete the hybrid authentication flow on the login page first.")
        return pfx_path

    def execute_script(self, script_relative_path: str, args: list) -> str:
        """Executes pwsh with the specified script and arguments."""
        script_path = os.path.join(os.path.dirname(__file__), script_relative_path)
        
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"PowerShell script not found at {script_path}")
            
        # Construct CLI command
        command = ["pwsh", "-NoProfile", "-NonInteractive", "-File", script_path] + args
        
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=True
            )
            if result.stderr and result.stderr.strip():
                logger.warning(f"PowerShell script '{script_path}' stderr diagnostics: {result.stderr.strip()}")
            return result.stdout
        except subprocess.CalledProcessError as e:
            logger.error(f"PowerShell script failed: {e.stderr}", exc_info=True)
            raise RuntimeError(f"PowerShell script execution failed: {e.stderr or e.stdout}")
        except FileNotFoundError:
            raise RuntimeError("PowerShell core ('pwsh') is not installed or not in PATH. Please install it (e.g. 'brew install powershell' on macOS).")
