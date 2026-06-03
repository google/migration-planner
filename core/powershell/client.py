import os
import subprocess
import logging

logger = logging.getLogger("PowerShellClient")

class PowerShellClient:
    def __init__(self, tenant_id, client_id, cert_dir="/Users/srishtinegi/Desktop/Test/certificates"):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.cert_dir = cert_dir

    def locate_certificate(self) -> str:
        """Finds the first .pfx or .pem certificate in the target directory."""
        if not os.path.exists(self.cert_dir):
            raise FileNotFoundError(f"Certificate directory does not exist: {self.cert_dir}")
        
        for file in os.listdir(self.cert_dir):
            if file.endswith(".pfx") or file.endswith(".pem"):
                return os.path.join(self.cert_dir, file)
                
        raise FileNotFoundError(f"No .pfx or .pem certificate found in {self.cert_dir}")

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
            return result.stdout
        except subprocess.CalledProcessError as e:
            logger.error(f"PowerShell script failed: {e.stderr}")
            raise RuntimeError(f"PowerShell script execution failed: {e.stderr or e.stdout}")
        except FileNotFoundError:
            raise RuntimeError("PowerShell core ('pwsh') is not installed or not in PATH. Please install it (e.g. 'brew install powershell' on macOS).")
