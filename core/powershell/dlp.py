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

    def fetch_sensitive_info_types(self) -> dict:
        """
        Executes the PowerShell script to retrieve Sensitive Information Types via Get-DlpSensitiveInformationType.
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

        logger.info("Executing fetch_sensitive_info_types script")
        
        try:
            raw_output = self.ps_client.execute_script("scripts/get_sensitive_info_types.ps1", args)
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
            except json.JSONDecodeError as e:
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
            logger.error("Error executing fetch_sensitive_info_types", exc_info=True)
            raise Exception(f"PowerShell script execution failed: {str(e)}")

    def export_custom_sit_xml(self, data: dict, output_dir: str = "scratch"):
        os.makedirs(output_dir, exist_ok=True)
        custom_sits = data.get("CustomRulePackages") or []
        for sit in custom_sits:
            name = sit.get("Name", "Unknown").replace(" ", "_")
            xml_content = sit.get("ClassificationRuleCollectionXml", "")
            if xml_content:
                with open(os.path.join(output_dir, f"{name}_rule_package.xml"), "w") as f:
                    f.write(xml_content)
                    
    def export_advanced_rules_txt(self, data: dict, output_dir: str = "scratch"):
        os.makedirs(output_dir, exist_ok=True)
        rules = data.get("Rules") or []
        with open(os.path.join(output_dir, "advanced_dlp_rules.txt"), "w") as f:
            for rule in rules:
                advanced_rule = rule.get("AdvancedRule")
                if advanced_rule:
                    f.write(f"Rule Name: {rule.get('Name')}\n")
                    f.write(f"Advanced Rule:\n{advanced_rule}\n")
                    f.write("-" * 80 + "\n")

    def export_to_csv(self, data_list: list, filename: str, output_dir: str = "scratch"):
        import csv, os
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, filename)
        if not data_list:
            open(filepath, 'a').close()
            return
        
        # Get all possible keys to avoid missing fieldnames error
        keys = set()
        for row in data_list:
            if isinstance(row, dict):
                keys.update(row.keys())
                
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(keys))
            writer.writeheader()
            for row in data_list:
                writer.writerow(row)
