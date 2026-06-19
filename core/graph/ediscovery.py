import requests
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class EDiscoveryFetcher:
    """Fetches eDiscovery cases using Delegated Authentication."""
    
    def __init__(self, token: str):
        self.token = token
        self.base_url = "https://graph.microsoft.com/v1.0"
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json"
        })

    def fetch_cases(self, csv_path: str = None, on_page_callback=None) -> Dict[str, Any]:
        """Fetches the list of eDiscovery cases."""
        endpoint = f"{self.base_url}/security/cases/ediscoveryCases"
        all_cases = []
        try:
            if csv_path:
                import csv
                with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=["id", "displayName", "status", "createdDateTime", "closedBy"])
                    writer.writeheader()
                    
            while endpoint:
                response = self.session.get(endpoint)
                response.raise_for_status()
                data = response.json()
                page_items = data.get("value", [])
                
                if csv_path and page_items:
                    import csv
                    with open(csv_path, 'a', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=["id", "displayName", "status", "createdDateTime", "closedBy"], extrasaction='ignore')
                        for item in page_items:
                            cb = item.get("closedBy", {})
                            cb_user = cb.get("user", {}) if isinstance(cb, dict) else {}
                            item["closedBy"] = cb_user.get("displayName", "")
                            writer.writerow(item)
                
                all_cases.extend(page_items)
                if on_page_callback:
                    on_page_callback(page_items)
                    
                endpoint = data.get("@odata.nextLink")

            return {"success": True, "data": all_cases}
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch eDiscovery cases: {e}")
            error_details = str(e)
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_details += " - " + e.response.json().get("error", {}).get("message", e.response.text)
                except Exception:
                    error_details += " - " + e.response.text
            return {"success": False, "error": error_details}
