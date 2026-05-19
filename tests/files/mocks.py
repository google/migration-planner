import json
import urllib.parse
import time
import random
from typing import List, Dict, Any, Optional
import threading
import os

class MockResponse:
    def __init__(self, status_code: int, body: Dict[str, Any]):
        self.status_code = status_code
        self.body = body
        
    def json(self):
        return self.body

class MockSession:
    def __init__(self, test_data: Dict[str, Any]):
        self.test_data = test_data
        
    def get(self, url: str, headers: Dict[str, str] = None, **kwargs):
        parsed_url = urllib.parse.urlparse(url)
        path = parsed_url.path
        
        if "sites/root" in path:
            root_id = self.test_data.get("root_site", "root")
            root_site = self.test_data["sites"].get(root_id, {"id": root_id, "displayName": "Root Site"})
            return MockResponse(200, root_site)
            
        elif "sites/delta" in path:
            all_sites = self.test_data.get("all_sites", [])
            return MockResponse(200, {"value": all_sites})
            
        elif "subscribedSkus" in path:
            licenses = self.test_data.get("licenses", [])
            return MockResponse(200, {"value": licenses})
            
        return MockResponse(404, {"error": {"message": "Not Found"}})

class MockTokenManager:
    def __init__(self, test_data: Dict[str, Any]):
        self.test_data = test_data
        self.session = MockSession(test_data)
        
    def get_valid_token_slot(self, logger=None):
        return {"token": "mock-token", "expires_at": time.time() + 3600}
        
    def get_session(self):
        return self.session
        
    def return_token_slot(self, token_data):
        pass

class MockUrlInvoker:
    def __init__(self, test_data: Dict[str, Any]):
        self.test_data = test_data
        self.token_manager = MockTokenManager(test_data)
        self.page_size = 5 # Default page size for simulation
        
    def invoke(self, base_url: str, batch: List[Dict[str, Any]], logger=None, stop_event=None, resource_type=None):
        responses = []
        for req in batch:
            req_id = req.get("id")
            url = req.get("url")
            
            parsed_url = urllib.parse.urlparse(url)
            path = parsed_url.path
            query_params = urllib.parse.parse_qs(parsed_url.query)
            
            # Simulate network delay
            time.sleep(random.uniform(0.01, 0.05))
            
            parts = path.split("/")
            
            # Handle /sites/delta
            if path == "/sites/delta":
                all_sites = self.test_data.get("all_sites", [])
                # Pagination
                skip = int(query_params.get("$skip", [0])[0])
                sliced_sites = all_sites[skip : skip + self.page_size]
                
                body = {"value": sliced_sites}
                if skip + self.page_size < len(all_sites):
                    body["@odata.nextLink"] = f"{base_url}{path}?$skip={skip + self.page_size}"
                    
                responses.append({"id": req_id, "status": 200, "body": body})
                
            # Handle /sites/{siteId}/sites
            elif len(parts) >= 3 and parts[-1] == "sites" and parts[-3] == "sites":
                site_id = parts[-2]
                subsite_ids = self.test_data["sites"].get(site_id, {}).get("subsites", [])
                
                # Pagination
                skip = int(query_params.get("$skip", [0])[0])
                sliced_ids = subsite_ids[skip : skip + self.page_size]
                
                value = []
                for sid in sliced_ids:
                    s_data = self.test_data["sites"].get(sid, {"id": sid, "displayName": f"Subsite {sid}"})
                    value.append(s_data)
                    
                body = {"value": value}
                if skip + self.page_size < len(subsite_ids):
                    body["@odata.nextLink"] = f"{base_url}{path}?$skip={skip + self.page_size}"
                    
                responses.append({"id": req_id, "status": 200, "body": body})
                
            # Handle /sites/{siteId}/lists
            elif len(parts) >= 3 and parts[-1] == "lists" and parts[-3] == "sites":
                site_id = parts[-2]
                list_ids = self.test_data["sites"].get(site_id, {}).get("lists", [])
                
                value = []
                for lid in list_ids:
                    l_data = self.test_data["lists"].get(lid, {"id": lid, "name": f"List {lid}"})
                    value.append(l_data)
                    
                responses.append({"id": req_id, "status": 200, "body": {"value": value}})
                
            # Handle /sites/{siteId}/drives
            elif len(parts) >= 3 and parts[-1] == "drives" and parts[-3] == "sites":
                site_id = parts[-2]
                drive_ids = self.test_data["sites"].get(site_id, {}).get("drives", [])
                
                value = []
                for did in drive_ids:
                    d_data = self.test_data["drives"].get(did, {"id": did, "name": f"Drive {did}", "driveType": "documentLibrary"})
                    value.append(d_data)
                    
                responses.append({"id": req_id, "status": 200, "body": {"value": value}})
                
            # Handle /drives/{driveId}/root/delta
            elif len(parts) >= 4 and parts[-1] == "delta" and parts[-2] == "root" and parts[-4] == "drives":
                drive_id = parts[-3]
                
                def is_ancestor_failed(item_id):
                    curr_id = item_id
                    while curr_id:
                        item = self.test_data["items"].get(curr_id)
                        if not item:
                            break
                        if item.get("fail", False):
                            return True
                        curr_id = item["parentReference"].get("id")
                    return False

                # Find all items for this drive
                drive_items = []
                for item in self.test_data["items"].values():
                    if item["parentReference"]["driveId"] == drive_id:
                        if os.environ.get("SIMULATE_FAILURES", "False").lower() == "true":
                            if is_ancestor_failed(item["id"]):
                                continue
                        drive_items.append(item)
                        
                # Pagination
                skip = int(query_params.get("$skip", [0])[0])
                sliced_items = drive_items[skip : skip + self.page_size]
                
                body = {"value": sliced_items}
                if skip + self.page_size < len(drive_items):
                    body["@odata.nextLink"] = f"{base_url}{path}?$skip={skip + self.page_size}"
                    
                responses.append({"id": req_id, "status": 200, "body": body})
            else:
                responses.append({"id": req_id, "status": 404, "body": {"error": {"message": f"Not Found: {path}"}}})
                
        return responses
