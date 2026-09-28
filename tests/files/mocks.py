import json
import urllib.parse
import time
import random
from typing import List, Dict, Any, Optional
import threading
import os

class MockResponse:
    def __init__(self, status_code: int, body: Dict[str, Any], text: str = None, headers: Dict[str, str] = None):
        self.status_code = status_code
        self.body = body
        # SpRestConnector reads resp.text on its 403/429/500/catch-all paths and
        # resp.headers on 429. Both are optional so existing callers are unaffected.
        self._text = text
        self.headers = headers if headers is not None else {}

    @property
    def text(self) -> str:
        """Serialises the body only if a caller actually reads it."""
        if self._text is not None:
            return self._text
        try:
            return json.dumps(self.body)
        except (TypeError, ValueError):
            return str(self.body)

    def json(self):
        return self.body

class MockSession:
    def __init__(self, test_data: Dict[str, Any]):
        self.test_data = test_data
        self.custom_responses = {} # path -> (status_code, body)
        
    def get(self, url: str, headers: Dict[str, str] = None, **kwargs):
        parsed_url = urllib.parse.urlparse(url)
        path = parsed_url.path
        if path.startswith("/v1.0"):
            path = path[5:]
            
        if path in self.custom_responses:
            status, body = self.custom_responses[path]
            return MockResponse(status, body)
            
        if "sites/root" in path:
            root_id = self.test_data.get("root_site", "root")
            root_site = self.test_data["sites"].get(root_id, {"id": root_id, "displayName": "Root Site"})
            return MockResponse(200, root_site)
            
        elif "sites/delta" in path or "/sites" in path:
            all_sites = [s for s in self.test_data.get("all_sites", []) if "parentReference" not in s]
            return MockResponse(200, {"value": all_sites})
            
        elif "subscribedSkus" in path:
            licenses = self.test_data.get("licenses", [])
            return MockResponse(200, {"value": licenses})
            
        parts = path.split("/")
        if "users" in parts:
            idx = parts.index("users")
            if len(parts) > idx + 2 and parts[idx + 2] == "drives":
                email_id = urllib.parse.unquote(parts[idx + 1])
                drives_list = list(self.test_data.get("drives", {}).keys())
                if drives_list:
                    import hashlib
                    h = int(hashlib.md5(email_id.encode('utf-8')).hexdigest(), 16)
                    drive_id = drives_list[h % len(drives_list)]
                else:
                    drive_id = "drive-mock"
                site_id = next((sid for sid, s in self.test_data.get("sites", {}).items() if drive_id in s.get("drives", [])), "root")
                curr_site = self.test_data.get("sites", {}).get(site_id)
                if curr_site:
                    while "parentReference" in curr_site and "siteId" in curr_site["parentReference"]:
                        parent_id = curr_site["parentReference"]["siteId"]
                        curr_site = self.test_data.get("sites", {}).get(parent_id)
                        if curr_site:
                            site_id = parent_id
                site_url = self.test_data.get("sites", {}).get(site_id, {}).get("webUrl", f"https://tenant.sharepoint.com/sites/{site_id}")
                body = {
                    "value": [
                        {
                            "sharePointIds": {
                                "listId": f"list-mock-{drive_id}",
                                "siteId": site_id,
                                "siteUrl": site_url,
                                "tenantId": "mock-tenant-id",
                                "webId": f"web-mock-{drive_id}"
                            }
                        }
                    ]
                }
                return MockResponse(200, body)
            
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
        
        self.drive_to_items = {}
        for item in self.test_data.get("items", {}).values():
            did = item["parentReference"]["driveId"]
            self.drive_to_items.setdefault(did, []).append(item)
        
    def invoke(self, base_url: str, batch: List[Dict[str, Any]], logger=None, stop_event=None, resource_type=None):
        responses = []
        for req in batch:
            req_id = req.get("id")
            url = req.get("url")
            
            parsed_url = urllib.parse.urlparse(url)
            path = parsed_url.path
            if path.startswith("/v1.0"):
                path = path[5:]
            query_params = urllib.parse.parse_qs(parsed_url.query)
            
            if path in self.token_manager.session.custom_responses:
                status, body = self.token_manager.session.custom_responses[path]
                responses.append({"id": req_id, "status": status, "body": body})
                continue
            
            # Simulate network delay
            time.sleep(random.uniform(0.001, 0.003))
            
            parts = path.split("/")
            
            # Handle /sites/delta
            if path in ["/sites/delta", "/sites"]:
                all_sites = [s for s in self.test_data.get("all_sites", []) if "parentReference" not in s]
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
                raw_drive_items = self.drive_to_items.get(drive_id, [])
                drive_items = []
                for item in raw_drive_items:
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


class MockSpSession:
    """Fake requests session for SharePoint REST calls used by Shallow Scan.

    Responses are registered against a substring of the endpoint URL. A key may
    be given several responses, which are then consumed in order and the last
    one repeats, so retry paths such as 429-then-200 can be exercised.

    Thread-safe: `_scan_document_libraries` drives this from a ThreadPoolExecutor,
    so the call log and the response queues are guarded by a lock.
    """

    def __init__(self):
        self.responses = {}
        self.calls = []
        self.lock = threading.Lock()

    def register(self, url_fragment: str, *responses: MockResponse):
        """Registers one or more responses for endpoints containing the fragment."""
        with self.lock:
            self.responses[url_fragment] = list(responses)

    def get(self, url: str, headers: Dict[str, str] = None, timeout: float = None, **kwargs):
        with self.lock:
            self.calls.append(url)
            for fragment, queued in self.responses.items():
                if fragment in url:
                    # Consume until one remains so the final response repeats.
                    return queued.pop(0) if len(queued) > 1 else queued[0]

        return MockResponse(404, {"error": {"message": f"Not Found: {url}"}})

    def get_calls(self) -> List[str]:
        """Returns a snapshot of every endpoint requested so far."""
        with self.lock:
            return list(self.calls)


class MockCertTokenManager:
    """Domain-keyed certificate token manager fake for Shallow Scan.

    Mirrors the subset of CertTokenManager that SpRestConnector depends on. Token
    slots are handed out without any real authentication, and refresh calls are
    recorded so HTTP 401 handling can be asserted.
    """

    def __init__(self, session: MockSpSession = None):
        self.session = session if session is not None else MockSpSession()
        self.authenticated_domains = []
        self.refresh_calls = []
        self.lock = threading.Lock()

    def ensure_domain_authenticated(self, sharepoint_domain: str, current_logger=None):
        with self.lock:
            if sharepoint_domain not in self.authenticated_domains:
                self.authenticated_domains.append(sharepoint_domain)

    def get_valid_token_slot(self, sharepoint_domain: str, current_logger=None) -> Dict[str, Any]:
        self.ensure_domain_authenticated(sharepoint_domain, current_logger)
        return {
            "token": "mock-sp-token",
            "expires_at": time.time() + 3600,
            "client_id": "mock-client-id",
            "domain": sharepoint_domain,
        }

    def return_token_slot(self, sharepoint_domain: str, token_data: Dict[str, Any]):
        pass

    def refresh_token_data(self, token_data: Dict[str, Any], current_logger=None) -> bool:
        with self.lock:
            self.refresh_calls.append(token_data.get("domain"))
        token_data["token"] = "mock-sp-token-refreshed"
        token_data["expires_at"] = time.time() + 3600
        return True

    def get_session(self) -> MockSpSession:
        return self.session
