# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import csv
import logging
import os
import requests

from core.graph.client import GraphClient

logger = logging.getLogger(__name__)

class SharePointDataTypesService:
    def __init__(self, client: GraphClient) -> None:
        self.client = client

    def get_data_types_summary(self, reports_dir: str) -> dict:
        """Runs the search queries to count document libraries, lists, and web pages."""
        token_slot = self.client.get_active_token()
        session = self.client.get_session()
        headers = {
            "Authorization": f"Bearer {token_slot['token']}",
            "Content-Type": "application/json"
        }
        
        url = "https://graph.microsoft.com/v1.0/search/query"
        
        # Base queries without region
        base_queries = {
            "Document Libraries": "contentclass:STS_List_DocumentLibrary",
            "Lists": "contentclass:STS_List",
            "Web Pages": "contentclass:STS_ListItem_WebPageLibrary"
        }
        
        results = {k: 0 for k in base_queries.keys()}
        regions = ["NAM", "EUR", "APC"]
        
        try:
            for region in regions:
                logger.info(f"Querying region: {region}")
                for name, query_str in base_queries.items():
                    payload = {
                        "requests": [{
                            "entityTypes": ["listItem"],
                            "query": {"queryString": query_str},
                            "size": 1,
                            "region": region
                        }]
                    }
                    
                    try:
                        logger.info(f"Submitting Graph Search Query for: {name} in {region}")
                        resp = session.post(url, headers=headers, json=payload, timeout=30.0)
                        
                        if resp.status_code != 200:
                            logger.warning(f"Error Response Body for {region}: {resp.text}")
                            resp.raise_for_status()
                            
                        data = resp.json()
                        hits_containers = data.get("value", [{}])[0].get("hitsContainers", [{}])
                        total = hits_containers[0].get("total", 0)
                        results[name] += total
                        
                    except requests.exceptions.HTTPError as e:
                        # If a multi-geo region doesn't exist for this tenant, log warning and continue
                        logger.warning(f"Failed to query {name} in region {region}. It may not be provisioned: {e}")
                        continue
            
            # Write to CSV in accordance with telemetry scaling guidelines
            os.makedirs(reports_dir, exist_ok=True)
            csv_path = os.path.join(reports_dir, "sharepoint_data_types.csv")
            tmp_path = csv_path + ".tmp"
            
            with open(tmp_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Data_Type", "Count"])
                for k, v in results.items():
                    writer.writerow([k, v])
            
            os.replace(tmp_path, csv_path)
            
            return results
        finally:
            self.client.release_token(token_slot)

def run_sharepoint_data_types_pipeline(client_id, client_secret, tenant_id) -> dict:
    """Pipeline entrypoint for SharePoint data types telemetry."""
    logger.info("Starting SharePoint Data Types Telemetry Pipeline...")
    
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=1,
        retries=5,
        backoff=2
    )
    logger.info("Attempting authenticate with Sites.Read.All...")
    client.authenticate(required_scopes=["Sites.Read.All"])
    logger.info("Authentication successful.")
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    telemetry_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(script_dir))), "telemetry")
    reports_dir = os.path.join(telemetry_dir, "reports", f"{tenant_id}_{client_id}")
    
    service = SharePointDataTypesService(client)
    sp_data = service.get_data_types_summary(reports_dir)
    
    logger.info("SharePoint Data Types Pipeline completed successfully.")
    client.close()
    return sp_data
