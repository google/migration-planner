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

import unittest
from unittest.mock import MagicMock, patch
from core.graph.directory import DirectoryService

class TestDirectoryService(unittest.TestCase):

    @patch('core.graph.directory.GraphClient')
    def test_get_directory_telemetry_success(self, MockGraphClient):
        mock_client = MockGraphClient()
        mock_client.get_active_token.return_value = {"token": "mock_token"}
        mock_session = MagicMock()
        mock_client.get_session.return_value = mock_session
        
        # Configure UrlInvoker mock
        mock_url_invoker = MagicMock()
        mock_client.url_invoker = mock_url_invoker
        
        mock_url_invoker.invoke.return_value = [
            {
                "id": "organization",
                "status": 200,
                "body": {"value": [{"displayName": "Test Org", "tenantType": "test"}]}
            },
            {
                "id": "domains",
                "status": 200,
                "body": {"value": [{"id": "test.com", "isDefault": True}]}
            },

            {"id": "total", "status": 200, "body": {"@odata.count": 10}},
            {"id": "security", "status": 200, "body": {"@odata.count": 2}},
            {"id": "distribution", "status": 200, "body": {"@odata.count": 3}},
            {"id": "mail_enabled_security", "status": 200, "body": {"@odata.count": 1}},
            {"id": "m365", "status": 200, "body": {"@odata.count": 4}},
            {"id": "dynamic", "status": 200, "body": {"@odata.count": 0}},
            {"id": "users_total", "status": 200, "body": {"@odata.count": 50}},
            {"id": "users_enabled", "status": 200, "body": {"@odata.count": 45}},
            {"id": "users_disabled", "status": 200, "body": {"@odata.count": 5}},
            {"id": "users_member", "status": 200, "body": {"@odata.count": 40}},
            {"id": "users_guest", "status": 200, "body": {"@odata.count": 10}}
        ]
        
        service = DirectoryService(mock_client)
        res = service.get_directory_telemetry()
        
        self.assertEqual(res["organization"][0]["displayName"], "Test Org")
        self.assertEqual(res["domains"][0]["id"], "test.com")

        self.assertEqual(res["group_counts"]["total"], 10)
        self.assertEqual(res["user_counts"]["total"], 50)

    @patch('core.graph.directory.GraphClient')
    def test_get_directory_telemetry_sync_failure_fallback(self, MockGraphClient):
        mock_client = MockGraphClient()
        mock_client.get_active_token.return_value = {"token": "mock_token"}
        mock_session = MagicMock()
        mock_client.get_session.return_value = mock_session
        
        mock_url_invoker = MagicMock()
        mock_client.url_invoker = mock_url_invoker
        
        # Configure responses where onpremises_sync fails with 500 Internal Server Error
        mock_url_invoker.invoke.return_value = [
            {
                "id": "organization",
                "status": 200,
                "body": {"value": [{"displayName": "Test Org"}]}
            },
            {
                "id": "domains",
                "status": 200,
                "body": {"value": [{"id": "test.com"}]}
            },

            {"id": "total", "status": 200, "body": {"@odata.count": 10}},
            {"id": "security", "status": 200, "body": {"@odata.count": 2}},
            {"id": "distribution", "status": 200, "body": {"@odata.count": 3}},
            {"id": "mail_enabled_security", "status": 200, "body": {"@odata.count": 1}},
            {"id": "m365", "status": 200, "body": {"@odata.count": 4}},
            {"id": "dynamic", "status": 200, "body": {"@odata.count": 0}},
            {"id": "users_total", "status": 200, "body": {"@odata.count": 50}},
            {"id": "users_enabled", "status": 200, "body": {"@odata.count": 45}},
            {"id": "users_disabled", "status": 200, "body": {"@odata.count": 5}},
            {"id": "users_member", "status": 200, "body": {"@odata.count": 40}},
            {"id": "users_guest", "status": 200, "body": {"@odata.count": 10}}
        ]
        
        service = DirectoryService(mock_client)
        # Should not raise exception
        res = service.get_directory_telemetry()
        
        self.assertEqual(res["organization"][0]["displayName"], "Test Org")
        self.assertEqual(res["domains"][0]["id"], "test.com")
        # should fall back to empty list on failure

        self.assertEqual(res["group_counts"]["total"], 10)

    @patch('core.graph.directory.GraphClient')
    def test_get_directory_telemetry_with_federated_domains(self, MockGraphClient):
        mock_client = MockGraphClient()
        mock_client.get_active_token.return_value = {"token": "mock_token"}
        mock_session = MagicMock()
        mock_client.get_session.return_value = mock_session
        
        mock_url_invoker = MagicMock()
        mock_client.url_invoker = mock_url_invoker
        
        mock_url_invoker.invoke.return_value = [
            {
                "id": "organization",
                "status": 200,
                "body": {"value": [{"displayName": "Test Org", "tenantType": "test"}]}
            },
            {
                "id": "domains",
                "status": 200,
                "body": {"value": [
                    {"id": "test.com", "authenticationType": "Managed", "isDefault": True},
                    {"id": "fed.com", "authenticationType": "Federated", "isDefault": False}
                ]}
            },
            {"id": "total", "status": 200, "body": {"@odata.count": 10}},
            {"id": "security", "status": 200, "body": {"@odata.count": 2}},
            {"id": "distribution", "status": 200, "body": {"@odata.count": 3}},
            {"id": "mail_enabled_security", "status": 200, "body": {"@odata.count": 1}},
            {"id": "m365", "status": 200, "body": {"@odata.count": 4}},
            {"id": "dynamic", "status": 200, "body": {"@odata.count": 0}},
            {"id": "users_total", "status": 200, "body": {"@odata.count": 50}},
            {"id": "users_enabled", "status": 200, "body": {"@odata.count": 45}},
            {"id": "users_disabled", "status": 200, "body": {"@odata.count": 5}},
            {"id": "users_member", "status": 200, "body": {"@odata.count": 40}},
            {"id": "users_guest", "status": 200, "body": {"@odata.count": 10}}
        ]
        
        # Mock federation configuration response
        mock_fed_resp = MagicMock()
        mock_fed_resp.status_code = 200
        mock_fed_resp.json.return_value = {
            "value": [
                {
                    "displayName": "Okta IdP",
                    "issuerUri": "http://okta.com/issuer"
                }
            ]
        }
        mock_session.get.return_value = mock_fed_resp
        
        service = DirectoryService(mock_client)
        res = service.get_directory_telemetry()
        
        self.assertEqual(res["domains"][0]["id"], "test.com")
        self.assertEqual(res["domains"][1]["id"], "fed.com")
        self.assertEqual(res["domains"][1]["federationDisplayName"], "Okta IdP")
        self.assertEqual(res["domains"][1]["federationIssuerUri"], "http://okta.com/issuer")
        
        # Verify federation config was queried exactly once
        mock_session.get.assert_called_once_with(
            "https://graph.microsoft.com/v1.0/domains/fed.com/federationConfiguration",
            headers={"Authorization": "Bearer mock_token", "Accept": "application/json"},
            timeout=30.0
        )

    @patch('core.graph.directory.GraphClient')
    def test_fetch_user_creation_logs_success(self, MockGraphClient):
        mock_client = MockGraphClient()
        mock_client.get_active_token.return_value = {"token": "mock_token"}
        mock_session = MagicMock()
        mock_client.get_session.return_value = mock_session
        
        # Mock responses for the two endpoints
        mock_add_resp = MagicMock()
        mock_add_resp.status_code = 200
        mock_add_resp.json.return_value = {
            "value": [
                {
                    "activityDisplayName": "Add user",
                    "initiatedBy": {
                        "user": {
                            "displayName": "Admin User",
                            "userPrincipalName": "admin@test.com",
                            "ipAddress": "192.168.1.1"
                        }
                    }
                }
            ]
        }
        
        mock_del_resp = MagicMock()
        mock_del_resp.status_code = 200
        mock_del_resp.json.return_value = {
            "value": [
                {
                    "activityDisplayName": "Delete user",
                    "initiatedBy": {
                        "app": {
                            "displayName": "Sync App",
                            "appId": "sync-app-id"
                        }
                    }
                }
            ]
        }
        
        mock_session.get.side_effect = [mock_add_resp, mock_del_resp]
        
        import tempfile
        import os
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "test_creation_logs.csv")
            
            service = DirectoryService(mock_client)
            captured_pages = []
            def callback(page):
                captured_pages.extend(page)
                
            service.fetch_user_creation_logs(csv_path, max_rows=50, on_page_callback=callback)
            
            # Verify CSV content
            self.assertTrue(os.path.exists(csv_path))
            with open(csv_path, 'r', encoding='utf-8') as f:
                content = f.read().splitlines()
                
            self.assertEqual(len(content), 3) # Header + 2 rows
            self.assertEqual(content[0], "Activity,Initiated By")
            
            import json
            expected_initiated_by_add = json.dumps({
                "user": {
                    "displayName": "Admin User",
                    "userPrincipalName": "admin@test.com",
                    "ipAddress": "192.168.1.1"
                }
            })
            expected_initiated_by_del = json.dumps({
                "app": {
                    "displayName": "Sync App",
                    "appId": "sync-app-id"
                }
            })
            
            self.assertEqual(content[1], f"Add user,\"{expected_initiated_by_add.replace('\"', '\"\"')}\"")
            self.assertEqual(content[2], f"Delete user,\"{expected_initiated_by_del.replace('\"', '\"\"')}\"")
            
            self.assertEqual(len(captured_pages), 2)
            self.assertEqual(captured_pages[0]["activity"], "Add user")
            self.assertEqual(captured_pages[1]["activity"], "Delete user")

    @patch('core.graph.directory.GraphClient')
    def test_fetch_user_creation_logs_permission_denied(self, MockGraphClient):
        mock_client = MockGraphClient()
        mock_client.get_active_token.return_value = {"token": "mock_token"}
        mock_session = MagicMock()
        mock_client.get_session.return_value = mock_session
        
        # Mock 403 Forbidden response
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_resp.text = "Forbidden"
        mock_session.get.return_value = mock_resp
        
        import tempfile
        import os
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "test_creation_logs_err.csv")
            
            service = DirectoryService(mock_client)
            captured_pages = []
            def callback(page):
                captured_pages.extend(page)
                
            service.fetch_user_creation_logs(csv_path, max_rows=50, on_page_callback=callback)
            
            # Verify CSV content contains ERROR row
            self.assertTrue(os.path.exists(csv_path))
            with open(csv_path, 'r', encoding='utf-8') as f:
                content = f.read().splitlines()
                
            self.assertEqual(len(content), 2) # Header + ERROR row
            self.assertEqual(content[0], "Activity,Initiated By")
            self.assertEqual(content[1], "ERROR,AuditLog.Read.All permission required. Please ensure this permission is granted to the application registration in Microsoft Entra ID.")
            
            self.assertEqual(len(captured_pages), 1)
            self.assertEqual(captured_pages[0]["activity"], "ERROR")
            self.assertTrue("permission required" in captured_pages[0]["initiatedBy"])

    @patch('core.graph.directory.GraphClient')
    def test_fetch_provisioning_logs_success(self, MockGraphClient):
        mock_client = MockGraphClient()
        mock_client.get_active_token.return_value = {"token": "mock_token"}
        mock_session = MagicMock()
        mock_client.get_session.return_value = mock_session
        
        # Mock Graph API response for provisioning logs
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "value": [
                {
                    "initiatedBy": {"user": {"displayName": "AD Sync"}},
                    "provisioningAction": "create",
                    "provisioningSteps": [{"name": "Step 1"}],
                    "servicePrincipal": {"displayName": "Azure AD Connect"},
                    "sourceSystem": {"displayName": "On-Premises AD"},
                    "targetSystem": {"displayName": "Entra ID"},
                    "tenantId": "tenant-123",
                    "provisioningStatusInfo": {"status": "success"}
                }
            ]
        }
        mock_session.get.return_value = mock_resp
        
        import tempfile
        import os
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "test_provisioning_logs.csv")
            
            service = DirectoryService(mock_client)
            captured_pages = []
            def callback(page):
                captured_pages.extend(page)
                
            service.fetch_provisioning_logs(csv_path, max_rows=50, on_page_callback=callback)
            
            self.assertTrue(os.path.exists(csv_path))
            with open(csv_path, 'r', encoding='utf-8') as f:
                content = f.read().splitlines()
                
            self.assertEqual(len(content), 2) # Header + 1 row
            self.assertEqual(content[0], "initiatedBy,provisioningAction,provisioningSteps,servicePrincipal,sourceSystem,targetSystem,tenantId,provisioningStatusInfo")
            
            self.assertEqual(len(captured_pages), 1)
            self.assertEqual(captured_pages[0]["provisioningAction"], "create")
            self.assertEqual(captured_pages[0]["tenantId"], "tenant-123")

    @patch('core.graph.directory.GraphClient')
    def test_fetch_provisioning_logs_permission_denied(self, MockGraphClient):
        mock_client = MockGraphClient()
        mock_client.get_active_token.return_value = {"token": "mock_token"}
        mock_session = MagicMock()
        mock_client.get_session.return_value = mock_session
        
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_resp.text = "Forbidden"
        mock_session.get.return_value = mock_resp
        
        import tempfile
        import os
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "test_provisioning_logs_err.csv")
            
            service = DirectoryService(mock_client)
            captured_pages = []
            def callback(page):
                captured_pages.extend(page)
                
            service.fetch_provisioning_logs(csv_path, max_rows=50, on_page_callback=callback)
            
            self.assertTrue(os.path.exists(csv_path))
            with open(csv_path, 'r', encoding='utf-8') as f:
                content = f.read().splitlines()
                
            self.assertEqual(len(content), 2)
            self.assertEqual(content[1].split(',')[0], "ERROR")
            self.assertTrue("permission required" in content[1])
            
            self.assertEqual(len(captured_pages), 1)
            self.assertEqual(captured_pages[0]["initiatedBy"], "ERROR")

if __name__ == '__main__':
    unittest.main()
