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

"""Backward compatibility facade for Data Security & Governance telemetry."""

import logging
from core.graph.security.sensitivity_labels import run_sensitivity_labels_pipeline
from core.graph.security.retention_policies import run_retention_policies_pipeline
from core.graph.security.dlp_policies import run_dlp_policies_pipeline
from core.graph.security.sensitive_info_types import run_sensitive_info_types_pipeline
from core.graph.security.authentication import run_authentication_pipeline
from core.graph.security.service_principals_sso import run_service_principals_sso_pipeline
from telemetry.security import DataSecurityGovernanceFrame

logger = logging.getLogger(__name__)

def run_security_governance_pipeline(client_id, client_secret, tenant_id) -> dict:
    """Legacy pipeline helper."""
    labels = None
    labels_error = None
    try:
        from core.graph.client import GraphClient
        from core.graph.security import SecurityService
        c = GraphClient(tenant_id=tenant_id, client_ids=client_id, client_secrets=client_secret, concurrency=1)
        c.authenticate()
        svc = SecurityService(c)
        labels = svc.fetch_sensitivity_labels()
        c.close()
    except Exception as e:
        labels_error = str(e)

    policies = None
    policies_error = None
    try:
        policies = run_retention_policies_pipeline(client_id, client_secret, tenant_id)
    except Exception as e:
        policies_error = str(e)

    return {
        "labels": labels,
        "labels_error": labels_error,
        "policies": policies,
        "policies_error": policies_error
    }

def fetch_sensitivity_labels_data(client_id, client_secret, tenant_id, csv_path=None, on_page_callback=None, is_cancelled_callback=None) -> dict:
    try:
        run_sensitivity_labels_pipeline(client_id, client_secret, tenant_id, csv_path, on_page_callback, is_cancelled_callback)
        return {"labels": [], "error": None}
    except Exception as e:
        return {"labels": None, "error": str(e)}

def fetch_service_principals_sso_data(client_id, client_secret, tenant_id, csv_path=None, on_page_callback=None, is_cancelled_callback=None) -> dict:
    try:
        run_service_principals_sso_pipeline(client_id, client_secret, tenant_id, csv_path, on_page_callback, is_cancelled_callback)
        return {"sso": [], "error": None}
    except Exception as e:
        return {"sso": None, "error": str(e)}

def fetch_retention_policies_data(client_id, client_secret, tenant_id) -> dict:
    try:
        policies = run_retention_policies_pipeline(client_id, client_secret, tenant_id)
        return {"policies": policies, "error": None}
    except Exception as e:
        return {"policies": None, "error": str(e)}

def fetch_dlp_policies_data(client_id, client_secret, tenant_id) -> dict:
    try:
        policies = run_dlp_policies_pipeline(client_id, client_secret, tenant_id)
        return {"policies": policies, "error": None}
    except Exception as e:
        return {"policies": None, "error": str(e)}

def fetch_sensitive_info_types_data(client_id, client_secret, tenant_id) -> dict:
    try:
        data = run_sensitive_info_types_pipeline(client_id, client_secret, tenant_id)
        return {"sit_data": data, "error": None}
    except Exception as e:
        return {"sit_data": None, "error": str(e)}

def fetch_authentication_data(client_id, client_secret, tenant_id, csv_path=None, on_page_callback=None, is_cancelled_callback=None) -> dict:
    try:
        run_authentication_pipeline(client_id, client_secret, tenant_id, csv_path, on_page_callback, is_cancelled_callback)
        return {"auth_data": {"ca_policies": []}, "error": None}
    except Exception as e:
        return {"auth_data": None, "error": str(e)}
