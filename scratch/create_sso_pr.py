import requests
import json

token = "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"
url = "https://api.github.com/repos/google/migration-planner/pulls"

headers = {
    "Authorization": f"token {token}",
    "Accept": "application/vnd.github.v3+json"
}

data = {
    "title": "feat: add Service Principals SSO modes metric to Data Security & Governance",
    "body": "This PR introduces a dedicated metric aggregation section for listing the SSO (Single Sign-On) modes of Service Principals (Enterprise Applications) underneath the Data Security & Governance telemetry module.\n\nIt natively queries the Microsoft Graph API `/servicePrincipals` endpoint using the `DirectoryService` batch system. The UI presents clean counts of SAML, OIDC, Password, and Null application modes natively, and integrates with the unified M365 telemetry exporter.",
    "head": "feat/service-principals-sso",
    "base": "splash-one"
}

response = requests.post(url, headers=headers, data=json.dumps(data))
print(f"Status: {response.status_code}")
if response.status_code in [201, 200]:
    print("Success:", response.json().get("html_url"))
else:
    print("Error:", response.text)
