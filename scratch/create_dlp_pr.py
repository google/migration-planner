import requests
import json

token = "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"
url = "https://api.github.com/repos/google/migration-planner/pulls"

headers = {
    "Authorization": f"token {token}",
    "Accept": "application/vnd.github.v3+json"
}

data = {
    "title": "feat: add support for listing DLP policies via PowerShell",
    "body": "This PR adds support for fetching and displaying Microsoft Purview Data Loss Prevention (DLP) Policies using the `Get-DlpCompliancePolicy` cmdlet.\n\nIt introduces a new PowerShell service (`get_dlp_policies.ps1`) to parse and join compliance rules, and integrates these findings into the Data Security & Governance telemetry module with a paginated data table, loading states, and CSV export functionality.",
    "head": "srishti-negi:feat/dlp-policies",
    "base": "splash-one"
}

response = requests.post(url, headers=headers, data=json.dumps(data))
print(f"Status: {response.status_code}")
if response.status_code in [201, 200]:
    print("Success:", response.json().get("html_url"))
else:
    print("Error:", response.text)
