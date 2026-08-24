import requests
import json

token = "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"
url = "https://api.github.com/repos/google/migration-planner/pulls"

headers = {
    "Authorization": f"token {token}",
    "Accept": "application/vnd.github.v3+json"
}

data = {
    "title": "feat: add Sensitive Information Types to DLP governance section",
    "body": "This PR introduces a dedicated section for listing Sensitive Information Types (SITs) underneath the DLP Governance section.\n\nIncludes an asynchronous PowerShell thread to fetch SIT metadata using `Connect-IPPSSession`, and dedicated pagination and CSV export UI mechanisms.",
    "head": "feat/dlp-sit",
    "base": "splash-one"
}

response = requests.post(url, headers=headers, data=json.dumps(data))
print(f"Status: {response.status_code}")
if response.status_code in [201, 200]:
    print("Success:", response.json().get("html_url"))
else:
    print("Error:", response.text)
