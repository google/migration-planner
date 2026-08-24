import requests
import json

token = "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"
url = "https://api.github.com/repos/google/migration-planner/pulls"

headers = {
    "Authorization": f"token {token}",
    "Accept": "application/vnd.github.v3+json"
}

data = {
    "title": "docs: add setup and usage instructions for Deal Assistant",
    "body": "This PR updates the README.md to be more user-friendly for Deal Assistant. It explicitly adds instructions on:\n1. The specific Graph API Application permissions required.\n2. How to upload the generated certificate to the Entra ID App Registration.\n3. How to assign the 'Compliance Administrator' and 'Compliance Data Administrator' roles via the Entra ID Roles page.",
    "head": "srishti-negi:feat/update-readme-instructions",
    "base": "main"
}

response = requests.post(url, headers=headers, data=json.dumps(data))
print(f"Status: {response.status_code}")
if response.status_code in [201, 200]:
    print("Success:", response.json().get("html_url"))
else:
    print("Error:", response.text)
