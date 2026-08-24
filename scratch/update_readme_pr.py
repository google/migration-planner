import requests
import json

token = "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"
url = "https://api.github.com/repos/google/migration-planner/pulls/68"

headers = {
    "Authorization": f"token {token}",
    "Accept": "application/vnd.github.v3+json"
}

data = {
    "base": "splash-one"
}

response = requests.patch(url, headers=headers, data=json.dumps(data))
print(f"Status: {response.status_code}")
if response.status_code in [200, 201]:
    print("Success:", response.json().get("html_url"))
else:
    print("Error:", response.text)
