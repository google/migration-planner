import sys, os, json
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
from core.graph.client import GraphClient

CLIENT_ID = "16e96d46-0e64-481a-872f-c538c1a99311"
CLIENT_SECRET = "j_L8Q~DsaImyAmDqU4u~kjz~43b15N1PzllASbNW"
TENANT_ID = "a8a95e4b-10a6-4526-a2cb-db7682eba2e6"

def run_debug():
    client = GraphClient(tenant_id=TENANT_ID, client_ids=CLIENT_ID, client_secrets=CLIENT_SECRET)
    client.authenticate()
    
    token_slot = client.get_active_token()
    session = client.get_session()
    headers = {
        "Authorization": f"Bearer {token_slot['token']}",
        "ConsistencyLevel": "eventual"
    }
    
    principals = []
    url = "https://graph.microsoft.com/v1.0/servicePrincipals?$select=id,appId,displayName,preferredSingleSignOnMode"
    
    try:
        while url:
            resp = session.get(url, headers=headers, timeout=30.0)
            if not resp.ok:
                print(f"Error {resp.status_code}: {resp.text}")
                break
            data = resp.json()
            principals.extend(data.get("value", []))
            url = data.get("@odata.nextLink")
    finally:
        client.release_token(token_slot)
            
    print(f"Total: {len(principals)}")
    saml_apps = [p for p in principals if p.get("preferredSingleSignOnMode") == "saml"]
    oidc_apps = [p for p in principals if p.get("preferredSingleSignOnMode") == "oidc"]
    print(f"SAML Apps: {len(saml_apps)}")
    print(f"OIDC Apps: {len(oidc_apps)}")
    if saml_apps:
        print("SAML Example:", json.dumps(saml_apps[0], indent=2))
    elif oidc_apps:
        print("OIDC Example:", json.dumps(oidc_apps[0], indent=2))
    elif principals:
        print("First Example:", json.dumps(principals[0], indent=2))

if __name__ == "__main__":
    run_debug()
