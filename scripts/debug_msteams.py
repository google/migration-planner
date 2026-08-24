import sys
import os

# Add the parent directory to the python path so we can import core modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.graph.client import GraphClient
from core.graph.reports import ReportsService

def debug_msteams(tenant_id, client_id, client_secret):
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret
    )
    
    print("Authenticating...")
    client.authenticate()
    
    service = ReportsService(client)
    
    print("Fetching MsTeams Activity Detail report...")
    url = "https://graph.microsoft.com/v1.0/reports/getTeamsTeamActivityDetail(period='D180')"
    
    import tempfile
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_filename = "debug_msteams.csv"
        try:
            service.download_report(url, temp_filename, temp_dir)
            file_path = os.path.join(temp_dir, temp_filename)
            
            print("\n--- REPORT FETCHED SUCESSFULLY ---\n")
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
                print(content)
                
            print("\n--- END OF REPORT ---")
        except Exception as e:
            print(f"Error fetching report: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python3 debug_msteams.py <tenant_id> <client_id> <client_secret>")
        sys.exit(1)
        
    debug_msteams(sys.argv[1], sys.argv[2], sys.argv[3])
