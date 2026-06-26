import os
import logging
from core.graph.client import GraphClient
from core.graph.reports import ReportsService

logger = logging.getLogger(__name__)

def run_msteams_pipeline(client_id, client_secret, tenant_id) -> str:
    """Pipeline specifically for MsTeams Overview telemetry data collection."""
    logger.info("Starting MsTeams Overview Telemetry Pipeline...")
    
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=2,
        retries=5,
        backoff=2
    )
    client.authenticate()
    service = ReportsService(client)
    
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    telemetry_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(script_dir))), "telemetry")
    reports_dir = os.path.join(telemetry_dir, "reports", f"{tenant_id}_{client_id}")
    
    output_filename = "msteams_activity.csv"
    temp_filename = output_filename + ".tmp"
    url = "https://graph.microsoft.com/v1.0/reports/getTeamsTeamActivityDetail(period='D180')"
    
    service.download_report(url, temp_filename, reports_dir)
    
    final_path = os.path.join(reports_dir, output_filename)
    tmp_path = os.path.join(reports_dir, temp_filename)
    if os.path.exists(tmp_path):
        os.replace(tmp_path, final_path)
    
    logger.info("MsTeams Overview Telemetry Pipeline completed successfully.")
    client.close()
    return final_path
