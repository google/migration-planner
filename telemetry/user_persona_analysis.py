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

"""Modular User Persona Analysis dataset extraction and processing pipelines."""

import os
import sqlite3
import logging
import pandas as pd
from core.graph.client import GraphClient
from core.graph.reports import ReportsService

# Bind to the async logger initialized in m365_telemetry.py
logger = logging.getLogger("M365TelemetryAsyncLogger.UserPersonaAnalysis")


def run_user_persona_pipeline(tenant_id: str, client_id: str, client_secret: str, output_csv_path: str, reports_dir: str = None) -> str:
    """Runs the full pipeline to download M365 reports and generate the user activity dataset.
    
    Returns the path to the generated CSV dataset.
    """
    if not reports_dir:
        reports_dir = os.path.join("telemetry", "reports", f"{tenant_id}_{client_id}")
    
    os.makedirs(reports_dir, exist_ok=True)
    
    # 1. Authenticate and initialize Graph clients
    logger.info("Initializing Graph connection for User Persona Analysis...")
    client = GraphClient(
        tenant_id=tenant_id,
        client_ids=client_id,
        client_secrets=client_secret,
        concurrency=2,
        retries=5,
        backoff=2
    )
    client.authenticate()
    reports_service = ReportsService(client)
    
    # 2. Define reports to download
    reports = [
        ("https://graph.microsoft.com/v1.0/reports/getOffice365ActiveUserDetail(period='D180')", "Office365ActiveUserDetail(180d).csv"),
        ("https://graph.microsoft.com/v1.0/reports/getEmailActivityUserDetail(period='D180')", "EmailActivityUserDetail(180d).csv"),
        ("https://graph.microsoft.com/v1.0/reports/getTeamsUserActivityUserDetail(period='D180')", "TeamsUserActivityUserDetail(180d).csv"),
        ("https://graph.microsoft.com/v1.0/reports/getOneDriveUsageAccountDetail(period='D180')", "OneDriveUsageAccountDetail(180d).csv"),
        ("https://graph.microsoft.com/v1.0/reports/getSharePointActivityUserDetail(period='D180')", "SharePointActivityUserDetail(180d).csv")
    ]
    
    logger.info("Downloading reports in batch concurrently...")
    reports_service.download_reports_batch(reports, reports_dir)
    logger.info("Reports download complete. Starting dataset generation...")
    
    # 3. Process and merge reports to create the dataset
    generate_user_activity_dataset(reports_dir, output_csv_path)
    
    logger.info(f"User Persona Analysis dataset successfully generated at: {output_csv_path}")
    return output_csv_path


def generate_user_activity_dataset(reports_dir: str, output_csv_path: str) -> None:
    """Streams and joins downloaded user reports via a temporary SQLite DB to preserve memory."""
    temp_db_path = os.path.join(reports_dir, "temp_persona_data.db")
    
    # Clean up any leftover DB from previous runs
    if os.path.exists(temp_db_path):
        try:
            os.remove(temp_db_path)
        except Exception:
            pass
        
    conn = sqlite3.connect(temp_db_path)
    cursor = conn.cursor()
    
    # Define mapping of CSV files to database tables
    report_files = {
        "Office365ActiveUserDetail(180d).csv": "office365_active_users",
        "EmailActivityUserDetail(180d).csv": "email_activity",
        "TeamsUserActivityUserDetail(180d).csv": "teams_activity",
        "OneDriveUsageAccountDetail(180d).csv": "onedrive_usage",
        "SharePointActivityUserDetail(180d).csv": "sharepoint_activity"
    }
    
    try:
        # 1. Load CSVs into SQLite tables in chunks
        for csv_name, table_name in report_files.items():
            csv_path = os.path.join(reports_dir, csv_name)
            if not os.path.exists(csv_path):
                logger.warning(f"Report file {csv_name} not found. Skipping table {table_name}.")
                continue
                
            logger.info(f"Importing {csv_name} into SQLite table {table_name}...")
            
            for chunk in pd.read_csv(csv_path, chunksize=20000, encoding="utf-8-sig"):
                # Clean column headers
                chunk.columns = chunk.columns.str.strip()
                chunk.to_sql(table_name, conn, if_exists="append", index=False)
                
        # Ensure base table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='office365_active_users';")
        if not cursor.fetchone():
            raise FileNotFoundError("Base table 'office365_active_users' could not be loaded because the required CSV is missing.")

        # 2. Create indices to optimize joins
        logger.info("Creating database indices for efficient join operations...")
        index_queries = [
            "CREATE INDEX IF NOT EXISTS idx_base_upn ON office365_active_users (`User Principal Name`);",
            "CREATE INDEX IF NOT EXISTS idx_email_upn ON email_activity (`User Principal Name`);",
            "CREATE INDEX IF NOT EXISTS idx_teams_upn ON teams_activity (`User Principal Name`);",
            "CREATE INDEX IF NOT EXISTS idx_onedrive_upn ON onedrive_usage (`Owner Principal Name`);",
            "CREATE INDEX IF NOT EXISTS idx_sp_upn ON sharepoint_activity (`User Principal Name`);"
        ]
        for query in index_queries:
            try:
                cursor.execute(query)
            except sqlite3.OperationalError as e:
                logger.warning(f"Failed to create index. Query: {query}. Error: {e}")
                
        conn.commit()
        
        # 3. Query combined fields and stream the results to CSV
        join_query = """
        SELECT 
            b.`User Principal Name` AS `User Principal Name`,
            b.`Has Exchange License` AS `Has Exchange License`,
            b.`Has OneDrive License` AS `Has OneDrive License`,
            b.`Has SharePoint License` AS `Has SharePoint License`,
            b.`Has Teams License` AS `Has Teams License`,
            e.`Send Count` AS `Email_Sends`,
            e.`Meeting Created Count` AS `Meetings_Organized_Via_Email`,
            t.`Private Chat Message Count` AS `Teams_Private_Chats`,
            t.`Meetings Attended Count` AS `Teams_Meetings_Attended`,
            o.`Active File Count` AS `OneDrive_Active_Files`,
            o.`Storage Used (Byte)` AS `OneDrive_Storage_Bytes`,
            s.`Viewed Or Edited File Count` AS `SharePoint_Files_Edited`,
            s.`Shared Internally File Count` AS `SharePoint_Shared_Internally`,
            s.`Shared Externally File Count` AS `SharePoint_Shared_Externally`
        FROM office365_active_users b
        LEFT JOIN email_activity e ON b.`User Principal Name` = e.`User Principal Name`
        LEFT JOIN teams_activity t ON b.`User Principal Name` = t.`User Principal Name`
        LEFT JOIN onedrive_usage o ON b.`User Principal Name` = o.`Owner Principal Name`
        LEFT JOIN sharepoint_activity s ON b.`User Principal Name` = s.`User Principal Name`
        """
        
        logger.info("Executing database join and streaming dataset to CSV...")
        
        # Ensure parent output directory exists
        output_dir = os.path.dirname(output_csv_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            
        # Delete output file if it already exists from a previous run
        if os.path.exists(output_csv_path):
            os.remove(output_csv_path)
            
        first_chunk = True
        
        def determine_access_profile(row):
            def is_truthy(val):
                if val is None:
                    return False
                if isinstance(val, bool):
                    return val
                s = str(val).strip().lower()
                return s in ['true', '1', '1.0', 'yes']

            apps = []
            if is_truthy(row.get('Has Exchange License')): apps.append('Email')
            if is_truthy(row.get('Has Teams License')): apps.append('Teams')
            if is_truthy(row.get('Has OneDrive License')) or is_truthy(row.get('Has SharePoint License')): apps.append('Files')
            
            if len(apps) == 3: return "Full Suite Access"
            if len(apps) == 0: return "Restricted / Unlicensed"
            return "Partial Access: " + " + ".join(apps)


        # Read chunks from SQLite database
        for chunk in pd.read_sql_query(join_query, conn, chunksize=20000):
            # Clean whitespaces in column values or headers if any
            chunk.columns = chunk.columns.str.strip()
            
            # Calculate App Access Profile
            chunk['App_Access_Profile'] = chunk.apply(determine_access_profile, axis=1)
            
            # Drop raw boolean license columns
            license_cols = [c for c in chunk.columns if c.startswith('Has ')]
            chunk = chunk.drop(columns=license_cols, errors='ignore')
            
            # Convert OneDrive Bytes to GB
            if 'OneDrive_Storage_Bytes' in chunk.columns:
                bytes_col = pd.to_numeric(chunk['OneDrive_Storage_Bytes'], errors='coerce').fillna(0)
                chunk['OneDrive_Storage_GB'] = (bytes_col / (1024 ** 3)).round(2)
                chunk = chunk.drop(columns=['OneDrive_Storage_Bytes'])
                
            # Fill missing values (NaN) with 0 for numerical columns
            numerical_cols = chunk.select_dtypes(include=['float64', 'int64']).columns
            chunk[numerical_cols] = chunk[numerical_cols].fillna(0)
            
            # Reorder columns: put App_Access_Profile next to User Principal Name
            cols = chunk.columns.tolist()
            if 'App_Access_Profile' in cols and 'User Principal Name' in cols:
                cols.insert(1, cols.pop(cols.index('App_Access_Profile')))
                chunk = chunk[cols]
                
            # Append chunk to output CSV
            chunk.to_csv(output_csv_path, mode='a', index=False, header=first_chunk)
            first_chunk = False
            
    finally:
        conn.close()
        # Delete temporary SQLite DB file
        if os.path.exists(temp_db_path):
            try:
                os.remove(temp_db_path)
                logger.info("Temporary database removed successfully.")
            except Exception as e:
                logger.warning(f"Could not remove temporary database file: {e}")
