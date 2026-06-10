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
import json
import sqlite3
import logging
import pandas as pd
from google import genai
from google.genai import types
from core.graph.client import GraphClient
from core.graph.reports import ReportsService

# Bind to the async logger initialized in m365_telemetry.py
logger = logging.getLogger("M365TelemetryAsyncLogger.UserPersonaAnalysis")


def run_user_persona_pipeline(tenant_id: str, client_id: str, client_secret: str, gemini_api_key: str, output_csv_path: str, reports_dir: str = None, status_callback=None) -> dict:
    """Runs the full pipeline to download M365 reports, generate user activity dataset,
    and query Gemini to build and classify user personas.
    
    Returns a dictionary containing the personas definition and user assignments.
    """
    if not reports_dir:
        reports_dir = os.path.join("telemetry", "reports", f"{tenant_id}_{client_id}")
    
    os.makedirs(reports_dir, exist_ok=True)
    
    # 1. Download reports & aggregate data
    if status_callback:
        status_callback("Fetching Reports")
        
    # Authenticate and initialize Graph clients
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
    
    reports = [
        ("https://graph.microsoft.com/v1.0/reports/getOffice365ActiveUserDetail(period='D180')", "Office365ActiveUserDetail(180d).csv"),
        ("https://graph.microsoft.com/v1.0/reports/getEmailActivityUserDetail(period='D180')", "EmailActivityUserDetail(180d).csv"),
        ("https://graph.microsoft.com/v1.0/reports/getTeamsUserActivityUserDetail(period='D180')", "TeamsUserActivityUserDetail(180d).csv"),
        ("https://graph.microsoft.com/v1.0/reports/getOneDriveUsageAccountDetail(period='D180')", "OneDriveUsageAccountDetail(180d).csv"),
        ("https://graph.microsoft.com/v1.0/reports/getSharePointActivityUserDetail(period='D180')", "SharePointActivityUserDetail(180d).csv")
    ]
    
    logger.info("Downloading reports in batch concurrently...")
    reports_service.download_reports_batch(reports, reports_dir)
    
    if status_callback:
        status_callback("Aggregating Data")
        
    logger.info("Reports download complete. Starting dataset generation...")
    generate_user_activity_dataset(reports_dir, output_csv_path)
    
    # 2. Call Gemini to define personas
    if status_callback:
        status_callback("Generating insights using Gemini")
        
    logger.info("Calling Gemini API to define personas...")
    personas_response = generate_personas_from_dataset(gemini_api_key, output_csv_path)
    
    # 3. Classify users in Python
    logger.info("Classifying users to generated personas in Python...")
    df_users = pd.read_csv(output_csv_path)
    personas_list = personas_response.get("personas", [])
    
    assigned_persona_ids = classify_users_to_personas(df_users, personas_list)
    df_users['Assigned_Persona_ID'] = assigned_persona_ids
    
    # Map persona IDs to Titles
    persona_titles = {p["id"]: p["title"] for p in personas_list}
    df_users['Assigned_Persona_Title'] = df_users['Assigned_Persona_ID'].map(lambda p_id: persona_titles.get(p_id, "Unknown"))
    
    # Save the updated dataset containing persona assignments
    df_users.to_csv(output_csv_path, index=False)
    
    return {
        "personas": personas_list,
        "dataset_path": output_csv_path
    }


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


def generate_personas_from_dataset(api_key: str, dataset_path: str) -> dict:
    """Invokes Gemini SDK to analyze M365 telemetry data and identify user personas."""
    if not os.path.exists(dataset_path):
        raise FileNotFoundError(f"Dataset not found at: {dataset_path}")
        
    # Read the entire CSV data
    with open(dataset_path, "r", encoding="utf-8") as f:
        csv_data = f.read()
    
    prompt = f"""You are an expert M365 telemetry and data analyst.
Below is the complete M365 user activity dataset (180 days) for all users in a tenant.

CSV Dataset:
{csv_data}

Based on this M365 dataset, analyze the behavioral patterns and define 3 to 5 distinct user personas that represent the typical usage behavior of the users in this tenant.

It is CRITICAL to include an 'Inactive or Idle Accounts' persona (with all metrics set to 'low') if there are accounts in the dataset with zero or near-zero activity across all telemetry counts. These represent inactive, unlicensed, or archive accounts.

For each persona, output:
1. A unique ID (e.g., "email_collaborator").
2. A title/headline (e.g., "Email Collaborator").
3. A representative emoji/icon (e.g., "📧").
4. A brief description of this persona.
5. 2 to 4 behavior patterns (bullet points explaining their characteristics).
6. A metric profile specifying the relative usage of each behavior category as "high", "medium", or "low".

Output your response strictly as a JSON object matching this schema:
{{
  "personas": [
    {{
      "id": "email_collaborator",
      "title": "Email Collaborator",
      "emoji": "📧",
      "description": "Users who heavily rely on Email communications...",
      "behavior_patterns": [
        "High volume of email sends",
        "Active calendar organizer"
      ],
      "metrics": {{
        "email_sends": "high",
        "meetings_organized": "medium",
        "teams_chats": "low",
        "teams_meetings": "low",
        "onedrive_files": "low",
        "sharepoint_edits": "low",
        "sharepoint_shared_internal": "low",
        "sharepoint_shared_external": "low",
        "onedrive_storage": "low"
      }}
    }}
  ]
}}

Do not include any other markdown formatting, code block fences, or text outside the JSON."""

    # Initialize the genai Client directly with the API key
    client = genai.Client(api_key=api_key)
    
    models = ["gemini-3.5-flash", "gemini-3.1-flash"]
    response = None
    last_err = None
    
    for model_name in models:
        logger.info(f"Attempting Gemini generation with SDK model: {model_name}...")
        try:
            res = client.models.generate_content(
                model=model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json"
                )
            )
            response = res
            break
        except Exception as e:
            logger.warning(f"Failed calling Gemini model {model_name} via SDK: {e}")
            last_err = e
            
    if response is None:
        if last_err:
            raise last_err
        raise ValueError("Failed to generate personas. No Gemini SDK models responded successfully.")
        
    try:
        candidate_text = response.text
        return json.loads(candidate_text)
    except (KeyError, IndexError, ValueError) as e:
        logger.error(f"Failed to parse Gemini response: {e}. Raw response: {response}")
        raise ValueError(f"Failed to parse Gemini response: {e}")


def classify_users_to_personas(df_users: pd.DataFrame, personas_data: list) -> list:
    """Clusts and maps M365 users to defined personas in Python using distance calculations."""
    cols_to_normalize = [
        'Email_Sends', 'Meetings_Organized_Via_Email', 
        'Teams_Private_Chats', 'Teams_Meetings_Attended', 
        'OneDrive_Active_Files', 'SharePoint_Files_Edited', 
        'SharePoint_Shared_Internally', 'SharePoint_Shared_Externally', 
        'OneDrive_Storage_GB'
    ]
    
    # Calculate ranks (0 to 1) for each column in the dataframe
    ranks_df = pd.DataFrame(index=df_users.index)
    for col in cols_to_normalize:
        if col in df_users.columns:
            if df_users[col].max() == df_users[col].min():
                # If all values are 0, rank is 0.0, else 0.5
                ranks_df[col] = 0.0 if df_users[col].max() == 0 else 0.5
            else:
                # Calculate percentile rank
                ranks_df[col] = df_users[col].rank(pct=True)
                # Force absolute 0 values to rank 0.0 so they don't get inflated due to ties at 0
                ranks_df.loc[df_users[col] == 0, col] = 0.0
        else:
            ranks_df[col] = 0.0

    # Map profile levels to numeric values
    level_map = {
        "high": 0.8,
        "medium": 0.4,
        "low": 0.1
    }

    # Define normalized mapping of persona metrics to df columns
    norm_metric_keys = {
        "emailsends": "Email_Sends",
        "meetingsorganized": "Meetings_Organized_Via_Email",
        "teamschats": "Teams_Private_Chats",
        "teamsmeetings": "Teams_Meetings_Attended",
        "onedrive_files": "OneDrive_Active_Files",  # backup keys
        "onedrivefiles": "OneDrive_Active_Files",
        "sharepointedits": "SharePoint_Files_Edited",
        "sharepointsharedinternal": "SharePoint_Shared_Internally",
        "sharepointsharedexternal": "SharePoint_Shared_Externally",
        "onedrivestorage": "OneDrive_Storage_GB"
    }

    user_personas = []
    
    for idx, row in df_users.iterrows():
        best_persona_id = None
        min_distance = float('inf')
        
        rank_row = ranks_df.loc[idx]
        
        for persona in personas_data:
            p_metrics = persona.get("metrics", {})
            
            # Normalize keys returned by LLM to lowercase without spaces/underscores/hyphens
            norm_p_metrics = {}
            for k, v in p_metrics.items():
                norm_k = str(k).lower().replace("_", "").replace("-", "").replace(" ", "")
                norm_p_metrics[norm_k] = v
                
            distance = 0.0
            
            for p_key, col_name in norm_metric_keys.items():
                # Get the value from the normalized metrics dict
                p_val_str = str(norm_p_metrics.get(p_key, "low")).lower()
                p_val = level_map.get(p_val_str, 0.1)
                user_val = rank_row[col_name]
                distance += (user_val - p_val) ** 2
                
            if distance < min_distance:
                min_distance = distance
                best_persona_id = persona["id"]
                
        user_personas.append(best_persona_id)
        
    return user_personas

