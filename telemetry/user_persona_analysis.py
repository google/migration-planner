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

"""Modular User Persona Analysis main entry point."""

import os
import logging
import pandas as pd
from core.graph.client import GraphClient
from core.graph.reports import ReportsService
from telemetry.persona import (
    generate_user_activity_dataset,
    generate_full_unfiltered_dataset,
    generate_personas_from_dataset,
    classify_users_to_personas,
    perform_kmeans_clustering,
    generate_kmeans_personas_gemini,
    select_telemetry_features_gemini,
    generate_personas_from_reduced_dataset
)

# Bind to the async logger initialized in m365_telemetry.py
logger = logging.getLogger("M365TelemetryAsyncLogger.UserPersonaAnalysis")


def run_user_persona_pipeline(tenant_id: str, client_id: str, client_secret: str, gemini_api_key: str, output_csv_path: str, strategy: str = "heuristic", reports_dir: str = None, status_callback=None) -> dict:
    """Runs the full pipeline to download M365 reports, generate user activity dataset,
    and query Gemini to build and classify user personas using the selected strategy.
    
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
    if strategy == "feature_selection":
        generate_full_unfiltered_dataset(reports_dir, output_csv_path)
    else:
        generate_user_activity_dataset(reports_dir, output_csv_path)
    
    # 2. Execute Selected Strategy
    if strategy == "kmeans":
        if status_callback:
            status_callback("Clustering the data")
            
        logger.info("Executing K-Means clustering strategy...")
        df_users = pd.read_csv(output_csv_path)
        
        # Run clustering
        df_clustered, cluster_summary, optimal_k = perform_kmeans_clustering(df_users)
        
        if status_callback:
            status_callback("Generating insights using Gemini")
            
        # Call Gemini to summarize centroids
        personas_response = generate_kmeans_personas_gemini(gemini_api_key, cluster_summary, optimal_k)
        
        personas_list = personas_response.get("personas", [])
        
        # Format assigned personas ID as cluster_X
        df_clustered['Assigned_Persona_ID'] = 'cluster_' + df_clustered['Cluster_ID'].astype(str)
        
        # Map persona IDs to Titles
        persona_titles = {p["id"]: p["title"] for p in personas_list}
        df_clustered['Assigned_Persona_Title'] = df_clustered['Assigned_Persona_ID'].map(lambda p_id: persona_titles.get(p_id, "Unknown"))
        
        # Save the updated clustered dataset
        df_clustered.to_csv(output_csv_path, index=False)
        
        return {
            "personas": personas_list,
            "dataset_path": output_csv_path
        }
        
    elif strategy == "feature_selection":
        if status_callback:
            status_callback("Selecting features")
            
        logger.info("Executing AI feature selection strategy - Step 1: Selecting features...")
        selected_features = select_telemetry_features_gemini(gemini_api_key, output_csv_path)
        logger.info(f"Features selected by Gemini: {selected_features}")
        
        # Reduce the dataset to only selected features + identity columns
        df_full = pd.read_csv(output_csv_path)
        keep_cols = ['User Principal Name', 'App_Access_Profile']
        valid_selected = [col for col in selected_features if col in df_full.columns]
        
        df_reduced = df_full[keep_cols + valid_selected]
        df_reduced.to_csv(output_csv_path, index=False)
        
        if status_callback:
            status_callback("Generating insights using Gemini")
            
        logger.info("Executing AI feature selection strategy - Step 2: Generating personas...")
        personas_response = generate_personas_from_reduced_dataset(gemini_api_key, output_csv_path, selected_features)
        
        # Classify users in Python
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
        
    else:
        # Heuristic Strategy
        if status_callback:
            status_callback("Generating insights using Gemini")
            
        logger.info("Calling Gemini API to define personas...")
        personas_response = generate_personas_from_dataset(gemini_api_key, output_csv_path)
        
        # Classify users in Python
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
