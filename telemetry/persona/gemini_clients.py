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

import os
import json
import logging
import pandas as pd
from google import genai
from google.genai import types

logger = logging.getLogger("M365TelemetryAsyncLogger.UserPersonaAnalysis.gemini_clients")


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
    
    logger.info("Calling Gemini generation with SDK model: gemini-flash-latest...")
    response = client.models.generate_content(
        model="gemini-flash-latest",
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json"
        )
    )
        
    try:
        candidate_text = response.text
        return json.loads(candidate_text)
    except (KeyError, IndexError, ValueError) as e:
        logger.error(f"Failed to parse Gemini response: {e}. Raw response: {response}")
        raise ValueError(f"Failed to parse Gemini response: {e}")


def generate_kmeans_personas_gemini(api_key: str, cluster_summary: pd.DataFrame, optimal_k: int) -> dict:
    """Sends K-Means centroids and user count data to Gemini to define visual personas."""
    cluster_data_text = cluster_summary.to_string()

    prompt = f"""You are an expert IT Operations and SaaS Licensing Analyst.
I have run a K-Means clustering algorithm on employee Microsoft 365 usage telemetry spanning a 180-day period.

The algorithm automatically determined that the workforce divides best into {optimal_k} distinct employee clusters.
Here is the centroid data (average behavior metrics and user count) for each cluster:

Centroid Summary:
{cluster_data_text}

Analyze the centroid values for each cluster in great detail and translate them into a distinct, creative user persona.
For each cluster, define:
1. An ID matching "cluster_[Cluster_ID]" (e.g. "cluster_0", "cluster_1").
2. A creative, professional persona name (title).
3. A representative emoji character.
4. A behavioral summary description (2-3 sentences).
5. 2 to 4 behavior patterns (bullet points explaining their characteristics based on the metric values).

Output your response strictly as a JSON object matching this schema:
{{
  "personas": [
    {{
      "id": "cluster_0",
      "title": "Collaborative Power User",
      "emoji": "🚀",
      "description": "Users in this cluster are highly active in Teams and email communication...",
      "behavior_patterns": [
        "Very high Teams chat volume",
        "Active meeting participant"
      ]
    }}
  ]
}}

Do not include any other markdown formatting, code block fences, or text outside the JSON."""

    # Initialize client directly with the API key
    client = genai.Client(api_key=api_key)
    
    logger.info("Calling K-Means Gemini summarization with SDK model: gemini-flash-latest...")
    response = client.models.generate_content(
        model="gemini-flash-latest",
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json"
        )
    )
        
    try:
        candidate_text = response.text
        return json.loads(candidate_text)
    except (KeyError, IndexError, ValueError) as e:
        logger.error(f"Failed to parse Gemini response: {e}. Raw response: {response}")
        raise ValueError(f"Failed to parse Gemini response: {e}")


def select_telemetry_features_gemini(api_key: str, dataset_path: str) -> list[str]:
    """Asks Gemini to analyze the CSV dataset headers and select the most relevant telemetry features
    to distinguish employee personas.
    """
    if not os.path.exists(dataset_path):
        raise FileNotFoundError(f"Dataset not found at: {dataset_path}")
        
    # Read the headers of the dataset to get available columns
    df_headers = pd.read_csv(dataset_path, nrows=0)
    available_cols = [col for col in df_headers.columns if col not in ['User Principal Name', 'App_Access_Profile']]

    prompt = f"""You are an expert IT Operations and SaaS Licensing Analyst.
I have combined several Microsoft 365 usage reports containing detailed employee activity logs.

Here is the list of all available telemetry columns in the dataset:
{json.dumps(available_cols, indent=2)}

From a classification standpoint, which of these telemetry features would be most relevant to cluster employees into distinct behavioral personas (e.g., active communicators, file-centric users, collaborative power users, inactive accounts)? Select a subset of these features (typically 3 to 6 columns) that are most informative and non-redundant.

Output your response strictly as a JSON object matching this schema:
{{
  "selected_features": [
    "Email_Send_Count",
    "Teams_Private_Chat_Message_Count"
  ]
}}

Do not include any other markdown formatting, code block fences, or text outside the JSON."""

    client = genai.Client(api_key=api_key)
    logger.info("Calling Gemini feature selection with SDK model: gemini-flash-latest...")
    response = client.models.generate_content(
        model="gemini-flash-latest",
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json"
        )
    )
    
    try:
        data = json.loads(response.text)
        return data.get("selected_features", [])
    except Exception as e:
        logger.error(f"Failed to parse selected features JSON: {e}. Raw: {response.text}")
        # Default fallback to all columns if LLM call fails
        return available_cols


def generate_personas_from_reduced_dataset(api_key: str, dataset_path: str, selected_features: list[str]) -> dict:
    """Reduces the dataset columns in Python and calls Gemini to define personas on the reduced set."""
    if not os.path.exists(dataset_path):
        raise FileNotFoundError(f"Dataset not found at: {dataset_path}")
        
    df = pd.read_csv(dataset_path)
    
    # Keep UPN and access profile, plus selected columns
    keep_cols = ['User Principal Name', 'App_Access_Profile']
    valid_selected = [col for col in selected_features if col in df.columns]
    
    df_reduced = df[keep_cols + valid_selected]
    csv_data = df_reduced.to_csv(index=False)
    
    metrics_schema_fields = {}
    for col in valid_selected:
        metrics_schema_fields[col] = "high/medium/low"

    prompt = f"""You are an expert M365 telemetry and data analyst.
Below is the reduced M365 user activity dataset (180 days) containing only the most distinguishing telemetry features selected for all users in a tenant.

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
      "metrics": {json.dumps(metrics_schema_fields)}
    }}
  ]
}}

Do not include any other markdown formatting, code block fences, or text outside the JSON."""

    client = genai.Client(api_key=api_key)
    logger.info("Calling Gemini generation with reduced dataset using SDK model: gemini-flash-latest...")
    response = client.models.generate_content(
        model="gemini-flash-latest",
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json"
        )
    )
        
    try:
        candidate_text = response.text
        return json.loads(candidate_text)
    except (KeyError, IndexError, ValueError) as e:
        logger.error(f"Failed to parse Gemini response: {e}. Raw response: {response}")
        raise ValueError(f"Failed to parse Gemini response: {e}")
