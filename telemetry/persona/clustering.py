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

import logging
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

logger = logging.getLogger("M365TelemetryAsyncLogger.UserPersonaAnalysis.clustering")


def classify_users_to_personas(df_users: pd.DataFrame, personas_data: list) -> list:
    """Clusts and maps M365 users to defined personas in Python using distance calculations."""
    # Dynamically normalize all numeric columns present in the dataframe
    cols_to_normalize = df_users.select_dtypes(include=['float64', 'int64']).columns.tolist()
    
    # Calculate ranks (0 to 1) for each column in the dataframe
    ranks_df = pd.DataFrame(index=df_users.index)
    for col in cols_to_normalize:
        if df_users[col].max() == df_users[col].min():
            # If all values are 0, rank is 0.0, else 0.5
            ranks_df[col] = 0.0 if df_users[col].max() == 0 else 0.5
        else:
            # Calculate percentile rank
            ranks_df[col] = df_users[col].rank(pct=True)
            # Force absolute 0 values to rank 0.0 so they don't get inflated due to ties at 0
            ranks_df.loc[df_users[col] == 0, col] = 0.0

    # Map profile levels to numeric values
    level_map = {
        "high": 0.8,
        "medium": 0.4,
        "low": 0.1
    }

    # Dynamically define normalized mapping of persona metrics to df columns
    norm_metric_keys = {}
    for col in cols_to_normalize:
        norm_k = str(col).lower().replace("_", "").replace("-", "").replace(" ", "")
        norm_metric_keys[norm_k] = col

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
                # For Strategy 3 (feature selection), only calculate distance for selected features
                if p_key not in norm_p_metrics:
                    continue
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


def perform_kmeans_clustering(df: pd.DataFrame, max_clusters: int = 6) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """Runs K-Means clustering on employee usage telemetry.
    Automatically determines the optimal number of clusters (2 to max_clusters) using Silhouette scores.
    """
    logger.info(f"Running K-Means clustering dynamic selection (Max: {max_clusters})...")
    
    # Select numerical columns for clustering
    cols_to_cluster = [
        'Email_Sends', 'Meetings_Organized_Via_Email', 
        'Teams_Private_Chats', 'Teams_Meetings_Attended', 
        'OneDrive_Active_Files', 'SharePoint_Files_Edited', 
        'SharePoint_Shared_Internally', 'SharePoint_Shared_Externally', 
        'OneDrive_Storage_GB'
    ]
    
    # Filter columns that actually exist in the dataframe
    cluster_cols = [c for c in cols_to_cluster if c in df.columns]
    if not cluster_cols:
        raise ValueError("No numerical columns found in the dataset to perform clustering.")
        
    X = df[cluster_cols].copy()
    
    # Fill NaN values with 0
    X = X.fillna(0.0)

    # Scale the metrics
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    best_k = 2
    best_score = -1
    best_kmeans = None

    # Test k up to min(max_clusters, number of rows - 1)
    limit_k = min(max_clusters, len(df) - 1)
    if limit_k < 2:
        kmeans = KMeans(n_clusters=len(df), random_state=42, n_init=10)
        labels = kmeans.fit_predict(X_scaled)
        df['Cluster_ID'] = labels
        
        centroid_cols = ['Cluster_ID'] + cluster_cols
        cluster_summary = df[centroid_cols].groupby('Cluster_ID').mean().round(2)
        cluster_summary['User_Count'] = df['Cluster_ID'].value_counts()
        return df, cluster_summary, len(df)

    for k in range(2, limit_k + 1):
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = kmeans.fit_predict(X_scaled)
        
        score = silhouette_score(X_scaled, labels)
        msg = f"  Tested k={k} -> Silhouette Score: {score:.4f}"
        logger.info(msg)
        
        if score > best_score:
            best_score = score
            best_k = k
            best_kmeans = kmeans

    optimal_msg = f"Optimal cluster count determined: {best_k} (Silhouette Score: {best_score:.4f})"
    logger.info(optimal_msg)
    
    df['Cluster_ID'] = best_kmeans.labels_

    # Calculate centroids
    centroid_cols = ['Cluster_ID'] + cluster_cols
    cluster_summary = df[centroid_cols].groupby('Cluster_ID').mean().round(2)
    cluster_summary['User_Count'] = df['Cluster_ID'].value_counts()
    
    return df, cluster_summary, best_k
