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

"""Constants for Migration Planner."""

GRAPH_BASE_URL: str = "https://graph.microsoft.com/v1.0"
TOKEN_URL_TEMPLATE: str = (
    "https://login.microsoftonline.com/{0}/oauth2/v2.0/token"
)
MAX_RETRIES: int = 10
BACKOFF: int = 2
SHOW_LOAD_MULTIPLIER = False
USE_MSFT_BACKOFF = True

# Chat Estimation Cost Weights (Seconds)
BLENDED_MSG_COST_SEC: float = 0.75
CHANNEL_COST_SEC: float = 10.0
MEMBERSHIP_COST_SEC: float = 1.0

# QPS Limits
CHANNEL_QPS: float = 2.0
MESSAGE_QPS: float = 26.0
MEMBERSHIP_QPS: float = 18.0
MAX_TEAMS_PER_BATCH: int = 1000

# --- UI Colors (Google Material 3) ---
COLOR_PRIMARY = "#0B57D0"  # Google Blue
COLOR_ON_PRIMARY = "#FFFFFF"  # White text on blue
COLOR_SURFACE = "#FFFFFF"  # Card background
COLOR_BACKGROUND = "#F0F2F5"  # App background (Light Gray/Blue tint)
COLOR_TEXT_MAIN = "#1F1F1F"  # High emphasis text
COLOR_TEXT_SUB = "#444746"  # Medium emphasis text
COLOR_OUTLINE = "#747775"  # Input borders
COLOR_OUTLINE_LIGHT = "#E0E2E0"  # Card borders
COLOR_TONAL_BG = "#D3E3FD"  # Light Blue (Secondary Container)
COLOR_TONAL_TEXT = "#041E49"  # Dark Blue (On Secondary Container)
COLOR_TONAL_HOVER = "#C2D0EA"  # Slightly darker for hover state
COLOR_SUCCESS = "#188038"  # Google Green
COLOR_ERROR = "#B3261E"  # GM3 Error Red
COLOR_ERROR_HOVER = "#8C1D18"  # Darker Red for hover
COLOR_PRIMARY_HOVER = "#0842a0"  # Darker Blue for hover
COLOR_SECONDARY_HOVER = "#F1F3F4"  # Light Gray for hover
COLOR_SURFACE_HOVER = "#EFF6FF"  # Light Blue for surface hover
COLOR_SURFACE_VARIANT = "#F8F9FA"  # Light Gray for advanced settings
COLOR_BATCH_BAR = "#8AB4F8"  # Light Blue for batch bars

# --- Fonts ---
FONT_HEADER_LARGE = ("Roboto", 32, "bold")
FONT_HEADER_MEDIUM = ("Roboto", 24, "bold")
FONT_HEADER_SMALL = ("Roboto", 18, "bold")
FONT_BODY_LARGE = ("Roboto", 14)
FONT_BODY_BOLD = ("Roboto", 14, "bold")
FONT_BODY_MEDIUM = ("Roboto", 12)
FONT_BODY_SMALL = ("Roboto", 11)
FONT_ICON_LARGE = ("Arial", 26)
FONT_ICON_MEDIUM = ("Arial", 24)

# --- ETA Calculation Parameters (Test/Alpha)---
ENABLE_EMAIL_ETA = True
ETA_EMAIL_GLOBAL_LIMIT = 1200
ETA_EMAIL_USER_LIMIT = 6
ETA_EMAIL_BATCH_SIZE = 1
ETA_EMAIL_BATCH_TIME = 6

IPA_ETA_MULTIPLIER = 1.5

FILES_GLOBAL_COUNT_LIMIT = 4  # 4 files/folders per second
FILES_GLOBAL_CORPUS_SIZE_LIMIT = (400 * 1024 * 1024 * 1024) // 3600  # 400 GB per hour in bytes per second

ENABLE_CALENDAR_ETA = False
ETA_CALENDAR_GLOBAL_LIMIT = 50
ETA_CALENDAR_USER_LIMIT = 1
ETA_CALENDAR_BATCH_SIZE = 8
ETA_CALENDAR_BATCH_TIME = 25

ENABLE_CONTACT_ETA = False
ETA_CONTACT_GLOBAL_LIMIT = 30
ETA_CONTACT_USER_LIMIT = 1
ETA_CONTACT_BATCH_SIZE = 10
ETA_CONTACT_BATCH_TIME = 25

ENABLE_IN_PLACE_ARCHIVE_ETA = True
ENABLE_SHARED_MAILBOX_ETA = True
ENABLE_GROUP_MAILBOX_ETA = True
