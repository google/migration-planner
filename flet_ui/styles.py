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

"""Design tokens, colors, typography, and theme definitions for the Flet UI."""

import flet as ft

# Colors - Brand & Surfaces (Consistent with flet_app and modern Material 3)
COLOR_PRIMARY = "#1E3A8A"
COLOR_PRIMARY_HOVER = "#172554"
COLOR_SECONDARY = "#3B82F6"
COLOR_SECONDARY_HOVER = "#DBEAFE"

COLOR_APP_BG = "#EEF2F6"
COLOR_SURFACE = "#FFFFFF"
COLOR_SURFACE_VARIANT = "#F1F5F9"
COLOR_HERO_BG = "#DCEBFE"

COLOR_BORDER = "#E2E8F0"
COLOR_BORDER_LIGHT = "#F1F5F9"
COLOR_OUTLINE = "#CBD5E1"
COLOR_OUTLINE_LIGHT = "#E2E8F0"
COLOR_HOVER_BG = "#F8FAFC"
COLOR_ICON_BG = "#F1F5F9"

COLOR_TONAL_BG = "#EFF6FF"
COLOR_TONAL_TEXT = "#1E40AF"

# Text Colors
COLOR_TEXT_PRIMARY = "#0F172A"
COLOR_TEXT_SECONDARY = "#64748B"
COLOR_TEXT_MUTED = "#94A3B8"
COLOR_TEXT_MAIN = "#0F172A"
COLOR_TEXT_SUB = "#64748B"
COLOR_TEXT_HERO = "#0F172A"
COLOR_TEXT_HERO_SUB = "#334155"
COLOR_TEXT_HERO_BADGE = "#1E40AF"

# Status Colors
COLOR_SUCCESS = "#10B981"
COLOR_WARNING = "#F59E0B"
COLOR_ERROR = "#DC2626"


def get_app_theme() -> ft.Theme:
    """Returns the custom Flet theme matching the flet_app design system."""
    return ft.Theme(
        color_scheme=ft.ColorScheme(
            primary=COLOR_PRIMARY,
            surface=COLOR_SURFACE,
            error=COLOR_ERROR,
        )
    )
