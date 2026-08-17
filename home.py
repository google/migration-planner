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

"""
Deal Assistant Platform - Main Application Entry Point (Flet UI)

Run using:
    python home.py
"""

import os
import sys
import ssl

# Ensure project root is in sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Workaround for macOS Python SSL Certificate errors when initializing Flet
ssl._create_default_https_context = ssl._create_unverified_context

import flet as ft
from flet_ui.styles import COLOR_APP_BG, get_app_theme
from flet_ui.views.home_view import HomeView
from flet_ui.views.migration_planner_view import MigrationPlannerPlaceholderView
from flet_ui.views.usage_adoption_view import UsageAdoptionPlaceholderView


def main(page: ft.Page):
    """Main application setup and navigation controller."""
    page.title = "Deal Assistant Platform"
    page.theme_mode = ft.ThemeMode.LIGHT
    page.theme = get_app_theme()
    page.bgcolor = COLOR_APP_BG
    page.padding = 24
    page.vertical_alignment = ft.MainAxisAlignment.CENTER
    page.horizontal_alignment = ft.CrossAxisAlignment.CENTER

    # Configure desktop window properties
    try:
        if hasattr(page, "window"):
            page.window.width = 1120
            page.window.height = 720
            page.window.min_width = 880
            page.window.min_height = 560
            page.window.alignment = ft.Alignment(0, 0)
    except Exception:
        pass

    def show_home():
        """Navigate to the main Home Screen."""
        page.controls.clear()
        page.add(
            HomeView(
                page=page,
                on_open_usage_adoption=show_usage_adoption,
                on_open_migration_planner=show_migration_planner,
            )
        )
        page.update()

    def show_usage_adoption():
        """Navigate to the Usage & Adoption module."""
        page.controls.clear()
        page.add(
            UsageAdoptionPlaceholderView(
                page=page,
                on_back=show_home,
            )
        )
        page.update()

    def show_migration_planner():
        """Navigate to the Migration Planner module."""
        page.controls.clear()
        page.add(
            MigrationPlannerPlaceholderView(
                page=page,
                on_back=show_home,
            )
        )
        page.update()

    # Initial view load
    show_home()


if __name__ == "__main__":
    # Ensure headless matplotlib backend does not conflict if telemetry charts are invoked
    try:
        import matplotlib

        matplotlib.use("Agg")
    except ImportError:
        pass

    ft.run(main)
