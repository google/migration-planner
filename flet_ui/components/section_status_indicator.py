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

"""Section status indicator component for sidebar navigation in Flet UI."""

from enum import Enum
from typing import Optional
import flet as ft
from flet_ui.styles import COLOR_ERROR, COLOR_PRIMARY, COLOR_TEXT_SECONDARY


class SectionStatus(str, Enum):
    """Status lifecycle states for telemetry section data fetches."""
    IDLE = "idle"
    LOADING = "loading"
    SUCCESS = "success"
    ERROR = "error"


class SectionStatusIndicator(ft.Container):
    """Reusable indicator control displaying section fetch status with tooltips."""

    def __init__(self, status: str = SectionStatus.IDLE, is_selected: bool = False):
        super().__init__()
        self.status = status
        self.is_selected = is_selected
        self.padding = ft.Padding(0, 0, 2, 0)
        self.content = self._build_indicator()

    def set_status(self, status: str, is_selected: Optional[bool] = None):
        """Updates current status and re-renders the indicator."""
        self.status = status
        if is_selected is not None:
            self.is_selected = is_selected
        self.content = self._build_indicator()
        try:
            self.update()
        except Exception:
            pass

    def _build_indicator(self) -> Optional[ft.Control]:
        """Builds the appropriate icon/spinner control for current status."""
        if self.status == SectionStatus.LOADING:
            self.tooltip = "Fetching telemetry data..."
            self.visible = True
            return ft.ProgressRing(
                width=15,
                height=15,
                stroke_width=2.2,
                color=COLOR_PRIMARY if self.is_selected else "#0284C7",
            )
        elif self.status == SectionStatus.SUCCESS:
            self.tooltip = "Fetch complete without errors"
            self.visible = True
            return ft.Icon(
                ft.Icons.CHECK_ROUNDED,
                size=18,
                color="#16A34A",
            )
        elif self.status == SectionStatus.ERROR:
            self.tooltip = "Fetch complete with errors"
            self.visible = True
            return ft.Icon(
                ft.Icons.CHECK_ROUNDED,
                size=18,
                color=COLOR_ERROR,
            )
        else:
            self.visible = False
            self.tooltip = None
            return None


def create_section_status_indicator(
    status: str = SectionStatus.IDLE, is_selected: bool = False
) -> Optional[ft.Control]:
    """Factory helper creating a status indicator control if status is active."""
    if status == SectionStatus.IDLE or not status:
        return None
    return SectionStatusIndicator(status=status, is_selected=is_selected)
