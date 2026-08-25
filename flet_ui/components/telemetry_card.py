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

"""Standardized, full-width Telemetry Card and Table component for M365 reports."""

import webbrowser
from typing import Any, Callable, Dict, List, Optional
import flet as ft
from flet_ui.styles import (
    COLOR_BORDER,
    COLOR_ERROR,
    COLOR_PRIMARY,
    COLOR_PRIMARY_HOVER,
    COLOR_SURFACE,
    COLOR_TEXT_PRIMARY,
    COLOR_TEXT_SECONDARY,
)


class TelemetryCard(ft.Container):
    """Consistent Card container for telemetry tables with locked grayed-out reload state and loading indicator."""

    def __init__(
        self,
        title: str,
        link_text: Optional[str] = None,
        link_url: Optional[str] = None,
        subtitle: Optional[str] = None,
        footnote: Optional[str] = None,
        on_reload: Optional[Callable[[], None]] = None,
        paginate: bool = True,
        page_size: int = 5,
        column_weights: Optional[List[int]] = None,
    ):
        super().__init__()
        self.card_title = title
        self.link_text = link_text
        self.link_url = link_url
        self.subtitle = subtitle
        self.footnote = footnote
        self.on_reload = on_reload
        self.paginate = paginate
        self.page_size = page_size
        self.column_weights = column_weights

        self.columns: List[str] = []
        self.all_rows: List[List[Any]] = []
        self.current_page = 0
        self.execution_time: Optional[float] = None
        self.is_loading = False
        self.error_message: Optional[str] = None

        self.bgcolor = "transparent"
        self.border = None
        self.border_radius = 0
        # Dedicated right margin/padding (18px) to prevent vertical scrollbar overlap
        self.padding = ft.Padding(0, 4, 18, 16)
        self.expand = True

        # Header controls
        self.refresh_indicator = ft.Row(
            tight=True,
            spacing=6,
            visible=False,
            controls=[
                ft.ProgressRing(width=14, height=14, stroke_width=2, color=COLOR_PRIMARY),
                ft.Text("Refetching data...", size=12, color=COLOR_TEXT_SECONDARY),
            ],
        )

        self.timer_badge = ft.Row(
            tight=True,
            spacing=4,
            visible=False,
            controls=[
                ft.Icon(ft.Icons.TIMER_OUTLINED, size=13, color=COLOR_TEXT_SECONDARY),
                ft.Text("0.00s", size=12, color=COLOR_TEXT_SECONDARY),
            ],
        )

        self.reload_btn = ft.OutlinedButton(
            content=ft.Row(
                tight=True,
                spacing=4,
                controls=[
                    ft.Icon(ft.Icons.REFRESH_ROUNDED, size=13, color=COLOR_PRIMARY),
                    ft.Text("Reload", size=12, weight=ft.FontWeight.W_600, color=COLOR_PRIMARY),
                ],
            ),
            style=ft.ButtonStyle(
                shape=ft.RoundedRectangleBorder(radius=6),
                padding=ft.Padding(10, 4, 10, 4),
            ),
            height=28,
            on_click=lambda _: self._trigger_reload(),
        )

        # Error banner control (above table)
        self.error_banner = ft.Container(
            bgcolor="#FEF2F2",
            border=ft.Border.all(1, "#FECACA"),
            border_radius=8,
            padding=ft.Padding(12, 10, 12, 10),
            visible=False,
            content=ft.Row(
                spacing=10,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                controls=[
                    ft.Icon(ft.Icons.ERROR_OUTLINE_ROUNDED, size=18, color=COLOR_ERROR),
                    ft.Text("", size=12, color=COLOR_ERROR, expand=True),
                    ft.TextButton(
                        "Retry",
                        style=ft.ButtonStyle(padding=ft.Padding(8, 4, 8, 4)),
                        on_click=lambda _: self._trigger_reload(),
                    ),
                ],
            ),
        )

        # Initial loading container (only for initial fetch when no rows exist yet)
        self.initial_loading_container = ft.Container(
            padding=ft.Padding(0, 24, 0, 24),
            alignment=ft.alignment.Alignment(0, 0),
            visible=False,
            content=ft.Column(
                tight=True,
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                spacing=12,
                controls=[
                    ft.ProgressRing(width=26, height=26, stroke_width=2.5, color=COLOR_PRIMARY),
                    ft.Text("Fetching data...", size=13, color=COLOR_TEXT_SECONDARY),
                ],
            ),
        )

        self.table_container = ft.Container(expand=True, opacity=1.0)
        self.pagination_row = ft.Row(
            alignment=ft.MainAxisAlignment.CENTER,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
            spacing=12,
            visible=False,
        )
        self.extra_container = ft.Container(visible=False, expand=True)

        self._build_card_layout()

    def _build_card_layout(self):
        """Constructs the composite card layout."""
        # Top Header row
        header_left_controls: List[ft.Control] = [
            ft.Text(
                self.card_title,
                size=18,
                weight=ft.FontWeight.BOLD,
                color=COLOR_TEXT_PRIMARY,
            )
        ]

        if self.link_text and self.link_url:
            header_left_controls.append(
                ft.TextButton(
                    content=ft.Row(
                        tight=True,
                        spacing=4,
                        controls=[
                            ft.Text(
                                self.link_text,
                                size=13,
                                weight=ft.FontWeight.W_600,
                                color=COLOR_PRIMARY,
                            ),
                            ft.Icon(
                                ft.Icons.OPEN_IN_NEW_ROUNDED,
                                size=13,
                                color=COLOR_PRIMARY,
                            ),
                        ],
                    ),
                    style=ft.ButtonStyle(
                        padding=ft.Padding(4, 0, 4, 0),
                    ),
                    on_click=lambda _: webbrowser.open(self.link_url),
                )
            )

        if self.subtitle:
            header_left_controls.append(
                ft.Text(self.subtitle, size=12, color=COLOR_TEXT_SECONDARY)
            )

        header_left = ft.Row(
            spacing=12,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
            controls=header_left_controls,
            expand=True,
        )

        header_right = ft.Row(
            tight=True,
            spacing=12,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
            controls=[
                self.refresh_indicator,
                self.timer_badge,
                self.reload_btn,
            ],
        )

        header_row = ft.Row(
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
            controls=[header_left, header_right],
        )

        card_content_controls: List[ft.Control] = [
            header_row,
            self.error_banner,
            self.initial_loading_container,
            self.table_container,
            self.pagination_row,
        ]

        if self.footnote:
            card_content_controls.append(
                ft.Container(
                    padding=ft.Padding(0, 6, 0, 0),
                    content=ft.Text(
                        self.footnote,
                        size=11,
                        italic=True,
                        color=COLOR_TEXT_SECONDARY,
                    ),
                )
            )

        card_content_controls.append(self.extra_container)

        self.content = ft.Column(
            spacing=10,
            controls=card_content_controls,
        )

    def set_extra_content(self, control: Optional[ft.Control]):
        """Sets custom extra controls below the table and pagination (e.g. charts, footnotes)."""
        if control is not None:
            self.extra_container.content = control
            self.extra_container.visible = True
        else:
            self.extra_container.visible = False
            self.extra_container.content = None
        try:
            self.update()
        except Exception:
            pass

    def set_loading(self, message: Optional[str] = None):
        """Displays loading state.
        - On initial load: Shows centered spinner.
        - On reload: Grays out table into locked state (opacity=0.45) and displays top circular loading indicator.
        """
        self.is_loading = True
        self.reload_btn.disabled = True
        self.error_banner.visible = False

        if not self.all_rows:
            # Initial load state (no table yet)
            display_msg = message or "Fetching data..."
            self.initial_loading_container.content.controls[1].value = display_msg
            self.initial_loading_container.visible = True
            self.table_container.visible = False
            self.pagination_row.visible = False
            self.refresh_indicator.visible = False
            self.extra_container.visible = False
        else:
            # Reload state: Keep table in place, grayed out / locked
            display_msg = message or "Refetching data..."
            self.initial_loading_container.visible = False
            self.table_container.visible = True
            self.table_container.opacity = 0.45
            self.table_container.disabled = True
            self.pagination_row.disabled = True
            self.refresh_indicator.controls[1].value = display_msg
            self.refresh_indicator.visible = True

        try:
            self.update()
        except Exception:
            pass

    def set_error(self, error_message: str):
        """Displays error banner above table and hides table on failure."""
        self.is_loading = False
        self.initial_loading_container.visible = False
        self.refresh_indicator.visible = False
        self.reload_btn.disabled = False

        # Hide table and pagination completely on failure
        self.table_container.visible = False
        self.pagination_row.visible = False
        self.extra_container.visible = False

        self.error_message = error_message
        self.error_banner.content.controls[1].value = error_message
        self.error_banner.visible = True
        try:
            self.update()
        except Exception:
            pass

    def set_data(
        self,
        columns: List[str],
        rows: List[List[Any]],
        execution_time: Optional[float] = None,
        column_weights: Optional[List[int]] = None,
        error_message: Optional[str] = None,
    ):
        """Populates full-width data table, unlocks locked state, and updates execution timer."""
        self.is_loading = False
        self.initial_loading_container.visible = False
        self.refresh_indicator.visible = False
        self.table_container.visible = True
        self.table_container.opacity = 1.0
        self.table_container.disabled = False
        self.pagination_row.disabled = False
        self.reload_btn.disabled = False

        if error_message:
            self.error_message = error_message
            self.error_banner.content.controls[1].value = error_message
            self.error_banner.visible = True
        else:
            self.error_banner.visible = False
            self.error_message = None

        self.columns = columns
        self.all_rows = rows
        self.current_page = 0
        self.execution_time = execution_time
        if column_weights:
            self.column_weights = column_weights

        if execution_time is not None:
            self.timer_badge.controls[1].value = f"{execution_time:.2f}s"
            self.timer_badge.visible = True

        self._render_table_page()
        try:
            self.update()
        except Exception:
            pass

    def _render_table_page(self):
        """Renders full-width table without horizontal scroll, with automatic text wrapping."""
        if not self.all_rows:
            self.table_container.content = ft.Container(
                padding=ft.Padding(0, 16, 0, 16),
                alignment=ft.alignment.Alignment(0, 0),
                content=ft.Text("No records found.", size=13, color=COLOR_TEXT_SECONDARY),
            )
            self.pagination_row.visible = False
            return

        total_rows = len(self.all_rows)
        num_cols = len(self.columns)

        # Resolve column weights for proportional full-width expansion
        weights = self.column_weights or [1] * num_cols
        if len(weights) < num_cols:
            weights = weights + [1] * (num_cols - len(weights))

        # Apply pagination only if enabled
        if self.paginate:
            start_idx = self.current_page * self.page_size
            end_idx = min(start_idx + self.page_size, total_rows)
            page_slice = self.all_rows[start_idx:end_idx]
        else:
            start_idx = 0
            end_idx = total_rows
            page_slice = self.all_rows

        # Build full-width Header Row
        header_cells = [
            ft.Container(
                expand=weights[i],
                padding=ft.Padding(12, 10, 12, 10),
                content=ft.Text(
                    col_name,
                    weight=ft.FontWeight.BOLD,
                    size=12,
                    color="#475569",
                ),
            )
            for i, col_name in enumerate(self.columns)
        ]
        table_header = ft.Container(
            bgcolor="#F8FAFC",
            border_radius=ft.BorderRadius(8, 8, 0, 0),
            content=ft.Row(
                spacing=0,
                vertical_alignment=ft.CrossAxisAlignment.CENTER,
                controls=header_cells,
            ),
        )

        # Build full-width Data Rows
        table_rows_controls: List[ft.Control] = [table_header]

        for r_idx, row_data in enumerate(page_slice):
            bg_color = COLOR_SURFACE if r_idx % 2 == 0 else "#FAFAFA"
            data_cells = []
            for c_idx, cell_val in enumerate(row_data):
                cell_text = str(cell_val) if cell_val is not None else "null"
                data_cells.append(
                    ft.Container(
                        expand=weights[c_idx],
                        padding=ft.Padding(12, 10, 12, 10),
                        content=ft.Text(
                            cell_text,
                            size=12,
                            color=COLOR_TEXT_PRIMARY,
                            no_wrap=False,  # Wrap text to new line (no horizontal scroll)
                            selectable=True,
                        ),
                    )
                )

            table_rows_controls.append(
                ft.Container(
                    bgcolor=bg_color,
                    border=ft.Border(
                        top=ft.BorderSide(1, "#F1F5F9"),
                        bottom=ft.BorderSide(1, "#E2E8F0") if r_idx == len(page_slice) - 1 else ft.BorderSide(0, "transparent"),
                    ),
                    content=ft.Row(
                        spacing=0,
                        vertical_alignment=ft.CrossAxisAlignment.CENTER,
                        controls=data_cells,
                    ),
                )
            )

        # Encase in full-width bordered container
        self.table_container.content = ft.Container(
            expand=True,
            border=ft.Border.all(1, "#E2E8F0"),
            border_radius=8,
            content=ft.Column(
                spacing=0,
                controls=table_rows_controls,
            ),
        )

        # Pagination controls
        if self.paginate and total_rows > self.page_size:
            total_pages = (total_rows + self.page_size - 1) // self.page_size
            self.pagination_row.visible = True
            self.pagination_row.controls = [
                ft.Text(
                    f"{start_idx + 1} - {end_idx} of {total_rows}",
                    size=12,
                    color=COLOR_TEXT_SECONDARY,
                ),
                ft.IconButton(
                    icon=ft.Icons.CHEVRON_LEFT_ROUNDED,
                    icon_size=18,
                    disabled=(self.current_page == 0),
                    on_click=lambda _: self._change_page(-1),
                ),
                ft.Text(
                    f"{self.current_page + 1} / {total_pages}",
                    size=12,
                    weight=ft.FontWeight.W_600,
                    color=COLOR_TEXT_PRIMARY,
                ),
                ft.IconButton(
                    icon=ft.Icons.CHEVRON_RIGHT_ROUNDED,
                    icon_size=18,
                    disabled=(self.current_page >= total_pages - 1),
                    on_click=lambda _: self._change_page(1),
                ),
            ]
        else:
            self.pagination_row.visible = False

    def _change_page(self, delta: int):
        """Advances or steps back pagination."""
        self.current_page += delta
        self._render_table_page()
        try:
            self.update()
        except Exception:
            pass

    def _trigger_reload(self):
        """Triggers card reload callback with immediate visual locking."""
        if self.on_reload and not self.is_loading:
            self.set_loading("Refetching data...")
            self.on_reload()
