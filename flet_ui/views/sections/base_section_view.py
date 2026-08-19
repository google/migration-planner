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

"""Base class for modular telemetry section views in Flet UI."""

import threading
from typing import Callable, List, Optional
import flet as ft
from flet_ui.components.section_status_indicator import SectionStatus


class BaseSectionView(ft.Container):
    """Abstract base container providing uniform state, UI callbacks, and status reporting."""

    def __init__(
        self,
        page: ft.Page,
        tenant: str = "",
        client: str = "",
        secret: str = "",
        on_status_change: Optional[Callable[[str], None]] = None,
    ):
        super().__init__()
        self.page_ref = page
        self.tenant = tenant
        self.client_id = client
        self.secret = secret
        self.on_status_change = on_status_change

        self.expand = True
        self.is_fetched = False
        self.is_fetching = False
        self.status = SectionStatus.IDLE
        self._registered_cards: List[ft.Control] = []

    def register_cards(self, *cards: ft.Control):
        """Registers telemetry card instances to track for errors upon fetch completion."""
        self._registered_cards.extend(cards)

    def _safe_run_on_ui(self, callback: Callable):
        """Dispatches UI updates safely on the event loop."""
        try:
            loop = getattr(self.page_ref, "loop", None)
            if loop and callable(getattr(loop, "is_running", None)) and loop.is_running() and not isinstance(loop, ft.Page):
                loop.call_soon_threadsafe(callback)
            else:
                callback()
        except Exception:
            callback()

    def _notify_status(self, status: str):
        """Notifies status change to parent dashboard."""
        self.status = status
        if self.on_status_change:
            try:
                self.on_status_change(status)
            except Exception:
                pass

    def _check_completion_status(self, cards: Optional[List[ft.Control]] = None) -> str:
        """Evaluates overall error status across registered or specified telemetry cards."""
        target_cards = cards or self._registered_cards
        has_error = any(getattr(c, "error_message", None) is not None for c in target_cards)
        return SectionStatus.ERROR if has_error else SectionStatus.SUCCESS

    def _reload_card(self, worker_func: Callable):
        """Asynchronously executes single card reload in a daemon thread."""
        self._notify_status(SectionStatus.LOADING)

        def _wrapper():
            try:
                worker_func(is_reload=True)
            finally:
                def _done():
                    self._notify_status(self._check_completion_status())
                self._safe_run_on_ui(_done)

        threading.Thread(target=_wrapper, daemon=True).start()
