from ui.exchange_online_ui import MigrationEstimatorTool
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from dataclasses import dataclass
from datetime import datetime, timedelta
import math
import os
import queue
import subprocess
import sys
import threading
import time
from tkinter import filedialog, messagebox
from typing import Any, Callable, Dict, List, Optional, Tuple
import urllib.parse
import webbrowser

import customtkinter as ctk
from estimators.estimator import Estimator
from estimators.factory import EstimatorFactory
import pandas as pd
import psutil
import requests
import urllib3
from util.auth_manager import TokenManager
from util.connectors import UrlInvoker
from util.enums import FailureType
from util.monitoring import ResourceMonitor
from util.utils import ScanConfig
from util.constants import *

class ChatMigrationEstimatorTool(ctk.CTk):
  """Main Application Class for Migration Planner."""

  def __init__(self):
    super().__init__()
    # Style Configuration
    ctk.set_appearance_mode("Light")
    ctk.set_default_color_theme("blue")

    self.configure(fg_color=COLOR_BACKGROUND)
    self.title("Migration Planner")
    self.geometry("950x900")

    self.log_queue = queue.Queue()
    self.log_buffer = []
    self.log_lock = threading.Lock()
    self.stop_scan_event = threading.Event()

    self.spinners_active = {}
    self.spinner_chars = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    self.spinner_indices = {}

    self.setup_variables()
    self.create_widgets()
    self.after(100, self.process_log_queue)

  def setup_variables(self):
    """Initializes all Tkinter variables."""
    self.tenant_id = ctk.StringVar()
    self.client_ids = ctk.StringVar()
    self.client_secrets = ctk.StringVar()
    self.user_source = ctk.StringVar(value="tenant")
    self.user_csv_path = ctk.StringVar()
    self.concurrency = ctk.IntVar(value=10)
    self.load_multiplier = ctk.IntVar(value=1)
    self.retries = ctk.IntVar(value=MAX_RETRIES)
    self.backoff = ctk.IntVar(value=BACKOFF)
    self.eta_max_batches = ctk.IntVar(value=50)
    self.parallel_batches = ctk.IntVar(value=10)
    self.scan_result_csv_path = ctk.StringVar()
    self.id_to_display_name = {}
    self.mode = ctk.StringVar(value="sampling")
    self.sample_percentage = ctk.DoubleVar(value=10.0)

  def create_widgets(self):
    """Creates the main UI layout."""
    # --- HEADER ---
    self.header_frame = ctk.CTkFrame(self, fg_color="transparent")
    self.header_frame.pack(fill="x", padx=30, pady=(20, 10))

    # Top Navigation Row
    self.nav_frame = ctk.CTkFrame(self.header_frame, fg_color="transparent")
    self.nav_frame.pack(fill="x", anchor="w", pady=(0, 10))

    self.btn_back = ctk.CTkButton(
        self.nav_frame,
        text="← Back to Selector",
        width=160,
        height=32,
        corner_radius=16,
        font=FONT_BODY_BOLD,
        fg_color="transparent",
        border_width=1,
        border_color=COLOR_OUTLINE,
        text_color=COLOR_PRIMARY,
        hover_color=COLOR_SECONDARY_HOVER,
        command=self.go_back_to_selector,
    )
    self.btn_back.pack(side="left", anchor="w")

    # Title Container
    self.title_frame = ctk.CTkFrame(self.header_frame, fg_color="transparent")
    self.title_frame.pack(fill="x", pady=(0, 5))

    # Title
    ctk.CTkLabel(
        self.title_frame,
        text="Migration Planner",
        font=FONT_HEADER_LARGE,
        text_color=COLOR_TEXT_MAIN,
    ).pack(anchor="center")

    ctk.CTkLabel(
        self.title_frame,
        text=(
            "Plan your migration with confidence. Your data remains local and"
            " secure."
        ),
        font=FONT_BODY_LARGE,
        text_color=COLOR_TEXT_SUB,
    ).pack(anchor="center", pady=(5, 0))

    # --- MAIN CARD ---
    self.main_card = ctk.CTkFrame(
        self,
        fg_color=COLOR_SURFACE,
        corner_radius=16,
        border_color=COLOR_OUTLINE_LIGHT,
        border_width=1,
    )
    self.main_card.pack(fill="both", expand=True, padx=30, pady=(10, 10))

    self.main_card.grid_rowconfigure(0, weight=1)
    self.main_card.grid_columnconfigure(0, weight=1)

    # --- VIEW CONTAINER ---
    self.view_container = ctk.CTkFrame(self.main_card, fg_color="transparent")
    self.view_container.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)

    # --- FOOTER ---
    self.footer = ctk.CTkFrame(
        self,
        fg_color=COLOR_SURFACE,
        height=80,
        corner_radius=16,
        border_color=COLOR_OUTLINE_LIGHT,
        border_width=1,
    )
    self.footer.pack(fill="x", padx=30, pady=(0, 30), side="bottom")

    # Primary Button
    self.btn_action_primary = ctk.CTkButton(
        self.footer,
        text="Primary",
        width=180,
        height=40,
        corner_radius=20,
        font=FONT_BODY_BOLD,
        fg_color=COLOR_PRIMARY,
        hover_color=COLOR_PRIMARY_HOVER,
    )

    # Secondary Button
    self.btn_action_secondary = ctk.CTkButton(
        self.footer,
        text="Secondary",
        width=140,
        height=40,
        corner_radius=20,
        font=FONT_BODY_BOLD,
        fg_color="transparent",
        border_width=1,
        border_color=COLOR_OUTLINE,
        text_color=COLOR_PRIMARY,
        hover_color=COLOR_SECONDARY_HOVER,
    )

    # Initialize Views
    self.build_config_view()
    self.build_progress_view()
    self.build_results_view()

    # Show Start Page
    self.show_config_view()

  def go_back_to_selector(self):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    selector_path = os.path.join(current_dir, "../migration_planner.py")
    subprocess.Popen(
        [sys.executable, selector_path],
        start_new_session=True,
        close_fds=True,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    self.destroy()

  # ==========================
  # VIEW: CONFIGURATION
  # ==========================


  def _build_auth_inputs(self, container):
    """Renders Tenant and Client credentials form."""
    inputs_frame = ctk.CTkFrame(
        container,
        fg_color=COLOR_SURFACE,
        border_color=COLOR_OUTLINE_LIGHT,
        border_width=1,
        corner_radius=8,
    )
    inputs_frame.pack(fill="x", pady=5)

    inner_pad = ctk.CTkFrame(inputs_frame, fg_color="transparent")
    inner_pad.pack(fill="x", padx=15, pady=15)

    self.create_entry(inner_pad, "Tenant ID", self.tenant_id)
    self.create_entry(inner_pad, "Client ID", self.client_ids)
    self.create_entry(inner_pad, "Client Secret", self.client_secrets, show="*")

  def _build_source_selection(self, container):
    """Renders radio button choices for tenant enumeration vs CSV file upload."""
    ctk.CTkLabel(
        container,
        text="Source:",
        font=FONT_BODY_BOLD,
        text_color=COLOR_TEXT_SUB,
    ).pack(anchor="w", pady=(15, 5))

    source_selection_frame = ctk.CTkFrame(container, fg_color="transparent")
    source_selection_frame.pack(fill="x", anchor="w")
    ctk.CTkRadioButton(
        source_selection_frame,
        text="Scan all teams and users",
        variable=self.user_source,
        value="tenant",
        border_color=COLOR_TEXT_SUB,
    ).pack(side="left", padx=20)
    ctk.CTkRadioButton(
        source_selection_frame,
        text="Upload CSV",
        variable=self.user_source,
        value="csv",
        border_color=COLOR_TEXT_SUB,
    ).pack(side="left")
    ctk.CTkButton(
        source_selection_frame,
        text="Browse",
        command=self.browse_user_csv,
        width=80,
        fg_color="transparent",
        hover_color=COLOR_SURFACE_HOVER,
        border_width=1,
        text_color=COLOR_PRIMARY,
        corner_radius=16,
    ).pack(side="left", padx=10)
    ctk.CTkLabel(
        source_selection_frame,
        textvariable=self.user_csv_path,
        text_color=COLOR_TEXT_SUB,
    ).pack(side="left")


  def toggle_mode_options(self):
    """Dynamically lock/unlock sampling percentage input."""
    if hasattr(self, "sample_entry"):
      if self.mode.get() == "sampling":
        self.sample_entry.configure(state="normal")
      else:
        self.sample_entry.configure(state="disabled")

  def _build_mode_selection(self, container):
    """Renders radio buttons for selecting heuristics vs deep sampling estimation."""
    ctk.CTkLabel(
        container,
        text="Estimation Mode:",
        font=FONT_BODY_BOLD,
        text_color=COLOR_TEXT_MAIN,
    ).pack(anchor="w", padx=15, pady=(10, 5))

    mode_frame = ctk.CTkFrame(container, fg_color="transparent")
    mode_frame.pack(fill="x", padx=15, pady=5)

    ctk.CTkRadioButton(
        mode_frame,
        text="Last 6 Months",
        variable=self.mode,
        value="heuristics",
        command=self.toggle_mode_options,
        border_color=COLOR_TEXT_SUB,
    ).pack(side="left", padx=(0, 20))

    ctk.CTkRadioButton(
        mode_frame,
        text="Deep Scan",
        variable=self.mode,
        value="sampling",
        command=self.toggle_mode_options,
        border_color=COLOR_TEXT_SUB,
    ).pack(side="left", padx=10)

    sampling_frame = ctk.CTkFrame(container, fg_color="transparent")
    sampling_frame.pack(fill="x", padx=15, pady=(5, 10))

    ctk.CTkLabel(
        sampling_frame,
        text="Sampling Percentage (%):",
        font=FONT_BODY_MEDIUM,
        text_color=COLOR_TEXT_SUB,
    ).pack(side="left", padx=(5, 10))

    self.sample_entry = ctk.CTkEntry(
        sampling_frame,
        textvariable=self.sample_percentage,
        width=60,
    )
    self.sample_entry.pack(side="left")

    # Prime initial lock state
    self.toggle_mode_options()

  def _build_advanced_performance_settings(self, container):
    """Renders sliders enabling user modification of task concurrency limits."""
    concurrency_frame = ctk.CTkFrame(container, fg_color="transparent")
    concurrency_frame.pack(fill="x", padx=15)

    ctk.CTkLabel(
        concurrency_frame, text="Concurrency:", text_color=COLOR_TEXT_SUB
    ).grid(row=0, column=0, sticky="w", padx=5, pady=5)
    slider = ctk.CTkSlider(
        concurrency_frame,
        from_=10,
        to=100,
        number_of_steps=9,
        variable=self.concurrency,
    )
    slider.grid(row=0, column=1, sticky="ew", padx=5, pady=5)
    ctk.CTkLabel(
        concurrency_frame,
        textvariable=self.concurrency,
        text_color=COLOR_TEXT_MAIN,
        width=30,
    ).grid(row=0, column=2, sticky="w", padx=5)
    ctk.CTkLabel(
        container,
        text=(
            "* Reduce concurrency if your CPU is slowing down or you see"
            " throttling errors in your logs."
        ),
        font=FONT_BODY_SMALL,
        text_color=COLOR_TEXT_SUB,
    ).pack(anchor="w", padx=25, pady=(2, 5))

    if SHOW_LOAD_MULTIPLIER:
      self.create_grid_entry(
          concurrency_frame, 1, 0, "Load Multiplier:", self.load_multiplier
      )

  def _build_advanced_plan_options(self, container):
    """Renders logic parameters for adjusting overall migration strategy batches."""
    ctk.CTkLabel(
        container,
        text="Migration Plan Options",
        font=FONT_BODY_BOLD,
        text_color=COLOR_TEXT_MAIN,
    ).pack(anchor="w", padx=15, pady=(15, 5))

    eta_settings_frame = ctk.CTkFrame(container, fg_color="transparent")
    eta_settings_frame.pack(fill="x", padx=15)

    ctk.CTkLabel(
        eta_settings_frame, text="Max Batches:", text_color=COLOR_TEXT_SUB
    ).grid(row=1, column=3, sticky="w", padx=5, pady=5)
    slider_max_batches = ctk.CTkSlider(
        eta_settings_frame,
        from_=10,
        to=100,
        number_of_steps=18,
        variable=self.eta_max_batches,
    )
    slider_max_batches.grid(row=1, column=4, sticky="ew", padx=5, pady=5)
    ctk.CTkLabel(
        eta_settings_frame,
        textvariable=self.eta_max_batches,
        text_color=COLOR_TEXT_MAIN,
        width=40,
    ).grid(row=1, column=5, sticky="w", padx=5)

    ctk.CTkLabel(
        container,
        text=(
            "* The migration plan will try to keep the total number of batches"
            " below this number."
        ),
        font=FONT_BODY_SMALL,
        text_color=COLOR_TEXT_SUB,
    ).pack(anchor="w", padx=25, pady=(2, 15))

  def build_config_view(self):
    """Builds the Configuration View layout through modular sub-constructors."""
    self.view_config = ctk.CTkFrame(self.view_container, fg_color="transparent")
    self.view_config.grid_columnconfigure(0, weight=1)
    self.view_config.grid_rowconfigure(2, weight=1)

    # Header Component
    header_container = ctk.CTkFrame(self.view_config, fg_color="transparent")
    header_container.grid(row=0, column=0, sticky="w", padx=25, pady=(20, 5))
    ctk.CTkLabel(
        header_container,
        text="How would you like to provide data?",
        font=FONT_HEADER_MEDIUM,
        text_color=COLOR_TEXT_MAIN,
    ).pack(anchor="w")

    # Realtime Security Disclaimer
    status_container = ctk.CTkFrame(self.view_config, fg_color="transparent")
    status_container.grid(row=1, column=0, sticky="w", padx=25, pady=(0, 20))
    ctk.CTkLabel(
        status_container,
        text="✔",
        text_color=COLOR_SUCCESS,
        font=FONT_BODY_MEDIUM,
    ).pack(side="left")
    ctk.CTkLabel(
        status_container,
        text=(
            " Data stays on your device. We never transmit credentials or data"
            " externally."
        ),
        font=FONT_BODY_MEDIUM,
        text_color=COLOR_TEXT_SUB,
    ).pack(side="left", padx=(5, 0))

    # Internal Scroll Container
    self.scroll_connect = ctk.CTkScrollableFrame(
        self.view_config,
        fg_color="transparent",
        scrollbar_button_color="white",
        scrollbar_button_hover_color=COLOR_SECONDARY_HOVER,
    )
    self.scroll_connect.grid(row=2, column=0, sticky="nsew", padx=15)

    ctk.CTkLabel(
        self.scroll_connect,
        text="Connect your Microsoft Azure account to fetch the data.",
        font=("Roboto", 13),
        text_color=COLOR_TEXT_SUB,
    ).pack(anchor="w", pady=(0, 15))

    # Modular Section Rendering
    self._build_auth_inputs(self.scroll_connect)
    self._build_source_selection(self.scroll_connect)

    # Advanced Frame Shell
    self.advanced_settings_frame = ctk.CTkFrame(
        self.scroll_connect, fg_color=COLOR_SURFACE_VARIANT, corner_radius=12
    )
    self.adv_visible = False
    self.advanced_toggle_button = ctk.CTkButton(
        self.scroll_connect,
        text="Show Advanced Settings ▼",
        command=self.toggle_adv,
        fg_color="transparent",
        hover_color=COLOR_SURFACE,
        text_color=COLOR_PRIMARY,
        anchor="w",
    )
    self.advanced_toggle_button.pack(anchor="w", pady=(15, 5))

    # Render modular sub-components inside advanced frame shell
    self._build_mode_selection(self.advanced_settings_frame)
    self._build_advanced_performance_settings(self.advanced_settings_frame)
    self._build_advanced_plan_options(self.advanced_settings_frame)

  # ==========================
  # VIEW: PROGRESS
  # ==========================
  def build_progress_view(self):
    self.view_progress = ctk.CTkScrollableFrame(
        self.view_container,
        fg_color="transparent",
        scrollbar_button_color="white",
        scrollbar_button_hover_color=COLOR_SECONDARY_HOVER,
    )

    ctk.CTkLabel(
        self.view_progress,
        text="Scan in progress...",
        font=FONT_HEADER_MEDIUM,
        text_color=COLOR_TEXT_MAIN,
    ).pack(anchor="w", padx=25, pady=(25, 5))
    ctk.CTkLabel(
        self.view_progress,
        text=(
            "This may take several minutes depending on the corpus of your"
            " tenant."
        ),
        font=FONT_BODY_MEDIUM,
        text_color=COLOR_TEXT_SUB,
    ).pack(anchor="w", padx=25)

    self.scan_container = ctk.CTkFrame(
        self.view_progress, fg_color="transparent"
    )
    self.scan_container.pack(fill="x", padx=25, pady=20)

    self.create_progress_row(
        self.scan_container, "users", "Scanning Users", is_user=True
    )
    self.create_progress_row(
        self.scan_container, "chats", "Scanning Private Chats"
    )
    self.create_progress_row(
        self.scan_container, "channels", "Scanning Channels"
    )
    self.prog_widgets = {}

  # ==========================
  # VIEW: RESULTS
  # ==========================
  def build_results_view(self):
    self.view_results = ctk.CTkScrollableFrame(
        self.view_container,
        fg_color="transparent",
        scrollbar_button_color="white",
        scrollbar_button_hover_color=COLOR_SECONDARY_HOVER,
    )

  def show_config_view(self):
    self.btn_action_secondary.configure(state="disabled")
    self.after(10, self.perform_view_switch)

  def perform_view_switch(self):
    for w in self.view_results.winfo_children():
      w.destroy()
    self.view_results.pack_forget()
    self.view_progress.pack_forget()

    if hasattr(self, "view_config") and self.view_config.winfo_exists():
      self.view_config.destroy()

    self.build_config_view()
    self.view_config.pack(fill="both", expand=True)

    if hasattr(self, "btn_export_logs"):
      self.btn_export_logs.destroy()

    self.btn_action_secondary.pack_forget()
    self.btn_action_primary.configure(
        text="Get migration estimates",
        command=self.start_scan,
        fg_color=COLOR_PRIMARY,
        hover_color=COLOR_PRIMARY_HOVER,
        state="normal",
    )
    self.btn_action_primary.pack(side="right", padx=25, pady=15)
    self.btn_action_secondary.configure(state="normal")

  def show_progress_view(self):
    self.view_config.pack_forget()
    self.view_results.pack_forget()
    self.view_progress.pack(fill="both", expand=True)

    self.btn_action_secondary.pack_forget()
    self.btn_action_primary.configure(
        text="Stop scan",
        command=self.stop_scan_logic,
        fg_color=COLOR_ERROR,
        hover_color=COLOR_ERROR_HOVER,
        width=180,
    )
    self.btn_action_primary.pack(side="right", padx=25, pady=15)

  def show_results_content(self, data):
    try:
      self.last_scan_data = data
      self.view_config.pack_forget()
      self.view_progress.pack_forget()

      for w in self.view_results.winfo_children():
        w.destroy()

      # Data Corpus Report Header
      ctk.CTkLabel(
          self.view_results,
          text="Data Corpus Report",
          font=FONT_HEADER_SMALL,
          text_color=COLOR_TEXT_MAIN,
      ).pack(anchor="w", padx=10, pady=(10, 0))
      ctk.CTkLabel(
          self.view_results,
          text="Review the analyzed data.",
          font=FONT_BODY_MEDIUM,
          text_color=COLOR_TEXT_SUB,
      ).pack(anchor="w", padx=10, pady=(0, 10))

      # 2. Teams & Channels Row (Restored to V1 Parity)
      total_channels = data.get("channels", 0) + data.get("private_channels", 0)
      has_team_data = "total_teams" in data and (
          data.get("total_teams", 0) > 0
          or total_channels > 0
          or data.get("channel_messages", 0) > 0
      )

      if has_team_data:
        card_frame_teams = ctk.CTkFrame(
            self.view_results, fg_color="transparent"
        )
        card_frame_teams.pack(fill="x", pady=10)

        if data.get("total_teams", 0) > 0:
          self.create_stat_card(
              card_frame_teams, "Teams", f"{data.get('total_teams', 0):,}", "🏢"
          )
        if total_channels > 0:
          self.create_stat_card(
              card_frame_teams, "Channels", f"{total_channels:,}", "#️⃣"
          )
        if data.get("channel_messages", 0) > 0:
          self.create_stat_card(
              card_frame_teams,
              "Channel Messages",
              f"{data.get('channel_messages', 0):,}",
              "📢",
          )

      # 3. Users & Private Chat Stats Row (V1 Parity Grouping)
      card_frame_user = ctk.CTkFrame(self.view_results, fg_color="transparent")
      card_frame_user.pack(fill="x", pady=10)

      self.create_stat_card(
          card_frame_user, "Users", f"{data['total_users']:,}", "👥"
      )

      if data.get("private_chats", 0) > 0:
        self.create_stat_card(
            card_frame_user,
            "Private Chats",
            f"{data.get('private_chats', 0):,}",
            "🗨️",
        )
      if data.get("private_chat_messages", 0) > 0:
        self.create_stat_card(
            card_frame_user,
            "Private Chat Messages",
            f"{data.get('private_chat_messages', 0):,}",
            "💬",
        )

      # Timeline
      ctk.CTkLabel(
          self.view_results,
          text="Timeline Estimates",
          font=FONT_HEADER_SMALL,
          text_color=COLOR_TEXT_MAIN,
      ).pack(anchor="w", padx=10, pady=(20, 5))
      ctk.CTkLabel(
          self.view_results,
          text=(
              "Projected migration timeline based on the proposed execution"
              " plan."
          ),
          font=FONT_BODY_MEDIUM,
          text_color=COLOR_TEXT_SUB,
      ).pack(anchor="w", padx=10, pady=(0, 10))

      # Total Footer
      foot = ctk.CTkFrame(self.view_results, fg_color="transparent")
      foot.pack(fill="x", pady=10)
      self.create_summary_box(
          foot, self.format_eta(data["total_eta"]), "Estimated Time"
      )
      self.create_summary_box(foot, f"{data['total_items']:,}", "Total Items")

      # --- NEW: Container for Paginated Content ---
      self.paginated_frame = ctk.CTkFrame(
          self.view_results, fg_color="transparent"
      )
      self.paginated_frame.pack(fill="x", expand=True)

      # Disclaimer
      disclaimer = (
          "* The estimations provided by this tool are calculated projections"
          " intended for preliminary planning only. Actual migration timelines"
          " (ETAs) and batch execution may vary based on, for example,"
          " real-time network conditions, source/target throttling policies,"
          " migration configurations, and the volume of delta migrations. The"
          " estimations do not constitute a performance guarantee or a binding"
          " service level agreement (SLA)."
      )
      ctk.CTkLabel(
          self.view_results,
          text=disclaimer,
          font=FONT_BODY_SMALL,
          text_color=COLOR_TEXT_SUB,
          wraplength=800,
          justify="left",
      ).pack(anchor="w", padx=10, pady=(10, 20))

      # Resources
      ctk.CTkLabel(
          self.view_results,
          text="RESOURCES",
          font=FONT_BODY_BOLD,
          text_color=COLOR_TEXT_SUB,
      ).pack(anchor="w", padx=10, pady=(10, 10))

      res_frame = ctk.CTkFrame(self.view_results, fg_color="transparent")
      res_frame.pack(fill="x", pady=0)
      res_frame.grid_columnconfigure(0, weight=1)
      res_frame.grid_columnconfigure(1, weight=1)

      self.create_resource_card(
          res_frame,
          0,
          "🚀",
          "Data migration (New)",
          "Our new migration platform for enterprise - totally free.",
          "Learn more",
          "https://support.google.com/a/answer/14012274?hl=en&ref_topic=14012345&sjid=3864823775656113447-NC",
      )
      self.create_resource_card(
          res_frame,
          1,
          "☑️",
          "Best Practices Guide",
          "Essential tips for a smooth transition to Google Workspace.",
          "Read guide",
          "https://support.google.com/a/topic/14012345?hl=en&ref_topic=13002773&sjid=3864823775656113447-NC",
      )

      self.view_results.pack(fill="both", expand=True)

      # Footer Config
      self.btn_action_primary.pack_forget()
      self.btn_action_secondary.pack_forget()
      if hasattr(self, "btn_export_logs"):
        self.btn_export_logs.destroy()

      self.btn_action_primary.configure(
          text="Export full report",
          command=self.export_current_report,
          fg_color=COLOR_PRIMARY,
          hover_color=COLOR_PRIMARY_HOVER,
          width=160,
          state="normal",
      )
      self.btn_action_primary.pack(side="right", padx=25, pady=15)

      self.btn_export_logs = ctk.CTkButton(
          self.footer,
          text="Export logs",
          command=self.export_logs,
          fg_color=COLOR_TONAL_BG,
          text_color=COLOR_TONAL_TEXT,
          hover_color=COLOR_TONAL_HOVER,
          border_width=0,
          font=FONT_BODY_BOLD,
          width=120,
          height=40,
          corner_radius=20,
      )
      self.btn_export_logs.pack(side="right", pady=15)

      self.btn_action_secondary.configure(
          text="Start new search", command=self.show_config_view, width=140
      )
      self.btn_action_secondary.pack(side="left", padx=(25, 0), pady=15)

      self.selected_page_size = "50"
      self.render_paginated_view(0)
    except Exception as e:
      print(f"ERROR in show_results_content: {e}")
      for w in self.view_results.winfo_children():
        w.destroy()
      ctk.CTkLabel(
          self.view_results,
          text=f"Error displaying results: {e}",
          wraplength=700,
      ).pack(padx=20, pady=20)
      self.view_results.pack(fill="both", expand=True)

  def render_paginated_view(self, page):
    """Renders a specific page of the Gantt chart and Batch Details."""
    try:
      # Save the current scroll position before clearing the UI
      try:
        current_scroll = self.view_results._parent_canvas.yview()[0]
      except:
        current_scroll = 0.0

      # Clear previous paginated content
      for w in self.paginated_frame.winfo_children():
        w.destroy()

      data = self.last_scan_data
      batches = data.get("batches", [])
      buckets = data.get("buckets", [])
      max_duration = data.get("total_eta", 0)

      # --- NEW: Calculate Page Size from Dropdown ---
      selected_size = getattr(self, "selected_page_size", "50")
      if selected_size == "All":
        actual_items_per_page = len(batches) if len(batches) > 0 else 50
        view_all = True
      else:
        actual_items_per_page = int(selected_size)
        view_all = False

      # Calculate Page Boundaries
      total_pages = max(1, math.ceil(len(batches) / actual_items_per_page))

      if view_all:
        page = 0

      start_idx = page * actual_items_per_page
      end_idx = min(start_idx + actual_items_per_page, len(batches))
      page_batches = batches[start_idx:end_idx]

      # Fast O(1) lookup to check if a batch belongs on this page
      page_batch_names = {w["name"] for w in page_batches}

      # --- Single Master Container ---
      master_container = ctk.CTkFrame(
          self.paginated_frame,
          fg_color=COLOR_SURFACE,
          corner_radius=12,
          border_color=COLOR_OUTLINE_LIGHT,
          border_width=1,
      )
      master_container.pack(fill="x", padx=10, pady=10)

      # --- Header Row (Title Left, Pagination Right) ---
      header_row = ctk.CTkFrame(master_container, fg_color="transparent")
      header_row.pack(fill="x", padx=20, pady=(15, 5))

      # Title (Left)
      ctk.CTkLabel(
          header_row,
          text="Batch Execution Plan",
          font=FONT_BODY_BOLD,
          text_color=COLOR_TEXT_MAIN,
      ).pack(side="left")

      # Pagination Controls (Right)
      # Always show if total batches > 50 so user can use the dropdown to expand
      if len(batches) > 50 or selected_size != "50":
        ctrl_frame = ctk.CTkFrame(header_row, fg_color="transparent")
        ctrl_frame.pack(side="right")

        btn_state_prev = "normal" if page > 0 and not view_all else "disabled"
        btn_state_next = (
            "normal" if page < total_pages - 1 and not view_all else "disabled"
        )

        # 1. First Page Button ("|<")
        btn_first = ctk.CTkButton(
            ctrl_frame,
            text="|<",
            width=30,
            height=30,
            font=FONT_BODY_BOLD,  # FIX: Set height=30
            command=lambda: self.render_paginated_view(0),
            state=btn_state_prev,
            fg_color="transparent",
            border_width=0,
            text_color=COLOR_TEXT_SUB,
            hover_color=COLOR_SECONDARY_HOVER,
        )
        btn_first.pack(side="left", padx=(0, 5))

        # 2. Page Label
        label_text = (
            "All Pages" if view_all else f"Page {page + 1} of {total_pages}"
        )
        ctk.CTkLabel(
            ctrl_frame,
            text=label_text,
            font=FONT_BODY_BOLD,  # FIX: Matched font size and weight to the buttons
            text_color=COLOR_TEXT_MAIN,
        ).pack(side="left", padx=10)

        # 3. Previous Page Button ("<")
        btn_prev = ctk.CTkButton(
            ctrl_frame,
            text="<",
            width=30,
            height=30,
            font=FONT_BODY_BOLD,  # FIX: Set height=30
            command=lambda: self.render_paginated_view(page - 1),
            state=btn_state_prev,
            fg_color="transparent",
            border_width=0,
            text_color=COLOR_TEXT_SUB,
            hover_color=COLOR_SECONDARY_HOVER,
        )
        btn_prev.pack(side="left", padx=2)

        # 4. Next Page Button (">")
        btn_next = ctk.CTkButton(
            ctrl_frame,
            text=">",
            width=30,
            height=30,
            font=FONT_BODY_BOLD,  # FIX: Set height=30
            command=lambda: self.render_paginated_view(page + 1),
            state=btn_state_next,
            fg_color="transparent",
            border_width=0,
            text_color=COLOR_TEXT_SUB,
            hover_color=COLOR_SECONDARY_HOVER,
        )
        btn_next.pack(
            side="left", padx=(2, 10)
        )  # FIX: Added right padding before dropdown

        # --- Page Size Dropdown ---
        def on_page_size_change(choice):
          self.selected_page_size = choice
          self.render_paginated_view(0)  # Re-render from page 0 on change

        page_size_dropdown = ctk.CTkOptionMenu(
            ctrl_frame,
            values=["50", "100", "200", "All"],
            command=on_page_size_change,
            width=80,
            height=30,
            font=FONT_BODY_BOLD,
            fg_color=COLOR_SURFACE,
            button_color=COLOR_SURFACE,
            button_hover_color=COLOR_SECONDARY_HOVER,
            text_color=COLOR_TEXT_MAIN,  # FIX: Changed text color to match label
            dropdown_font=FONT_BODY_MEDIUM,
        )
        page_size_dropdown.set(selected_size)
        page_size_dropdown.pack(side="left", padx=(5, 0))

      ctk.CTkLabel(
          master_container,
          text=(
              "Each row represents a bucket of batches which can be executed in"
              " parallel to the other buckets."
          ),
          font=FONT_BODY_MEDIUM,
          text_color=COLOR_TEXT_SUB,
      ).pack(anchor="w", padx=20, pady=(0, 15))

      # 3. Parallel Batches Plan (Gantt Chart)
      if buckets:
        # Timeline Scale Header
        tl_row = ctk.CTkFrame(master_container, fg_color="transparent")
        tl_row.pack(fill="x", padx=10, pady=(0, 10))
        timeline_scale = ctk.CTkFrame(tl_row, fg_color="transparent", height=20)
        timeline_scale.pack(side="left", fill="x", expand=True, padx=(15, 100))

        num_ticks = 6
        for i in range(num_ticks):
          fraction = i / (num_ticks - 1)
          time_val = max_duration * fraction
          if max_duration < 120:
            val = round(time_val)
            label_text = f"{val} Hours"
          else:
            val = round(time_val / 24)
            label_text = f"{val} Days"
          if i == 0:
            label_text = "0"
            anchor_pos = "w"
          elif i == num_ticks - 1:
            anchor_pos = "e"
          else:
            anchor_pos = "center"
          ctk.CTkLabel(
              timeline_scale,
              text=label_text,
              font=FONT_BODY_SMALL,
              text_color=COLOR_TEXT_SUB,
          ).place(relx=fraction, rely=0.5, anchor=anchor_pos)

        # Render Buckets
        for b in buckets:
          b_row = ctk.CTkFrame(master_container, fg_color="transparent")
          b_row.pack(fill="x", padx=10, pady=(0, 10))

          track = ctk.CTkFrame(
              b_row, fg_color=COLOR_BACKGROUND, height=24, corner_radius=8
          )
          track.pack(side="left", fill="x", expand=True, padx=10)

          inner_track = ctk.CTkFrame(
              track, fg_color="transparent", height=24, corner_radius=8
          )
          inner_track.pack(fill="both", expand=True, padx=4)

          # Pre-calculate widths to maintain global time scale
          raw_widths = []
          for batch in b["batches"]:
            w = batch["eta"] / max_duration if max_duration > 0 else 0
            raw_widths.append(w)

          min_vis_width = 0.06
          visual_widths = [max(w, min_vis_width) for w in raw_widths]
          total_vis = sum(visual_widths)
          scale_factor = 1.0 if total_vis <= 1.0 else 1.0 / total_vis

          current_relx = 0
          for i, batch in enumerate(b["batches"]):
            final_width = visual_widths[i] * scale_factor

            # ONLY RENDER segment if it belongs to the current page
            if batch["name"] in page_batch_names:
              segment = ctk.CTkFrame(
                  inner_track,
                  fg_color=COLOR_TONAL_BG,
                  corner_radius=12,
                  border_width=1,
                  border_color="white",
              )
              segment.place(
                  relx=current_relx,
                  rely=0.05,
                  relwidth=final_width,
                  relheight=0.9,
              )

              label_text = (
                  batch["name"]
                  if final_width > 0.12
                  else batch["name"].replace("Batch ", "W")
              )
              ctk.CTkLabel(
                  segment,
                  text=label_text,
                  font=("Roboto", 10, "bold"),
                  text_color=COLOR_TONAL_TEXT,
              ).place(relx=0.5, rely=0.5, anchor="center", relheight=0.9)

            # Keep advancing current_relx so the timeline gaps are accurate
            current_relx += final_width

          ctk.CTkLabel(
              b_row,
              text=self.format_eta(b["total"]),
              width=100,
              anchor="e",
              font=FONT_BODY_BOLD,
              text_color=COLOR_TEXT_MAIN,
          ).pack(side="left")

      # Visual Separator between Gantt chart and Batch Details
      ctk.CTkFrame(
          master_container, height=1, fg_color=COLOR_OUTLINE_LIGHT
      ).pack(fill="x", padx=20, pady=15)

      # 4. Batch Details
      if page_batches:
        ctk.CTkLabel(
            master_container,
            text="Batch Details",
            font=FONT_BODY_BOLD,
            text_color=COLOR_TEXT_MAIN,
        ).pack(anchor="w", padx=20, pady=(0, 5))

        ctk.CTkLabel(
            master_container,
            text=(
                "The batches are numbered in the order of their proposed"
                " execution."
            ),
            font=FONT_BODY_MEDIUM,
            text_color=COLOR_TEXT_SUB,
        ).pack(anchor="w", padx=20, pady=(0, 15))

        max_batch_eta = max(w["eta"] for w in batches) if batches else 1
        for w in page_batches:
          self.create_batch_bar(master_container, w, max_batch_eta)

      self.view_results.update_idletasks()
      try:
        self.view_results._parent_canvas.yview_moveto(current_scroll)
      except:
        pass
    except Exception as e:
      print(f"ERROR in render_paginated_view: {e}")
      ctk.CTkLabel(
          self.paginated_frame,
          text=f"Error rendering batch details: {e}",
          wraplength=700,
      ).pack(padx=20, pady=20)

  # --- Widget Generators ---
  def create_entry(self, parent, label, var, show=None):
    f = ctk.CTkFrame(parent, fg_color="transparent")
    f.pack(fill="x", pady=5)
    ctk.CTkLabel(
        f, text=label, width=100, anchor="w", text_color=COLOR_TEXT_SUB
    ).pack(side="left")
    ctk.CTkEntry(
        f,
        textvariable=var,
        show=show,
        height=40,
        corner_radius=4,
        border_width=1,
        border_color=COLOR_OUTLINE,
        fg_color="transparent",
        text_color=COLOR_TEXT_MAIN,
    ).pack(side="left", fill="x", expand=True)

  def create_grid_entry(self, parent, r, c, txt, var):
    ctk.CTkLabel(parent, text=txt, text_color=COLOR_TEXT_SUB).grid(
        row=r, column=c, sticky="w", padx=5, pady=5
    )
    ctk.CTkEntry(
        parent,
        textvariable=var,
        width=80,
        corner_radius=4,
        border_width=1,
        border_color=COLOR_OUTLINE,
        fg_color="transparent",
        text_color=COLOR_TEXT_MAIN,
    ).grid(row=r, column=c + 1, sticky="w", padx=5, pady=5)

  def create_progress_row(self, parent, key, title, is_user=False):
    f = ctk.CTkFrame(parent, fg_color="transparent")
    f.pack(fill="x", pady=10)
    top = ctk.CTkFrame(f, fg_color="transparent")
    top.pack(fill="x")
    lbl_icon = ctk.CTkLabel(
        top,
        text="○",
        font=FONT_HEADER_SMALL,
        width=30,
        text_color=COLOR_TEXT_SUB,
    )
    lbl_icon.pack(side="left", padx=(0, 10))
    ctk.CTkLabel(
        top, text=title, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN
    ).pack(side="left")

    if is_user or key == "entities":
      bar = ctk.CTkProgressBar(
          f,
          height=8,
          mode="indeterminate",
          progress_color=COLOR_PRIMARY,
          fg_color=COLOR_OUTLINE_LIGHT,
          corner_radius=4,
      )
    else:
      bar = ctk.CTkProgressBar(
          f,
          height=8,
          progress_color=COLOR_PRIMARY,
          fg_color=COLOR_OUTLINE_LIGHT,
          corner_radius=4,
      )
      bar.set(0)
    bar.pack(fill="x", pady=10)

    lbl_status = ctk.CTkLabel(
        f, text="Waiting...", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB
    )
    lbl_status.pack(anchor="w")

    if not hasattr(self, "prog_widgets"):
      self.prog_widgets = {}
    self.prog_widgets[key] = {
        "bar": bar,
        "lbl": lbl_status,
        "icon": lbl_icon,
    }
    if key in ["users", "entities"]:
      self.prog_user = bar
      self.lbl_user_status = lbl_status

  def create_stat_card(self, parent, title, value, icon, sub=None):
    f = ctk.CTkFrame(
        parent,
        fg_color=COLOR_SURFACE,
        width=180,
        height=120,
        corner_radius=12,
        border_color=COLOR_OUTLINE_LIGHT,
        border_width=1,
    )
    f.pack(side="left", padx=10, fill="both", expand=True)
    ctk.CTkLabel(f, text=icon, font=FONT_ICON_LARGE, anchor="center").pack(
        pady=(20, 0)
    )
    ctk.CTkLabel(
        f,
        text=value,
        font=FONT_HEADER_MEDIUM,
        text_color=COLOR_TEXT_MAIN,
        anchor="center",
    ).pack()
    ctk.CTkLabel(
        f,
        text=title,
        font=FONT_BODY_MEDIUM,
        text_color=COLOR_TEXT_SUB,
        anchor="center",
    ).pack(pady=(0, 5))
    if sub:
      ctk.CTkLabel(
          f,
          text=sub,
          font=FONT_BODY_SMALL,
          text_color=COLOR_TEXT_SUB,
          anchor="center",
      ).pack(pady=(0, 5))

  def create_resource_card(
      self, parent, column_index, icon, title, desc, link_text, link_url
  ):
    f = ctk.CTkFrame(
        parent,
        fg_color=COLOR_SURFACE,
        height=100,
        corner_radius=12,
        border_color=COLOR_OUTLINE_LIGHT,
        border_width=1,
    )
    f.grid(row=0, column=column_index, sticky="ew", padx=10, pady=5)
    ctk.CTkLabel(f, text=icon, font=FONT_ICON_MEDIUM).pack(
        side="left", padx=20, anchor="n", pady=20
    )
    content = ctk.CTkFrame(f, fg_color="transparent")
    content.pack(side="left", fill="both", expand=True, pady=15, padx=(0, 15))
    ctk.CTkLabel(
        content,
        text=title,
        font=FONT_BODY_BOLD,
        text_color=COLOR_TEXT_MAIN,
        anchor="w",
    ).pack(fill="x")
    ctk.CTkLabel(
        content,
        text=desc,
        font=FONT_BODY_MEDIUM,
        text_color=COLOR_TEXT_SUB,
        anchor="w",
        wraplength=250,
        justify="left",
    ).pack(fill="x", pady=(2, 5))
    link = ctk.CTkLabel(
        content,
        text=f"{link_text} ↗",
        font=FONT_BODY_BOLD,
        text_color=COLOR_PRIMARY,
        anchor="w",
    )
    link.pack(fill="x")
    link.bind("<Button-1>", lambda e: webbrowser.open(link_url))
    link.configure(cursor="hand2")

  def create_batch_bar(self, parent, batch, max_eta):
    f = ctk.CTkFrame(parent, fg_color="transparent")
    f.pack(fill="x", padx=20, pady=8)

    # Safely capture core metric values
    t_teams = batch.get("total_teams", 0)
    t_chans = batch.get("total_channels", 0)
    t_msgs = batch.get("total_channel_messages", 0)

    teams_str = self.format_metric(t_teams)
    channels_str = self.format_metric(t_chans)
    msgs_str = self.format_metric(t_msgs)
    info = (
        f"{batch['name']} - {teams_str} 👥  |  {channels_str} 📺  |  "
        f"{msgs_str} 💬"
    )
    ctk.CTkLabel(
        f,
        text=info,
        width=350,
        anchor="w",
        font=FONT_BODY_MEDIUM,
        text_color=COLOR_TEXT_MAIN,
    ).pack(side="left")

    if max_eta > 0:
      pixel_width = int((batch["eta"] / max_eta) * 350)
    else:
      pixel_width = 0

    # Ensure a tiny minimum width (20px) so the bar is always visible
    w_width = max(20, pixel_width)

    bar = ctk.CTkFrame(
        f, width=w_width, height=16, fg_color=COLOR_BATCH_BAR, corner_radius=8
    )
    bar.pack(side="left", padx=10)
    ctk.CTkLabel(
        f,
        text=self.format_eta(batch["eta"]),
        font=FONT_BODY_BOLD,
        text_color=COLOR_TEXT_MAIN,
    ).pack(side="left")

  def format_metric(self, value):
    if value >= 1_000_000:
      return f"{value/1_000_000:.1f}M"
    elif value >= 1_000:
      return f"{value/1_000:.1f}K"
    else:
      return str(int(value))

  def format_eta(self, hours):
    if hours < 24:
      return f"{math.ceil(hours)} Hours"
    else:
      days = int(hours // 24)
      rem = hours % 24
      rem_hours = math.ceil(rem)
      if rem_hours == 24:
        days += 1
        rem_hours = 0
      return f"{days} Days, {rem_hours} Hours"

  def create_summary_box(self, parent, top_text, bot_text):
    f = ctk.CTkFrame(
        parent,
        fg_color=COLOR_SURFACE,
        height=90,
        corner_radius=12,
        border_color=COLOR_OUTLINE_LIGHT,
        border_width=1,
    )
    f.pack(side="left", padx=10, expand=True, fill="x")
    ctk.CTkLabel(
        f, text=top_text, font=FONT_HEADER_MEDIUM, text_color=COLOR_TEXT_MAIN
    ).pack(pady=(20, 0))
    ctk.CTkLabel(
        f, text=bot_text, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB
    ).pack(pady=(0, 20))

  def toggle_adv(self):
    if self.adv_visible:
      self.advanced_settings_frame.pack_forget()
      self.advanced_toggle_button.configure(text="Show Advanced Settings ▼")
      self.adv_visible = False
    else:
      self.advanced_settings_frame.pack(
          fill="x", pady=10, after=self.advanced_toggle_button
      )
      self.advanced_toggle_button.configure(text="Hide Advanced Settings ▲")
      self.adv_visible = True

  def browse_user_csv(self):
    f = filedialog.askopenfilename(filetypes=[("CSV", "*.csv")])
    if f:
      self.user_csv_path.set(f)

  def export_report(self, data):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    f = filedialog.asksaveasfilename(
        initialfile=f"teams_migration_report_{ts}.csv", defaultextension=".csv"
    )
    if f:
      if hasattr(self, "df_teams_output") and self.df_teams_output is not None:
        self.df_teams_output.to_csv(f, index=False)

  def export_current_report(self):
    if hasattr(self, "last_scan_data"):
      self.export_report(self.last_scan_data)

  def export_logs(self):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    f = filedialog.asksaveasfilename(
        initialfile=f"logs_{ts}.log",
        defaultextension=".log",
        filetypes=[("Log Files", "*.log"), ("All Files", "*.*")],
    )
    if f:
      with self.log_lock:
        content = "\n".join(self.log_buffer)
      with open(f, "w", encoding="utf-8") as file:
        file.write(content)

  # ==========================
  # LOGIC & EXECUTION
  # ==========================
  def animate_spinner(self, source):
    if source not in self.spinners_active or not self.spinners_active[source]:
      return
    idx = self.spinner_indices.get(source, 0)
    icon = self.spinner_chars[idx % len(self.spinner_chars)]
    self.spinner_indices[source] = idx + 1
    if source in self.prog_widgets:
      widget = self.prog_widgets[source]["icon"]
      if widget.winfo_exists():  # <-- Add this check!
        widget.configure(text=icon, text_color=COLOR_PRIMARY)
    self.after(80, lambda: self.animate_spinner(source))

  def update_progress(self, msg):
    if isinstance(msg, str):
      self.log_buffer.append(msg)
    elif isinstance(msg, dict):
      mtype = msg.get("type")
      if mtype == "user_discovery":
        if not self.view_progress.winfo_viewable():
          self.show_progress_view()
        count = msg.get("count", 0)
        user_count = msg.get("user_count", 0)
        status = msg.get("status", "Scanning...")
        if "users" in self.prog_widgets:
          widget = self.prog_widgets["users"]["lbl"]
          if widget.winfo_exists():
            text = f"{count} users" if count != 1 else f"{count} user"
            widget.configure(text=text)
          if not self.spinners_active.get("users"):
            self.spinners_active["users"] = True
            self.animate_spinner("users")
        if status == "Done":
          self.spinners_active["users"] = False
          if "users" in self.prog_widgets:
            widget_icon = self.prog_widgets["users"]["icon"]
            if widget_icon.winfo_exists():
              widget_icon.configure(text="✓", text_color=COLOR_SUCCESS)
            widget_bar = self.prog_widgets["users"]["bar"]
            if widget_bar.winfo_exists():
              widget_bar.stop()
              widget_bar.configure(mode="determinate")
              widget_bar.set(1.0)
      elif mtype == "phase_status":
        source = msg.get("source")
        status = msg.get("status")
        if status == "running":
          self.spinners_active[source] = True
          self.animate_spinner(source)
        elif status == "complete":
          self.spinners_active[source] = False
          if source in self.prog_widgets:
            widget_icon = self.prog_widgets[source]["icon"]
            if widget_icon.winfo_exists():
              widget_icon.configure(text="✓", text_color=COLOR_SUCCESS)
            widget_bar = self.prog_widgets[source]["bar"]
            if widget_bar.winfo_exists():
              if widget_bar.cget("mode") == "indeterminate":
                widget_bar.stop()
                widget_bar.configure(mode="determinate")
              widget_bar.set(1.0)
            if source == "plan_generation":
              widget_lbl = self.prog_widgets[source]["lbl"]
              if widget_lbl.winfo_exists():
                widget_lbl.configure(
                    text=(
                        "Plan generated, please wait while we prepare the final"
                        " dashboard..."
                    )
                )
      elif mtype == "scan_progress":
        source = msg.get("source")
        val = msg.get("progress", 0.0)
        cumulative = msg.get("cumulative", 0)
        users_proc = msg.get("processed", 0)
        users_fail = msg.get("failed", 0)
        users_success = msg.get("success", users_proc - users_fail)
        users_partially_failed = msg.get("partially_failed", 0)
        users_skipped = msg.get("skipped", 0)
        users_tot = msg.get("total", 0)
        entity_type = msg.get("entity_type", "Users")
        if source in self.prog_widgets:
          widget = self.prog_widgets[source]["bar"]
          if widget.winfo_exists():
            widget.set(val)
          if source == "calendars":
            extra = msg.get("extra_text", "")
            widget_lbl = self.prog_widgets[source]["lbl"]
            if widget_lbl.winfo_exists():
              widget_lbl.configure(
                  text=(
                      f"{entity_type}: {users_proc - users_fail} succeeded ,"
                      f" {users_fail} failed | {extra}"
                  )
              )
          elif source == "plan_generation":
            widget_lbl = self.prog_widgets[source]["lbl"]
            if widget_lbl.winfo_exists():
              widget_lbl.configure(text=msg.get("extra_text", ""))
          else:
            if source == "chats":
              label = "Chat Messages"
            elif source == "channels":
              label = "Channel Messages"
            else:
              label = source

            widget_lbl = self.prog_widgets[source]["lbl"]
            if widget_lbl.winfo_exists():
              text_parts = [
                  (
                      f"{entity_type}:"
                      f" {users_proc - users_fail - users_partially_failed}"
                      " succeeded"
                  ),
                  f"{users_fail} failed",
              ]

              if users_partially_failed > 0:
                text_parts.append(f"{users_partially_failed} partially failed")
              if users_skipped > 0:
                text_parts.append(f"{users_skipped} skipped")

              base_text = ", ".join(text_parts)
              extra_text = msg.get("extra_text", None)
              if extra_text:
                base_text += f" | {extra_text}"
              final_text = f"{base_text} | {label}: {cumulative:,}"

              widget_lbl.configure(text=final_text)
      elif mtype == "complete":
        self.show_results_content(msg["data"])
      elif mtype == "error":
        messagebox.showerror(
            "Operation Failed", msg.get("message", "An unknown error occurred")
        )
        self.show_config_view()

  def process_log_queue(self):
    try:
      while not self.log_queue.empty():
        self.update_progress(self.log_queue.get_nowait())
    except:
      pass
    finally:
      self.after(100, self.process_log_queue)

  def start_scan(self):
    disclaimer_text = (
        "The estimations provided by this tool are calculated projections"
        " intended for preliminary planning only. Actual migration timelines"
        " (ETAs) and batch execution may vary based on real-time network"
        " conditions, source/target throttling policies, migration"
        " configurations, and the volume of delta migrations. The estimates do"
        " not constitute a performance guarantee or a binding service level"
        " agreement (SLA)."
    )
    should_proceed = messagebox.askokcancel(
        title="Estimation Disclaimer",
        message=disclaimer_text,
        parent=self,
    )
    if not should_proceed:
      return

    self.stop_scan_event.clear()
    self.chat_scan_results = None
    with self.log_lock:
      self.log_buffer = []
    self.spinners_active = {}
    self.spinner_indices = {}
    for w in self.scan_container.winfo_children():
      w.destroy()
    self.prog_widgets = {}

    self.create_progress_row(
        self.scan_container, "users", "Scanning Users", is_user=True
    )
    self.prog_user.start()
    self.create_progress_row(
        self.scan_container, "chats", "Scanning Private Chats"
    )
    self.create_progress_row(
        self.scan_container, "channels", "Scanning Channels"
    )

    self.create_progress_row(
        self.scan_container, "plan_generation", "Generating Migration Plan"
    )

    threading.Thread(target=self.execute_migration_scan).start()

  def stop_scan_logic(self):
    self.btn_action_primary.configure(state="disabled", text="Stopping scan...")
    self.stop_scan_event.set()
    with self.log_lock:
      self.log_buffer.append("Scan Stopped.")

  def log_msg(self, text):
    with self.log_lock:
      self.log_buffer.append(text)

  def ui_update(self, type, **kwargs):
    data = {"type": type}
    data.update(kwargs)
    self.log_queue.put(data)

  def execute_migration_scan(self):
    """Orchestrates the end-to-end migration estimation scan."""
    monitor = None
    try:
      self.log_msg("--- Starting Batch Scan ---")
      monitor = ResourceMonitor()
      monitor.start()
      start_time = time.time()

      # 1. Preparation
      config = self._get_scan_configuration()

      # 2. Authentication
      manager = self._authenticate_if_needed(config)

      self.factory = EstimatorFactory(
          config, manager, self.log_msg, self.stop_scan_event, None
      )

      # 3. User Discovery
      all_users, existing_data = self._resolve_target_users(config, manager)

      # 4. Build Batch List
      csv_rows, stats = self._prepare_batch_list(config, all_users, existing_data)

      for row in csv_rows:
        self.id_to_display_name[row["User ID / Group ID"]] = row[
            "User Principal Name / Group Mail"
        ]

      self.factory.set_id_to_display_name(self.id_to_display_name)
      user_csv_rows = [row for row in csv_rows if row["Type"] == "User"]

      self._run_scan_phases(config, manager, user_csv_rows, stats)

      # 6. Analysis & Reporting
      self._generate_final_report(
          config,
          user_csv_rows,
          stats,
          monitor,
          start_time,
      )

    except Exception as e:
      self.log_msg(f"Process failed: {e}")
      print(f"Process failed: {e}")
      self.ui_update("error", message=str(e))
    finally:
      if monitor is not None:
        monitor.stop()

  def _get_scan_configuration(self) -> ScanConfig:
    """Reads configuration variables from UI controls."""
    return ScanConfig(
        tenant_id=self.tenant_id.get().strip(),
        client_ids=[
            x.strip() for x in self.client_ids.get().split(",") if x.strip()
        ],
        client_secrets=[
            x.strip() for x in self.client_secrets.get().split(",") if x.strip()
        ],
        user_source=self.user_source.get(),
        csv_path=self.user_csv_path.get(),
        scan_chat=True,
        concurrency=self.concurrency.get(),
        load_multiplier=self.load_multiplier.get(),
        retries=self.retries.get(),
        backoff=self.backoff.get(),
        parallel_batches=self.parallel_batches.get(),
        mode=self.mode.get(),
        sample_percentage=self.sample_percentage.get(),
    )

  def _authenticate_if_needed(
      self, config: ScanConfig, use_single_app: bool = False
  ) -> Optional[TokenManager]:
    """Authenticates with MS Graph if any scanning is required."""
    if config.tenant_id and config.client_ids and config.client_secrets:
      manager = TokenManager(
          config.tenant_id,
          config.client_ids,
          config.client_secrets,
          config.concurrency if use_single_app is False else 1,
          config.retries,
          config.backoff,
      )
      return manager
    return None

  def _resolve_target_users(
      self,
      config: ScanConfig,
      manager: Optional[TokenManager],
  ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Resolves the list of users to process, either from CSV or Tenant."""
    existing_data = {}
    users_to_resolve = []
    all_users = []

    # 1. Parse CSV if applicable
    if config.user_source == "csv":
      if not config.csv_path or not os.path.exists(config.csv_path):
        raise Exception("CSV path invalid or file not found.")

      df_input = pd.read_csv(config.csv_path)
      df_input.columns = df_input.columns.str.strip()
      col = next(
          (
              c
              for c in ["User Principal Name", "Email", "UPN"]
              if c in df_input.columns
          ),
          None,
      )

      if col:
        for _, row in df_input.iterrows():
          upn = str(row[col]).lower().strip()
          existing_data[upn] = row.to_dict()

        users_to_resolve = df_input[col].dropna().unique().tolist()
      else:
        raise Exception(
            "CSV must contain 'User Principal Name' column."
        )

    # 2. Authenticate
    if not manager:
      if config.user_source == "tenant":
        raise Exception("Missing Credentials for Tenant Scan.")
      else:
        raise Exception(
            "Missing Credentials for Delta Scan (CSV missing some columns)."
        )
    # Determine scopes based on what is missing
    required_scopes = [
        "User.Read.All", 
        "Reports.Read.All", 
        "Chat.Read.All", 
        "ChannelMessage.Read.All",
        "ChannelSettings.Read.All",
        "TeamMember.Read.All",
        "Group.Read.All",
        "TeamsActivity.Read.All",
    ]

    manager.authenticate_all(None, required_scopes=required_scopes)

    self.ui_update("user_discovery", status="Fetching...", count=0)

    # 3. Resolve Users
    if config.user_source == "csv":
      self.log_msg("Delta Scan required. Resolving User IDs...")
      all_users = self._resolve_from_csv(manager, users_to_resolve)
    else:
      all_users = self._get_all_users_graph(manager)

    # Apply Load Multiplier
    mult = max(1, config.load_multiplier)
    if mult > 1:
      all_users = all_users * mult

    self.ui_update(
        "user_discovery",
        status="Done",
        count=len(all_users),
        user_count=len(all_users),
    )
    return all_users, existing_data

  def _prepare_batch_list(
      self,
      config: ScanConfig,
      all_users: List[Dict[str, Any]],
      existing_data: Dict[str, Any],
  ) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Prepares the initial list of user rows and stats."""
    csv_rows = []

    for u in all_users:
      upn = u["userPrincipalName"]
      key = str(upn).lower().strip()
      row = {
          "User Principal Name / Group Mail": upn,
          "User ID / Group ID": u["id"],
          "Type": "User",
      }
      csv_rows.append(row)

    stats = {}
    return csv_rows, stats





  def _run_scan_phases(
      self,
      config: ScanConfig,
      manager: Optional[TokenManager],
      csv_rows: List[Dict[str, Any]],
      stats: Dict[str, int],
  ) -> None:
    """Executes the data fetching phases."""
    self.ui_update("phase_status", source="chat", status="running")
    self._run_chat_phase(config, stats)
    self.ui_update("phase_status", source="chat", status="complete")



  def _generate_final_report(
      self,
      config: ScanConfig,
      csv_rows: List[Dict[str, Any]],
      stats: Dict[str, int],
      monitor: ResourceMonitor,
      start_time: float,
  ) -> None:
    """Calculates final stats, batches, and exports data."""
    self.ui_update("phase_status", source="plan_generation", status="running")
    time.sleep(0.5)

    self.ui_update(
        "scan_progress",
        source="plan_generation",
        progress=0.33,
        extra_text="Calculating ETAs...",
    )
    time.sleep(0.5)

    # 🚨 RICH FORMATTER: Process Chat estimation batches into the visualizer timeline.
    if hasattr(self, "chat_scan_batches") and self.chat_scan_batches:
      self.log_msg("Formatting Chat simulation batches...")
      # Performance tuning: Swift list comprehension allows instantaneous RAM-efficient duplication for flat primitive dict lists.
      chat_batches = [b.copy() for b in self.chat_scan_batches]

      num_parallel = max(1, self.parallel_batches.get())
      num_buckets = min(num_parallel, max(1, len(chat_batches)))
      buckets = [
          {"id": i + 1, "total": 0.0, "batches": []} for i in range(num_buckets)
      ]

      # Sort batches by ETA descending for optimal bucket packing
      chat_batches.sort(key=lambda x: x.get("eta", 0.0), reverse=True)

      for batch in chat_batches:
        target = min(buckets, key=lambda b: b["total"])
        target["batches"].append(batch)
        target["total"] += batch.get("eta", 0.0)

      all_chunks_with_time = []
      for b_idx, buck in enumerate(buckets):
        curr_t = 0.0
        for chunk in buck["batches"]:
          chunk["start_time"] = curr_t
          chunk["bucket_idx"] = b_idx
          curr_t += chunk.get("eta", 0.0)
          all_chunks_with_time.append(chunk)

      all_chunks_with_time.sort(
          key=lambda x: (x.get("start_time", 0.0), x.get("bucket_idx", 0))
      )

      batches = []
      for i, chunk in enumerate(all_chunks_with_time):
        batch_name = f"Batch {i+1}"
        chunk["name"] = batch_name
        batches.append(chunk)

      from util.db_manager import DatabaseManager
      with DatabaseManager("data/chat_migration_v2.db") as db:
        teams_cached = db.get_roster_teams()
        if teams_cached:
          for t in teams_cached:
            if t.get("id") and t.get("displayName"):
              self.id_to_display_name[t["id"]] = t["displayName"]

      teams_rows = []
      for chunk in all_chunks_with_time:
        if "team_ids" in chunk:
          for team_id in chunk["team_ids"]:
            display_name = self.id_to_display_name.get(team_id, team_id)
            teams_rows.append({
                "Team ID": team_id,
                "Team Name": display_name,
                "Suggested Batch": chunk["name"]
            })
      if teams_rows:
        self.df_teams_output = pd.DataFrame(teams_rows)

      total_eta = max((b["total"] for b in buckets), default=0.0)
    else:
      batches = []
      buckets = []
      total_eta = 0.0

    monitor.stop()
    monitor.join()
    elapsed = str(timedelta(seconds=int(time.time() - start_time)))
    avg_cpu, max_cpu, avg_ram, max_ram = monitor.get_stats()
    total_ram_gb = psutil.virtual_memory().total / (1024**3)
    total_cpu_cores = psutil.cpu_count(logical=True)

    self.log_msg("\n" + "=" * 40)
    self.log_msg(f"TOTAL TIME: {elapsed}")
    if hasattr(self, "chat_scan_results") and self.chat_scan_results:
      res = self.chat_scan_results
      self.log_msg(f"Total Users: {res.get('total_users', 0)}")
      self.log_msg(f"Total Teams: {res.get('total_teams', 0)}")
      self.log_msg(f"Channels: {res.get('channels', 0)}")
      self.log_msg(f"Channel Messages: {res.get('channel_messages', 0)}")
      self.log_msg(f"Private Chats: {res.get('private_chats', 0)}")
      self.log_msg(f"Private Chat Messages: {res.get('private_chat_messages', 0)}")
    self.log_msg(f"System: {total_cpu_cores} Cores, {total_ram_gb:.1f}GB RAM")
    self.log_msg(f"CPU Avg/Peak: {avg_cpu:.1f}% / {max_cpu:.1f}%")
    self.log_msg(f"RAM Avg/Peak: {avg_ram:.1f}% / {max_ram:.1f}%")
    self.log_msg("=" * 40)

    self.ui_update(
        "scan_progress",
        source="plan_generation",
        progress=0.66,
        extra_text="Generating reports...",
    )
    time.sleep(0.5)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = os.path.join("outputs", ts)
    os.makedirs(output_dir, exist_ok=True)

    logs_path = os.path.join(output_dir, f"logs_{ts}.log")

    if hasattr(self, "df_teams_output") and self.df_teams_output is not None:
      teams_report_path = os.path.join(output_dir, f"teams_report_{ts}.csv")
      self.df_teams_output.to_csv(teams_report_path, index=False)

      teams_batches_dir = os.path.join(output_dir, "suggested teams batches")
      os.makedirs(teams_batches_dir, exist_ok=True)
      unique_team_batches = self.df_teams_output["Suggested Batch"].unique()
      for batch in unique_team_batches:
        if not batch:
          continue
        batch_data = self.df_teams_output[self.df_teams_output["Suggested Batch"] == batch].copy()
        batch_export = batch_data[["Team ID"]].rename(
            columns={"Team ID": "Source MicrosoftTeamsID"}
        )
        safe_name = batch.replace(" ", "")
        batch_path = os.path.join(teams_batches_dir, f"{safe_name}.csv")
        batch_export.to_csv(batch_path, index=False)

    with self.log_lock:
      log_content = "\n".join(self.log_buffer)
    with open(logs_path, "w", encoding="utf-8") as f:
      f.write(log_content)

    result_data = {
        "total_users": 0,
        "total_items": 0,
        "total_eta": total_eta,
        "batches": batches,
        "buckets": buckets,
    }

    if hasattr(self, "chat_scan_results") and self.chat_scan_results:
      res = self.chat_scan_results
      result_data.update({
          "total_users": res.get("total_users", 0),
          "total_teams": res.get("total_teams", 0),
          "channels": res.get("channels", 0),
          "private_channels": res.get("private_channels", 0),
          "channel_messages": res.get("channel_messages", 0),
          "private_chats": res.get("private_chats", 0),
          "private_chat_messages": res.get("private_chat_messages", 0),
          "total_items": res.get("channel_messages", 0) + res.get("private_chat_messages", 0),
      })

    self.ui_update("phase_status", source="plan_generation", status="complete")
    time.sleep(2)
    self.ui_update("complete", data=result_data)





  def _get_all_users_graph(self, manager):
    users = []
    url = f"{GRAPH_BASE_URL}/users?$select=id,userPrincipalName&$top=999"
    token_data = manager.get_valid_token_slot()
    token = token_data["token"]
    session = manager.get_session()
    headers = {"Authorization": f"Bearer {token}"}
    try:
      while url and not self.stop_scan_event.is_set():
        # Check mid-loop for extremely long tenant scans
        if time.time() > token_data["expires_at"]:
          manager.return_token_slot(token_data)
          token_data = manager.get_valid_token_slot()
          token = token_data["token"]
          headers = {"Authorization": f"Bearer {token}"}

        r = session.get(url, headers=headers)
        if r.status_code != 200:
          break
        d = r.json()
        users.extend(d.get("value", []))
        url = d.get("@odata.nextLink")
        self.ui_update(
            "user_discovery", count=len(users), status="Scanning Tenant..."
        )
    finally:
      manager.return_token_slot(token_data)
    return users

  def _resolve_from_csv(self, manager, emails, url=None):
    if not emails:
      return []
    resolved = []
    if url is None:
      url = (
          "{GRAPH_BASE_URL}/users?$filter=userPrincipalName eq"
          " '{cln}'&$select=id,userPrincipalName"
      )

    def resolve_one(email):
      if self.stop_scan_event.is_set():
        return None
      token_data = manager.get_valid_token_slot()
      t = token_data["token"]
      s = manager.get_session()
      h = {"Authorization": f"Bearer {t}", "ConsistencyLevel": "eventual"}
      try:
        cln = email.replace("'", "''")
        u = url.format(GRAPH_BASE_URL=GRAPH_BASE_URL, cln=cln)
        r = s.get(u, headers=h)
        if r.status_code == 200 and r.json().get("value"):
          return r.json()["value"][0]
      except:
        pass
      finally:
        manager.return_token_slot(token_data)

    with ThreadPoolExecutor(max_workers=min(50, len(emails))) as exc:
      futures = [exc.submit(resolve_one, e) for e in emails]
      for f in as_completed(futures):
        if self.stop_scan_event.is_set():
          break
        res = f.result()
        if res:
          resolved.append(res)
          self.ui_update(
              "user_discovery", count=len(resolved), status="Resolving CSV..."
          )
    return resolved

  def _run_chat_phase(self, config: ScanConfig, stats: Dict[str, int]) -> None:
    """Hooks and orchestrates the encapsulated chat estimating lifecycle."""
    self.log_msg("\n--- Launching CHAT / TEAMS Integration Engine ---")
    try:
      # Retrieve singleton configured through the master factory
      estimator = self.factory.get_chat_estimator(hard_reset=True)
      chat_failures = []

      # The estimator will fire internal proxies which link directly back to our UI updater
      context_payload = {"ui_callback": self.ui_update}
      results = estimator.calculate_resource_count(
          context_payload, chat_failures
      )

      if results:
        self.log_msg("Chat Estimation success. Merging consolidated metrics...")
        # Cache onto local context instance for audit verification
        self.chat_scan_results = results

        c_msg = results.get("channel_messages", 0)
        p_msg = results.get("private_chat_messages", 0)
        self.log_msg(f"Captured: {c_msg} Channel Msgs, {p_msg} Chat Msgs.")

        # Calculate resource migration ETA
        eta = estimator.calculate_migration_eta(results)
        self.log_msg(f"Systemic Chat Overlap ETA: {eta:.2f} Hours")

        # Persist internal visualizer collection into memory for reporting layer
        self.chat_scan_batches = getattr(estimator, "last_batches", [])

        # Add data to final summary stats
        stats["chat_channels"] = results.get("channels", 0)
        stats["chat_messages"] = c_msg + p_msg

      else:
        self.log_msg("[WARNING] Service execution yielded empty metrics.")

    except Exception as e:
      self.log_msg(f"Critical integration trap within Chat phase: {e}")
      import traceback

      traceback.print_exc()

