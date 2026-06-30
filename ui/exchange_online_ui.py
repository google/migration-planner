from tkinter import filedialog, messagebox
import customtkinter as ctk
import re
from typing import Any, Callable, Dict, List, Optional, Tuple
import queue
import threading
import os
import math
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
import pandas as pd
import psutil
import time
import os
import subprocess
import sys

from util.connectors import TokenManager, UrlInvoker
from util.monitoring import ResourceMonitor
from util.utils import ScanConfig
from util.constants import *
from estimators.factory import EstimatorFactory
import ui.utils as ui_utils
from util.eo_utils import fetch_user_batch_data, fetch_calendar_events, calculate_batch_duration
from util.enums import FailureType

class MigrationEstimatorTool(ctk.CTk):
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
    self.scan_email = ctk.BooleanVar(value=True)
    self.scan_contact = ctk.BooleanVar(value=True)
    self.scan_calendar = ctk.BooleanVar(value=True)
    self.scan_in_place_archives = ctk.BooleanVar(value=True)
    self.scan_shared_mail_boxes = ctk.BooleanVar(value=True)
    self.scan_group_mail_boxes = ctk.BooleanVar(value=True)
    self.concurrency = ctk.IntVar(value=10)
    self.load_multiplier = ctk.IntVar(value=1)
    self.retries = ctk.IntVar(value=MAX_RETRIES)
    self.backoff = ctk.IntVar(value=BACKOFF)
    self.eta_min_users = ctk.IntVar(value=400)
    self.eta_max_users = ctk.IntVar(value=5000)
    self.eta_max_batches = ctk.IntVar(value=50)
    self.parallel_batches = ctk.IntVar(value=10)
    self.scan_result_csv_path = ctk.StringVar()
    self.id_to_display_name = {}

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

    # Title
    ctk.CTkLabel(
        self.header_frame,
        text="Migration Planner",
        font=FONT_HEADER_LARGE,
        text_color=COLOR_TEXT_MAIN,
    ).pack(anchor="center")

    ctk.CTkLabel(
        self.header_frame,
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
  def build_config_view(self):
    # """Builds the Configuration View."""
    ui_utils.build_configuration_view(self, ctk)

    # Header
    ui_utils.build_header(self, ctk)

    # Status Line
    ui_utils.build_status_line(self, ctk)

    # Main Content
    ui_utils.build_mail_input_frame(self, ctk)

    ctk.CTkLabel(
        self.scroll_connect,
        text="Source:",
        font=FONT_BODY_BOLD,
        text_color=COLOR_TEXT_SUB,
    ).pack(anchor="w", pady=(15, 5))

    source_selection_frame = ctk.CTkFrame(
        self.scroll_connect, fg_color="transparent"
    )
    source_selection_frame.pack(fill="x", anchor="w")
    ctk.CTkRadioButton(
        source_selection_frame,
        text="Scan All Users",
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

    # Advanced Settings
    ui_utils.build_advanced_settings_frame(self, ctk)

    # Advanced Content
    ui_utils.build_eo_resource_checkbox_list(self, ctk)
    
    # Concurrency settings
    ui_utils.build_concurrency_settings_slider(self, ctk)

    # Migration Plan Options
    ui_utils.build_migration_plan_options(self, ctk)

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
        self.scan_container, "entities", "Scanning Entities", is_user=True
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

      # Cards
      card_frame = ctk.CTkFrame(self.view_results, fg_color="transparent")
      card_frame.pack(fill="x", pady=10)

      self.create_stat_card(
          card_frame, "Users", f"{data['total_users']:,}", "👥"
      )
      self.create_stat_card(
          card_frame, "Emails", f"{data['total_emails']:,}", "📩"
      )
      self.create_stat_card(
          card_frame,
          "Calendar Events",
          f"{data['total_events']:,}",
          "📅",
          sub=f"({data['total_calendars']:,} Calendars)",
      )
      self.create_stat_card(
          card_frame, "Contacts", f"{data['total_contacts']:,}", "📞"
      )
      self.create_stat_card(
          card_frame, "In-Place Archives", f"{data['total_in_place_archives']:,}", "🗃️"
      )
      self.create_stat_card(
          card_frame, "Shared Mails", f"{data['total_shared_mails']:,}", "👥📧"
      )
      self.create_stat_card(
          card_frame, 
          "Group Mails", 
          f"{data['total_group_mails']:,}",
          "👥📧",
          sub=f"({data['total_group_threads']:,} {'Group Thread' if data['total_group_threads'] == 1 else 'Group Threads'})",
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
        tl_scale = ctk.CTkFrame(tl_row, fg_color="transparent", height=20)
        tl_scale.pack(side="left", fill="x", expand=True, padx=(15, 100))

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
              tl_scale,
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

  def create_progress_row(self, parent, key, title, is_user=False, mode=None):
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

    if mode is None:
      if is_user or key == "users":
        mode = "indeterminate"
      else:
        mode = "determinate"

    bar = ctk.CTkProgressBar(
        f,
        height=8,
        mode=mode,
        progress_color=COLOR_PRIMARY,
        fg_color=COLOR_OUTLINE_LIGHT,
        corner_radius=4,
    )
    if mode == "determinate":
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
      self, parent, col_idx, icon, title, desc, link_text, link_url
  ):
    f = ctk.CTkFrame(
        parent,
        fg_color=COLOR_SURFACE,
        height=100,
        corner_radius=12,
        border_color=COLOR_OUTLINE_LIGHT,
        border_width=1,
    )
    f.grid(row=0, column=col_idx, sticky="ew", padx=10, pady=5)
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
    users_str = self.format_metric(batch["users"])
    emails_str = self.format_metric(batch["total_emails"])
    events_str = self.format_metric(batch["total_events"])
    contacts_str = self.format_metric(batch["total_contacts"])
    in_place_archive_str = self.format_metric(batch["total_in_place_archives"])
    shared_mailbox_str = self.format_metric(batch["total_shared_mails"])
    group_mail_box_str = self.format_metric(batch["total_group_mails"])
    info = (
        f"{batch['name']} - {users_str} 👥  |  {emails_str} 📩  |  {events_str}"
        f" 📅  |  {contacts_str} 📞 |  {in_place_archive_str} 🗃️ |  {shared_mailbox_str} 👥📧 | {group_mail_box_str} 👥📧"
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
      self.adv_frame.pack_forget()
      self.btn_adv.configure(text="Show Advanced Settings ▼")
      self.adv_visible = False
    else:
      self.adv_frame.pack(fill="x", pady=10, after=self.btn_adv)
      self.btn_adv.configure(text="Hide Advanced Settings ▲")
      self.adv_visible = True

  def browse_user_csv(self):
    f = filedialog.askopenfilename(filetypes=[("CSV", "*.csv")])
    if f:
      self.user_csv_path.set(f)

  def export_report(self, data):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    f = filedialog.asksaveasfilename(
        initialfile=f"migration_report_{ts}.csv", defaultextension=".csv"
    )
    if f and "df" in data:
      data["df"].to_csv(f, index=False)

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
        widget.configure(
            text=icon, text_color=COLOR_PRIMARY
        ) 
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
        shared_mailbox_count = msg.get("shared_mailbox_count", 0)
        group_count = msg.get("group_count", 0)
        status = msg.get("status", "Scanning...")
        if "users" in self.prog_widgets:
          widget = self.prog_widgets["users"]["lbl"]
          if widget.winfo_exists():
            text = f"{count} entities (users / shared mailboxes / group mailboxes)"
            if user_count > 0:
              text += f" | {user_count} users" if user_count > 1 else f" | {user_count} user"
            if shared_mailbox_count > 0:
              text += f" | {shared_mailbox_count} shared mailboxes" if shared_mailbox_count > 1 else f" | {shared_mailbox_count} shared mailbox"
            if group_count > 0:
              text += f" | {group_count} groups" if group_count > 1 else f" | {group_count} group"
            widget.configure(
                text=text
            )
          if not self.spinners_active.get("users"):
            self.spinners_active["users"] = True
            self.animate_spinner("users")
        if status == "Done":
          self.spinners_active["users"] = False
          if "users" in self.prog_widgets:
            widget_icon = self.prog_widgets["users"]["icon"]
            if widget_icon.winfo_exists():
              widget_icon.configure(
                  text="✓", text_color=COLOR_SUCCESS
              )
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
              widget_icon.configure(
                  text="✓", text_color=COLOR_SUCCESS
              )
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
                      f"{entity_type}: {users_proc - users_fail} succeeded , {users_fail}"
                      f" failed | {extra}"
                  )
              )
          elif source == "plan_generation":
            widget_lbl = self.prog_widgets[source]["lbl"]
            if widget_lbl.winfo_exists():
              widget_lbl.configure(
                  text=msg.get("extra_text", "")
              )
          else:
            if source == "messages":
              label = "Emails"
            elif source == "contacts":
              label = "Contacts"
            elif source == "in_place_archives":
              label = "Archived Emails"
            elif source == "shared_mails":
              label = "Emails"
            elif source == "group_mails":
              label = "Posts"
            else:
              label = source
              
            widget_lbl = self.prog_widgets[source]["lbl"]
            if widget_lbl.winfo_exists():
              text_parts = [
                  f"{entity_type}: {users_proc - users_fail - users_partially_failed} succeeded",
                  f"{users_fail} failed"
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
              
              widget_lbl.configure(
                  text=final_text
              )
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
    with self.log_lock:
      self.log_buffer = []
    self.spinners_active = {}
    self.spinner_indices = {}
    for w in self.scan_container.winfo_children():
      w.destroy()
    self.prog_widgets = {}

    self.create_progress_row(
        self.scan_container, "users", "Scanning Entities", is_user=True
    )
    self.prog_user.start()
    if self.scan_email.get():
      self.create_progress_row(
          self.scan_container, "messages", "Scanning Emails"
      )
    if self.scan_contact.get():
      self.create_progress_row(
          self.scan_container, "contacts", "Scanning Contacts"
      )
    if self.scan_calendar.get():
      self.create_progress_row(
          self.scan_container, "calendars", "Scanning Calendars"
      )
    if self.scan_in_place_archives.get():
      self.create_progress_row(
          self.scan_container, "in_place_archives", "Scanning In-Place Archives"
      )
    if self.scan_shared_mail_boxes.get():
      self.create_progress_row(
          self.scan_container, "shared_mails", "Scanning Shared Mailboxes"
      )
    if self.scan_group_mail_boxes.get():
      self.create_progress_row(
          self.scan_container, "group_mails", "Scanning Group Mailboxes"
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
      one_token_per_app_manager = None
      if config.scan_in_place_archives:
        one_token_per_app_manager = self._authenticate_if_needed(config, use_single_app=True)
      self.factory = EstimatorFactory(config, manager, one_token_per_app_manager, self.log_msg, self.stop_scan_event, None)
      
      # 3. User Discovery
      all_users, existing_data, groups, shared_mailboxes = self._resolve_target_users(config, manager, one_token_per_app_manager)

      # 4. Build Batch List
      csv_rows, stats = self._prepare_batch_list(
          config, all_users, existing_data, groups, shared_mailboxes
      )

      for row in csv_rows:
        self.id_to_display_name[row["User ID / Group ID"]] = row["User Principal Name / Group Mail"]

      self.factory.set_id_to_display_name(self.id_to_display_name)
      user_csv_rows = [row for row in csv_rows if row["Type"] == "User"]
      shared_csv_rows = [row for row in csv_rows if row["Type"] == "Shared"]
      group_mail_rows = [row for row in csv_rows if row["Type"] == "Group Mailbox"]

      self._run_scan_phases(config, manager, one_token_per_app_manager, user_csv_rows, shared_csv_rows, group_mail_rows, stats)

      # 6. Analysis & Reporting
      self._generate_final_report(config, user_csv_rows, shared_csv_rows, group_mail_rows, stats, monitor, start_time)

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
        scan_email=self.scan_email.get(),
        scan_contact=self.scan_contact.get(),
        scan_calendar=self.scan_calendar.get(),
        scan_in_place_archives=self.scan_in_place_archives.get(),
        scan_shared_mail_boxes=self.scan_shared_mail_boxes.get(),
        scan_group_mail_boxes=self.scan_group_mail_boxes.get(),
        concurrency=self.concurrency.get(),
        load_multiplier=self.load_multiplier.get(),
        retries=self.retries.get(),
        backoff=self.backoff.get(),
        eta_max_users=self.eta_max_users.get(),
        parallel_batches=self.parallel_batches.get(),
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
      self, config: ScanConfig, manager: Optional[TokenManager], one_token_per_app_manager: Optional[TokenManager] = None
  ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Resolves the list of users to process, either from CSV or Tenant."""
    existing_data = {}
    users_to_resolve = []
    groups_to_resolve = []
    all_users = []

    # Flags to determine if we need to scan
    have_email = False
    have_contact = False
    have_calendar = False
    have_in_place_archives = False
    have_shared_mails = False
    have_group_mailboxes = False

    # 1. Parse CSV if applicable
    if config.user_source == "csv":
      if not config.csv_path or not os.path.exists(config.csv_path):
        raise Exception("CSV path invalid or file not found.")

      df_input = pd.read_csv(config.csv_path)
      df_input.columns = df_input.columns.str.strip()
      rename_map = {
          "Email Id": "User Principal Name",
          "Calendar Event Count": "Event Count",
      }
      df_input.rename(columns=rename_map, inplace=True)
      col = next(
          (
              c
              for c in ["User Principal Name", "Email", "UPN"]
              if c in df_input.columns
          ),
          None,
      )

      if col:
        if "Email Count" in df_input.columns:
          have_email = True
        if "Contact Count" in df_input.columns:
          have_contact = True
        if (
            "Calendar Count" in df_input.columns
            and "Event Count" in df_input.columns
        ):
          have_calendar = True
        if "Group Post Count" in df_input.columns:
          have_group_mailboxes = True
        if "Shared Mail Count" in df_input.columns:
          have_shared_mails = True
        if "In Place Archive Count" in df_input.columns:
          have_in_place_archives = True

        have_type_col = "Type" in df_input.columns
        for _, row in df_input.iterrows():
          upn = str(row[col]).lower().strip()
          existing_data[upn] = row.to_dict()
          mail_type = row["Type"] if have_type_col else "User"
          if mail_type not in ["User", "Shared", "Group Mailbox"]:
            self.log_msg(f"Unknown mail type for {upn}: {mail_type}. Proceeding with the assumption it is a user!")
            mail_type = "User"
          
          if mail_type == "Group Mailbox":
            groups_to_resolve.append(row[col])

        users_to_resolve = df_input[col].dropna().unique().tolist()
      else:
        raise Exception(
            "CSV must contain 'Email Id' or 'User Principal Name' column."
        )

      # Strip all the white spaces
      users_to_resolve = [user.strip() for user in users_to_resolve]
      groups_to_resolve = [group.strip() for group in groups_to_resolve]

    # 2. Determine if we need live scanning
    scanning_required = (
        (config.scan_email and not have_email)
        or (config.scan_contact and not have_contact)
        or (config.scan_calendar and not have_calendar)
        or (config.scan_in_place_archives and not have_in_place_archives)
        or (config.scan_shared_mail_boxes and not have_shared_mails)
        or (config.scan_group_mail_boxes and not have_group_mailboxes)
    )

    # 3. Authenticate if required
    if scanning_required or config.user_source == "tenant":
      if not manager:
        # If we need to scan but have no manager (missing creds)
        if config.user_source == "tenant":
          raise Exception("Missing Credentials for Tenant Scan.")
        else:
          raise Exception(
              "Missing Credentials for Delta Scan (CSV missing some columns)."
          )
      if not one_token_per_app_manager and config.scan_in_place_archives and not have_in_place_archives:
        raise Exception("Missing Credentials for In Place Archive Scan.")

      # Determine scopes based on what is missing
      required_scopes = ["User.Read.All"]
      if config.scan_email and not have_email:
        required_scopes.append("Mail.Read")
      if config.scan_contact and not have_contact:
        required_scopes.append("Contacts.Read")
      if config.scan_calendar and not have_calendar:
        required_scopes.append("Calendars.Read")
      if config.scan_shared_mail_boxes and not have_shared_mails:
        required_scopes.append("MailboxSettings.Read")
        if not (config.scan_email and not have_email):          # If Email Scan is diabled then add Mail.Read as otherwise Mail.Read is alreay added
          required_scopes.append("Mail.Read")
      if config.scan_in_place_archives and not have_in_place_archives:
        required_scopes.append("MailboxFolder.Read.All")
      if config.scan_group_mail_boxes and not have_group_mailboxes:
        required_scopes.append("Group.Read.All")

      manager.authenticate_all(self.log_msg, required_scopes=required_scopes)
      if one_token_per_app_manager:
        one_token_per_app_manager.authenticate_all(self.log_msg, required_scopes=[
          "User.Read.All", "MailboxFolder.Read.All"])
    else:
      self.log_msg(
          "Skipping Authentication (All required data present in CSV)."
      )

    self.ui_update("user_discovery", status="Fetching...", count=0)

    # 4. Resolve Users
    if config.user_source == "csv":
      if scanning_required:
        self.log_msg("Delta Scan required. Resolving User IDs...")
        all_users = self._resolve_from_csv(manager, users_to_resolve)
      else:
        self.log_msg("Using CSV data directly...")
        all_users = [
            {"userPrincipalName": u, "id": None} for u in users_to_resolve
        ]
    else:
      all_users = self._get_all_users_graph(manager)
    
    shared_mailboxes = []
    if config.scan_shared_mail_boxes and scanning_required:
      estimator = self.factory.get_shared_mailbox_estimator(hard_reset=True)
      shared_mailbox_ids = estimator._get_shared_mail_boxes([user["id"] for user in all_users], [])
      personal_users = [user for user in all_users if user["id"] not in shared_mailbox_ids]
      shared_mailboxes = [user for user in all_users if user["id"] in shared_mailbox_ids]
      all_users = personal_users

    groups = []
    if config.scan_group_mail_boxes:
      estimator = self.factory.get_group_mailbox_estimator(hard_reset=True)
      if config.user_source == "csv":
        if not have_group_mailboxes:
          self.log_msg("Delta Scan required. Resolving Group IDs...")
          url = (
              "{GRAPH_BASE_URL}/groups?$filter=mail eq"
              " '{cln}'&$select=id,mail"
          )
          groups = self._resolve_from_csv(manager, groups_to_resolve, url)
        else:
          groups = [{"id": None, "mail": row} for row in groups_to_resolve]
      else:
        groups = estimator.get_group_id_to_mail_mapping()
          
    # Apply Load Multiplier
    mult = max(1, config.load_multiplier)
    if mult > 1:
      all_users = all_users * mult

    self.ui_update(
      "user_discovery", 
      status="Done", 
      count=len(all_users) + len(shared_mailboxes) + len(groups), 
      user_count=len(all_users), 
      shared_mailbox_count=len(shared_mailboxes), 
      group_count=len(groups)
    )
    return all_users, existing_data, groups, shared_mailboxes

  def _prepare_batch_list(
      self,
      config: ScanConfig,
      all_users: List[Dict[str, Any]],
      existing_data: Dict[str, Any],
      groups: List[Dict[str, Any]],
      shared_mailboxes: List[Dict[str, Any]],
  ) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Prepares the initial list of user rows and stats."""
    csv_rows = []

    # Check what we have in existing_data
    sample = next(iter(existing_data.values())) if existing_data else {}
    have_email = "Email Count" in sample
    have_contact = "Contact Count" in sample
    have_calendar = "Calendar Count" in sample and "Event Count" in sample
    have_in_place_archives = "In Place Archive Count" in sample
    have_shared_mails = "Shared Mail Count" in sample
    have_group_mails = "Group Post Count" in sample and "Group Thread Count" in sample

    def safe_int(val):
      try:
        return int(float(val))
      except:
        return 0
    
    def _is_valid_email( val):
      return bool(re.match(r'^[^@]+@[^@]+\.[^@]+$', val))

    for u in all_users:
      upn = u["userPrincipalName"]
      key = str(upn).lower().strip()
      otherAliases = []
      if "proxyAddresses" in u:
        for alias in u["proxyAddresses"]:
          alias_str = str(alias).strip()
          if alias_str.lower().startswith("smtp:"):
            email_alias = alias_str[5:].lower().strip()
            if email_alias != key and _is_valid_email(email_alias):
              otherAliases.append(email_alias)

      row = {
          "User Principal Name / Group Mail": upn,
          "User ID / Group ID": u["id"],
          "Other Aliases": ';'.join(otherAliases),
          "Type": "User",
          "Email Count": 0,
          "Contact Count": 0,
          "Calendar Count": 0,
          "Event Count": 0,
          "In Place Archive Count": 0,
          "Shared Mail Count": 0,
          "Group Post Count": 0,
          "Group Thread Count": 0
      }
      if key in existing_data:
        src = existing_data[key]
        if have_email and config.scan_email:
          row["Email Count"] = safe_int(src.get("Email Count", 0))
        if have_contact and config.scan_contact:
          row["Contact Count"] = safe_int(src.get("Contact Count", 0))
        if have_calendar and config.scan_calendar:
          row["Calendar Count"] = safe_int(src.get("Calendar Count", 0))
          row["Event Count"] = safe_int(src.get("Event Count", 0))
        if have_in_place_archives and config.scan_in_place_archives:
          row["In Place Archive Count"] = safe_int(src.get("In Place Archive Count", 0))
        if have_shared_mails and config.scan_shared_mail_boxes:
          row["Shared Mail Count"] = safe_int(src.get("Shared Mail Count", 0))
      csv_rows.append(row)
    
    for u in groups:
      mail_id = u["mail"]
      key = str(mail_id).lower().strip()
      row = {
          "User Principal Name / Group Mail": mail_id,
          "User ID / Group ID": u["id"],
          "Type": "Group Mailbox",
          "Email Count": 0,
          "Contact Count": 0,
          "Calendar Count": 0,
          "Event Count": 0,
          "In Place Archive Count": 0,
          "Shared Mail Count": 0,
          "Group Post Count": 0,
          "Group Thread Count": 0
      }
      if key in existing_data:
        src = existing_data[key]
        if have_group_mails and config.scan_group_mail_boxes:
          row["Group Post Count"] = safe_int(src.get("Group Post Count", 0))
          row["Group Thread Count"] = safe_int(src.get("Group Thread Count", 0))
      csv_rows.append(row)

    for mailbox in shared_mailboxes:
      row = {
          "User Principal Name / Group Mail": mailbox["userPrincipalName"],
          "User ID / Group ID": mailbox["id"],
          "Type": "Shared",
          "Email Count": 0,
          "Contact Count": 0,
          "Calendar Count": 0,
          "Event Count": 0,
          "In Place Archive Count": 0,
          "Shared Mail Count": 0,
          "Group Post Count": 0,
          "Group Thread Count": 0
      }
      key = str(mailbox["userPrincipalName"]).lower().strip()
      if key in existing_data:
        src = existing_data[key]
        if have_shared_mails and config.scan_shared_mail_boxes:
          row["Shared Mail Count"] = safe_int(src.get("Shared Mail Count", 0))
      csv_rows.append(row)

    stats = {
        "emails": sum(r["Email Count"] for r in csv_rows),
        "contacts": sum(r["Contact Count"] for r in csv_rows),
        "calendars": sum(r["Calendar Count"] for r in csv_rows),
        "events": sum(r["Event Count"] for r in csv_rows),
        "in_place_archives": sum(r["In Place Archive Count"] for r in csv_rows),
        "shared_mails": sum(r["Shared Mail Count"] for r in csv_rows),
        "group_mails": sum(r["Group Post Count"] for r in csv_rows),
        "group_threads": sum(r["Group Thread Count"] for r in csv_rows)
    }
    return csv_rows, stats

  def run_batch_scan(
    self, 
    resource_type: str,
    config: ScanConfig,
    user_chunks: List[List[Dict[str, Any]]],
    manager: Optional[TokenManager],
    one_token_per_app_manager: Optional[TokenManager],
    stats: Dict[str, int],
    total_users: int,
    log_freq: int = 10
  ) -> List[Dict[str, str]]: 
    self.log_msg(f"\n--- Starting {resource_type.upper()} Scan ---")
    processed_users = 0
    phase_total = 0
    users_failed = 0
    users_partially_failed = 0

    executor = ThreadPoolExecutor(max_workers=config.concurrency)
    estimator: Estimator = None
    if resource_type == "in_place_archives":
      estimator = self.factory.get_in_place_archive_estimator(use_delta_api=False)
    elif resource_type == "shared_mails":
      estimator = self.factory.get_shared_mailbox_estimator()
    
    total_failures = []
    partial_failures = []

    # Failsafe to avoid thread leak in case of unexpected failures
    chunk_count = 0
    try:
      future_to_chunk_map: Dict[Future, List] = {}
      future_to_failures_map: Dict[Future, List[Dict[str, Any]]] = {}
      for chunk in user_chunks:
        failures = []
        future = executor.submit(estimator.calculate_resource_count, 
            {"user_ids" : [row["User ID / Group ID"] if row["User ID / Group ID"] is not None else row["User Principal Name / Group Mail"] for row in chunk]}, failures)
        future_to_chunk_map[future] = chunk
        future_to_failures_map[future] = failures

      for f in as_completed(future_to_chunk_map):
        if self.stop_scan_event.is_set():
          break

        chunk = future_to_chunk_map[f]
        chunk_result = f.result()
        chunk_count += 1
      
        processed_users += len(chunk)
        chunk_total = sum(value for user_id, value in chunk_result.items())
        phase_total += chunk_total
        
        id_to_upn_map = {row["User ID / Group ID"]: row["User Principal Name / Group Mail"] for row in chunk if row.get("User ID / Group ID") is not None}
        
        complete_failures = [failure for failure in future_to_failures_map[f] if failure["isPartial"] == False]
        chunk_partial_failures = [failure for failure in future_to_failures_map[f] if failure["isPartial"] == True]
        total_failures += [
          {
            "user": id_to_upn_map.get(failure["userId"], failure["userId"]), 
            "cause": f"[{failure['statusCode'] if 'statusCode' in failure else None}] {failure['message']}", 
            "type": failure.get("type")
          } for failure in complete_failures if failure["userId"] is not None] 
        partial_failures += [
          {
            "user": id_to_upn_map.get(failure["userId"], failure["userId"]), 
            "cause": f"[{failure['statusCode'] if 'statusCode' in failure else None}] {failure['message']}", 
            "failed_resource": failure["folderId"] if "folderId" in failure else None,
            "type": failure.get("type")
          } for failure in chunk_partial_failures if failure["userId"] is not None]
        users_failed += len(
            set(failure["userId"] for failure in complete_failures if failure["userId"] is not None)
        )
        users_partially_failed += len(
            set(failure["userId"] for failure in chunk_partial_failures if failure["userId"] is not None)
        )

        # Update Progress
        if chunk_count % log_freq == 0 or chunk_count == len(user_chunks):
          self.log_msg(
                f"Processed {processed_users}/{total_users} | Failed:"
                f" {users_failed}/{total_users} | {resource_type}: {phase_total}"
            )
        prog = processed_users / total_users if total_users > 0 else 0
        self.ui_update(
          "scan_progress",
          entity_type="Users" if resource_type == "in_place_archives" else "Shared Mailboxes",
          source=resource_type,
          progress=prog,
          cumulative=phase_total,
          processed=processed_users,
          failed=users_failed,
          partially_failed=users_partially_failed,
          total=total_users,
          extra_text="",
        )

        # Update stats
        for user in chunk:
          # Use the same key resolution logic as was used for submission
          key = user["User ID / Group ID"] if user["User ID / Group ID"] is not None else user["User Principal Name / Group Mail"]
          
          if resource_type == "in_place_archives":
            user["In Place Archive Count"] = chunk_result.get(key, 0)
          elif resource_type == "shared_mails":
            user["Shared Mail Count"] = chunk_result.get(key, 0)
        
        stats[resource_type] += chunk_total
        
      if self.stop_scan_event.is_set():
        pending_users_count = total_users - processed_users
        self.ui_update(
          "scan_progress",
          entity_type="Users" if resource_type == "in_place_archives" else "Shared Mailboxes",
          source=resource_type,
          progress=1.0,
          cumulative=phase_total,
          processed=processed_users,
          success=processed_users - users_failed,
          failed=users_failed,
          skipped=pending_users_count,
          total=total_users,
          extra_text="",
        )
        executor.shutdown(wait=False, cancel_futures=True)
    except Exception as e:
      self.ui_update(
        "scan_progress",
        entity_type="Users" if resource_type == "in_place_archives" else "Shared Mailboxes",
        source=resource_type,
        progress=1.0,
        cumulative=phase_total,
        processed=total_users,
        failed=total_users - processed_users + users_failed,
        total=total_users,
        extra_text="",
      )
      self.log_msg(f"Error in {resource_type} scan: {e}")
    finally:
      executor.shutdown(wait=False)
      estimator.shutdown()

    return total_failures, partial_failures

  def _run_group_mail_phases(
      self,
      config: ScanConfig,
      manager: Optional[TokenManager],
      group_chunks: List[List[Dict[str, Any]]],
      stats: Dict[str, int],
      total_groups: int
  ) -> None:
    if self.stop_scan_event.is_set():
      return
    """Executes the data fetching phases for group mailboxes."""
    self.log_msg("\n--- Starting GROUP MAIL BOX Scan ---")
    processed_groups = 0
    phase_total = 0
    phase_threads = 0
    groups_failed = 0
    resource_type = "group_mails"
    log_freq = 10

    executor = ThreadPoolExecutor(max_workers=config.concurrency)
    estimator = self.factory.get_group_mailbox_estimator(hard_reset=True)
    
    total_failures = []

    # Failsafe to avoid thread leak in case of unexpected failures
    chunk_count = 0
    try:
      future_to_chunk_map: Dict[Future, List] = {}
      future_to_failures_map: Dict[Future, List[Dict[str, Any]]] = {}
      for chunk in group_chunks:
        failures = []
        future = executor.submit(estimator.calculate_resource_count, 
            {"group_ids" : [row["User ID / Group ID"] for row in chunk]}, failures)
        future_to_chunk_map[future] = chunk
        future_to_failures_map[future] = failures

      for f in as_completed(future_to_chunk_map):
        if self.stop_scan_event.is_set():
          break

        chunk = future_to_chunk_map[f]
        chunk_result_posts, chunk_result_threads = f.result()
        chunk_count += 1
      
        processed_groups += len(chunk)
        chunk_posts = sum(value for user_id, value in chunk_result_posts.items())
        chunk_threads = sum(value for user_id, value in chunk_result_threads.items())
        phase_total += chunk_posts
        phase_threads += chunk_threads
        complete_failures = [failure for failure in future_to_failures_map[f] if failure["isPartial"] == False]

        id_to_upn_map = {row["User ID / Group ID"]: row["User Principal Name / Group Mail"] for row in chunk if row.get("User ID / Group ID") is not None}

        total_failures += [
          {
            "group": id_to_upn_map.get(failure["groupId"], failure["groupId"]), 
            "cause": f"[{failure['statusCode'] if 'statusCode' in failure else None}] {failure['message']}", 
            "type": failure.get("type")
          } for failure in complete_failures if failure.get("groupId") is not None] 
        
        groups_failed += len(
            set(failure["groupId"] for failure in complete_failures if failure.get("groupId") is not None)
        )

        # Update Progress
        if chunk_count % log_freq == 0 or chunk_count == len(group_chunks):
          self.log_msg(
                f"Processed {processed_groups}/{total_groups} | Failed:"
                f" {groups_failed}/{total_groups} | Threads: {phase_threads} | Posts: {phase_total}"
            )
        prog = processed_groups / total_groups if total_groups > 0 else 0
        self.ui_update(
          "scan_progress",
          entity_type="Group Mailboxes",
          source=resource_type,
          progress=prog,
          cumulative=phase_total,
          processed=processed_groups,
          failed=groups_failed,
          total=total_groups,
          extra_text=f"Threads: {phase_threads}",
        )

        # Update stats
        for group in chunk:
          key = group["User ID / Group ID"]
          group["Group Post Count"] = chunk_result_posts.get(key, 0)
          group["Group Thread Count"] = chunk_result_threads.get(key, 0)
        
        stats[resource_type] += chunk_posts
        stats["group_threads"] += chunk_threads
        
      if self.stop_scan_event.is_set():
        pending_groups_count = total_groups - processed_groups
        self.log_msg(
          f"Processed {processed_groups}/{total_groups} | Failed:"
          f" {groups_failed}/{total_groups} | Skipped: {pending_groups_count}/{total_groups} | Threads: {phase_threads} | Posts: {phase_total}"
        )
        self.ui_update(
          "scan_progress",
          entity_type="Group Mailboxes",
          source=resource_type,
          progress=1.0,
          cumulative=phase_total,
          processed=processed_groups,
          success=processed_groups - groups_failed,
          failed=groups_failed,
          skipped=pending_groups_count,
          total=total_groups,
          extra_text=f"Threads: {phase_threads}",
        )
        executor.shutdown(wait=False, cancel_futures=True)
    except Exception as e:
      self.ui_update(
        "scan_progress",
        entity_type="Group Mailboxes",
        source=resource_type,
        progress=1.0,
        cumulative=phase_total,
        processed=total_groups,
        failed=total_groups - processed_groups + groups_failed,
        total=total_groups,
        extra_text=f"Threads: {phase_threads}",
      )
      self.log_msg(f"Error in group mail box scan: {e}")
    finally:
      executor.shutdown(wait=False)
      estimator.shutdown()

    return total_failures


  def _run_scan_phases(
      self,
      config: ScanConfig,
      manager: Optional[TokenManager],
      one_token_per_app_manager: Optional[TokenManager],
      csv_rows: List[Dict[str, Any]],
      shared_csv_rows: List[Dict[str, Any]],
      group_csv_rows: List[Dict[str, Any]],
      stats: Dict[str, int],
  ) -> None:
    """Executes the data fetching phases."""
    num_apps = len(config.client_ids) if config.client_ids else 1
    workers = num_apps * config.concurrency
    batch_size = min(self.concurrency.get() // 10, 20)
    user_chunks = [
        csv_rows[i : i + batch_size]
        for i in range(0, len(csv_rows), batch_size)
    ]

    shared_chunks = [
        shared_csv_rows[i : i + batch_size]
        for i in range(0, len(shared_csv_rows), batch_size)
    ]

    group_chunks = [
        group_csv_rows[i : i + batch_size]
        for i in range(0, len(group_csv_rows), batch_size)
    ]
    total_users = len(csv_rows)
    total_shared = len(shared_csv_rows)
    total_groups = len(group_csv_rows)

    has_email_data = any(r["Email Count"] > 0 for r in csv_rows)
    has_contact_data = any(r["Contact Count"] > 0 for r in csv_rows)
    has_calendar_data = any(r["Calendar Count"] > 0 for r in csv_rows)
    has_in_place_archives_data = any(r["In Place Archive Count"] > 0 for r in csv_rows)
    has_shared_mails_data = any(r["Shared Mail Count"] > 0 for r in shared_csv_rows)
    has_group_mails_data = any(r["Group Post Count"] > 0 for r in group_csv_rows)
    failed_emails = []
    failed_contacts = []
    failed_calendars = []
    failed_in_place_archives = []
    failed_shared_mails = []
    failed_group_mails = []
    partial_in_place_archive_failures = []
    partial_shared_mail_box_failures = []

    can_scan = manager is not None

    if config.scan_email:
      if can_scan and (not has_email_data or config.user_source == "tenant"):
        self.ui_update("phase_status", source="messages", status="running")
        failed_emails = self.run_batch_phase_ui(
            user_chunks, "messages", manager, workers, stats, total_users
        )
        self.ui_update("phase_status", source="messages", status="complete")
      else:
        self.log_msg("Skipping Email Scan (Data present or No Auth)")
        self.ui_update("phase_status", source="messages", status="running")
        self.ui_update(
            "scan_progress",
            source="messages",
            progress=1.0,
            cumulative=stats["emails"],
            processed=total_users,
            total=total_users,
        )
        self.ui_update("phase_status", source="messages", status="complete")

    if config.scan_contact:
      if can_scan and (not has_contact_data or config.user_source == "tenant"):
        self.ui_update("phase_status", source="contacts", status="running")
        failed_contacts = self.run_batch_phase_ui(
            user_chunks, "contacts", manager, workers, stats, total_users
        )
        self.ui_update("phase_status", source="contacts", status="complete")
      else:
        self.log_msg("Skipping Contact Scan (Data present or No Auth)")
        self.ui_update("phase_status", source="contacts", status="running")
        self.ui_update(
            "scan_progress",
            source="contacts",
            progress=1.0,
            cumulative=stats["contacts"],
            processed=total_users,
            total=total_users,
        )
        self.ui_update("phase_status", source="contacts", status="complete")

    if config.scan_calendar:
      if can_scan and (not has_calendar_data or config.user_source == "tenant"):
        self.ui_update("phase_status", source="calendars", status="running")
        failed_calendars = self.run_batch_phase_ui(
            user_chunks, "calendars", manager, workers, stats, total_users
        )
        self.ui_update("phase_status", source="calendars", status="complete")
      else:
        self.log_msg("Skipping Calendar Scan (Data present or No Auth)")
        self.ui_update("phase_status", source="calendars", status="running")
        extra = (
            f"Calendars: {stats['calendars']:,} | Events: {stats['events']:,}"
        )
        self.ui_update(
            "scan_progress",
            source="calendars",
            progress=1.0,
            cumulative=0,
            processed=total_users,
            total=total_users,
            extra_text=extra,
        )
        self.ui_update("phase_status", source="calendars", status="complete")

    if config.scan_in_place_archives:
      if can_scan and (not has_in_place_archives_data or config.user_source == "tenant"):
        self.ui_update("phase_status", source="in_place_archives", status="running")
        failed_in_place_archives, partial_in_place_archive_failures = self.run_batch_scan(
            "in_place_archives", 
            config,
            user_chunks, manager, one_token_per_app_manager, stats, total_users
        )
        self.ui_update("phase_status", source="in_place_archives", status="complete")
      else:
        self.log_msg("Skipping In Place Archive Scan (Data present or No Auth)")
        self.ui_update("phase_status", source="in_place_archives", status="running")
        self.ui_update(
            "scan_progress",
            source="in_place_archives",
            progress=1.0,
            cumulative=stats["in_place_archives"],
            processed=total_users,
            total=total_users,
        )
        self.ui_update("phase_status", source="in_place_archives", status="complete")

    if config.scan_shared_mail_boxes:
      if can_scan and (not has_shared_mails_data or config.user_source == "tenant"):
        self.ui_update("phase_status", source="shared_mails", status="running")
        failed_shared_mails, partial_shared_mail_box_failures = self.run_batch_scan(
            "shared_mails", 
            config,
            shared_chunks, manager, None, stats, total_shared
        )
        self.ui_update("phase_status", source="shared_mails", status="complete")
      else:
        self.log_msg("Skipping Shared Mail Box Scan (Data present or No Auth)")
        self.ui_update("phase_status", source="shared_mails", status="running")
        self.ui_update(
            "scan_progress",
            entity_type="Shared Mailboxes",
            source="shared_mails",
            progress=1.0,
            cumulative=stats["shared_mails"],
            processed=total_shared,
            total=total_shared,
        )
        self.ui_update("phase_status", source="shared_mails", status="complete")
    
    if config.scan_group_mail_boxes:
      if can_scan and (not has_group_mails_data or config.user_source == "tenant"):
        self.ui_update("phase_status", source="group_mails", status="running")
        failed_group_mails = self._run_group_mail_phases(
            config, manager, group_chunks, stats, total_groups
        )
        self.ui_update("phase_status", source="group_mails", status="complete")
      else:
        self.log_msg("Skipping Group Mail Box Scan (Data present or No Auth)")
        self.ui_update("phase_status", source="group_mails", status="running")
        self.ui_update(
            "scan_progress",
            entity_type="Group Mailboxes",
            source="group_mails",
            progress=1.0,
            cumulative=stats["group_mails"],
            processed=total_groups,
            total=total_groups,
        )
        self.ui_update("phase_status", source="group_mails", status="complete")

    # --- LOG FAILED USERS SUMMARY ---
    if failed_emails or failed_calendars or failed_contacts or failed_in_place_archives or failed_shared_mails or failed_group_mails:
      self.log_msg("\n" + "=" * 40)

      if failed_emails:
        self.log_msg("Email Migration Failures")
        for f in failed_emails:
          self.log_msg(f"User: {f['user']} | Cause: {f['cause']}")
        self.log_msg("")  # Add blank line

      if failed_calendars:
        self.log_msg("Calendar Migration Failures")
        for f in failed_calendars:
          self.log_msg(f"User: {f['user']} | Cause: {f['cause']}")
        self.log_msg("")  # Add blank line

      if failed_contacts:
        self.log_msg("Contacts Migration Failures")
        for f in failed_contacts:
          self.log_msg(f"User: {f['user']} | Cause: {f['cause']}")
        self.log_msg("")  # Add blank line
      
      if failed_in_place_archives:
        self.log_msg("In Place Archive Migration Failures")
        for f in failed_in_place_archives:
          prefix = "[WARNING]" if f.get("type", None) == FailureType.NOT_FOUND else "[ERROR]"
          user_upn = self.id_to_display_name[f["user"]] if f["user"] in self.id_to_display_name else f["user"]
          self.log_msg(f"{prefix} User: {user_upn} | Cause: {f['cause']}")
        self.log_msg("")  # Add blank line
      
      if failed_shared_mails:
        self.log_msg("Shared Mail Box Migration Failures")
        for f in failed_shared_mails:
          prefix = "[WARNING]" if f.get("type", None) == FailureType.NOT_FOUND else "[ERROR]"
          shared_mailbox_upn = self.id_to_display_name[f["user"]] if f["user"] in self.id_to_display_name else f["user"]
          self.log_msg(f"{prefix} Shared Mail Box: {shared_mailbox_upn} | Cause: {f['cause']}")
        self.log_msg("")  # Add blank line
      
      if failed_group_mails:
        self.log_msg("Group Mail Box Migration Failures")
        for f in failed_group_mails:
          prefix = "[WARNING]" if f.get("type", None) == FailureType.NOT_FOUND else "[ERROR]"
          group_name = self.id_to_display_name[f["group"]] if f["group"] in self.id_to_display_name else f["group"]
          self.log_msg(f"{prefix} Group: {group_name} | Cause: {f['cause']}")
        self.log_msg("")  # Add blank line

      self.log_msg("=" * 40)
    
    if partial_in_place_archive_failures or partial_shared_mail_box_failures:
      if partial_in_place_archive_failures:
        self.log_msg("In Place Archive Migration Partial Failures")
        for f in partial_in_place_archive_failures:
          prefix = "[WARNING]" if f.get("type", None) == FailureType.NOT_FOUND else "[ERROR]"
          self.log_msg(f"{prefix} User: {f['user']} | Cause: {f['cause']} | Failed Resource: {f['failed_resource']}")
        self.log_msg("")  # Add blank line
      
      if partial_shared_mail_box_failures:
        self.log_msg("Shared Mail Box Migration Partial Failures")
        for f in partial_shared_mail_box_failures:
          prefix = "[WARNING]" if f.get("type", None) == FailureType.NOT_FOUND else "[ERROR]"
          self.log_msg(f"{prefix} Shared Mail Box: {f['user']} | Cause: {f['cause']} | Failed Resource: {f['failed_resource']}")
        self.log_msg("")  # Add blank line

      self.log_msg("=" * 40)

  def _generate_final_report(
      self,
      config: ScanConfig,
      csv_rows: List[Dict[str, Any]],
      shared_csv_rows: List[Dict[str, Any]],
      group_csv_rows: List[Dict[str, Any]],
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

    csv_rows.extend(shared_csv_rows)
    csv_rows.extend(group_csv_rows)

    df = pd.DataFrame(csv_rows)
    df, batches, total_eta, buckets = self.calculate_migration_batches(df)      # TODO Check

    monitor.stop()
    monitor.join()
    elapsed = str(timedelta(seconds=int(time.time() - start_time)))
    avg_cpu, max_cpu, avg_ram, max_ram = monitor.get_stats()
    total_ram_gb = psutil.virtual_memory().total / (1024**3)
    total_cpu_cores = psutil.cpu_count(logical=True)

    self.log_msg("\n" + "=" * 40)
    self.log_msg(f"TOTAL TIME: {elapsed}")
    self.log_msg(f"Total Users / Groups: {len(csv_rows)}")
    self.log_msg(
        f"Emails: {stats['emails']} | Contacts: {stats['contacts']} |"
        f" Calendars: {stats['calendars']} | Events: {stats['events']} |"
        f" In Place Archives: {stats['in_place_archives']} |"
        f" Shared Mails: {stats['shared_mails']} |"
        f" Group Mails: {stats['group_mails']} | Group Threads: {stats['group_threads']}"
    )
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

    # Prepare Export
    output_map = {
        "User Principal Name / Group Mail": "Email Id",
        "Event Count": "Calendar Event Count",
    }
    df_output = df.rename(columns=output_map)

    final_columns = ["Email Id", "Other Aliases", "Suggested Batch", "Type"]
    if config.scan_email:
      final_columns.append("Email Count")
    if config.scan_contact:
      final_columns.append("Contact Count")
    if config.scan_calendar:
      final_columns.extend(["Calendar Count", "Calendar Event Count"])
    if config.scan_in_place_archives:
      final_columns.append("In Place Archive Count")
    if config.scan_shared_mail_boxes:
      final_columns.append("Shared Mail Count")
    if config.scan_group_mail_boxes:
      final_columns.append("Group Post Count")
      final_columns.append("Group Thread Count")

    final_columns = [c for c in final_columns if c in df_output.columns]
    df_output = df_output[final_columns]

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = os.path.join("outputs", ts)
    os.makedirs(output_dir, exist_ok=True)

    report_path = os.path.join(output_dir, f"user_report_{ts}.csv")
    logs_path = os.path.join(output_dir, f"logs_{ts}.log")

    df_output.to_csv(report_path, index=False)

    batches_dir = os.path.join(output_dir, "suggested batches")
    os.makedirs(batches_dir, exist_ok=True)

    unique_batches = df_output["Suggested Batch"].unique()
    for batch in unique_batches:
      if not batch:
        continue
      batch_data = df_output[df_output["Suggested Batch"] == batch].copy()
      batch_export = batch_data[["Email Id"]].rename(
          columns={"Email Id": "Source Exchange Email"}
      )
      safe_name = batch.replace(" ", "")
      batch_path = os.path.join(batches_dir, f"{safe_name}.csv")
      batch_export.to_csv(batch_path, index=False)

    with self.log_lock:
      log_content = "\n".join(self.log_buffer)
    with open(logs_path, "w", encoding="utf-8") as f:
      f.write(log_content)

    result_data = {
        "total_users": len(df),
        "total_emails": stats["emails"],
        "total_contacts": stats["contacts"],
        "total_calendars": stats["calendars"],
        "total_events": stats["events"],
        "total_in_place_archives": stats["in_place_archives"],
        "total_shared_mails": stats["shared_mails"],
        "total_group_mails": stats["group_mails"],
        "total_group_threads": stats["group_threads"],
        "total_items": (
            stats["emails"]
            + stats["contacts"]
            + stats["calendars"]
            + stats["events"]
            + stats["in_place_archives"]
            + stats["shared_mails"]
            + stats["group_mails"]
            + stats["group_threads"]
        ),
        "total_eta": total_eta,
        "batches": batches,
        "df": df_output,
        "buckets": buckets,
    }
    self.ui_update("phase_status", source="plan_generation", status="complete")
    time.sleep(2)
    self.ui_update("complete", data=result_data)

  def run_batch_phase_ui(
      self, chunks, res_type, manager, workers, stats, total_users
  ):
    if self.stop_scan_event.is_set():
      return
    self.log_msg(f"\n--- Starting {res_type.upper()} Scan ---")

    completed_chunks = 0
    users_processed = 0
    users_failed = 0
    phase_failures = []
    phase_total = 0
    phase_events = 0
    phase_cals = 0

    executor = ThreadPoolExecutor(max_workers=workers)
    try:
      # Map the Future to its specific chunk so we know exactly how many users it contained
      futures = {
          executor.submit(
              fetch_user_batch_data,
              chunk,
              res_type,
              manager,
              self.log_msg,
              self.stop_scan_event,
          ): chunk
          for chunk in chunks
      }

      for f in as_completed(futures):
        if self.stop_scan_event.is_set():
          executor.shutdown(wait=False, cancel_futures=True)
          return []

        # Get the original chunk associated with this completed future
        chunk = futures[f]

        try:
          r = f.result()
          users_failed += r.get("failed", 0)
          phase_failures.extend(r.get("failed_details", []))

          if res_type == "messages":
            val = r["emails"]
            stats["emails"] += val
            phase_total += val
          elif res_type == "contacts":
            val = r["contacts"]
            stats["contacts"] += val
            phase_total += val
          elif res_type == "calendars":
            c = r["calendars"]
            e = r["events"]
            stats["calendars"] += c
            stats["events"] += e
            phase_cals += c
            phase_events += e
        except Exception as e:
          users_failed += len(chunk)
          for u in chunk:
            phase_failures.append({
                "user": u["User Principal Name / Group Mail"],
                "cause": f"Chunk failed: {e}",
            })
          pass

        completed_chunks += 1
        users_processed += len(chunk)

        log_freq = max(10, workers // 5)

        if completed_chunks % log_freq == 0 or users_processed == total_users:
          if res_type == "calendars":
            self.log_msg(
                f"Processed {users_processed}/{total_users} | Failed:"
                f" {users_failed}/{total_users} | Calendars: {phase_cals} |"
                f" Events: {phase_events}"
            )
          else:
            label = "messages" if res_type == "messages" else "contacts"
            self.log_msg(
                f"Processed {users_processed}/{total_users} | Failed:"
                f" {users_failed}/{total_users} | {label}: {phase_total}"
            )

        prog = users_processed / total_users if total_users > 0 else 0
        extra = ""
        if res_type == "calendars":
          extra = f"Calendars: {phase_cals:,} | Events: {phase_events:,}"

        self.ui_update(
            "scan_progress",
            source=res_type,
            progress=prog,
            cumulative=phase_total,
            processed=users_processed,
            failed=users_failed,
            total=total_users,
            extra_text=extra,
        )
    finally:
      executor.shutdown(wait=True)

    return phase_failures

  def calculate_migration_batches(self, df):
    # Ensure numeric columns
    target_cols = ["Email Count", "Contact Count", "Event Count", "In Place Archive Count", "Shared Mail Count", "Group Post Count"]
    for col in target_cols:
      if col not in df.columns:
        df[col] = 0
      else:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # 1. Sort Users (Descending - Heaviest first for optimal packing)
    df["SortMetric"] = df.apply(
        lambda x: max(
            (x["Email Count"] + x["Shared Mail Count"] + x["Group Post Count"]), (x["Event Count"] + x["Contact Count"]), x["In Place Archive Count"]
        ),
        axis=1,
    )
    df_sorted_base = df.sort_values(by="SortMetric", ascending=False).copy()

    user_min_limit = self.eta_min_users.get()
    user_max_limit = self.eta_max_users.get()
    num_parallel = max(1, self.parallel_batches.get())
    max_allowed_batches = self.eta_max_batches.get()

    # Define automated candidate targets in hours (3h, 6h, 12h, 18h, 1d, 1.5d, 2d, 3d, 5d, 7d, 10d, 15d, 20d, 30d, 45d, 60d)
    candidate_hours = [
        3,
        6,
        12,
        18,
        24,
        36,
        48,
        72,
        120,
        168,
        240,
        360,
        480,
        720,
        1080,
        1440,
    ]

    self.log_msg(
        "\n--- 🧠 Auto-Optimizing Target ETA (Constraints: Max"
        f" {max_allowed_batches} Batches, Target Range:"
        f" {user_min_limit}-{user_max_limit} Users Per Batch) ---"
    )

    best_total_eta = float("inf")
    best_plan = None
    fallback_plan = None
    min_batches_seen = float("inf")

    # TODO Remove the need to provide config when only ETA is needed
    # Helper: Calculate ETA for subset
    config = self._get_scan_configuration()
    in_place_archive_estimator = None
    if ENABLE_IN_PLACE_ARCHIVE_ETA and self.factory.one_token_per_app_manager is not None:
      in_place_archive_estimator = self.factory.get_in_place_archive_estimator(use_delta_api=True)
    if ENABLE_SHARED_MAILBOX_ETA:
      shared_mail_box_estimator = self.factory.get_shared_mailbox_estimator()
    if ENABLE_GROUP_MAILBOX_ETA:
      group_mail_box_estimator = self.factory.get_group_mailbox_estimator(hard_reset=True)

    def get_batch_eta(subset_df):
      eta_email = 0.0
      if ENABLE_EMAIL_ETA:
        eta_email = calculate_batch_duration(
            subset_df["Email Count"].tolist(),
            ETA_EMAIL_GLOBAL_LIMIT,
            ETA_EMAIL_USER_LIMIT,
            ETA_EMAIL_BATCH_SIZE,
            ETA_EMAIL_BATCH_TIME,
        )
      eta_calendar = 0.0
      if ENABLE_CALENDAR_ETA:
        eta_calendar = calculate_batch_duration(
            subset_df["Event Count"].tolist(),
            ETA_CALENDAR_GLOBAL_LIMIT,
            ETA_CALENDAR_USER_LIMIT,
            ETA_CALENDAR_BATCH_SIZE,
            ETA_CALENDAR_BATCH_TIME,
        )
      eta_contact = 0.0
      if ENABLE_CONTACT_ETA:
        eta_contact = calculate_batch_duration(
            subset_df["Contact Count"].tolist(),
            ETA_CONTACT_GLOBAL_LIMIT,
            ETA_CONTACT_USER_LIMIT,
            ETA_CONTACT_BATCH_SIZE,
            ETA_CONTACT_BATCH_TIME,
        )
      eta_in_place_archive = 0.0
      if ENABLE_IN_PLACE_ARCHIVE_ETA and in_place_archive_estimator is not None:
        eta_in_place_archive = in_place_archive_estimator.calculate_migration_eta(
          {
            "item_counts": subset_df["In Place Archive Count"].tolist(),
            "global_limit": ETA_EMAIL_GLOBAL_LIMIT,
            "user_limit": ETA_EMAIL_USER_LIMIT,
            "batch_size": ETA_EMAIL_BATCH_SIZE,
            "batch_time": ETA_EMAIL_BATCH_TIME,
            "multiplier": IPA_ETA_MULTIPLIER
          }
        )
      eta_shared_mail_box = 0.0
      if ENABLE_SHARED_MAILBOX_ETA:
        eta_shared_mail_box = shared_mail_box_estimator.calculate_migration_eta(
          {
            "item_counts": subset_df["Shared Mail Count"].tolist(),
            "global_limit": ETA_EMAIL_GLOBAL_LIMIT,
            "user_limit": ETA_EMAIL_USER_LIMIT,
            "batch_size": ETA_EMAIL_BATCH_SIZE,
            "batch_time": ETA_EMAIL_BATCH_TIME,
          }
        )
      eta_group_mail_box = 0.0
      if ENABLE_GROUP_MAILBOX_ETA:
        eta_group_mail_box = group_mail_box_estimator.calculate_migration_eta(
          {
            "item_counts": subset_df["Group Post Count"].tolist(),
            "global_limit": ETA_EMAIL_GLOBAL_LIMIT,
            "user_limit": ETA_EMAIL_USER_LIMIT,
            "batch_size": ETA_EMAIL_BATCH_SIZE,
            "batch_time": ETA_EMAIL_BATCH_TIME,
          }
        )

      if ENABLE_IN_PLACE_ARCHIVE_ETA and in_place_archive_estimator is not None:
        in_place_archive_estimator.shutdown()
      if ENABLE_SHARED_MAILBOX_ETA:
        shared_mail_box_estimator.shutdown()
      if ENABLE_GROUP_MAILBOX_ETA:
        group_mail_box_estimator.shutdown()

      return max(eta_email, eta_calendar + eta_contact, eta_in_place_archive, eta_shared_mail_box, eta_group_mail_box)

    # 2. Iterate through candidates
    for target_hours in candidate_hours:
      df_sorted = df_sorted_base.copy()
      df_sorted["Suggested Batch"] = ""

      total_users = len(df_sorted)
      start_idx = 0
      raw_chunks = []

      # Partitioning Loop
      while start_idx < total_users:
        remaining_users = total_users - start_idx
        current_max = min(remaining_users, user_max_limit)
        current_min = min(user_min_limit, remaining_users)

        # Binary Search for Optimal Size based on the current target_hours
        min_subset = df_sorted.iloc[start_idx : start_idx + current_min]
        if get_batch_eta(min_subset) > target_hours:
          chosen_size = current_min
        else:
          max_subset = df_sorted.iloc[start_idx : start_idx + current_max]
          if get_batch_eta(max_subset) <= target_hours:
            chosen_size = current_max
          else:
            low = current_min
            high = current_max
            chosen_size = high
            while low <= high:
              mid = (low + high) // 2
              subset = df_sorted.iloc[start_idx : start_idx + mid]
              eta = get_batch_eta(subset)

              if eta > target_hours:
                chosen_size = mid
                high = mid - 1
              else:
                low = mid + 1

        end_idx = start_idx + chosen_size
        final_subset = df_sorted.iloc[start_idx:end_idx]
        w_eta = get_batch_eta(final_subset)

        # Store the chunk data
        raw_chunks.append({
            "start_idx": start_idx,
            "end_idx": end_idx,
            "users": len(final_subset),
            "total_emails": int(final_subset["Email Count"].sum()),
            "total_contacts": int(final_subset["Contact Count"].sum()),
            "total_events": int(final_subset["Event Count"].sum()),
            "total_in_place_archives": int(final_subset["In Place Archive Count"].sum()),
            "total_shared_mails": int(final_subset["Shared Mail Count"].sum()),
            "total_group_mails": int(final_subset["Group Post Count"].sum()),
            "total_group_threads": int(final_subset["Group Thread Count"].sum()),
            "eta": w_eta,
        })
        start_idx = end_idx

      # 3. Schedule Chunks into Buckets
      num_buckets = min(num_parallel, len(raw_chunks))

      if num_buckets == 0:
        total_eta = 0
        buckets = []
        final_batches_list = []
      else:
        buckets = [
            {"id": i + 1, "total": 0.0, "batches": []}
            for i in range(num_buckets)
        ]
        for chunk in raw_chunks:
          target = min(buckets, key=lambda b: b["total"])
          target["batches"].append(chunk)
          target["total"] += chunk["eta"]

        total_eta = max(b["total"] for b in buckets)

        # Reverse and Name
        all_chunks_with_time = []
        buckets.reverse()

        for b_idx, b in enumerate(buckets):
          batches_list = b["batches"]
          if isinstance(batches_list, list):
            batches_list.reverse()
          current_time = 0.0
          for chunk in batches_list:
            chunk["start_time"] = current_time
            chunk["bucket_idx"] = b_idx
            current_time += chunk["eta"]
            all_chunks_with_time.append(chunk)

        all_chunks_with_time.sort(
            key=lambda x: (x["start_time"], x["bucket_idx"])
        )

        final_batches_list = []
        for i, chunk in enumerate(all_chunks_with_time):
          batch_name = f"Batch {i+1}"
          chunk["name"] = batch_name
          final_batches_list.append(chunk)
          col_idx = df_sorted.columns.get_loc("Suggested Batch")
          df_sorted.iloc[chunk["start_idx"] : chunk["end_idx"], col_idx] = (
              batch_name
          )

      num_batches = len(final_batches_list)
      self.log_msg(
          f"Evaluated Target {target_hours}h: Generated {num_batches} batches |"
          f" Total ETA: {self.format_eta(total_eta)}"
      )

      # 4. Selection Logic
      if num_batches <= max_allowed_batches:
        if total_eta < best_total_eta:
          best_total_eta = total_eta
          best_plan = (df_sorted, final_batches_list, total_eta, buckets)

      # Keep a fallback in case NO plan has <= 50 batches
      if num_batches < min_batches_seen:
        min_batches_seen = num_batches
        fallback_plan = (df_sorted, final_batches_list, total_eta, buckets)

    # 5. Assign Final Best Plan
    if best_plan is not None:
      df_final, final_batches_list, total_eta, buckets = best_plan
    else:
      df_final, final_batches_list, total_eta, buckets = fallback_plan

    self.log_msg(f"\n" + "=" * 60)
    self.log_msg(
        f"🏆 OPTIMAL PLAN SELECTED: {len(final_batches_list)} Batches | TOTAL"
        f" PROJECT ETA: {self.format_eta(total_eta)}"
    )
    self.log_msg("=" * 60 + "\n")

    # Log the final details of the best plan
    for chunk in final_batches_list:
      self.log_msg(
          f"{chunk['name']}: {chunk['users']} Users | ETA:"
          f" {self.format_eta(chunk['eta'])} | Starts @"
          f" {self.format_eta(chunk['start_time'])}"
      )

    if ENABLE_IN_PLACE_ARCHIVE_ETA and in_place_archive_estimator is not None:
      in_place_archive_estimator.shutdown()
    if ENABLE_SHARED_MAILBOX_ETA:
      shared_mail_box_estimator.shutdown()

    return df_final, final_batches_list, total_eta, buckets

  def _get_all_users_graph(self, manager):
    users = []
    url = f"{GRAPH_BASE_URL}/users?$select=id,userPrincipalName,proxyAddresses&$top=999"
    token_data = manager.get_valid_token_slot(self.log_msg)
    token = token_data["token"]
    session = manager.get_session()
    headers = {"Authorization": f"Bearer {token}"}
    try:
      while url and not self.stop_scan_event.is_set():
        # Check mid-loop for extremely long tenant scans
        if time.time() > token_data["expires_at"]:
          manager.return_token_slot(token_data)
          token_data = manager.get_valid_token_slot(self.log_msg)
          token = token_data["token"]
          headers = {"Authorization": f"Bearer {token}"}

        r = session.get(url, headers=headers, timeout=60)
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

  def _resolve_from_csv(self, manager, emails, url = None):
    if not emails:
      return []
    resolved = []
    if url is None:
      url = ("{GRAPH_BASE_URL}/users?$filter=userPrincipalName eq"
              " '{cln}'&$select=id,userPrincipalName"
            )

    def resolve_one(email):
      if self.stop_scan_event.is_set():
        return None
      token_data = manager.get_valid_token_slot(self.log_msg)
      t = token_data["token"]
      s = manager.get_session()
      h = {"Authorization": f"Bearer {t}", "ConsistencyLevel": "eventual"}
      try:
        cln = email.replace("'", "''")
        u = url.format(GRAPH_BASE_URL = GRAPH_BASE_URL, cln = cln)
        r = s.get(u, headers=h, timeout=60)
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

if __name__ == "__main__":
  """Application entry point."""
  import urllib3
  # Suppress SSL warnings for cleaner console output
  urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

  # Configure High DPI scaling if necessary (Windows)
  try:
    from ctypes import windll

    windll.shcore.SetProcessDpiAwareness(1)
  except Exception:
    pass

  app = MigrationEstimatorTool()
  app.mainloop()