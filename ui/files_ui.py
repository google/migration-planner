from ui.exchange_online_ui import MigrationEstimatorTool
from ui import utils as ui_utils
from util.constants import *
import customtkinter as ctk
import time
from tkinter import messagebox
from util.monitoring import ResourceMonitor
from estimators.factory import EstimatorFactory

import json

class FileMigrationEstimatorTool(MigrationEstimatorTool):
  def __init__(self):
    super().__init__()
    self.factory = None

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

    # Advanced Settings
    ui_utils.build_advanced_settings_frame(self, ctk)
    
    # Concurrency settings
    ui_utils.build_concurrency_settings_slider(self, ctk, useConcurrencyHeading=True)

    # Migration Plan Options
    ui_utils.build_migration_plan_options(self, ctk)

    # Prepare a bucket ranges UI element
    ui_utils.build_file_distribution_bucket_ranges(self, ctk)

    # Input for lower count limit for large resources
    ui_utils.build_large_resource_limit_input(self, ctk)

  def update_progress(self, msg):
    if isinstance(msg, str):
      self.log_buffer.append(msg)
    elif isinstance(msg, dict):
      mtype = msg.get("type")
      if mtype == "site_discovery":
        if not self.view_progress.winfo_viewable():
          self.show_progress_view()
        count = msg.get("count", 0)
        status = msg.get("status", "Scanning...")
        if "subsites" in self.prog_widgets:
          widget = self.prog_widgets["subsites"]["lbl"]
          if widget.winfo_exists():
            widget.configure(
                text=f"{count} subsites found"
            )
          if not self.spinners_active.get("subsites"):
            self.spinners_active["subsites"] = True
            self.animate_spinner("subsites")
        if status == "Done":
          self.spinners_active["subsites"] = False
          if "subsites" in self.prog_widgets:
            widget_icon = self.prog_widgets["subsites"]["icon"]
            if widget_icon.winfo_exists():
              widget_icon.configure(
                  text="✓", text_color=COLOR_SUCCESS
              )
            widget_bar = self.prog_widgets["subsites"]["bar"]
            if widget_bar.winfo_exists():
              widget_bar.stop()
              widget_bar.configure(mode="determinate")
              widget_bar.set(1.0)
      elif mtype == "drive_discovery":
        count = msg.get("count", 0)
        status = msg.get("status", "Scanning...")
        if "drives" in self.prog_widgets:
          widget = self.prog_widgets["drives"]["lbl"]
          if widget.winfo_exists():
            widget.configure(
                text=f"{count} drives found"
            )
          if not self.spinners_active.get("drives"):
            self.spinners_active["drives"] = True
            self.animate_spinner("drives")
        if status == "Done":
          self.spinners_active["drives"] = False
          if "drives" in self.prog_widgets:
            widget_icon = self.prog_widgets["drives"]["icon"]
            if widget_icon.winfo_exists():
              widget_icon.configure(
                  text="✓", text_color=COLOR_SUCCESS
              )
            widget_bar = self.prog_widgets["drives"]["bar"]
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
        users_partially_failed = msg.get("partially_failed", 0)
        users_tot = msg.get("total", 0)
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
                      f"Users: {users_proc - users_fail} succeeded , {users_fail}"
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
              label = "In Place Archive Count"
            elif source == "group_mail_boxes":
              label = "Group Mail Count"
            widget_lbl = self.prog_widgets[source]["lbl"]
            if widget_lbl.winfo_exists():
              text_parts = [
                  f"Users: {users_proc - users_fail - users_partially_failed} succeeded",
                  f"{users_fail} failed"
              ]
              
              if users_partially_failed > 0:
                  text_parts.append(f"{users_partially_failed} partially failed")
                  
              base_text = ", ".join(text_parts)
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

  def execute_migration_scan(self, config):
    """Orchestrates the end-to-end migration estimation scan."""
    monitor = None
    try:
      print("Reached Execute Migration Stage")
      self.log_msg("--- Starting Batch Scan ---")
      monitor = ResourceMonitor()
      monitor.start()
      start_time = time.time()

      # # 2. Authentication
      if not self.factory:
        self.factory = EstimatorFactory(config)
      
      manager = self.factory.get_manager()
      manager.authenticate_all(self.log_msg, required_scopes=["Sites.Read.All", "Files.Read.All"])
      estimator = self.factory.get_files_estimator(self.ui_update)

      # Calculate resource metrics for the tenant. Progress update to be made directly in the backend.
      failures = []
      file_metrics = estimator.calculate_resource_metrics({}, failures)

      print(json.dumps(file_metrics, indent=4))
      self.ui_update("complete", data=file_metrics)

      # # 4. Build Batch List
      # csv_rows, stats = self._prepare_batch_list(
      #     config, all_users, existing_data
      # )

      # # 5. Execution
      # self._run_scan_phases(config, manager, one_token_per_app_manager, csv_rows, stats)

      # # 6. Analysis & Reporting
      # self._generate_final_report(config, csv_rows, stats, monitor, start_time)

    except Exception as e:
      print(e)
      # self.log_msg(f"Process failed: {e}")
      raise e
    
  # ==========================
  # VIEW: PROGRESS
  # ==========================
  def build_progress_view(self):
    super().build_progress_view()

  # ==========================
  # VIEW: RESULTS
  # ==========================
  def build_results_view(self):
    super().build_results_view()

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
          card_frame, "Max Effective Depth", f"{data['maxEffectiveDepth']:,}", "👥"
      )
      self.create_stat_card(
          card_frame, "Max Folder Depth", f"{data['maxFolderDepth']:,}", "📩"
      )
      self.create_stat_card(
          card_frame, "Max Subsite Depth", f"{data['maxSubsiteDepth']:,}", "📞"
      )
      self.create_stat_card(
          card_frame, "List Count", f"{data['listCount']:,}", "🗃️"
      )
      # self.create_stat_card(
      #     card_frame, "Group Mailbox Mails", f"{data['total_group_mailboxes']:,}", "👥📧"
      # )

      # # Timeline
      # ctk.CTkLabel(
      #     self.view_results,
      #     text="Timeline Estimates",
      #     font=FONT_HEADER_SMALL,
      #     text_color=COLOR_TEXT_MAIN,
      # ).pack(anchor="w", padx=10, pady=(20, 5))
      # ctk.CTkLabel(
      #     self.view_results,
      #     text=(
      #         "Projected migration timeline based on the proposed execution"
      #         " plan."
      #     ),
      #     font=FONT_BODY_MEDIUM,
      #     text_color=COLOR_TEXT_SUB,
      # ).pack(anchor="w", padx=10, pady=(0, 10))

      # # Total Footer
      # foot = ctk.CTkFrame(self.view_results, fg_color="transparent")
      # foot.pack(fill="x", pady=10)
      # self.create_summary_box(
      #     foot, self.format_eta(data["total_eta"]), "Estimated Time"
      # )
      # self.create_summary_box(foot, f"{data['total_items']:,}", "Total Items")

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


  def start_scan(self):
    print("Invoked Start Scan")
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

    config = self._get_scan_configuration()

    self.stop_scan_event.clear()
    with self.log_lock:
      self.log_buffer = []
    self.spinners_active = {}
    self.spinner_indices = {}
    for w in self.scan_container.winfo_children():
      w.destroy()
      
    self.prog_widgets = {}

    self.create_progress_row(self.scan_container, "subsites", "Subsite Discovery", mode="indeterminate")
    self.create_progress_row(self.scan_container, "drives", "Drive Parsing and Tree Creation", mode="indeterminate")
    self.create_progress_row(self.scan_container, "drive_parsing", "Drive Tree Parsing", mode="determinate")

    import threading
    threading.Thread(target=self.execute_migration_scan, args=(config,)).start()