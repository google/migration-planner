from ui.exchange_online_ui import MigrationEstimatorTool
from ui import utils as ui_utils
from util.constants import *
import customtkinter as ctk
import time
from tkinter import messagebox
from util.monitoring import ResourceMonitor
from estimators.factory import EstimatorFactory
from util.enums import FailureType

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
        team_site_count = msg.get("teamSiteCount", 0)
        personal_site_count = msg.get("personalSiteCount", 0)
        list_count = msg.get("listCount", 0)
        drive_count = msg.get("driveCount", 0)
        license_count = msg.get("licenseCount", 0)
        status = msg.get("status", "Scanning...")
        if "sites" in self.prog_widgets:
          widget = self.prog_widgets["sites"]["lbl"]
          bar = self.prog_widgets["sites"]["bar"]
          if status == "Fetching...":
            bar.start()
          if widget.winfo_exists():
            text = f"Sites: {count}"
            if team_site_count > 0:
              text += f" | Team Sites: {team_site_count}"
            if personal_site_count > 0:
              text += f" | Personal Sites: {personal_site_count}"
            if list_count > 0:
              text += f" | Lists: {list_count}"
            if drive_count > 0:
              text += f" | Drives: {drive_count}"
            if license_count > 0:
              text += f" | Licenses: {license_count}"
            widget.configure(
                text=text
            )
          if not self.spinners_active.get("sites"):
            self.spinners_active["sites"] = True
            self.animate_spinner("sites")
        if status == "Done":
          self.spinners_active["sites"] = False
          if "sites" in self.prog_widgets:
            widget_icon = self.prog_widgets["sites"]["icon"]
            if widget_icon.winfo_exists():
              widget_icon.configure(
                  text="✓", text_color=COLOR_SUCCESS
              )
            widget_bar = self.prog_widgets["sites"]["bar"]
            if widget_bar.winfo_exists():
              widget_bar.stop()
              widget_bar.configure(mode="determinate")
              widget_bar.set(1.0)
      elif mtype == "drive_discovery":
        count = msg.get("count", 0)
        folder_count = msg.get("folderCount", 0)
        file_count = msg.get("fileCount", 0)
        shortcut_count = msg.get("shortcutCount", 0)
        status = msg.get("status", "Scanning...")
        if "drives" in self.prog_widgets:
          widget = self.prog_widgets["drives"]["lbl"]
          bar = self.prog_widgets["drives"]["bar"]
          if status == "Fetching...":
            bar.start()
          text = f"Drives: {count}"
          if folder_count > 0:
            text += f" | Folders: {folder_count}"
          if file_count > 0:
            text += f" | Files: {file_count}"
          if shortcut_count > 0:
            text += f" | Shortcuts: {shortcut_count}"
          if widget.winfo_exists():
            widget.configure(
                text=text
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
        entity_type = msg.get("entity_type", "Drives")
        if source in self.prog_widgets:
          widget = self.prog_widgets[source]["bar"]
          if widget.winfo_exists():
            widget.set(val)
          if source == "drive_parsing":
            label = "Files / Folders"
          widget_lbl = self.prog_widgets[source]["lbl"]
          if widget_lbl.winfo_exists():
            text_parts = [
                f"{entity_type}: {users_proc - users_fail - users_partially_failed} succeeded",
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

  def _get_display_name(
    self,
    id
  ):
    return self.id_to_display_name.get(id, id)

  def execute_migration_scan(self, config):
    """Orchestrates the end-to-end migration estimation scan."""
    monitor = None
    try:
      self.log_msg("--- Starting Batch Scan ---")
      monitor = ResourceMonitor()
      monitor.start()
      start_time = time.time()

      # # 2. Authentication
      id_to_display = {
        "maxEffectiveDepth": "Max Effective Depth",
        "maxFolderDepth": "Max Folder Depth",
        "maxSubsiteDepth": "Max Subsite Depth",
        "subsiteCount": "Subsite Count",
        "shortcutCount": "Shortcut Count",
        "listCount": "List Count",
        "subsite_count": "Subsite Count",
        "documentLibrary": "Document Library",
        "personalDrive": "Personal Drive",
        "businessDrive": "Business Drive",
        "unknownDrive": "Unknown Drive",
        "folderCount": "Folder Count",
        "fileCount": "File Count",
      }

      self.id_to_display_name = id_to_display
      self.factory = EstimatorFactory(config, logger=self.log_msg, stop_event=self.stop_scan_event, id_to_display_name=id_to_display)
      
      manager = self.factory.get_manager()
      manager.authenticate_all(self.log_msg, required_scopes=["Sites.Read.All", "Files.Read.All", "LicenseAssignment.Read.All"])
      estimator = self.factory.get_files_estimator(progress_update_callback=self.ui_update, hard_reset=True)

      # Calculate resource metrics for the tenant. Progress update to be made directly in the backend.
      failures = []
      file_metrics = estimator.calculate_resource_metrics({}, failures)

      self.log_msg("\n" + "=" * 60)
      self.log_msg("📊 Failures and Warnings:")
      for failure in failures:
        prefix = "[WARNING] " if failure.get("type", None) == FailureType.NOT_FOUND else "[ERROR] "
        self.log_msg(prefix + str(failure))

      self.log_msg("=" * 60)
      self.ui_update("complete", data=file_metrics)
      
      # # 6. Analysis & Reporting
      # self._generate_final_report(config, csv_rows, stats, monitor, start_time)

    except Exception as e:
      self.log_msg(f"Process failed: {e}")
      self.ui_update("error", message=str(e))
    
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

      # Cards for simple metrics
      card_frame = ctk.CTkFrame(self.view_results, fg_color="transparent")
      card_frame.pack(fill="x", pady=10)

      self.create_stat_card(card_frame, "Max Effective Depth", f"{data.get('maxEffectiveDepth', 0):,}", "👥")
      self.create_stat_card(card_frame, "Max Folder Depth", f"{data.get('maxFolderDepth', 0):,}", "📁")
      self.create_stat_card(card_frame, "Max Subsite Depth", f"{data.get('maxSubsiteDepth', 0):,}", "🌐")
      self.create_stat_card(card_frame, "Subsite Count", f"{data.get('subsite_count', data.get('subSiteCount', 0)):,}", "🏢")
      self.create_stat_card(card_frame, "Shortcut Count", f"{data.get('shortcutCount', 0):,}", "🔗")
      self.create_stat_card(card_frame, "List Count", f"{data.get('listCount', 0):,}", "🗃️")
      self.create_stat_card(card_frame, "Folder Count", f"{data.get('folderCount', 0):,}", "📁")
      self.create_stat_card(card_frame, "File Count", f"{data.get('fileCount', 0):,}", "📄")
      
      if "folder_file_size" in data:
          self.create_stat_card(card_frame, "Folder File Size", f"{data['folder_file_size']:,} KB", "💾")

      # File Size Distribution
      dist_data = data.get("tenantLevelFileSizeDistribution", data.get("fileSizeDistribution"))
      if dist_data:
          ctk.CTkLabel(
              self.view_results,
              text="File Size Distribution",
              font=FONT_HEADER_SMALL,
              text_color=COLOR_TEXT_MAIN,
          ).pack(anchor="w", padx=10, pady=(20, 5))
          
          dist_frame = ctk.CTkFrame(self.view_results, fg_color=COLOR_SURFACE, corner_radius=12, border_color=COLOR_OUTLINE_LIGHT, border_width=1)
          dist_frame.pack(fill="x", padx=10, pady=5)
          
          buckets = dist_data.get("Buckets", dist_data.get("buckets", []))
          for bucket in buckets:
              range_vals = bucket.get("sizeRange", (0, 0))
              range_str = f"{str(range_vals[0])} - {str(range_vals[1])} KB"
              file_ids = bucket.get("fileIDs", [])
              count = bucket.get("count", len(file_ids))
              
              row_frame = ctk.CTkFrame(dist_frame, fg_color="transparent")
              row_frame.pack(fill="x", padx=15, pady=5)
              
              ctk.CTkLabel(row_frame, text=range_str, font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN, width=150, anchor="w").pack(side="left")
              
              if file_ids:
                  files_str = ", ".join(file_ids)
                  ctk.CTkLabel(row_frame, text=f"Files: {files_str}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB).pack(side="left", padx=10)
              else:
                  ctk.CTkLabel(row_frame, text=f"Count: {count}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB).pack(side="left", padx=10)

      # Large Resources
      if "tenantLevelLargeResources" in data:
          ctk.CTkLabel(
              self.view_results,
              text="Large Resources (10)",
              font=FONT_HEADER_SMALL,
              text_color=COLOR_TEXT_MAIN,
          ).pack(anchor="w", padx=10, pady=(20, 5))
          
          if len(data["tenantLevelLargeResources"]) > 10:
              ctk.CTkLabel(
                  self.view_results,
                  text="* There are more large resources. Please export the full report to view their details.",
                  font=FONT_BODY_SMALL,
                  text_color=COLOR_TEXT_SUB,
              ).pack(anchor="w", padx=10, pady=(0, 10))
          
          res_container = ctk.CTkFrame(self.view_results, fg_color=COLOR_SURFACE, corner_radius=12, border_color=COLOR_OUTLINE_LIGHT, border_width=1)
          res_container.pack(fill="x", padx=10, pady=5)
          
          # Header row
          header_row = ctk.CTkFrame(res_container, fg_color=COLOR_SURFACE_VARIANT, height=30)
          header_row.pack(fill="x", padx=5, pady=5)
          ctk.CTkLabel(header_row, text="Type", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN, width=100, anchor="w").pack(side="left", padx=10)
          ctk.CTkLabel(header_row, text="ID", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN, width=200, anchor="w").pack(side="left", padx=10)
          ctk.CTkLabel(header_row, text="Count", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN, width=100, anchor="w").pack(side="left", padx=10)
          ctk.CTkLabel(header_row, text="Limit", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN, width=100, anchor="w").pack(side="left", padx=10)

          for i, res in enumerate(data["tenantLevelLargeResources"][0:10]):         # only showing first 10 resources
              bg_color = COLOR_SURFACE if i % 2 == 0 else COLOR_SURFACE_VARIANT
              row_frame = ctk.CTkFrame(res_container, fg_color=bg_color, height=30)
              row_frame.pack(fill="x", padx=5, pady=2)
              
              ctk.CTkLabel(row_frame, text=str(res.get('Type', res.get('type'))), font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, width=100, anchor="w").pack(side="left", padx=10)
              ctk.CTkLabel(row_frame, text=str(res.get('Id', res.get('id'))), font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, width=200, anchor="w").pack(side="left", padx=10)
              ctk.CTkLabel(row_frame, text=f"{res.get('subTreeCount', 0):,}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, width=100, anchor="w").pack(side="left", padx=10)
              ctk.CTkLabel(row_frame, text=f"{res.get('Limit', res.get('limit', 0)):,}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, width=100, anchor="w").pack(side="left", padx=10)

      # License Metrics
      if "licenseMetrics" in data:
          ctk.CTkLabel(
              self.view_results,
              text="License Metrics",
              font=FONT_HEADER_SMALL,
              text_color=COLOR_TEXT_MAIN,
          ).pack(anchor="w", padx=10, pady=(20, 5))
          
          license_frame = ctk.CTkFrame(self.view_results, fg_color=COLOR_SURFACE, corner_radius=12, border_color=COLOR_OUTLINE_LIGHT, border_width=1)
          license_frame.pack(fill="x", padx=10, pady=5)
          
          metrics = data["licenseMetrics"]
          
          # Total License Count Row
          row1 = ctk.CTkFrame(license_frame, fg_color="transparent")
          row1.pack(fill="x", padx=15, pady=5)
          ctk.CTkLabel(row1, text="Total License Count", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN, width=200, anchor="w").pack(side="left")
          ctk.CTkLabel(row1, text=f"User: {metrics.get('totalLicenseCount', {}).get('User', 0):,}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB, width=150, anchor="w").pack(side="left")
          ctk.CTkLabel(row1, text=f"Company: {metrics.get('totalLicenseCount', {}).get('Company', 0):,}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB, width=150, anchor="w").pack(side="left")

          # Consumed Units Row
          row2 = ctk.CTkFrame(license_frame, fg_color="transparent")
          row2.pack(fill="x", padx=15, pady=5)
          ctk.CTkLabel(row2, text="Consumed Units", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN, width=200, anchor="w").pack(side="left")
          ctk.CTkLabel(row2, text=f"User: {metrics.get('consumedUnits', {}).get('User', 0):,}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB, width=150, anchor="w").pack(side="left")
          ctk.CTkLabel(row2, text=f"Company: {metrics.get('consumedUnits', {}).get('Company', 0):,}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB, width=150, anchor="w").pack(side="left")

          # Alloted Units Row
          row3 = ctk.CTkFrame(license_frame, fg_color="transparent")
          row3.pack(fill="x", padx=15, pady=5)
          ctk.CTkLabel(row3, text="Alloted Units", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN, width=200, anchor="w").pack(side="left")
          ctk.CTkLabel(row3, text=f"User: {metrics.get('totalAllotedUnits', {}).get('User', 0):,}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB, width=150, anchor="w").pack(side="left")
          ctk.CTkLabel(row3, text=f"Company: {metrics.get('totalAllotedUnits', {}).get('Company', 0):,}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB, width=150, anchor="w").pack(side="left")

      # Drive Details
      if "driveMetrics" in data:
          ctk.CTkLabel(
              self.view_results,
              text="Drive Details (Top 10)",
              font=FONT_HEADER_SMALL,
              text_color=COLOR_TEXT_MAIN,
          ).pack(anchor="w", padx=10, pady=(20, 5))

          drive_count = len(data["driveMetrics"])
          if drive_count > 10:
              ctk.CTkLabel(
                  self.view_results,
                  text="* There are more drives. Please export the full report to view their details.",
                  font=FONT_BODY_SMALL,
                  text_color=COLOR_TEXT_SUB,
              ).pack(anchor="w", padx=10, pady=(0, 10))

          # Scrollable frame for drives
          drives_scroll = ctk.CTkScrollableFrame(
              self.view_results,
              fg_color="transparent",
              height=500,
              scrollbar_button_color="white",
              scrollbar_button_hover_color=COLOR_SECONDARY_HOVER,
          )
          drives_scroll.pack(fill="x", padx=10, pady=5)

          # Sort drives by maxEffectiveDepth descending
          sorted_drives = sorted(data["driveMetrics"].items(), key=lambda item: item[1].get("maxEffectiveDepth", 0), reverse=True)
          top_10_drives = sorted_drives[:10]
          print(f"DEBUG: driveMetrics count = {len(data['driveMetrics'])}")
          print(f"DEBUG: top_10_drives count = {len(top_10_drives)}")
          for d_id, _ in top_10_drives:
              print(f"DEBUG: Top drive: {d_id}")

          for drive_id, drive_data in top_10_drives:
              # Header frame for toggle
              drive_header = ctk.CTkFrame(drives_scroll, fg_color=COLOR_SURFACE_VARIANT, corner_radius=8)
              drive_header.pack(fill="x", pady=2)
              
              # Details frame (initially hidden)
              drive_details = ctk.CTkFrame(drives_scroll, fg_color=COLOR_SURFACE, corner_radius=12, border_color=COLOR_OUTLINE_LIGHT, border_width=1)
              drive_details.is_expanded = False
              
              def toggle_drive(frame, btn, d_id, header):
                  if frame.is_expanded:
                      frame.pack_forget()
                      frame.is_expanded = False
                      if btn:
                          btn.configure(text=f"Drive: {self._get_display_name(d_id)}... ▼")
                  else:
                      frame.pack(fill="x", pady=2, padx=10, after=header)
                      frame.is_expanded = True
                      if btn:
                          btn.configure(text=f"Drive: {self._get_display_name(d_id)}... ▲")

              btn_toggle = ctk.CTkButton(
                  drive_header,
                  text=f"Drive: {self._get_display_name(drive_id)}... ▼",
                  fg_color="transparent",
                  text_color=COLOR_PRIMARY,
                  hover=False,
                  anchor="w",
              )
              btn_toggle.pack(fill="x", padx=5, pady=5)
              
              btn_toggle.configure(command=lambda f=drive_details, b=btn_toggle, d=drive_id, h=drive_header: toggle_drive(f, b, d, h))

              # Fill details frame
              ctk.CTkLabel(drive_details, text=f"Drive: {self._get_display_name(drive_id)}", font=FONT_BODY_BOLD, text_color=COLOR_PRIMARY).pack(anchor="w", padx=10, pady=(5, 2))
              
              ctk.CTkLabel(drive_details, text=f"Max Effective Depth: {drive_data.get('maxEffectiveDepth', 0)}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(anchor="w", padx=10, pady=2)
              ctk.CTkLabel(drive_details, text=f"Shortcut Count: {drive_data.get('shortcutCount', 0)}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(anchor="w", padx=10, pady=2)
              ctk.CTkLabel(drive_details, text=f"Folder Count: {drive_data.get('folderCount', 0)}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(anchor="w", padx=10, pady=2)
              ctk.CTkLabel(drive_details, text=f"File Count: {drive_data.get('fileCount', 0)}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(anchor="w", padx=10, pady=2)
              
              # Buckets inside drive
              if "fileSizeDistribution" in drive_data:
                  ctk.CTkLabel(drive_details, text="File Size Distribution:", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(anchor="w", padx=10, pady=(5, 2))
                  for bucket in drive_data["fileSizeDistribution"].get("buckets", []):
                      range_vals = bucket.get("sizeRange", (0, 0))
                      range_str = f"{range_vals[0]} - {range_vals[1]} KB"
                      count = bucket.get("count", 0)
                      ctk.CTkLabel(drive_details, text=f"  {range_str}: {count} files", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB).pack(anchor="w", padx=20)

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


  def export_current_report(self):
    if not hasattr(self, "last_scan_data"):
      return
    
    data = self.last_scan_data
    
    # Exclude complex structures for summary
    summary_data = {k: v for k, v in data.items() if k not in ["driveMetrics", "licenseMetrics", "siteMetrics", "tenantLevelFileSizeDistribution", "tenantLevelLargeResources"]}
    
    from tkinter import filedialog
    from datetime import datetime
    import csv
    import json
    
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    f = filedialog.asksaveasfilename(
        initialfile=f"migration_report_{ts}.csv", defaultextension=".csv"
    )
    
    if not f:
      return
        
    with open(f, "w", newline="", encoding="utf-8") as csvfile:
      writer = csv.writer(csvfile)
      
      # Section 1: Summary Metrics
      writer.writerow(["Summary Metrics", "Value"])
      for k, v in summary_data.items():
        if k == "driveCounts":
          for sub_k, sub_v in v.items():
            writer.writerow([f"DriveCount {self._get_display_name(sub_k)}", sub_v])
        else:
          writer.writerow([self._get_display_name(k), v])
      
      writer.writerow([]) # Blank line separator
      
      # Section 2: License Metrics
      writer.writerow(["License Metrics", ""])
      license_data = data.get("licenseMetrics", {})
      writer.writerow(["Total License Count (User)", license_data.get("totalLicenseCount", {}).get("User", 0)])
      writer.writerow(["Total License Count (Company)", license_data.get("totalLicenseCount", {}).get("Company", 0)])
      writer.writerow(["Consumed Units (User)", license_data.get("consumedUnits", {}).get("User", 0)])
      writer.writerow(["Consumed Units (Company)", license_data.get("consumedUnits", {}).get("Company", 0)])
      writer.writerow(["Alloted Units (User)", license_data.get("totalAllotedUnits", {}).get("User", 0)])
      writer.writerow(["Alloted Units (Company)", license_data.get("totalAllotedUnits", {}).get("Company", 0)])
      
      writer.writerow([]) # Blank line separator
      
      # Section 3: File Size Distribution
      writer.writerow(["File Size Distribution", ""])
      writer.writerow(["Range (KB)", "Count"])
      dist_data = data.get("tenantLevelFileSizeDistribution", {})
      buckets = dist_data.get("buckets", [])
      for bucket in buckets:
        range_vals = bucket.get("sizeRange", (0, 0))
        range_str = f"{range_vals[0]} - {range_vals[1]}"
        count = bucket.get("count", 0)
        writer.writerow([range_str, count])
        
      writer.writerow([]) # Blank line separator
      
      # Section 4: Large Resources
      writer.writerow(["Large Resources", ""])
      writer.writerow(["Type", "ID", "SubTreeCount", "Limit", "Drive"])
      large_resources = data.get("tenantLevelLargeResources", [])
      for res in large_resources:
        writer.writerow([
            res.get("Type", res.get("type", "")),
            res.get("Id", res.get("id", "")),
            res.get("subTreeCount", 0),
            res.get("Limit", res.get("limit", 0)),
            self._get_display_name(res.get("drive", ""))
        ])
        
      writer.writerow([]) # Blank line separator
      
      # Section 5: Site Details
      writer.writerow(["Site Details", ""])
      writer.writerow(["Site Name", "Site Level"])
      site_metrics = data.get("siteMetrics", {})
      for site_id, s_data in site_metrics.items():
        writer.writerow([self._get_display_name(site_id), s_data.get("siteLevel", 0)])
        
      writer.writerow([]) # Blank line separator
      
      # Section 6: Drive Details
      writer.writerow(["Drive Details", ""])
      
      # Determine all unique bucket ranges across all drives to create columns
      drive_metrics = data.get("driveMetrics", {})
      all_buckets = set()
      for d_data in drive_metrics.values():
        for bucket in d_data.get("fileSizeDistribution", {}).get("buckets", []):
            range_vals = bucket.get("sizeRange", (0, 0))
            all_buckets.add(range_vals)
            
      sorted_buckets = sorted(list(all_buckets))
      bucket_cols = [f"Bucket_{b[0]}_{b[1]}" for b in sorted_buckets]
      
      headers = ["Drive Name", "Max Effective Depth", "Folder Count", "File Count", "Shortcut Count"] + bucket_cols
      writer.writerow(headers)
      
      for drive_id, d_data in drive_metrics.items():
        row = [
            self._get_display_name(drive_id),
            d_data.get("maxEffectiveDepth", 0),
            d_data.get("folderCount", 0),
            d_data.get("fileCount", 0),
            d_data.get("shortcutCount", 0)
        ]
        
        # Add bucket counts
        drive_buckets = {f"Bucket_{b.get('sizeRange', (0,0))[0]}_{b.get('sizeRange', (0,0))[1]}": b.get("count", 0) for b in d_data.get("fileSizeDistribution", {}).get("buckets", [])}
        for b_col in bucket_cols:
            row.append(drive_buckets.get(b_col, 0))
            
        writer.writerow(row)


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

    self.create_progress_row(self.scan_container, "sites", "Site Discovery", mode="indeterminate")
    self.create_progress_row(self.scan_container, "drives", "Drive Discovery", mode="indeterminate")
    self.create_progress_row(self.scan_container, "drive_parsing", "Drive Scan", mode="determinate")
    self.create_progress_row(self.scan_container, "plan_generation", "Generating Migration Plan", mode="determinate")

    import threading
    threading.Thread(target=self.execute_migration_scan, args=(config,)).start()