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
import pandas as pd
import math

class FileMigrationEstimatorTool(MigrationEstimatorTool):
  def __init__(self):
    super().__init__()
    self.factory = None

  # ==========================
  # VIEW: CONFIGURATION
  # ==========================
  def build_config_view(self):
    # """Builds the Configuration View."""
    self.include_personal_sites = ctk.BooleanVar(value=True)
    self.include_team_sites = ctk.BooleanVar(value=True)
    
    ui_utils.build_configuration_view(self, ctk)

    # Header
    ui_utils.build_header(self, ctk)

    # Status Line
    ui_utils.build_status_line(self, ctk)

    # Main Content
    ui_utils.build_mail_input_frame(self, ctk)

    # Advanced Settings
    ui_utils.build_advanced_settings_frame(self, ctk)
    
    # Site Options
    ctk.CTkLabel(
        self.adv_frame,
        text="Site Types to Scan",
        font=FONT_BODY_BOLD,
        text_color=COLOR_TEXT_MAIN,
    ).pack(anchor="w", padx=15, pady=(10, 5))
    
    site_options_frame = ctk.CTkFrame(self.adv_frame, fg_color="transparent")
    site_options_frame.pack(fill="x", padx=15)
    
    ctk.CTkCheckBox(
        site_options_frame,
        text="Personal Sites (OneDrive)",
        variable=self.include_personal_sites,
        corner_radius=4,
        fg_color=COLOR_PRIMARY,
        border_color=COLOR_TEXT_SUB,
    ).pack(side="left", padx=10)
    
    ctk.CTkCheckBox(
        site_options_frame,
        text="Team Sites",
        variable=self.include_team_sites,
        corner_radius=4,
        fg_color=COLOR_PRIMARY,
        border_color=COLOR_TEXT_SUB,
    ).pack(side="left", padx=10)
    
    # Concurrency settings
    ui_utils.build_concurrency_settings_slider(self, ctk, useConcurrencyHeading=True)

    # Migration Plan Options
    ui_utils.build_migration_plan_options(self, ctk)

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
        main_part = [
            f"{entity_type}: {users_proc - users_fail - users_partially_failed} succeeded",
            f"{users_fail} failed"
        ]

        if source in self.prog_widgets:
          widget = self.prog_widgets[source]["bar"]
          if widget.winfo_exists():
            widget.set(val)
          if source == "drive_parsing":
            folder_count = msg.get("folderCount", 0)
            file_count = msg.get("fileCount", 0)
            max_depth = msg.get("maxDepth", 0)
            folder_exceeding = msg.get("folderCountExceedingDepthLimit", 0)
            file_exceeding = msg.get("fileCountExceedingDepthLimit", 0)
            
            text_parts = []
            if folder_count > 0 or file_count > 0:
                text_parts.append(f"Folders: {folder_count}")
                text_parts.append(f"Files: {file_count}")
                text_parts.append(f"Max Depth: {max_depth}")
            
            if folder_exceeding > 0 or file_exceeding > 0:
                text_parts.append(f"Folders with Depth > Limit: {folder_exceeding}")
                text_parts.append(f"Files with Depth > Limit: {file_exceeding}")
                
            skipped_folders = msg.get("skippedFolderCount", 0)
            if skipped_folders > 0:
                text_parts.append(f"Skipped Roots: {skipped_folders}")
                
            final_text = " | ".join(main_part + text_parts)
            widget_lbl = self.prog_widgets[source]["lbl"]
            if widget_lbl.winfo_exists():
                widget_lbl.configure(text=final_text)

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
        "subsiteCount": "Site/Subsite Count",
        "shortcutCount": "Shortcut Count",
        "listCount": "List Count",
        "subsite_count": "Site/Subsite Count",
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
      self.ui_update("scan_progress", source="plan_generation", progress=0.5, status="running", extra_text="Calculating migration batches...")
      
      # Extract siteMetrics and build DataFrame
      site_metrics = file_metrics.get("siteMetrics", {})
      site_data = []
      for site_id, s_data in site_metrics.items():
        site_data.append({
            "Site Id": site_id,
            "Resource Count": s_data.get("resourceCount", 0),
            "Folder Count": s_data.get("folderCount", 0),
            "File Count": s_data.get("fileCount", 0),
            "Shortcut Count": s_data.get("shortcutCount", 0),
            "Corpus Size": s_data.get("totalSize", 0)
        })
      df = pd.DataFrame(site_data)
      
      df_final, batches_list, total_eta, buckets = self.calculate_migration_batches(df)
      
      file_metrics["batches"] = batches_list
      file_metrics["buckets"] = buckets
      file_metrics["total_eta"] = total_eta
      file_metrics["df"] = df_final
      
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

  def calculate_migration_batches(self, df):
    # Ensure numeric columns
    if "Resource Count" not in df.columns:
      df["Resource Count"] = 0
    else:
      df["Resource Count"] = pd.to_numeric(df["Resource Count"], errors="coerce").fillna(0)

    # 1. Sort Sites (Descending - Heaviest first)
    df_sorted_base = df.sort_values(by="Resource Count", ascending=False).copy()

    user_min_limit = self.eta_min_users.get()
    user_max_limit = self.eta_max_users.get()
    num_parallel = min(4, max(1, self.parallel_batches.get()))
    max_allowed_batches = self.eta_max_batches.get()

    candidate_hours = [3, 6, 12, 18, 24, 36, 48, 72, 120, 168, 240, 360, 480, 720, 1080, 1440]

    best_total_eta = float("inf")
    best_plan = None
    fallback_plan = None
    min_batches_seen = float("inf")

    def get_batch_eta(subset_df):
      estimator = self.factory.get_files_estimator()
      items = []
      for _, row in subset_df.iterrows():
        items.append({
            "size": row.get("Corpus Size", 0),
            "files": int(row.get("File Count", 0)),
            "folders": int(row.get("Folder Count", 0)),
            "shortcuts": int(row.get("Shortcut Count", 0))
        })
        
      data = {
        "items": items,
        "FILES_GLOBAL_COUNT_LIMIT": FILES_GLOBAL_COUNT_LIMIT,
        "FILES_GLOBAL_CORPUS_SIZE_LIMIT": FILES_GLOBAL_CORPUS_SIZE_LIMIT,
      }
      return estimator.calculate_migration_eta(data)

    # Iterate through candidates
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

        # Binary Search for Optimal Size
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
            "sites": len(final_subset),
            "resource_count": int(final_subset["Resource Count"].sum()),
            "folder_count": int(final_subset["Folder Count"].sum()) if "Folder Count" in final_subset.columns else 0,
            "file_count": int(final_subset["File Count"].sum()) if "File Count" in final_subset.columns else 0,
            "shortcut_count": int(final_subset["Shortcut Count"].sum()) if "Shortcut Count" in final_subset.columns else 0,
            "corpus_size": float(final_subset["Corpus Size"].sum()) if "Corpus Size" in final_subset.columns else 0.0,
            "eta": w_eta,
        })
        start_idx = end_idx

      # Schedule Chunks into Buckets
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

      # Selection Logic
      if num_batches <= max_allowed_batches:
        if total_eta < best_total_eta:
          best_total_eta = total_eta
          best_plan = (df_sorted, final_batches_list, total_eta, buckets)

      if num_batches < min_batches_seen:
        min_batches_seen = num_batches
        fallback_plan = (df_sorted, final_batches_list, total_eta, buckets)

    if best_plan is not None:
      df_final, final_batches_list, total_eta, buckets = best_plan
    else:
      df_final, final_batches_list, total_eta, buckets = fallback_plan

    return df_final, final_batches_list, total_eta, buckets

  def format_size(self, size_in_bytes):
    if size_in_bytes >= 1024**3:
      return f"{size_in_bytes / (1024**3):.2f} GB"
    elif size_in_bytes >= 1024**2:
      return f"{size_in_bytes / (1024**2):.2f} MB"
    elif size_in_bytes >= 1024:
      return f"{size_in_bytes / 1024:.2f} KB"
    else:
      return f"{size_in_bytes} Bytes"

  def create_batch_bar(self, parent, batch, max_eta):
    f = ctk.CTkFrame(parent, fg_color="transparent")
    f.pack(fill="x", padx=20, pady=8)
    
    sites_str = self.format_metric(batch.get("sites", 0))
    folders_str = self.format_metric(batch.get("folder_count", 0))
    files_str = self.format_metric(batch.get("file_count", 0))
    shortcuts_str = self.format_metric(batch.get("shortcut_count", 0))
    size_str = self.format_size(batch.get("corpus_size", 0))
    
    info = (
        f"{batch['name']} - {sites_str} 🏢  |  {folders_str} 📁  |  {files_str} 📄  |  {shortcuts_str} 🔗  |  {size_str} 💾"
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
      self.create_stat_card(card_frame, "Site/Subsite Count", f"{data.get('subsite_count', data.get('subSiteCount', 0)):,}", "🏢")
      self.create_stat_card(card_frame, "Shortcut Count", f"{data.get('shortcutCount', 0):,}", "🔗")
      self.create_stat_card(card_frame, "List Count", f"{data.get('listCount', 0):,}", "🗃️")
      self.create_stat_card(card_frame, "Folder Count", f"{data.get('folderCount', 0):,}", "📁")
      self.create_stat_card(card_frame, "File Count", f"{data.get('fileCount', 0):,}", "📄")
      self.create_stat_card(card_frame, "Document Library Count", f"{data.get('driveCounts', {}).get("documentLibrary"):,}", "📁")
      self.create_stat_card(card_frame, "Folder Count (exceeding depth limit)", f"{data.get('folderCountExceedingDepthLimit', 0):,}", "📁")
      self.create_stat_card(card_frame, "File Count (exceeding depth limit)", f"{data.get('fileCountExceedingDepthLimit', 0):,}", "📄")
      self.create_stat_card(card_frame, "Large Resource Count", f"{data.get('tenantLevelLargeResourceCount', 0):,}", "📄")
      
      if "folder_file_size" in data:
          self.create_stat_card(card_frame, "Folder File Size", f"{data['folder_file_size']:,} KB", "💾")

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

      # Container for Paginated Content
      self.paginated_frame = ctk.CTkFrame(
          self.view_results, fg_color="transparent"
      )
      self.paginated_frame.pack(fill="x", expand=True)

      # Drive Details
      # if "driveMetrics" in data:
      #     ctk.CTkLabel(
      #         self.view_results,
      #         text="Drive Details (Top 10)",
      #         font=FONT_HEADER_SMALL,
      #         text_color=COLOR_TEXT_MAIN,
      #     ).pack(anchor="w", padx=10, pady=(20, 5))

      #     drive_count = len(data["driveMetrics"])
      #     if drive_count > 10:
      #         ctk.CTkLabel(
      #             self.view_results,
      #             text="* There are more drives. Please export the full report to view their details.",
      #             font=FONT_BODY_SMALL,
      #             text_color=COLOR_TEXT_SUB,
      #         ).pack(anchor="w", padx=10, pady=(0, 10))

      #     # Scrollable frame for drives
      #     drives_scroll = ctk.CTkScrollableFrame(
      #         self.view_results,
      #         fg_color="transparent",
      #         height=500,
      #         scrollbar_button_color="white",
      #         scrollbar_button_hover_color=COLOR_SECONDARY_HOVER,
      #     )
      #     drives_scroll.pack(fill="x", padx=10, pady=5)

      #     # Sort drives by maxEffectiveDepth descending
      #     sorted_drives = sorted(data["driveMetrics"].items(), key=lambda item: item[1].get("maxEffectiveDepth", 0), reverse=True)
      #     top_10_drives = sorted_drives[:10]
      #     print(f"DEBUG: driveMetrics count = {len(data['driveMetrics'])}")
      #     print(f"DEBUG: top_10_drives count = {len(top_10_drives)}")
      #     for d_id, _ in top_10_drives:
      #         print(f"DEBUG: Top drive: {d_id}")

      #     for drive_id, drive_data in top_10_drives:
      #         # Header frame for toggle
      #         drive_header = ctk.CTkFrame(drives_scroll, fg_color=COLOR_SURFACE_VARIANT, corner_radius=8)
      #         drive_header.pack(fill="x", pady=2)
              
      #         # Details frame (initially hidden)
      #         drive_details = ctk.CTkFrame(drives_scroll, fg_color=COLOR_SURFACE, corner_radius=12, border_color=COLOR_OUTLINE_LIGHT, border_width=1)
      #         drive_details.is_expanded = False
              
      #         def toggle_drive(frame, btn, d_id, header):
      #             if frame.is_expanded:
      #                 frame.pack_forget()
      #                 frame.is_expanded = False
      #                 if btn:
      #                     btn.configure(text=f"Drive: {self._get_display_name(d_id)}... ▼")
      #             else:
      #                 frame.pack(fill="x", pady=2, padx=10, after=header)
      #                 frame.is_expanded = True
      #                 if btn:
      #                     btn.configure(text=f"Drive: {self._get_display_name(d_id)}... ▲")

      #         btn_toggle = ctk.CTkButton(
      #             drive_header,
      #             text=f"Drive: {self._get_display_name(drive_id)}... ▼",
      #             fg_color="transparent",
      #             text_color=COLOR_PRIMARY,
      #             hover=False,
      #             anchor="w",
      #         )
      #         btn_toggle.pack(fill="x", padx=5, pady=5)
              
      #         btn_toggle.configure(command=lambda f=drive_details, b=btn_toggle, d=drive_id, h=drive_header: toggle_drive(f, b, d, h))

      #         # Fill details frame
      #         ctk.CTkLabel(drive_details, text=f"Drive: {self._get_display_name(drive_id)}", font=FONT_BODY_BOLD, text_color=COLOR_PRIMARY).pack(anchor="w", padx=10, pady=(5, 2))
              
      #         ctk.CTkLabel(drive_details, text=f"Max Effective Depth: {drive_data.get('maxEffectiveDepth', 0)}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(anchor="w", padx=10, pady=2)
      #         ctk.CTkLabel(drive_details, text=f"Shortcut Count: {drive_data.get('shortcutCount', 0)}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(anchor="w", padx=10, pady=2)
      #         ctk.CTkLabel(drive_details, text=f"Folder Count: {drive_data.get('folderCount', 0)}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(anchor="w", padx=10, pady=2)
      #         ctk.CTkLabel(drive_details, text=f"File Count: {drive_data.get('fileCount', 0)}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(anchor="w", padx=10, pady=2)
      #         ctk.CTkLabel(drive_details, text=f"Folder Count (exceeding depth limit): {drive_data.get('folderCountExceedingDepthLimit', 0)}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(anchor="w", padx=10, pady=2)
      #         ctk.CTkLabel(drive_details, text=f"File Count (exceeding depth limit): {drive_data.get('fileCountExceedingDepthLimit', 0)}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN).pack(anchor="w", padx=10, pady=2)
              
      #         # Buckets inside drive
      #         if "fileSizeDistribution" in drive_data:
      #             ctk.CTkLabel(drive_details, text="File Size Distribution:", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN).pack(anchor="w", padx=10, pady=(5, 2))
      #             for bucket in drive_data["fileSizeDistribution"].get("buckets", []):
      #                 range_vals = bucket.get("sizeRange", (0, 0))
      #                 range_str = f"{range_vals[0]} - {range_vals[1]} KB"
      #                 count = bucket.get("count", 0)
      #                 ctk.CTkLabel(drive_details, text=f"  {range_str}: {count} files", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB).pack(anchor="w", padx=20)

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
      # if "tenantLevelLargeResources" in data:
      #     ctk.CTkLabel(
      #         self.view_results,
      #         text="Large Resources (10)",
      #         font=FONT_HEADER_SMALL,
      #         text_color=COLOR_TEXT_MAIN,
      #     ).pack(anchor="w", padx=10, pady=(20, 5))
          
      #     if len(data["tenantLevelLargeResources"]) > 10:
      #         ctk.CTkLabel(
      #             self.view_results,
      #             text="* There are more large resources. Please export the full report to view their details.",
      #             font=FONT_BODY_SMALL,
      #             text_color=COLOR_TEXT_SUB,
      #         ).pack(anchor="w", padx=10, pady=(0, 10))
          
      #     res_container = ctk.CTkFrame(self.view_results, fg_color=COLOR_SURFACE, corner_radius=12, border_color=COLOR_OUTLINE_LIGHT, border_width=1)
      #     res_container.pack(fill="x", padx=10, pady=5)
          
      #     # Header row
      #     header_row = ctk.CTkFrame(res_container, fg_color=COLOR_SURFACE_VARIANT, height=30)
      #     header_row.pack(fill="x", padx=5, pady=5)
      #     ctk.CTkLabel(header_row, text="Type", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN, width=100, anchor="w").pack(side="left", padx=10)
      #     ctk.CTkLabel(header_row, text="ID", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN, width=200, anchor="w").pack(side="left", padx=10)
      #     ctk.CTkLabel(header_row, text="Count", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN, width=100, anchor="w").pack(side="left", padx=10)

      #     for i, res in enumerate(data["tenantLevelLargeResources"][0:10]):         # only showing first 10 resources
      #         bg_color = COLOR_SURFACE if i % 2 == 0 else COLOR_SURFACE_VARIANT
      #         row_frame = ctk.CTkFrame(res_container, fg_color=bg_color, height=30)
      #         row_frame.pack(fill="x", padx=5, pady=2)
              
      #         ctk.CTkLabel(row_frame, text=str(res.get('Type', res.get('type'))), font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, width=100, anchor="w").pack(side="left", padx=10)
      #         ctk.CTkLabel(row_frame, text=str(res.get('Id', res.get('id'))), font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, width=200, anchor="w").pack(side="left", padx=10)
      #         ctk.CTkLabel(row_frame, text=f"{res.get('subTreeCount', 0):,}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_MAIN, width=100, anchor="w").pack(side="left", padx=10)

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


  def export_current_report(self):
    if not hasattr(self, "last_scan_data"):
      return
    
    data = self.last_scan_data
    
    # Exclude complex structures for summary
    summary_data = {k: v for k, v in data.items() if k not in [
        "driveMetrics", 
        "licenseMetrics", 
        "siteMetrics", 
        "tenantLevelFileSizeDistribution", 
        "tenantLevelLargeResources",
        "maxFolderDepth",
        "maxSubsiteDepth",
        "subsiteCount",
        "batches",
        "buckets",
        "total_eta",
        "df"
      ]
    }
    
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
      writer.writerow(["Type", "ID", "SubTreeCount", "Drive"])
      large_resources = data.get("tenantLevelLargeResources", [])
      for res in large_resources:
        writer.writerow([
            res.get("Type", res.get("type", "")),
            res.get("Id", res.get("id", "")),
            res.get("subTreeCount", 0),
            self._get_display_name(res.get("drive", ""))
        ])
        
      writer.writerow([]) # Blank line separator
      
      # Section 5: Site Details
      writer.writerow(["Site Details", ""])
      writer.writerow(["Site Name", "Site Level", "Folder Count", "File Count", "Resource Count", "Corpus Size", "Suggested Batch"])
      site_metrics = data.get("siteMetrics", {})
      df = data.get("df")
      
      for site_id, s_data in site_metrics.items():
        batch_name = ""
        if df is not None:
            match = df[df["Site Id"] == site_id]
            if not match.empty:
                batch_name = match["Suggested Batch"].iloc[0]
                
        writer.writerow([
            self._get_display_name(site_id), 
            s_data.get("siteLevel", 0),
            s_data.get("folderCount", 0),
            s_data.get("fileCount", 0),
            s_data.get("resourceCount", 0),
            self.format_size(s_data.get("totalSize", 0)),
            batch_name
        ])
        
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


  def _get_scan_configuration(self):
    config = super()._get_scan_configuration()
    config.includePersonalSites = self.val_include_personal_sites
    config.includeTeamSites = self.val_include_team_sites
    return config

  def start_scan(self):
    print("Invoked Start Scan")
    
    if not self.include_personal_sites.get() and not self.include_team_sites.get():
      messagebox.showerror("Validation Error", "At least one site type (Personal or Team) must be selected!")
      return
      
    # Save values to regular variables to avoid thread-safety issues in Tkinter
    self.val_include_personal_sites = self.include_personal_sites.get()
    self.val_include_team_sites = self.include_team_sites.get()
      
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
    self.create_progress_row(self.scan_container, "drive_parsing", "Metrics Calculation", mode="determinate")
    self.create_progress_row(self.scan_container, "plan_generation", "Generating Migration Plan", mode="determinate")

    import threading
    threading.Thread(target=self.execute_migration_scan, args=(config,)).start()