from ui.exchange_online_ui import MigrationEstimatorTool
from ui import utils as ui_utils
from util.constants import *
from datetime import timedelta, datetime
import os
import customtkinter as ctk
import time
import psutil
from tkinter import messagebox
from util.monitoring import ResourceMonitor
from estimators.factory import EstimatorFactory
from util.enums import FailureType
import json
import pandas as pd
import math
import re

def format_range(low, high):
  def format_boundary(kb_val):
    if kb_val >= 1024**4:
      pb = kb_val / (1024**4)
      if pb.is_integer():
        return f"{int(pb)} PB"
      return f"{pb:.2f} PB"
    elif kb_val >= 1024**3:
      tb = kb_val / (1024**3)
      if tb.is_integer():
        return f"{int(tb)} TB"
      return f"{tb:.2f} TB"
    elif kb_val >= 1024**2:
      gb = kb_val / (1024**2)
      if gb.is_integer():
        return f"{int(gb)} GB"
      return f"{gb:.2f} GB"
    elif kb_val >= 1024:
      mb = kb_val / 1024
      if mb.is_integer():
        return f"{int(mb)} MB"
      return f"{mb:.2f} MB"
    else:
      return f"{kb_val} KB"

  if low == 0:
    return f"< {format_boundary(high)}"
  elif high == float("inf"):
    return f">= {format_boundary(low)}"
  else:
    adjusted_low = low - 1 if low % 1024 == 1 else low
    return f"{format_boundary(adjusted_low)} - {format_boundary(high)}"

def get_bucket_column_header(low, high):
  return f"Files {format_range(low, high)}"


class FileMigrationEstimatorTool(MigrationEstimatorTool):
  def __init__(self):
    try:
      self.show_eta = os.environ.get("SHOW_ETA", "true").lower() == "true"
    except:
      self.show_eta = False

    super().__init__()
    self.factory = None

  def setup_variables(self):
    super().setup_variables()
    self.include_personal_sites = ctk.BooleanVar(value=True)
    self.include_team_sites = ctk.BooleanVar(value=False)
    self.eta_min_users = ctk.IntVar(value=1000)
    self.eta_max_users = ctk.IntVar(value=5000)

  def _is_valid_email(self, val):
    return bool(re.match(r'^[^@]+@[^@]+\.[^@]+$', val))

  def _is_valid_url(self, val):
    return val.startswith("http://") or val.startswith("https://")

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
      text="Scan All Sites",
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
        text="SharePoint Sites",
        variable=self.include_team_sites,
        corner_radius=4,
        fg_color=COLOR_PRIMARY,
        border_color=COLOR_TEXT_SUB,
    ).pack(side="left", padx=10)
    
    # Concurrency settings
    ui_utils.build_concurrency_settings_slider(self, ctk, useConcurrencyHeading=True)

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
            bar.configure(mode="indeterminate")
            bar.start()
          if widget.winfo_exists():
            text = f"Sites: {count}"
            if team_site_count > 0:
              text += f" | SharePoint Sites: {team_site_count}"
            if personal_site_count > 0:
              text += f" | Personal (OneDrive) Sites: {personal_site_count}"
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
            bar.configure(mode="indeterminate")
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
                if self.show_eta:
                  plan_text = "Plan generated, please wait while we prepare the final dashboard..."
                else:
                  plan_text = "Report generated, please wait while we prepare the final dashboard..."
                widget_lbl.configure(
                    text=plan_text
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

  def _try_get_metrics_from_csv_report(self, config):
    if self.user_source.get() != "csv" or not config.csv_path or not os.path.exists(config.csv_path):
      return None
      
    try:
      df = pd.read_csv(config.csv_path)
    except Exception:
      return None

    df.columns = df.columns.str.strip()

    req_cols = {
        "Subsite Count",
        "DL Count",
        "List Count",
        "Folder Count",
        "File Count",
        "Shortcut Count",
        "Folder Count > Depth Limit 100",
        "File Count > Depth Limit 100",
        "Folder with > 500k item count",
        "Corpus Size",
    }
    id_col = "Site URL/Name" if "Site URL/Name" in df.columns else ("Site Id" if "Site Id" in df.columns else ("Entity" if "Entity" in df.columns else None))
    if not id_col or not req_cols.issubset(df.columns):
      return None

    for idx, row in df.iterrows():
      val = str(row[id_col]).strip()
      if "/personal/" not in val.lower():
        raise ValueError(f"Row {idx+2}: URL '{val}' does not contain '/personal/'. Only OneDrive personal site reports are supported.")

    def _parse_size_str(val):
      if isinstance(val, (int, float)):
        return float(val)
      s = str(val).strip()
      match = re.match(r'^([\d\.]+)\s*([A-Za-z]+)$', s)
      if not match:
        try:
          return float(s)
        except Exception:
          return 0.0
      num, unit = float(match.group(1)), match.group(2).upper()
      multipliers = {
          "PB": 1024**5, "TB": 1024**4, "GB": 1024**3,
          "MB": 1024**2, "KB": 1024, "BYTES": 1
      }
      return num * multipliers.get(unit, 1)

    site_metrics = {}
    for _, row in df.iterrows():
      site_id = str(row[id_col]).strip()
      folder_cnt = int(pd.to_numeric(row.get("Folder Count", 0), errors="coerce") or 0)
      file_cnt = int(pd.to_numeric(row.get("File Count", 0), errors="coerce") or 0)
      shortcut_cnt = int(pd.to_numeric(row.get("Shortcut Count", 0), errors="coerce") or 0)
      res_cnt = int(pd.to_numeric(row.get("Resource Count", folder_cnt + file_cnt + shortcut_cnt), errors="coerce") or 0)
      
      site_metrics[site_id] = {
          "subsiteCount": int(pd.to_numeric(row.get("Subsite Count", 0), errors="coerce") or 0),
          "dlCount": int(pd.to_numeric(row.get("DL Count", 0), errors="coerce") or 0),
          "listCount": int(pd.to_numeric(row.get("List Count", 0), errors="coerce") or 0),
          "folderCount": folder_cnt,
          "fileCount": file_cnt,
          "shortcutCount": shortcut_cnt,
          "folderCountExceedingDepthLimit": int(pd.to_numeric(row.get("Folder Count > Depth Limit 100", 0), errors="coerce") or 0),
          "fileCountExceedingDepthLimit": int(pd.to_numeric(row.get("File Count > Depth Limit 100", 0), errors="coerce") or 0),
          "largeResourceCount": int(pd.to_numeric(row.get("Folder with > 500k item count", 0), errors="coerce") or 0),
          "totalSize": _parse_size_str(row.get("Corpus Size", 0)),
          "resourceCount": res_cnt,
      }

    self.skipped_actual_scan = True
    dl_total = int(pd.to_numeric(df.get("DL Count", pd.Series([0])), errors="coerce").fillna(0).sum())
    return {
        "siteMetrics": site_metrics,
        "siteCount": len(df),
        "subsiteCount": int(pd.to_numeric(df.get("Subsite Count", pd.Series([0])), errors="coerce").fillna(0).sum()),
        "personalSiteCount": len(df),
        "teamSiteCount": 0,
        "driveCounts": {"documentLibrary": dl_total},
        "personalSiteDLCount": dl_total,
        "teamSiteDLCount": 0,
        "folderCount": int(pd.to_numeric(df.get("Folder Count", pd.Series([0])), errors="coerce").fillna(0).sum()),
        "fileCount": int(pd.to_numeric(df.get("File Count", pd.Series([0])), errors="coerce").fillna(0).sum()),
        "shortcutCount": int(pd.to_numeric(df.get("Shortcut Count", pd.Series([0])), errors="coerce").fillna(0).sum()),
        "listCount": int(pd.to_numeric(df.get("List Count", pd.Series([0])), errors="coerce").fillna(0).sum()),
        "folderCountExceedingDepthLimit": int(pd.to_numeric(df.get("Folder Count > Depth Limit 100", pd.Series([0])), errors="coerce").fillna(0).sum()),
        "fileCountExceedingDepthLimit": int(pd.to_numeric(df.get("File Count > Depth Limit 100", pd.Series([0])), errors="coerce").fillna(0).sum()),
        "tenantLevelLargeResourceCount": int(pd.to_numeric(df.get("Folder with > 500k item count", pd.Series([0])), errors="coerce").fillna(0).sum()),
        "siteClassification": {site_id: "personal" for site_id in site_metrics.keys()},
        "licenseMetrics": {},
        "tenantLevelFileSizeDistribution": {},
        "tenantLevelLargeResources": [],
    }

  def _get_input_from_csv_if_uploaded(self, config):
    if self.user_source.get() != "csv":
      return {}
      
    if not config.csv_path or not os.path.exists(config.csv_path):
      raise Exception("CSV path invalid or file not found.")
    
    df_input = pd.read_csv(config.csv_path)
    df_input.columns = df_input.columns.str.strip()
    
    include_personal = self.include_personal_sites.get()
    include_team = self.include_team_sites.get()
    
    email_ids = []
    site_urls = []

    def _get_entity_type(val):
      val = str(val).strip().lower()
      if self._is_valid_email(val):
        return "onedrive"
      elif self._is_valid_url(val):
        return "sharepoint"
      else:
        raise ValueError(f"Invalid entity type: {val}")
    
    for _, row in df_input.iterrows():
      entity = str(row["Entity"]).strip()
      row_type = _get_entity_type(entity)
      if row_type == "onedrive":
        email_ids.append(entity)
      elif row_type == "sharepoint":
        site_urls.append(entity)
        
    return {"emailIds": email_ids, "siteUrls": site_urls}

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
        "siteCount": "Site Collection Count",
        "documentLibrary": "Document Library",
        "personalDrive": "Personal Drive",
        "businessDrive": "Business Drive",
        "unknownDrive": "Unknown Drive",
        "folderCount": "Folder Count",
        "fileCount": "File Count",
        "folderCountExceedingDepthLimit": "Folder Count > Depth Limit",
        "fileCountExceedingDepthLimit": "File Count > Depth Limit",
        "tenantLevelLargeResourceCount": "Tenant Level Large Resource Count"
      }

      self.id_to_display_name = id_to_display
      self.factory = EstimatorFactory(config, logger=self.log_msg, stop_event=self.stop_scan_event, id_to_display_name=id_to_display)
      
      self.skipped_actual_scan = False
      file_metrics = self._try_get_metrics_from_csv_report(config)
      failures = []
      if file_metrics is None:
        manager = self.factory.get_manager()
        manager.authenticate_all(self.log_msg, required_scopes=["Sites.Read.All", "Files.Read.All", "LicenseAssignment.Read.All"])
        estimator = self.factory.get_files_estimator(progress_update_callback=self.ui_update, hard_reset=True)

        # Calculate resource metrics for the tenant. Progress update to be made directly in the backend.
        input_map = self._get_input_from_csv_if_uploaded(config)
        file_metrics = estimator.calculate_resource_metrics(input_map, failures)
      else:
        estimator = self.factory.get_files_estimator(progress_update_callback=self.ui_update, hard_reset=True)
        self.ui_update("site_discovery", status="Done", count=file_metrics.get("siteCount", 0))
        self.ui_update("drive_discovery", status="Done", count=sum(file_metrics.get("driveCounts", {}).values()))
        self.ui_update("scan_progress", source="drive_parsing", progress=1.0)
        self.ui_update("phase_status", source="drive_parsing", status="complete")

      self.log_msg("\n" + "=" * 60)
      self.log_msg("📊 Failures and Warnings:")
      for failure in failures:
        prefix = "[WARNING] " if failure.get("type", None) == FailureType.NOT_FOUND.name else "[ERROR] "
        self.log_msg(prefix + str(failure))

      self.log_msg("=" * 60)
      self.ui_update("scan_progress", source="plan_generation", progress=0.5, status="running", extra_text="Calculating migration batches...")
      
      # Extract siteMetrics and build DataFrame
      site_metrics = file_metrics.get("siteMetrics", {})
      if not site_metrics:
        raise Exception("No sites were scanned successfully. Please check Azure app permissions or organization access policies.")
      site_data = []
      for site_id, s_data in site_metrics.items():
        site_data.append({
            "Site Id": site_id,
            "Subsite Count": s_data.get("subsiteCount", 0),
            "DL Count": s_data.get("dlCount", 0),
            "List Count": s_data.get("listCount", 0),
            "Folder Count": s_data.get("folderCount", 0),
            "File Count": s_data.get("fileCount", 0),
            "Shortcut Count": s_data.get("shortcutCount", 0),
            "Folder Count > Depth Limit 100": s_data.get("folderCountExceedingDepthLimit", 0),
            "File Count > Depth Limit 100": s_data.get("fileCountExceedingDepthLimit", 0),
            "Folder with > 500k item count": s_data.get("largeResourceCount", 0),
            "Corpus Size": s_data.get("totalSize", 0),
            "Resource Count": s_data.get("resourceCount", 0)
        })
      df = pd.DataFrame(site_data)
      
      if self.show_eta:
        df_final, batches_list, total_eta, buckets = self.calculate_migration_batches(df, file_metrics.get("licenseMetrics", {}))
        
        file_metrics["batches"] = batches_list
        file_metrics["buckets"] = buckets
        file_metrics["total_eta"] = total_eta
        file_metrics["df"] = df_final
        base_df = df_final
      else:
        base_df = df

      self.ui_update(
          "scan_progress",
          source="plan_generation",
          progress=0.66,
          status="running",
          extra_text="Generating reports..."
      )
      
      ts = datetime.now().strftime("%Y%m%d_%H%M%S")
      output_dir = os.path.join("outputs", ts)
      os.makedirs(output_dir, exist_ok=True)
      
      report_path = os.path.join(output_dir, f"site_report_{ts}.csv")
      logs_path = os.path.join(output_dir, f"logs_{ts}.log")

      monitor.stop()
      monitor.join()
      elapsed = str(timedelta(seconds=int(time.time() - start_time)))
      avg_cpu, max_cpu, avg_ram, max_ram = monitor.get_stats()
      total_ram_gb = psutil.virtual_memory().total / (1024**3)
      total_cpu_cores = psutil.cpu_count(logical=True)

      total_corpus = sum([s_data.get("totalSize", 0) for s_data in site_metrics.values()])
      self.log_msg("\n" + "=" * 40)
      self.log_msg(f"TOTAL TIME: {elapsed}")
      self.log_msg(
          f"Site Collections: {file_metrics.get('siteCount', 0):,} | Subsites: {file_metrics.get('subsiteCount', 0):,} | DLs: {sum(file_metrics.get('driveCounts', {}).values()):,} |"
          f" Folders: {file_metrics.get('folderCount', 0):,} | Files: {file_metrics.get('fileCount', 0):,} |"
          f" Shortcuts: {file_metrics.get('shortcutCount', 0):,} | Lists: {file_metrics.get('listCount', 0):,}"
      )
      self.log_msg(f"Total Size: {self.format_size(total_corpus)}")
      self.log_msg(f"System: {total_cpu_cores} Cores, {total_ram_gb:.1f}GB RAM")
      self.log_msg(f"CPU Avg/Peak: {avg_cpu:.1f}% / {max_cpu:.1f}%")
      self.log_msg(f"RAM Avg/Peak: {avg_ram:.1f}% / {max_ram:.1f}%")
      self.log_msg("=" * 40)
      
      # Create resolved copy of DataFrame for export to CSV and batches
      df_output = base_df.copy()
      original_site_ids = df_output["Site Id"].copy()
      df_output["Site Id"] = df_output["Site Id"].apply(self._get_display_name)
      df_output["Corpus Size"] = df_output["Corpus Size"].apply(self.format_size)
      df_output.rename(columns={"Site Id": "Site URL/Name"}, inplace=True)
      if "SortMetric" in df_output.columns:
        df_output.drop(columns=["SortMetric"], inplace=True)
      if "Resource Count" in df_output.columns:
        df_output.drop(columns=["Resource Count"], inplace=True)
      
      df_output.to_csv(report_path, index=False)
      
      batches_dir = os.path.join(output_dir, "suggested batches")
      os.makedirs(batches_dir, exist_ok=True)
      
      if self.show_eta:
        unique_batches = df_output["Suggested Batch"].unique()
        for batch in unique_batches:
          if not batch:
            continue
          batch_data = df_output[df_output["Suggested Batch"] == batch].copy()
          mapping = file_metrics.get("siteIdToMail")
          if mapping:
            batch_orig_ids = original_site_ids.loc[batch_data.index]
            entities = batch_orig_ids.map(mapping).fillna(batch_data["Site URL/Name"])
            batch_export = pd.DataFrame({"Entity": entities})
          else:
            batch_export = batch_data[["Site URL/Name"]].rename(
                columns={"Site URL/Name": "Entity"}
            )
          safe_name = batch.replace(" ", "")
          batch_path = os.path.join(batches_dir, f"{safe_name}.csv")
          batch_export.to_csv(batch_path, index=False)
        
      with self.log_lock:
        log_content = "\n".join(self.log_buffer)
      with open(logs_path, "w", encoding="utf-8") as f:
        f.write(log_content)
      
      self.ui_update("phase_status", source="plan_generation", status="complete")
      time.sleep(2)
      self.ui_update("complete", data=file_metrics)

    except Exception as e:
      self.log_msg(f"Process failed: {e}")
      self.ui_update("error", message=str(e))
    finally:
      if monitor is not None:
        monitor.stop()
    
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

  def calculate_migration_batches(self, df, licenseMetrics):
    # Ensure numeric columns
    if "Resource Count" not in df.columns:
      df["Resource Count"] = 0
    else:
      df["Resource Count"] = pd.to_numeric(df["Resource Count"], errors="coerce").fillna(0)

    if "Corpus Size" not in df.columns:
      df["Corpus Size"] = 0
    else:
      df["Corpus Size"] = pd.to_numeric(df["Corpus Size"], errors="coerce").fillna(0)

    df["SortMetric"] = df.apply(
      lambda x: max(
          (x["Corpus Size"] / FILES_GLOBAL_CORPUS_SIZE_LIMIT), (x["Resource Count"] / FILES_GLOBAL_COUNT_LIMIT)
      ),
      axis=1,
    )
    # 1. Sort Sites (Descending - Heaviest first)
    df_sorted_base = df.sort_values(by="SortMetric", ascending=False).copy()

    user_min_limit = self.val_eta_min_users
    user_max_limit = self.val_eta_max_users
    num_parallel = min(4, max(1, self.val_parallel_batches))
    max_allowed_batches = self.val_eta_max_batches

    candidate_hours = [3, 6, 12, 18, 24, 36, 48, 72, 120, 168, 240, 360, 480, 720, 1080, 1440]

    best_total_eta = float("inf")
    best_plan = None
    fallback_plan = None
    min_batches_seen = float("inf")

    def get_batch_eta(subset_df):
      def _get_qps_from_license_count():
        # Calculate number of licenses required
        license_count = licenseMetrics.get("totalAllotedUnits", {}).get("User", 0) + licenseMetrics.get("totalAllotedUnits", {}).get("Company", 0)
        if license_count <= 1000:
          qps = 4.8
        elif license_count <= 5000:
          qps = 9.6
        elif license_count <= 15000:
          qps = 14.4
        elif license_count <= 50000:
          qps = 19.2
        else:
          qps = 24
        
        return qps

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
        "FILES_GLOBAL_COUNT_LIMIT": _get_qps_from_license_count(),
        "FILES_GLOBAL_CORPUS_SIZE_LIMIT": FILES_GLOBAL_CORPUS_SIZE_LIMIT,
      }
      return estimator.calculate_migration_eta(data)

    # Iterate through candidates
    for target_hours in candidate_hours:
      for current_parallel in range(1, num_parallel + 1):
        df_sorted = df_sorted_base.copy()
        df_sorted["Suggested Batch"] = ""

        # 2. Greedy Lane Assignment
        lanes = [{"total_time": 0.0, "sites": []} for _ in range(current_parallel)]
        
        for _, row in df_sorted.iterrows():
          # Calculate time for this single site
          site_df = pd.DataFrame([row])
          site_time = get_batch_eta(site_df)
          
          # Find lane with min total time
          target_lane = min(lanes, key=lambda l: l["total_time"])
          target_lane["sites"].append(row)
          target_lane["total_time"] += site_time

        # 3. Per-Lane Batching (Binary Search)
        final_buckets = []
        
        for lane_idx, lane in enumerate(lanes):
          lane_df = pd.DataFrame(lane["sites"])
          if lane_df.empty:
            continue
            
          total_users = len(lane_df)
          start_idx = 0
          raw_chunks = []

          # Partitioning Loop (Same as original but within lane)
          while start_idx < total_users:
            remaining_users = total_users - start_idx
            current_max = min(remaining_users, user_max_limit)
            current_min = min(user_min_limit, remaining_users)

            # Binary Search for Optimal Size
            min_subset = lane_df.iloc[start_idx : start_idx + current_min]
            if get_batch_eta(min_subset) > target_hours:
              chosen_size = current_min
            else:
              max_subset = lane_df.iloc[start_idx : start_idx + current_max]
              if get_batch_eta(max_subset) <= target_hours:
                chosen_size = current_max
              else:
                low = current_min
                high = current_max
                chosen_size = high
                while low <= high:
                  mid = (low + high) // 2
                  subset = lane_df.iloc[start_idx : start_idx + mid]
                  eta = get_batch_eta(subset)

                  if eta > target_hours:
                    chosen_size = mid
                    high = mid - 1
                  else:
                    low = mid + 1

            end_idx = start_idx + chosen_size
            final_subset = lane_df.iloc[start_idx:end_idx]
            w_eta = get_batch_eta(final_subset)

            raw_chunks.append({
                "start_idx": start_idx,
                "end_idx": end_idx,
                "sites": len(final_subset),
                "dl_count": int(final_subset["DL Count"].sum()) if "DL Count" in final_subset.columns else 0,
                "resource_count": int(final_subset["Resource Count"].sum()),
                "folder_count": int(final_subset["Folder Count"].sum()) if "Folder Count" in final_subset.columns else 0,
                "file_count": int(final_subset["File Count"].sum()) if "File Count" in final_subset.columns else 0,
                "shortcut_count": int(final_subset["Shortcut Count"].sum()) if "Shortcut Count" in final_subset.columns else 0,
                "corpus_size": float(final_subset["Corpus Size"].sum()) if "Corpus Size" in final_subset.columns else 0.0,
                "eta": w_eta,
                "df_subset": final_subset
            })
            start_idx = end_idx

          final_buckets.append({
              "id": lane_idx + 1,
              "total": sum(c["eta"] for c in raw_chunks),
              "batches": raw_chunks
          })

        # 4. Consolidation & Naming
        total_eta = max(b["total"] for b in final_buckets) if final_buckets else 0
        
        all_chunks_with_time = []
        for b_idx, b in enumerate(final_buckets):
          current_time = 0.0
          for chunk in b["batches"]:
            chunk["start_time"] = current_time
            chunk["bucket_idx"] = b_idx
            current_time += chunk["eta"]
            all_chunks_with_time.append(chunk)

        all_chunks_with_time.sort(key=lambda x: (x["start_time"], x["bucket_idx"]))

        final_batches_list = []
        for i, chunk in enumerate(all_chunks_with_time):
          batch_name = f"Batch {i+1}"
          chunk["name"] = batch_name
          final_batches_list.append(chunk)
          
          for _, row in chunk["df_subset"].iterrows():
              site_id = row["Site Id"]
              df_sorted.loc[df_sorted["Site Id"] == site_id, "Suggested Batch"] = batch_name

        num_batches = len(final_batches_list)
        self.log_msg(
            f"Evaluated Target {target_hours}h with {current_parallel} lanes: Generated {num_batches} batches | Total ETA: {self.format_eta(total_eta)}"
        )

        # 5. Selection Logic
        if num_batches <= max_allowed_batches:
          if total_eta < best_total_eta:
            best_total_eta = total_eta
            best_plan = (df_sorted, final_batches_list, total_eta, final_buckets)

        if num_batches < min_batches_seen:
          min_batches_seen = num_batches
          fallback_plan = (df_sorted, final_batches_list, total_eta, final_buckets)

    if best_plan is not None:
      df_final, final_batches_list, total_eta, buckets = best_plan
    else:
      df_final, final_batches_list, total_eta, buckets = fallback_plan

    return df_final, final_batches_list, total_eta, buckets

  def format_size(self, size_in_bytes):
    if size_in_bytes >= 1024**5:
      return f"{size_in_bytes / (1024**5):.2f} PB"
    elif size_in_bytes >= 1024**4:
      return f"{size_in_bytes / (1024**4):.2f} TB"
    elif size_in_bytes >= 1024**3:
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
    dl_count = self.format_metric(batch.get("dl_count", 0))
    folders_str = self.format_metric(batch.get("folder_count", 0))
    files_str = self.format_metric(batch.get("file_count", 0))
    shortcuts_str = self.format_metric(batch.get("shortcut_count", 0))
    size_str = self.format_size(batch.get("corpus_size", 0))
    
    info = (
        f"{batch['name']} - {sites_str} 🏢  |  {dl_count} 📦  |  {folders_str} 📁  |  {files_str} 📄  |  {shortcuts_str} 🔗  |  {size_str} 💾"
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

      self.create_stat_card(card_frame, "Total Corpus Size", f"{self.format_size(sum([entry.get('totalSize', 0) for entry in data.get('siteMetrics', {}).values()]))}", "🏢")
      self.create_stat_card(card_frame, "Site Collection Count", f"{data.get('siteCount'):,}", "🏢")
      self.create_stat_card(card_frame, "Subsite Count", f"{data.get('subsiteCount'):,}", "🏢")
      self.create_stat_card(card_frame, "Document Library Count", f"{sum(data.get('driveCounts', {}).values()):,}", "📁")
      self.create_stat_card(card_frame, "Folder Count", f"{data.get('folderCount', 0):,}", "📁")
      self.create_stat_card(card_frame, "File Count", f"{data.get('fileCount', 0):,}", "📄")
      self.create_stat_card(card_frame, "Shortcut Count", f"{data.get('shortcutCount', 0):,}", "🔗")
      self.create_stat_card(card_frame, "List Count", f"{data.get('listCount', 0):,}", "🗃️")
      self.create_stat_card(card_frame, "Folder count beyond depth limit 100", f"{data.get('folderCountExceedingDepthLimit', 0):,}", "📁")
      self.create_stat_card(card_frame, "File count beyond depth limit 100", f"{data.get('fileCountExceedingDepthLimit', 0):,}", "📄")
      self.create_stat_card(card_frame, "Large Resource Count (Folders with >500k items)", f"{data.get('tenantLevelLargeResourceCount', 0):,}", "📄")

      if self.show_eta:
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

      # Container for Paginated Content
      self.paginated_frame = ctk.CTkFrame(
          self.view_results, fg_color="transparent"
      )
      self.paginated_frame.pack(fill="x", expand=True)

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
          
          # Header Row
          header_frame = ctk.CTkFrame(dist_frame, fg_color="transparent")
          header_frame.pack(fill="x", padx=15, pady=(10, 5))
          ctk.CTkLabel(header_frame, text="Range", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN, width=200, anchor="w").pack(side="left")
          ctk.CTkLabel(header_frame, text="Count", font=FONT_BODY_BOLD, text_color=COLOR_TEXT_MAIN, width=150, anchor="w").pack(side="left")
          
          # Data Rows
          for bucket in buckets:
              range_vals = bucket.get("sizeRange", (0, 0))
              range_str = format_range(range_vals[0], range_vals[1])
              file_ids = bucket.get("fileIDs", [])
              count = bucket.get("count", len(file_ids))
              
              row_frame = ctk.CTkFrame(dist_frame, fg_color="transparent")
              row_frame.pack(fill="x", padx=15, pady=3)
              
              ctk.CTkLabel(row_frame, text=range_str, font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB, width=200, anchor="w").pack(side="left")
              ctk.CTkLabel(row_frame, text=f"{count:,}", font=FONT_BODY_MEDIUM, text_color=COLOR_TEXT_SUB, width=150, anchor="w").pack(side="left")

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

      if self.show_eta:
        self.selected_page_size = "50"
        self.render_paginated_view(0)

    except Exception as e:
      for w in self.view_results.winfo_children():
        w.destroy()
      ctk.CTkLabel(
          self.view_results,
          text=f"Error displaying results: {e}",
          wraplength=700,
      ).pack(padx=20, pady=20)
      self.view_results.pack(fill="both", expand=True)

  def _validate_csv(self):
    csv_path = self.user_csv_path.get()
    if not csv_path or not os.path.exists(csv_path):
      messagebox.showerror("Validation Error", "CSV path invalid or file not found.")
      raise ValueError("CSV path invalid or file not found.")

    try:
      df = pd.read_csv(csv_path)
    except Exception as e:
      messagebox.showerror("Validation Error", f"Failed to read CSV file: {e}")
      raise ValueError(f"Failed to read CSV file: {e}")

    df.columns = df.columns.str.strip()

    # 1. No empty rows
    if df.empty:
      messagebox.showerror("Validation Error", "CSV file is empty.")
      raise ValueError("CSV file is empty.")

    # 2. No empty/null fields/cells present in any row
    if df.isnull().any().any():
      messagebox.showerror("Validation Error", "CSV contains empty or null values.")
      raise ValueError("CSV contains empty or null values.")

    for col in df.columns:
      if (df[col].astype(str).str.strip() == "").any():
        messagebox.showerror("Validation Error", f"CSV contains empty values in column '{col}'.")
        raise ValueError(f"CSV contains empty values in column '{col}'.")

    # 3. Check columns and types
    include_personal = self.include_personal_sites.get()
    include_team = self.include_team_sites.get()

    if "Site URL/Name" in df.columns:
      df.rename(columns={"Site URL/Name": "Entity"}, inplace=True)
    elif "Site Id" in df.columns and "Entity" not in df.columns:
      df.rename(columns={"Site Id": "Entity"}, inplace=True)

    expected_cols = {"Entity"}
    report_cols = {
        "Subsite Count",
        "DL Count",
        "List Count",
        "Folder Count",
        "File Count",
        "Shortcut Count",
        "Folder Count > Depth Limit 100",
        "File Count > Depth Limit 100",
        "Folder with > 500k item count",
        "Corpus Size",
    }
    is_report_csv = "Entity" in df.columns and report_cols.issubset(df.columns)

    if is_report_csv and include_team:
      messagebox.showerror("Validation Error", "When calculating ETA from an uploaded site report CSV, SharePoint Sites must not be selected in Site Types to Scan.")
      raise ValueError("SharePoint Sites selected for report CSV")

    if set(df.columns) != expected_cols and not is_report_csv:
      messagebox.showerror("Validation Error", "CSV must contain exactly the 'Entity' column or valid site report columns.")
      raise ValueError("CSV must contain exactly the 'Entity' column or valid site report columns.")

    if not is_report_csv:
      for idx, row in df.iterrows():
        entity = str(row["Entity"]).strip()
        if not self._is_valid_email(entity) and not self._is_valid_url(entity):
          messagebox.showerror("Validation Error", f"Row {idx+2}: Entity '{entity}' is not a valid UPN/Email ID or Site Collection URL.")
          raise ValueError(f"Invalid Email ID / Site Collection Url at row {idx+2}")
        
        if self._is_valid_email(entity) and not include_personal:
          messagebox.showerror("Validation Error", f"Row {idx+2}: Entity '{entity}' is a UPN/Email ID. Please enable Include Personal Sites in the config.")
          raise ValueError(f"Unexpected Email ID at row {idx+2}")
        
        if self._is_valid_url(entity) and not include_team:
          messagebox.showerror("Validation Error", f"Row {idx+2}: Entity '{entity}' is a Site Collection URL. Please enable Include Sharepoint Sites in the config.")
          raise ValueError(f"Unexpected Site Collection URL at row {idx+2}")

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
      
      total_corpus_size = sum([entry.get('totalSize', 0) for entry in data.get('siteMetrics', {}).values()])
      summary_rows = [
          ("Total Corpus Size", self.format_size(total_corpus_size)),
          ("Site Collection Count", data.get("siteCount", 0)),
          ("Subsite Count", data.get("subsiteCount", 0)),
          ("Personal (OneDrive) Site / Subsite Count", data.get("personalSiteCount", 0)),
          ("SharePoint Site / Subsite Count", data.get("teamSiteCount", 0)),
          ("DL Count", sum(data.get("driveCounts", {}).values())),
          ("Personal (OneDrive) DL Count", data.get("personalSiteDLCount", 0)),
          ("SharePoint DL Count", data.get("teamSiteDLCount", 0)),
          ("List Count", data.get("listCount", 0)),
          ("Folder Count", data.get("folderCount", 0)),
          ("File Count", data.get("fileCount", 0)),
          ("Shortcut Count", data.get("shortcutCount", 0)),
          ("Folder count beyond depth limit 100", data.get("folderCountExceedingDepthLimit", 0)),
          ("File count beyond depth limit 100", data.get("fileCountExceedingDepthLimit", 0)),
          ("Large Resource Count (Folders with >500k items)", data.get("tenantLevelLargeResourceCount", 0))
      ]
      
      for label, val in summary_rows:
          writer.writerow([label, val])
      
      writer.writerow([]) # Blank line separator
      
      # Section 2: License Metrics
      license_data = data.get("licenseMetrics", {})
      writer.writerow(["Total License Count", license_data.get("totalAllotedUnits", {}).get("User", 0) + license_data.get("totalAllotedUnits", {}).get("Company", 0)])
      
      writer.writerow([]) # Blank line separator
      
      # Section 3: File Size Distribution
      writer.writerow(["File Size Distribution", ""])
      writer.writerow(["Range", "Count"])
      dist_data = data.get("tenantLevelFileSizeDistribution", {})
      buckets = dist_data.get("buckets", [])
      for bucket in buckets:
        range_vals = bucket.get("sizeRange", (0, 0))
        range_str = format_range(range_vals[0], range_vals[1])
        count = bucket.get("count", 0)
        writer.writerow([range_str, count])
        
      writer.writerow([]) # Blank line separator
      
      # Section 4: Large Resources
      if len(data.get("tenantLevelLargeResources", [])) > 0:
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
      
      if len(data.get("siteMetrics", {}).items()) > 0:
        # Section 5: Site Details
        writer.writerow(["Site Details", ""])
        if "siteIdToMail" not in data:
          row = ["Site Collection", "Subsite Count", "DL Count", "List Count", "Folder Count", "File Count", "Shortcut Count", "Folder Count > Depth Limit 100", "File Count > Depth Limit 100", "Folder with > 500k item count", "Corpus Size"]
        else:
          row = ["Site Collection", "Email Id", "Subsite Count", "DL Count", "List Count", "Folder Count", "File Count", "Shortcut Count", "Folder Count > Depth Limit 100", "File Count > Depth Limit 100", "Folder with > 500k item count", "Corpus Size"]

        if self.show_eta:
          row.append("Suggested Batch")

        writer.writerow(row)
        site_metrics = data.get("siteMetrics", {})

        personal_site_metrics = {}
        team_site_metrics = {}

        for key, value in site_metrics.items():
          if data.get("siteClassification", {}).get(key, "") == "personal":
            personal_site_metrics[key] = value
          elif data.get("siteClassification", {}).get(key, "") == "teams":
            team_site_metrics[key] = value
          else:
            raise Exception(f"Invalid Site Type found for site {key}")

        df = data.get("df")
        
        site_metric_arr = [personal_site_metrics, team_site_metrics]
        headers_arr = ["Personal (OneDrive) Sites", "SharePoint Sites"]

        for idx in range(0, len(site_metric_arr)):
          if len(site_metric_arr[idx]) == 0:
            continue
            
          writer.writerow([headers_arr[idx]])
          curr_site_metrics = site_metric_arr[idx]

          for site_id, s_data in curr_site_metrics.items():
            batch_name = ""
            if df is not None:
                match = df[df["Site Id"] == site_id]
                if not match.empty and self.show_eta:
                    batch_name = match["Suggested Batch"].iloc[0]

            if "siteIdToMail" not in data:
              row = [
                  self._get_display_name(site_id), 
                  s_data.get("subsiteCount", 0),
                  s_data.get("dlCount", 0),
                  s_data.get("listCount", 0),
                  s_data.get("folderCount", 0),
                  s_data.get("fileCount", 0),
                  s_data.get("shortcutCount", 0),
                  s_data.get("folderCountExceedingDepthLimit", 0),
                  s_data.get("fileCountExceedingDepthLimit", 0),
                  s_data.get("largeResourceCount", 0),
                  self.format_size(s_data.get("totalSize", 0))
              ]
            else:
              row = [
                self._get_display_name(site_id), 
                data.get("siteIdToMail", {}).get(site_id, ""),
                s_data.get("subsiteCount", 0),
                s_data.get("dlCount", 0),
                s_data.get("listCount", 0),
                s_data.get("folderCount", 0),
                s_data.get("fileCount", 0),
                s_data.get("shortcutCount", 0),
                s_data.get("folderCountExceedingDepthLimit", 0),
                s_data.get("fileCountExceedingDepthLimit", 0),
                s_data.get("largeResourceCount", 0),
                self.format_size(s_data.get("totalSize", 0))
            ]

            if self.show_eta:
              row.append(batch_name)
            writer.writerow(row)
          
          writer.writerow([]) # Blank line separator

  def _get_scan_configuration(self):
    config = super()._get_scan_configuration()
    config.includePersonalSites = self.val_include_personal_sites
    config.includeTeamSites = self.val_include_team_sites
    return config

  def start_scan(self):    
    if not self.include_personal_sites.get() and not self.include_team_sites.get():
      messagebox.showerror("Validation Error", "At least one site type (Personal (OneDrive) or SharePoint) must be selected!")
      return

    # ETA to be only shown for OneDrive sites atm
    self.show_eta = (os.environ.get("SHOW_ETA", "true").lower() == "true") and (self.include_personal_sites.get() and not self.include_team_sites.get())  
    
    if self.user_source.get() == "csv":
      self._validate_csv()

    # Save values to regular variables to avoid thread-safety issues in Tkinter
    self.val_include_personal_sites = self.include_personal_sites.get()
    self.val_include_team_sites = self.include_team_sites.get()
    self.val_eta_min_users = self.eta_min_users.get()
    self.val_eta_max_users = self.eta_max_users.get()
    self.val_parallel_batches = self.parallel_batches.get()
    self.val_eta_max_batches = self.eta_max_batches.get()
      
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

    self.create_progress_row(self.scan_container, "sites", "Site Discovery", mode="determinate")
    self.create_progress_row(self.scan_container, "drives", "Drive Discovery", mode="determinate")
    self.create_progress_row(self.scan_container, "drive_parsing", "Metrics Calculation", mode="determinate")

    if self.show_eta:
      plan_text = "Generating Migration Plan"
    else:
      plan_text = "Generating Estimation Report"
      
    self.create_progress_row(self.scan_container, "plan_generation", plan_text, mode="determinate")

    import threading
    threading.Thread(target=self.execute_migration_scan, args=(config,)).start()

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
  
  app = FileMigrationEstimatorTool()
  app.mainloop()