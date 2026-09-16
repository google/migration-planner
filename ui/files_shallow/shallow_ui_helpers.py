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

"""UI helpers and configuration state handlers for Shallow Scan."""

from util.constants import (
    COLOR_PRIMARY,
    COLOR_TEXT_MAIN,
    COLOR_TEXT_SUB,
    FONT_BODY_BOLD,
    FONT_BODY_SMALL,
)


def build_shallow_scan_toggle(tool, ctk):
  """Builds the Shallow Scan toggle switch at the top of Advanced Settings."""
  shallow_frame = ctk.CTkFrame(tool.adv_frame, fg_color="transparent")
  shallow_frame.pack(fill="x", padx=15, pady=(12, 5))

  tool.switch_shallow_scan = ctk.CTkSwitch(
      shallow_frame,
      text="Shallow Scan",
      variable=tool.shallow_scan,
      command=lambda: on_shallow_scan_toggle(tool),
      font=FONT_BODY_BOLD,
      text_color=COLOR_TEXT_MAIN,
      progress_color=COLOR_PRIMARY,
  )
  tool.switch_shallow_scan.pack(side="left", padx=(5, 10))

  ctk.CTkLabel(
      shallow_frame,
      text=(
          "* Fast tenant-wide estimation using usage reports and item counts."
      ),
      font=FONT_BODY_SMALL,
      text_color=COLOR_TEXT_SUB,
  ).pack(side="left", padx=5)


def on_shallow_scan_toggle(tool):
  """Handles enabling/disabling UI controls when Shallow Scan is toggled."""
  is_shallow = tool.shallow_scan.get()

  additional_checkboxes = [
      (tool.include_recycle_bin_contents, getattr(tool, "cb_recycle_bin", None)),
      (tool.include_file_versions, getattr(tool, "cb_file_versions", None)),
      (tool.scan_encrypted_files, getattr(tool, "cb_encrypted_files", None)),
      (tool.generate_folder_amr_map, getattr(tool, "cb_depth_report", None)),
  ]

  if is_shallow:
    # Uncheck and disable incompatible additional settings
    for var, widget in additional_checkboxes:
      var.set(False)
      if widget is not None:
        widget.configure(state="disabled")

    # Ensure Personal Sites (OneDrive) is selected; temporarily disable SharePoint Sites
    tool.include_personal_sites.set(True)
    tool.include_team_sites.set(False)
    if hasattr(tool, "cb_sharepoint_sites") and tool.cb_sharepoint_sites is not None:
      tool.cb_sharepoint_sites.configure(state="disabled")

    # Temporarily enforce 'Scan All Sites' source and disable CSV upload
    tool._saved_user_csv_path = tool.user_csv_path.get()
    tool.user_source.set("tenant")
    tool.user_csv_path.set("")
    if hasattr(tool, "rb_upload_csv") and tool.rb_upload_csv is not None:
      tool.rb_upload_csv.configure(state="disabled")
    if hasattr(tool, "btn_browse_csv") and tool.btn_browse_csv is not None:
      tool.btn_browse_csv.configure(state="disabled")
  else:
    # Re-enable all additional settings checkboxes
    for _, widget in additional_checkboxes:
      if widget is not None:
        widget.configure(state="normal")

    # Re-enable SharePoint Sites checkbox
    if hasattr(tool, "cb_sharepoint_sites") and tool.cb_sharepoint_sites is not None:
      tool.cb_sharepoint_sites.configure(state="normal")

    # Re-enable CSV upload controls and restore previous CSV path if any
    if hasattr(tool, "rb_upload_csv") and tool.rb_upload_csv is not None:
      tool.rb_upload_csv.configure(state="normal")
    if hasattr(tool, "btn_browse_csv") and tool.btn_browse_csv is not None:
      tool.btn_browse_csv.configure(state="normal")
    if getattr(tool, "_saved_user_csv_path", ""):
      tool.user_csv_path.set(tool._saved_user_csv_path)
