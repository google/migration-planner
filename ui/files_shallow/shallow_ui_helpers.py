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

"""UI helpers, certificate modal prompt, and Shallow Scan formatting utilities."""

import logging
from tkinter import messagebox
import pandas as pd
from core.cert_auth import (
    check_certificate_exists,
    generate_certificate,
    load_certificate,
)
from util.constants import (
    COLOR_BACKGROUND,
    COLOR_ERROR,
    COLOR_OUTLINE,
    COLOR_OUTLINE_LIGHT,
    COLOR_PRIMARY,
    COLOR_PRIMARY_HOVER,
    COLOR_SECONDARY_HOVER,
    COLOR_SURFACE,
    COLOR_TEXT_MAIN,
    COLOR_TEXT_SUB,
    COLOR_TONAL_BG,
    COLOR_TONAL_HOVER,
    COLOR_TONAL_TEXT,
    FONT_BODY_BOLD,
    FONT_BODY_MEDIUM,
    FONT_BODY_SMALL,
    FONT_HEADER_MEDIUM,
    FONT_HEADER_SMALL,
)

logger = logging.getLogger(__name__)


def format_stat_value(val) -> str:
  """Formats numeric stat values with commas, or returns string as-is (e.g. 'N/A')."""
  if isinstance(val, (int, float)) and not isinstance(val, bool):
    return f"{int(val):,}"
  return str(val)


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
          "* Fast estimation using SharePoint REST StorageMetrics and ItemCounts."
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
    if (
        hasattr(tool, "cb_sharepoint_sites")
        and tool.cb_sharepoint_sites is not None
    ):
      tool.cb_sharepoint_sites.configure(state="disabled")
  else:
    # Re-enable all additional settings checkboxes
    for _, widget in additional_checkboxes:
      if widget is not None:
        widget.configure(state="normal")

    # Re-enable SharePoint Sites checkbox
    if (
        hasattr(tool, "cb_sharepoint_sites")
        and tool.cb_sharepoint_sites is not None
    ):
      tool.cb_sharepoint_sites.configure(state="normal")


class CertDecryptionErrorDialog:
  """Exact certificate decryption error modal dialog from splash-one."""

  def __init__(self, parent, error_message: str, ctk):
    self.dialog = ctk.CTkToplevel(parent)
    self.dialog.title("Certificate Decryption Error")
    self.dialog.geometry("500x260")
    self.dialog.resizable(False, False)
    self.dialog.transient(parent)
    self.dialog.grab_set()

    # Center relative to parent window
    parent_x = parent.winfo_rootx()
    parent_y = parent.winfo_rooty()
    parent_w = parent.winfo_width()
    parent_h = parent.winfo_height()
    x = parent_x + (parent_w - 500) // 2
    y = parent_y + (parent_h - 260) // 2
    self.dialog.geometry(f"+{x}+{y}")

    self.result = None  # "retry", "generate", or None

    self.dialog.configure(fg_color=COLOR_SURFACE)

    pad_frame = ctk.CTkFrame(self.dialog, fg_color="transparent")
    pad_frame.pack(fill="both", expand=True, padx=24, pady=24)

    lbl_msg = ctk.CTkLabel(
        pad_frame,
        text=(
            "Unable to decrypt existing certificate passkey using the provided"
            " Client Secret. How would you like to proceed?"
        ),
        font=FONT_BODY_MEDIUM,
        text_color=COLOR_TEXT_MAIN,
        wraplength=450,
        justify="left",
        anchor="w",
    )
    lbl_msg.pack(anchor="w", pady=(0, 10))

    lbl_detail = ctk.CTkLabel(
        pad_frame,
        text=f"Error details: {error_message}",
        font=FONT_BODY_SMALL,
        text_color=COLOR_ERROR,
        wraplength=450,
        justify="left",
        anchor="w",
    )
    lbl_detail.pack(anchor="w", pady=(0, 24))

    btn_frame = ctk.CTkFrame(pad_frame, fg_color="transparent")
    btn_frame.pack(fill="x", side="bottom")

    self.btn_retry = ctk.CTkButton(
        btn_frame,
        text="Retry with existing secret",
        font=FONT_BODY_BOLD,
        width=180,
        height=36,
        fg_color="transparent",
        border_width=1,
        border_color=COLOR_OUTLINE,
        text_color=COLOR_PRIMARY,
        hover_color=COLOR_SECONDARY_HOVER,
        command=self._on_retry,
    )
    self.btn_retry.pack(side="left")

    self.btn_generate = ctk.CTkButton(
        btn_frame,
        text="Generate new certificate",
        font=FONT_BODY_BOLD,
        width=200,
        height=36,
        fg_color=COLOR_PRIMARY,
        text_color="white",
        hover_color=COLOR_PRIMARY_HOVER,
        command=self._on_generate,
    )
    self.btn_generate.pack(side="right")

    self.dialog.protocol("WM_DELETE_WINDOW", self._on_close)

  def _on_retry(self):
    self.result = "retry"
    self.dialog.grab_release()
    self.dialog.destroy()

  def _on_generate(self):
    self.result = "generate"
    self.dialog.grab_release()
    self.dialog.destroy()

  def _on_close(self):
    self.result = None
    self.dialog.grab_release()
    self.dialog.destroy()


def _show_cert_upload_instructions_modal(
    parent, pem_path: str, client_id: str, secret: str, tenant_id: str, ctk
) -> bool:
  """Displays certificate upload instructions screen matching splash-one when a new certificate is generated."""
  confirmed = {"proceed": False}

  modal = ctk.CTkToplevel(parent)
  modal.title("Certificate Upload")
  modal.geometry("700x520")
  modal.configure(fg_color=COLOR_SURFACE)
  modal.transient(parent)
  modal.grab_set()

  parent_x = parent.winfo_rootx()
  parent_y = parent.winfo_rooty()
  parent_w = parent.winfo_width()
  parent_h = parent.winfo_height()
  x = parent_x + (parent_w - 700) // 2
  y = parent_y + (parent_h - 520) // 2
  modal.geometry(f"+{x}+{y}")

  cert_container = ctk.CTkFrame(modal, fg_color="transparent")
  cert_container.pack(pady=30, padx=40, fill="both", expand=True)

  ctk.CTkLabel(
      cert_container,
      text="Certificate Upload",
      font=FONT_HEADER_MEDIUM,
      text_color=COLOR_PRIMARY,
  ).pack(anchor="w", pady=(0, 15))

  intro_text = (
      "A new security certificate has been generated for hybrid authentication."
  )
  ctk.CTkLabel(
      cert_container,
      text=intro_text,
      font=FONT_BODY_MEDIUM,
      text_color=COLOR_TEXT_MAIN,
      justify="left",
      wraplength=620,
  ).pack(anchor="w", pady=(0, 10))

  # Prominent Configuration Callout Card matching splash-one
  cert_info_card = ctk.CTkFrame(
      cert_container,
      fg_color=COLOR_TONAL_BG,
      border_width=1,
      border_color=COLOR_OUTLINE_LIGHT,
      corner_radius=8,
  )
  cert_info_card.pack(fill="x", pady=(15, 25))

  header_row = ctk.CTkFrame(cert_info_card, fg_color="transparent")
  header_row.pack(fill="x", padx=15, pady=(15, 5))

  ctk.CTkLabel(
      header_row,
      text="Upload Instructions",
      font=FONT_BODY_BOLD,
      text_color=COLOR_TONAL_TEXT,
      justify="left",
  ).pack(side="left")

  def _copy_path(btn):
    parent.clipboard_clear()
    parent.clipboard_append(pem_path)
    btn.configure(text="Copied ✓")
    modal.after(1500, lambda: btn.configure(text="Copy Path"))

  btn_copy = ctk.CTkButton(
      header_row,
      text="Copy Path",
      width=95,
      height=28,
      fg_color=COLOR_SURFACE,
      text_color=COLOR_PRIMARY,
      hover_color=COLOR_SECONDARY_HOVER,
      font=FONT_BODY_SMALL,
  )
  btn_copy.configure(command=lambda b=btn_copy: _copy_path(b))
  btn_copy.pack(side="right")

  instructions_body = (
      f"1. Locate the certificate file generated at:\n   {pem_path}\n\n"
      f"2. Log in to the Microsoft Azure portal and navigate to the App Registration with Client ID:\n   {client_id}\n\n"
      "3. Upload the certificate under:\n   Certificates & secrets -> Certificates -> Upload certificate"
  )
  ctk.CTkLabel(
      cert_info_card,
      text=instructions_body,
      font=FONT_BODY_MEDIUM,
      text_color=COLOR_TEXT_MAIN,
      justify="left",
      wraplength=580,
  ).pack(anchor="w", padx=15, pady=(0, 15))

  def on_cert_continue_clicked():
    """Validates certificate after user claims to have uploaded it and continues."""
    try:
      load_certificate(secret, tenant_id=tenant_id, client_id=client_id)
      confirmed["proceed"] = True
      modal.grab_release()
      modal.destroy()
    except Exception as e:
      logger.error(f"Certificate validation failed: {e}", exc_info=True)
      messagebox.showerror(
          "Certificate Verification Error",
          f"Unable to verify certificate.\n\nError: {e}",
          parent=modal,
      )

  def on_cancel():
    confirmed["proceed"] = False
    modal.grab_release()
    modal.destroy()

  modal.protocol("WM_DELETE_WINDOW", on_cancel)

  ctk.CTkButton(
      cert_container,
      text="Continue",
      command=on_cert_continue_clicked,
      height=40,
      corner_radius=20,
      font=FONT_BODY_BOLD,
      fg_color=COLOR_PRIMARY,
      hover_color=COLOR_PRIMARY_HOVER,
  ).pack(fill="x", side="bottom")

  parent.wait_window(modal)
  return confirmed["proceed"]


def ensure_certificates_and_prompt(tool, config, ctk) -> bool:
  """Verifies or generates certificates matching exact splash-one flow.

  - If certificate exists and unlocks with client secret: proceeds immediately
    without generating a new certificate or showing upload instructions.
  - If certificate exists but decryption fails: shows CertDecryptionErrorDialog
    with 'Retry with existing secret' and 'Generate new certificate'.
  - If certificate does not exist: generates new certificate and shows upload
    instructions screen.
  """
  tenant = config.tenant_id
  for client, secret in zip(config.client_ids, config.client_secrets):
    if check_certificate_exists(tenant_id=tenant, client_id=client):
      try:
        # Decrypt the PFX certificate using the client secret
        load_certificate(secret, tenant_id=tenant, client_id=client)
        tool.log_msg(
            f"Existing security certificate for App {client[:5]}... unlocked and validated."
        )
      except Exception as e:
        logger.error(f"Certificate decryption/load failed: {e}", exc_info=True)
        err_dialog = CertDecryptionErrorDialog(tool, str(e), ctk)
        tool.wait_window(err_dialog.dialog)

        if err_dialog.result == "retry":
          logger.info("Option 1 chosen: Retry connection with correct client secret.")
          return False
        elif err_dialog.result == "generate":
          logger.info("Option 2 chosen: Overwrite and generate a new certificate.")
          try:
            pem_path, _ = generate_certificate(
                secret, tenant_id=tenant, client_id=client
            )
            if not _show_cert_upload_instructions_modal(
                tool, pem_path, client, secret, tenant, ctk
            ):
              return False
          except Exception as gen_err:
            logger.error(f"Certificate generation failed: {gen_err}", exc_info=True)
            messagebox.showerror(
                "Certificate Generation Error",
                f"Unable to generate certificate: {gen_err}",
                parent=tool,
            )
            return False
        else:
          logger.info("Decryption modal closed without option selection.")
          return False
    else:
      try:
        # Generate new certificate and pfx encrypted with the client secret
        pem_path, _ = generate_certificate(
            secret, tenant_id=tenant, client_id=client
        )
        if not _show_cert_upload_instructions_modal(
            tool, pem_path, client, secret, tenant, ctk
        ):
          return False
      except Exception as e:
        logger.error(f"Certificate generation failed: {e}", exc_info=True)
        messagebox.showerror(
            "Certificate Generation Error",
            f"Unable to generate certificate: {e}",
            parent=tool,
        )
        return False

  return True


def calculate_batches_with_shallow_exclusions(tool, df: pd.DataFrame, license_metrics: dict):
  """Excludes sites with >200k items from ETA calculation during Shallow Scan.

  Marks excluded sites with Suggested Batch = 'Deep Scan Recommended' so they
  are natively exported to outputs/<ts>/suggested_batches/DeepScanRecommended.csv.
  """
  if not getattr(tool, "val_shallow_scan", False):
    return tool.calculate_migration_batches(df, license_metrics)

  warn_col = "Entities with > 200k item count"
  if warn_col not in df.columns:
    numeric_warn = pd.Series([0] * len(df), index=df.index)
  else:
    numeric_warn = pd.to_numeric(df[warn_col], errors="coerce").fillna(0)

  df_eligible = df[numeric_warn == 0].copy()
  df_excluded = df[numeric_warn > 0].copy()

  if not df_excluded.empty:
    df_excluded["Suggested Batch"] = "Deep Scan Recommended"
    tool.log_msg(
        f"[Shallow Scan] Excluding {len(df_excluded)} site(s) with >200k items "
        "from ETA calculation (marked as 'Deep Scan Recommended')."
    )

  if not df_eligible.empty:
    # Convert N/A string columns to 0 temporarily for numeric ETA/batching calculation
    na_cols = [
        "Shortcut Count",
        "Folder Count > Depth Limit 100",
        "File Count > Depth Limit 100",
    ]
    for col in na_cols:
      if col in df_eligible.columns:
        df_eligible[col] = pd.to_numeric(
            df_eligible[col], errors="coerce"
        ).fillna(0)

    df_eligible_final, batches_list, total_eta, buckets = (
        tool.calculate_migration_batches(df_eligible, license_metrics)
    )

    # Restore N/A display values for shallow scan unsupported metrics
    for col in na_cols:
      if col in df_eligible_final.columns:
        df_eligible_final[col] = "N/A"
    for b in batches_list:
      b["shortcut_count"] = "N/A"
    for bucket in buckets:
      for b in bucket.get("batches", []):
        b["shortcut_count"] = "N/A"
  else:
    df_eligible_final = df_eligible
    batches_list, total_eta, buckets = [], 0.0, []

  df_combined = pd.concat([df_eligible_final, df_excluded], ignore_index=True)
  return df_combined, batches_list, total_eta, buckets
