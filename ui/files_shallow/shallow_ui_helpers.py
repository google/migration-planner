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

import datetime
import email.utils
import logging
import math
import threading
from tkinter import messagebox
from typing import Optional
from urllib.parse import urlparse
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from core.cert_auth import (
    check_certificate_exists,
    generate_certificate,
    load_certificate,
)
from util.auth_manager import TokenManager
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
    GRAPH_BASE_URL,
    THROTTLING_STATUS_CODES,
)
from util.files_shallow.cert_token_manager import CertTokenManager

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
  """Handles enabling/disabling UI controls when Shallow Scan is toggled.

  Shallow Scan reads aggregate StorageMetrics and ItemCounts only, so settings
  that require per-file requests or a folder tree are cleared and disabled.
  Source selection is left untouched: OneDrive, SharePoint, or both are all
  supported, since every document library is scanned through the same
  per-library REST path.
  """
  is_shallow = tool.shallow_scan.get()

  additional_checkboxes = [
      (tool.include_recycle_bin_contents, getattr(tool, "cb_recycle_bin", None)),
      (tool.include_file_versions, getattr(tool, "cb_file_versions", None)),
      (tool.scan_encrypted_files, getattr(tool, "cb_encrypted_files", None)),
      (tool.generate_folder_amr_map, getattr(tool, "cb_depth_report", None)),
  ]

  for var, widget in additional_checkboxes:
    if is_shallow:
      var.set(False)
    if widget is not None:
      widget.configure(state="disabled" if is_shallow else "normal")


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
      text="I have uploaded the Certificate",
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


# Transient 5xx retries allowed during the pre-flight check. Throttling
# (THROTTLING_STATUS_CODES) is never retried here; it is reported instead.
PREFLIGHT_MAX_RETRIES = 2

# Pre-flight stages, phrased to complete "Stopped while ...".
_STAGE_SIGN_IN = "signing in to Microsoft Entra ID"
_STAGE_RESOLVE_DOMAIN = "looking up the SharePoint address via Microsoft Graph"
_STAGE_CERT_TOKEN = "requesting the certificate-based SharePoint token"
_STAGE_SHAREPOINT = "calling the SharePoint REST API"


class ThrottledError(Exception):
  """Raised when Microsoft throttles a pre-flight request (HTTP 429/503).

  Attributes:
    status_code: HTTP status returned by Microsoft.
    retry_after_seconds: Seconds from the Retry-After header, or None if absent.
    retry_at: Local time when retrying is expected to succeed, or None.
    stage: Pre-flight step that was throttled (see _STAGE_* constants).
    diagnostics: Error codes and request id, for logs and support only.
  """

  def __init__(
      self,
      status_code: int,
      retry_after_seconds: Optional[int],
      stage: str,
      diagnostics: str = "",
  ) -> None:
    self.status_code = status_code
    self.retry_after_seconds = retry_after_seconds
    self.stage = stage
    self.diagnostics = diagnostics
    self.retry_at = (
        datetime.datetime.now()
        + datetime.timedelta(seconds=retry_after_seconds)
        if retry_after_seconds is not None
        else None
    )
    super().__init__(self.summary)

  @classmethod
  def from_response(cls, response, stage: str) -> "ThrottledError":
    headers = getattr(response, "headers", None) or {}
    return cls(
        status_code=response.status_code,
        retry_after_seconds=_parse_retry_after(headers.get("Retry-After")),
        stage=stage,
        diagnostics=_throttling_diagnostics(response),
    )

  @property
  def is_service_busy(self) -> bool:
    """True for HTTP 503, which can mean throttling or a busy/unavailable service."""
    return self.status_code == 503

  @property
  def summary(self) -> str:
    """One-line message, e.g. 'Microsoft is throttling this app (HTTP 429). Retry after 300 s.'"""
    if self.retry_after_seconds is None:
      retry_hint = "Retry in a few minutes."
    else:
      retry_hint = f"Retry after {self.retry_after_seconds} s."
    if self.is_service_busy:
      return (
          "Microsoft is throttling this app or the service is busy"
          f" (HTTP {self.status_code}). {retry_hint}"
      )
    return f"Microsoft is throttling this app (HTTP {self.status_code}). {retry_hint}"

  @property
  def log_message(self) -> str:
    """Summary plus the throttled step and diagnostics, for the run log."""
    message = f"{self.summary} Stopped while {self.stage}."
    if self.diagnostics:
      message += f" ({self.diagnostics})"
    return message

  @property
  def dialog_title(self) -> str:
    """Title of the throttling dialog shown to the user."""
    if self.is_service_busy:
      return "Microsoft Service Busy or Throttling"
    return "Microsoft Is Throttling This App"

  @property
  def dialog_message(self) -> str:
    """Body of the throttling dialog shown to the user."""
    if self.retry_at is not None:
      wait_hint = f"Wait until about {self.retry_at:%H:%M} before retrying."
    else:
      wait_hint = "Wait a few minutes before retrying."
    if self.is_service_busy:
      explanation = (
          "Microsoft is either temporarily limiting requests from this app in"
          " your tenant, or the service is busy or briefly unavailable."
      )
    else:
      explanation = (
          "Microsoft is temporarily limiting requests from this app in your"
          " tenant. This usually follows heavy scanning, or other tools using"
          " the same app registration."
      )
    return (
        f"{self.summary}\n\n{explanation}\n\n{wait_hint} Retrying earlier can"
        " extend the throttling."
    )


def _parse_retry_after(value) -> Optional[int]:
  """Parses a Retry-After header (delta-seconds or HTTP-date) into seconds."""
  if value is None:
    return None
  value = str(value).strip()
  if not value:
    return None
  try:
    return max(0, math.ceil(float(value)))
  except ValueError:
    pass
  try:
    retry_at = email.utils.parsedate_to_datetime(value)
  except (TypeError, ValueError):
    return None
  if retry_at.tzinfo is None:
    retry_at = retry_at.replace(tzinfo=datetime.timezone.utc)
  now = datetime.datetime.now(datetime.timezone.utc)
  return max(0, math.ceil((retry_at - now).total_seconds()))


def _throttling_diagnostics(response) -> str:
  """Extracts error codes and request id from a throttled response."""
  codes = []
  try:
    body = response.json()
  except Exception:  # pylint: disable=broad-except
    body = None
  if isinstance(body, dict):
    error = body.get("error")
    if isinstance(error, dict):
      # Graph and SharePoint: nested {"code": ..., "innerError": {...}}.
      node = error
      while isinstance(node, dict) and len(codes) < 5:
        if node.get("code"):
          codes.append(str(node["code"]))
        node = node.get("innerError") or node.get("innererror")
    elif isinstance(error, str):
      # Entra ID token endpoint: {"error": ..., "error_codes": [...]}.
      codes.append(error)
      codes.extend(f"AADSTS{code}" for code in body.get("error_codes", [])[:3])
  parts = []
  if codes:
    parts.append("error: " + " > ".join(codes))
  headers = getattr(response, "headers", None) or {}
  request_id = (
      headers.get("request-id")
      or headers.get("x-ms-request-id")
      or headers.get("SPRequestGuid")
  )
  if request_id:
    parts.append(f"request-id: {request_id}")
  return ", ".join(parts)


def _raise_if_throttled(response, stage: str) -> None:
  if response.status_code in THROTTLING_STATUS_CODES:
    raise ThrottledError.from_response(response, stage)


def _throttling_from_exception(error: BaseException, stage: str) -> Optional[ThrottledError]:
  """Finds a throttled HTTP response anywhere in an exception's cause chain.

  TokenManager and CertTokenManager wrap HTTP errors in ConnectionError, so the
  original requests.HTTPError (and its response) is on __cause__.
  """
  seen = set()
  current = error
  while current is not None and id(current) not in seen:
    seen.add(id(current))
    response = getattr(current, "response", None)
    if getattr(response, "status_code", None) in THROTTLING_STATUS_CODES:
      return ThrottledError.from_response(response, stage)
    current = current.__cause__ or current.__context__
  return None


def _call_raising_throttling(func, stage: str):
  """Runs func, converting a throttled HTTP failure into ThrottledError."""
  try:
    return func()
  except Exception as error:  # pylint: disable=broad-except
    throttled = _throttling_from_exception(error, stage)
    if throttled is not None:
      raise throttled from error
    raise


def _apply_preflight_retry_policy(session) -> None:
  """Stops a session from silently waiting out Microsoft throttling.

  The shared session policy honours Retry-After on 429/503 with no cap, up to
  config.retries times, which made this check look frozen for up to ~50
  minutes. Here throttled responses are returned immediately so they can be
  reported, and only brief transient 5xx errors are retried.
  """
  if not isinstance(session, requests.Session):
    return  # Test doubles provide their own session.
  retry = Retry(
      total=PREFLIGHT_MAX_RETRIES,
      backoff_factor=1,
      status_forcelist=[500, 502, 504],
      allowed_methods=["GET", "POST"],
      respect_retry_after_header=False,
      raise_on_status=False,
  )
  session.mount("https://", HTTPAdapter(max_retries=retry))


def validate_certificate_auth(config, current_logger=None) -> tuple[bool, str]:
  """Verifies that certificate-based SharePoint REST authentication works.

  Uses only the permissions the scan already requires:
  1. Resolves the tenant SharePoint domain via Graph (client secret auth).
  2. Acquires a certificate-signed token for that domain for every app.
  3. Calls the SharePoint REST root web endpoint with each token.

  This function performs no UI work and is safe to run on a worker thread.

  Returns:
    Tuple of (success, detail). On success, detail is the SharePoint domain.
    On failure, detail is a diagnostic message intended for logs only.

  Raises:
    ThrottledError: Microsoft throttled one of the requests (HTTP 429/503).
      Raised immediately instead of waiting out Retry-After.
  """
  log = current_logger or (lambda _msg: None)
  try:
    with TokenManager(
        tenant_id=config.tenant_id,
        client_ids=config.client_ids,
        client_secrets=config.client_secrets,
        concurrency=1,
        retries=config.retries,
        backoff=config.backoff,
    ) as graph_manager:
      _apply_preflight_retry_policy(graph_manager.session)
      _call_raising_throttling(
          lambda: graph_manager.authenticate_all(current_logger), _STAGE_SIGN_IN
      )
      graph_token = graph_manager.get_valid_token_slot()
      log("Certificate check: looking up the SharePoint address via Microsoft Graph...")
      resp = graph_manager.session.get(
          f"{GRAPH_BASE_URL}/sites/root?$select=webUrl",
          headers={"Authorization": f"Bearer {graph_token['token']}"},
          timeout=30.0,
      )
      _raise_if_throttled(resp, _STAGE_RESOLVE_DOMAIN)
      if resp.status_code != 200:
        return False, (
            f"Unable to resolve SharePoint root site (HTTP {resp.status_code}):"
            f" {resp.text}"
        )
      web_url = resp.json().get("webUrl", "")

    domain = urlparse(web_url).netloc
    if not domain:
      return False, f"Unable to resolve SharePoint domain from '{web_url}'."

    with CertTokenManager(
        tenant_id=config.tenant_id,
        client_ids=config.client_ids,
        client_secrets=config.client_secrets,
        concurrency=1,
        retries=config.retries,
        backoff=config.backoff,
    ) as cert_manager:
      _apply_preflight_retry_policy(cert_manager.session)
      cert_manager.load_or_generate_all_certificates(current_logger)
      log(f"Certificate check: requesting a certificate-based SharePoint token for {domain}...")
      _call_raising_throttling(
          lambda: cert_manager.ensure_domain_authenticated(domain, current_logger),
          _STAGE_CERT_TOKEN,
      )
      # With concurrency=1 the domain pool holds exactly one slot per app.
      for _ in cert_manager.apps:
        token_data = cert_manager.get_valid_token_slot(domain, current_logger)
        resp = cert_manager.session.get(
            f"https://{domain}/_api/web?$select=Title",
            headers={
                "Authorization": f"Bearer {token_data['token']}",
                "Accept": "application/json;odata=nometadata",
            },
            timeout=30.0,
        )
        _raise_if_throttled(resp, _STAGE_SHAREPOINT)
        if resp.status_code != 200:
          return False, (
              "SharePoint REST check failed for App"
              f" {token_data['client_id'][:5]}... (HTTP {resp.status_code}):"
              f" {resp.text}"
          )
    return True, domain
  except ThrottledError:
    raise
  except Exception as e:  # pylint: disable=broad-except
    return False, str(e)


def run_certificate_auth_check(tool, config, ctk) -> bool:
  """Runs the certificate auth check behind a blocking progress dialog.

  The check runs on a worker thread so the window stays responsive. On failure,
  a Retry/Cancel popup is shown; Retry re-runs the check, Cancel aborts. If
  Microsoft throttles the app (HTTP 429/503), the check stops immediately and a
  dedicated throttling popup states when to retry.

  Returns:
    True if certificate authentication succeeded, False if the user cancelled.
  """
  while True:
    outcome = {}

    dialog = ctk.CTkToplevel(tool)
    dialog.title("Verifying Certificate")
    dialog.geometry("440x150")
    dialog.resizable(False, False)
    dialog.configure(fg_color=COLOR_SURFACE)
    dialog.transient(tool)
    dialog.grab_set()

    parent_x = tool.winfo_rootx()
    parent_y = tool.winfo_rooty()
    parent_w = tool.winfo_width()
    parent_h = tool.winfo_height()
    x = parent_x + (parent_w - 440) // 2
    y = parent_y + (parent_h - 150) // 2
    dialog.geometry(f"+{x}+{y}")

    # The in-flight check cannot be interrupted, so ignore the close button.
    dialog.protocol("WM_DELETE_WINDOW", lambda: None)

    pad_frame = ctk.CTkFrame(dialog, fg_color="transparent")
    pad_frame.pack(fill="both", expand=True, padx=24, pady=24)

    ctk.CTkLabel(
        pad_frame,
        text="Verifying certificate authentication...",
        font=FONT_BODY_BOLD,
        text_color=COLOR_TEXT_MAIN,
        anchor="w",
    ).pack(anchor="w", pady=(0, 16))

    progress = ctk.CTkProgressBar(
        pad_frame, mode="indeterminate", progress_color=COLOR_PRIMARY
    )
    progress.pack(fill="x")
    progress.start()

    def _worker():
      try:
        outcome["result"] = validate_certificate_auth(config, tool.log_msg)
      except ThrottledError as throttled:
        outcome["throttled"] = throttled
      except Exception as error:  # pylint: disable=broad-except
        outcome["result"] = (False, f"Unexpected error: {error}")
      finally:
        # Always signal completion so the progress dialog can never hang.
        outcome["done"] = True

    def _poll():
      if outcome.get("done"):
        progress.stop()
        dialog.grab_release()
        dialog.destroy()
      else:
        dialog.after(200, _poll)

    tool.log_msg("Verifying certificate authentication with Microsoft...")
    threading.Thread(target=_worker, daemon=True).start()
    dialog.after(200, _poll)
    tool.wait_window(dialog)

    throttled = outcome.get("throttled")
    if throttled is not None:
      logger.warning(f"Certificate authentication check stopped: {throttled.log_message}")
      tool.log_msg(f"Certificate authentication check stopped: {throttled.log_message}")
      retry = messagebox.askretrycancel(
          title=throttled.dialog_title,
          message=throttled.dialog_message,
          icon="warning",
          parent=tool,
      )
      if not retry:
        logger.info("Certificate authentication check cancelled by user (throttled).")
        return False
      continue

    success, detail = outcome.get("result", (False, "Check did not complete."))
    if success:
      tool.log_msg(f"Certificate authentication verified for {detail}.")
      return True

    logger.error(f"Certificate authentication check failed: {detail}")
    tool.log_msg(f"Certificate authentication check failed: {detail}")

    retry = messagebox.askretrycancel(
        title="Certificate Authentication Failed",
        message=(
            "Certificate authentication is not working. Please check that you"
            " have uploaded the certificate and that your credentials are"
            " correct."
        ),
        icon="warning",
        parent=tool,
    )
    if not retry:
      logger.info("Certificate authentication check cancelled by user.")
      return False


SHALLOW_NA_COLS = [
    "Shortcut Count",
    "Folder Count > Depth Limit 100",
    "File Count > Depth Limit 100",
]


def _calculate_batches_with_na_columns(tool, df: pd.DataFrame, license_metrics: dict):
  """Runs batch calculation, temporarily treating 'N/A' metric columns as 0.

  Only columns that actually contain 'N/A' values (e.g. from a Shallow Scan or
  an uploaded Shallow Scan site report) are coerced and restored afterwards, so
  real Deep Scan values are preserved for display.
  """
  df = df.copy()
  na_cols = [
      col
      for col in SHALLOW_NA_COLS
      if col in df.columns
      and df[col].astype(str).str.strip().str.upper().str.startswith("N/A").any()
  ]
  for col in na_cols:
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

  df_final, batches_list, total_eta, buckets = tool.calculate_migration_batches(
      df, license_metrics
  )

  # Restore N/A display values for metrics unsupported by Shallow Scan
  for col in na_cols:
    if col in df_final.columns:
      df_final[col] = "N/A"
  if "Shortcut Count" in na_cols:
    for b in batches_list:
      b["shortcut_count"] = "N/A"
    for bucket in buckets:
      for b in bucket.get("batches", []):
        b["shortcut_count"] = "N/A"

  return df_final, batches_list, total_eta, buckets


def get_large_resource_banner_text(tool, data: dict):
  """Returns the Shallow Scan banner describing how >500k sites affected the ETA.

  Live API scans keep the original banner. For uploaded site reports (ETA
  recalculation only), the banner follows the Shallow Scan toggle and always
  states whether >500k sites were excluded. Returns None when Shallow Scan is
  off (Deep Scan ETA shows no banner) or when there is nothing to report.
  """
  large_entity_count = int(data.get("tenantLevelLargeResourceCount", 0) or 0)
  entity_label = "entity contains" if large_entity_count == 1 else "entities contain"

  if not getattr(tool, "skipped_actual_scan", False):
    if data.get("isShallowScan") and large_entity_count > 0:
      return (
          f"⚠️ {large_entity_count:,} {entity_label} items >500k. Sites containing these entities were excluded from ETA calculations.\n"
          "Recommendation: Run a Deep Scan with 'Generate Depth Report for Large Resources' enabled on these resources using the exported 'DeepScanRecommended.csv' in suggested_batches."
      )
    return None

  is_shallow = getattr(tool, "val_shallow_scan", False)
  if not is_shallow:
    return None

  show_eta = getattr(tool, "show_eta", True)

  df = data.get("df")
  if df is not None and "Suggested Batch" in df.columns:
    excluded_site_count = int((df["Suggested Batch"] == "Deep Scan Recommended").sum())
  else:
    # No plan DataFrame (e.g. ETA hidden): Shallow Scan still marks every large site.
    excluded_site_count = sum(
        1
        for s in data.get("siteMetrics", {}).values()
        if int(pd.to_numeric(s.get("largeResourceCount", 0), errors="coerce") or 0) > 0
    )

  if excluded_site_count > 0:
    site_label = "site" if excluded_site_count == 1 else "sites"
    verb = "was" if excluded_site_count == 1 else "were"
    return (
        f"⚠️ {large_entity_count:,} {entity_label} items >500k. {excluded_site_count:,} {site_label}"
        f" containing these entities {verb} excluded from ETA calculations.\n"
        "Recommendation: Run a Deep Scan with 'Generate Depth Report for Large Resources' enabled on"
        " these resources using the exported 'DeepScanRecommended.csv' in suggested_batches."
    )
  if show_eta:
    return (
        "ℹ️ Shallow Scan: No entities with >500k items found. All sites are included in ETA"
        " calculations."
    )
  return None


def calculate_batches_with_shallow_exclusions(tool, df: pd.DataFrame, license_metrics: dict):
  """Excludes sites with >500k items from ETA calculation during Shallow Scan.

  Exclusion is driven solely by the Shallow Scan toggle. When the toggle is off,
  every site (including ones with >500k-item entities) is planned like a Deep
  Scan, even if the data came from an uploaded Shallow Scan site report.

  Marks excluded sites with Suggested Batch = 'Deep Scan Recommended' so they
  are natively exported to outputs/<ts>/suggested_batches/DeepScanRecommended.csv.
  """
  if not getattr(tool, "val_shallow_scan", False):
    return _calculate_batches_with_na_columns(tool, df, license_metrics)

  large_col = "Entities with > 500k item count"
  if large_col not in df.columns:
    numeric_large = pd.Series([0] * len(df), index=df.index)
  else:
    numeric_large = pd.to_numeric(df[large_col], errors="coerce").fillna(0)

  df_eligible = df[numeric_large == 0].copy()
  df_excluded = df[numeric_large > 0].copy()

  if not df_excluded.empty:
    df_excluded["Suggested Batch"] = "Deep Scan Recommended"
    tool.log_msg(
        f"[Shallow Scan] Excluding {len(df_excluded)} site(s) with >500k items "
        "from ETA calculation (marked as 'Deep Scan Recommended')."
    )

  if not df_eligible.empty:
    df_eligible_final, batches_list, total_eta, buckets = (
        _calculate_batches_with_na_columns(tool, df_eligible, license_metrics)
    )
  else:
    df_eligible_final = df_eligible
    batches_list, total_eta, buckets = [], 0.0, []

  df_combined = pd.concat([df_eligible_final, df_excluded], ignore_index=True)
  return df_combined, batches_list, total_eta, buckets
