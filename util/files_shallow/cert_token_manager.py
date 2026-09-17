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

"""Certificate-based OAuth2 token manager for SharePoint REST APIs."""

import base64
import json
import logging
import queue
import threading
import time
from typing import Any, Callable, Dict, List, Optional
import uuid

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
import requests

from core import cert_auth
from util.auth_manager import TokenManager
from util.constants import TOKEN_URL_TEMPLATE

logger = logging.getLogger(__name__)


class CertTokenManager(TokenManager):
  """Manages SharePoint REST API tokens using X.509 certificate assertions."""

  def __init__(
      self,
      tenant_id: str,
      client_ids: List[str],
      client_secrets: List[str],
      concurrency: int = 10,
      retries: int = 5,
      backoff: int = 2,
  ) -> None:
    super().__init__(
        tenant_id=tenant_id,
        client_ids=client_ids,
        client_secrets=client_secrets,
        concurrency=max(1, concurrency),
        retries=0,
        backoff=backoff,
    )
    self.retries = min(5, max(1, retries))
    self._lock = threading.Lock()
    self._domain_queues: Dict[str, queue.Queue] = {}
    self._app_certs: Dict[str, Dict[str, str]] = {}

  def load_or_generate_all_certificates(
      self, current_logger: Optional[Callable[[str], None]] = None
  ) -> List[str]:
    """Loads or generates certificates for all configured apps.

    Returns:
      List of absolute paths to generated/existing certificate.pem files.
    """
    pem_paths = []
    for client_id, client_secret in self.apps:
      if not cert_auth.check_certificate_exists(
          tenant_id=self.tenant_id, client_id=client_id
      ):
        if current_logger:
          current_logger(
              f"Generating local X.509 certificate for App {client_id[:5]}..."
          )
        pem_path, _ = cert_auth.generate_certificate(
            client_secret=client_secret,
            tenant_id=self.tenant_id,
            client_id=client_id,
        )
      else:
        _, pem_path, _ = cert_auth.get_cert_paths(
            tenant_id=self.tenant_id, client_id=client_id
        )

      private_key_pem, thumbprint = cert_auth.load_certificate(
          client_secret=client_secret,
          tenant_id=self.tenant_id,
          client_id=client_id,
      )
      self._app_certs[client_id] = {
          "private_key_pem": private_key_pem,
          "thumbprint": thumbprint,
          "pem_path": pem_path,
      }
      pem_paths.append(pem_path)
    return pem_paths

  def _create_client_assertion(self, client_id: str) -> str:
    """Creates an RS256 signed JWT assertion for Microsoft Entra ID."""
    cert_info = self._app_certs.get(client_id)
    if not cert_info:
      self.load_or_generate_all_certificates()
      cert_info = self._app_certs.get(client_id)
    if not cert_info:
      raise ValueError(f"Certificate not loaded for client_id {client_id[:5]}")

    token_url = TOKEN_URL_TEMPLATE.format(self.tenant_id)
    now = int(time.time())
    thumbprint_bytes = bytes.fromhex(cert_info["thumbprint"])
    x5t = (
        base64.urlsafe_b64encode(thumbprint_bytes)
        .rstrip(b"=")
        .decode("ascii")
    )

    header = {
        "alg": "RS256",
        "typ": "JWT",
        "x5t": x5t,
    }
    payload = {
        "aud": token_url,
        "iss": client_id,
        "sub": client_id,
        "jti": str(uuid.uuid4()),
        "nbf": now - 60,
        "exp": now + 600,
    }

    header_b64 = (
        base64.urlsafe_b64encode(
            json.dumps(header, separators=(",", ":")).encode("utf-8")
        )
        .rstrip(b"=")
    )
    payload_b64 = (
        base64.urlsafe_b64encode(
            json.dumps(payload, separators=(",", ":")).encode("utf-8")
        )
        .rstrip(b"=")
    )
    signing_input = header_b64 + b"." + payload_b64

    private_key = serialization.load_pem_private_key(
        cert_info["private_key_pem"].encode("utf-8"), password=None
    )
    signature = private_key.sign(
        signing_input,
        padding.PKCS1v15(),
        hashes.SHA256(),
    )
    signature_b64 = base64.urlsafe_b64encode(signature).rstrip(b"=")
    return (signing_input + b"." + signature_b64).decode("ascii")

  def _acquire_token_for_domain(
      self,
      client_id: str,
      sharepoint_domain: str,
      current_logger: Optional[Callable[[str], None]] = None,
  ) -> Dict[str, Any]:
    """Acquires an OAuth2 access token for a specific SharePoint domain."""
    url = TOKEN_URL_TEMPLATE.format(self.tenant_id)
    assertion = self._create_client_assertion(client_id)
    scope = f"https://{sharepoint_domain}/.default"
    data = {
        "client_id": client_id,
        "client_assertion_type": (
            "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
        ),
        "client_assertion": assertion,
        "scope": scope,
        "grant_type": "client_credentials",
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    try:
      response = self.session.post(
          url, headers=headers, data=data, timeout=30.0
      )
      response.raise_for_status()
      token_resp = response.json()
      access_token = token_resp["access_token"]
      expires_in = int(token_resp.get("expires_in", 3599))
      return {
          "token": access_token,
          "expires_at": time.time() + expires_in - 600,
          "client_id": client_id,
          "domain": sharepoint_domain,
      }
    except requests.exceptions.RequestException as error:
      error_detail = ""
      if error.response is not None:
        error_detail = f": {error.response.text}"
      msg = (
          f"Certificate authentication failed for {sharepoint_domain} "
          f"(App {client_id[:5]}...){error_detail}. Ensure certificate.pem is "
          "uploaded to Azure App Registration under Certificates & secrets."
      )
      if current_logger:
        current_logger(msg)
      raise ConnectionError(msg) from error

  def ensure_domain_authenticated(
      self,
      sharepoint_domain: str,
      current_logger: Optional[Callable[[str], None]] = None,
  ) -> None:
    """Initializes token slots for a SharePoint domain if not already present."""
    with self._lock:
      if sharepoint_domain in self._domain_queues:
        return
      domain_queue = queue.Queue()
      for client_id, _ in self.apps:
        token_data = self._acquire_token_for_domain(
            client_id, sharepoint_domain, current_logger
        )
        for _ in range(self.concurrency):
          domain_queue.put(token_data)
      self._domain_queues[sharepoint_domain] = domain_queue
      if current_logger:
        current_logger(
            f"Successfully authenticated SharePoint REST certificate token for "
            f"{sharepoint_domain}."
        )

  def get_valid_token_slot(
      self,
      sharepoint_domain: str,
      current_logger: Optional[Callable[[str], None]] = None,
  ) -> Dict[str, Any]:
    """Checks out a valid token slot for the given SharePoint domain."""
    if sharepoint_domain not in self._domain_queues:
      self.ensure_domain_authenticated(sharepoint_domain, current_logger)

    domain_queue = self._domain_queues[sharepoint_domain]
    token_data = domain_queue.get()

    if time.time() > token_data["expires_at"]:
      with self._lock:
        if time.time() > token_data["expires_at"]:
          if current_logger:
            current_logger(
                f"Refreshing SharePoint token for {sharepoint_domain} "
                f"(App {token_data['client_id'][:5]}...)..."
            )
          try:
            refreshed = self._acquire_token_for_domain(
                token_data["client_id"], sharepoint_domain, current_logger
            )
            token_data["token"] = refreshed["token"]
            token_data["expires_at"] = refreshed["expires_at"]
          except Exception as e:
            if current_logger:
              current_logger(f"Failed to refresh SharePoint token: {e}")
    return token_data

  def return_token_slot(
      self, sharepoint_domain: str, token_data: Dict[str, Any]
  ) -> None:
    """Returns a checked-out token slot back to the domain pool."""
    if sharepoint_domain in self._domain_queues:
      self._domain_queues[sharepoint_domain].put(token_data)

  def refresh_token_data(
      self,
      token_data: Dict[str, Any],
      current_logger: Optional[Callable[[str], None]] = None,
  ) -> bool:
    """Forces an inline token refresh on HTTP 401."""
    try:
      with self._lock:
        refreshed = self._acquire_token_for_domain(
            token_data["client_id"], token_data["domain"], current_logger
        )
        token_data["token"] = refreshed["token"]
        token_data["expires_at"] = refreshed["expires_at"]
        return True
    except Exception as e:
      if current_logger:
        current_logger(f"Inline SharePoint token refresh failed: {e}")
      return False
