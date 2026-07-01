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

"""Certificate helper management functions for local authentication mapping to Microsoft Graph API."""

import os
import logging
import datetime
from typing import Tuple
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import pkcs12

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def update_log_directory(tenant_id: str = None, client_id: str = None) -> None:
    """No-op as logging is handled centrally by the root logger."""
    pass


def get_project_root() -> str:
    """Helper to locate the root path of migration-planner."""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def get_cert_paths(cert_dir: str = "certificate", tenant_id: str = None, client_id: str = None) -> Tuple[str, str, str]:
    """Returns absolute paths for cert directory, certificate.pem, and passkey.pfx."""
    root = get_project_root()
    if tenant_id and client_id:
        dir_path = os.path.join(root, cert_dir, f"{tenant_id}_{client_id}")
    else:
        dir_path = os.path.join(root, cert_dir)
    pem_path = os.path.join(dir_path, "certificate.pem")
    pfx_path = os.path.join(dir_path, "passkey.pfx")
    return dir_path, pem_path, pfx_path

def check_certificate_exists(cert_dir: str = "certificate", tenant_id: str = None, client_id: str = None) -> bool:
    """Checks if the certificate directory exists and contains a valid cert.pfx file."""
    _, _, pfx_path = get_cert_paths(cert_dir, tenant_id, client_id)
    exists = os.path.exists(pfx_path)
    logger.info("Checking if certificate exists at %s: %s", pfx_path, exists)
    return exists

def generate_certificate(client_secret: str, cert_dir: str = "certificate", common_name: str = "LocalAppHybridAuth", tenant_id: str = None, client_id: str = None) -> Tuple[str, str]:
    """Generates a self-signed certificate and PFX bundle using client_secret as the password.
    
    Returns:
        Tuple containing absolute paths to the generated cert.pem and cert.pfx files.
    """
    logger.info("Initializing certificate generation flow...")
    dir_path, pem_path, pfx_path = get_cert_paths(cert_dir, tenant_id, client_id)
    
    os.makedirs(dir_path, exist_ok=True)
    logger.info("Certificate directory confirmed: %s", dir_path)

    secret_bytes = client_secret.encode('utf-8')

    logger.info("Generating secure RSA private key...")
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )

    logger.info("Generating self-signed certificate with CN: %s...", common_name)
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, common_name),
    ])

    cert = x509.CertificateBuilder().subject_name(
        subject
    ).issuer_name(
        issuer
    ).public_key(
        private_key.public_key()
    ).serial_number(
        x509.random_serial_number()
    ).not_valid_before(
        datetime.datetime.now(datetime.timezone.utc)
    ).not_valid_after(
        # Valid for 2 years
        datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=730)
    ).sign(private_key, hashes.SHA256())

    logger.info("Writing PEM public certificate to %s...", pem_path)
    with open(pem_path, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    
    logger.info("Serializing and writing PFX encrypted with client secret to %s...", pfx_path)
    pfx_bytes = pkcs12.serialize_key_and_certificates(
        name=common_name.encode('utf-8'),
        key=private_key,
        cert=cert,
        cas=None,
        encryption_algorithm=serialization.BestAvailableEncryption(secret_bytes) 
    )
    with open(pfx_path, "wb") as f:
        f.write(pfx_bytes)
        
    logger.info("Certificate generation complete! Files written successfully.")
    return pem_path, pfx_path

def load_certificate(client_secret: str, cert_dir: str = "certificate", tenant_id: str = None, client_id: str = None) -> Tuple[str, str]:
    """Decrypts PFX bundle using client_secret, extracting private key PEM and SHA1 thumbprint.
    
    Returns:
        Tuple containing private_key_pem (str) and thumbprint (str).
    """
    logger.info("Loading certificate files...")
    _, _, pfx_path = get_cert_paths(cert_dir, tenant_id, client_id)
    
    if not os.path.exists(pfx_path):
        raise FileNotFoundError(f"PFX certificate file not found at {pfx_path}")

    logger.info("Unlocking PFX file with client secret...")
    with open(pfx_path, "rb") as f:
        pfx_data = f.read()

    password_bytes = client_secret.encode('utf-8')
    private_key, certificate, _ = pkcs12.load_key_and_certificates(
        pfx_data, 
        password_bytes
    )
    
    logger.info("Extracting private key PEM...")
    private_key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    ).decode("utf-8")
    
    logger.info("Calculating certificate SHA1 fingerprint...")
    thumbprint = certificate.fingerprint(hashes.SHA1()).hex()
    logger.info("Loaded certificate successfully. Thumbprint: %s", thumbprint)
    
    return private_key_pem, thumbprint
