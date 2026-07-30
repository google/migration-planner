"""Utility to detect encryption in file headers."""

CFBF_MAGIC = b'\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1'
PDF_MAGIC = b'%PDF'
ZIP_MAGIC = b'PK\x03\x04'

DRM_CONTENT_BYTES = "DRMContent".encode('utf-16-le')
DATA_SPACES_BYTES = "DataSpaces".encode('utf-16-le')
IRM_SERVICES_BYTES = "/MicrosoftIRMServices".encode('ascii')

class EncryptionStatus:
    RMS_ENCRYPTED_OFFICE = "RMS/MIP ENCRYPTED (Office Document with DRM Content)"
    RMS_ENCRYPTED_PDF = "RMS/MIP ENCRYPTED (PDF with MicrosoftIRMServices)"
    UNENCRYPTED_OR_STANDARD_PDF = "NOT ENCRYPTED (Standard PDF, no Microsoft RMS found in header)"
    UNENCRYPTED_ZIP_OR_OFFICE = "NOT ENCRYPTED (Standard Office Document / ZIP)"
    UNENCRYPTED_OR_OTHER = "LIKELY NOT ENCRYPTED (Unsupported format, no encryption markers)"
    UNKNOWN_TOO_SMALL = "UNKNOWN (File too small to analyze)"

def starts_with(header: bytes, prefix: bytes) -> bool:
    return header.startswith(prefix)

def contains_bytes(header: bytes, target: bytes) -> bool:
    return target in header

def detect_encryption(header: bytes) -> str:
    if len(header) < 4:
        return EncryptionStatus.UNKNOWN_TOO_SMALL

    # 1. Check for Office CFBF Container (MIP/RMS)
    if starts_with(header, CFBF_MAGIC):
        if contains_bytes(header, DRM_CONTENT_BYTES) or contains_bytes(header, DATA_SPACES_BYTES):
            return EncryptionStatus.RMS_ENCRYPTED_OFFICE

    # 2. Check for PDF (MIP/RMS Protected PDFs)
    if starts_with(header, PDF_MAGIC):
        if contains_bytes(header, IRM_SERVICES_BYTES):
            return EncryptionStatus.RMS_ENCRYPTED_PDF
        return EncryptionStatus.UNENCRYPTED_OR_STANDARD_PDF

    # 3. Check for standard ZIP (Unencrypted Office files are standard ZIPs)
    if starts_with(header, ZIP_MAGIC):
        return EncryptionStatus.UNENCRYPTED_ZIP_OR_OFFICE

    return EncryptionStatus.UNENCRYPTED_OR_OTHER

def is_encrypted(status: str) -> bool:
    return status in [EncryptionStatus.RMS_ENCRYPTED_OFFICE, EncryptionStatus.RMS_ENCRYPTED_PDF]
