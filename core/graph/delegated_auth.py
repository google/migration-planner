import msal
import logging
import threading
import socket
from typing import List, Optional

from msal.oauth2cli.authcode import AuthCodeReceiver

logger = logging.getLogger(__name__)

def get_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    port = s.getsockname()[1]
    s.close()
    return port

class DelegatedAuthClient:
    """Manages Microsoft Graph API authentication using MSAL native capabilities."""
    
    # Shared in-memory token cache to persist tokens across UI reloads 
    # without writing sensitive tokens to disk.
    _shared_token_cache = msal.TokenCache()
    
    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.authority = f"https://login.microsoftonline.com/{tenant_id}"
        
        try:
            self.app = msal.ConfidentialClientApplication(
                client_id,
                client_credential=client_secret,
                authority=self.authority,
                token_cache=self._shared_token_cache
            )
        except Exception as e:
            if "Unable to get authority configuration for" in str(e):
                raise Exception("Incorrect Tenant ID.")
            raise e
            
        self._lock = threading.Lock()

    @staticmethod
    def _parse_error(desc: str) -> str:
        if "AADSTS7000215" in desc or "AADSTS7000222" in desc:
            return "Incorrect Client Secret."
        if "AADSTS700016" in desc:
            return "Incorrect Client ID."
        if "AADSTS90002" in desc or "AADSTS900023" in desc:
            return "Incorrect Tenant ID."
        return desc.split('\n')[0].split('\r')[0] if desc else "Unknown error."

    def get_token(self, scopes: List[str], force_interactive: bool = False) -> Optional[str]:
        try:
            with self._lock:
                result = None
                accounts = self.app.get_accounts()
                
                if accounts and not force_interactive:
                    result = self.app.acquire_token_silent(scopes, account=accounts[0])
                    
                if not result:
                    logger.info("No valid token found in cache. Using MSAL native AuthCodeReceiver popup flow.")
                    
                    port = get_free_port()
                    redirect_uri = f"http://localhost:{port}"
                    
                    auth_url = self.app.get_authorization_request_url(
                        scopes, 
                        redirect_uri=redirect_uri,
                        prompt="select_account"
                    )
                    auth_url += "&response_mode=form_post"
                    
                    with AuthCodeReceiver(port=port) as receiver:
                        auth_response = receiver.get_auth_response(
                            auth_uri=auth_url,
                            timeout=120
                        )
                    
                    if not auth_response:
                        raise Exception("Delegated Auth Cancelled or timed out waiting for browser popup.")
                        
                    if "error" in auth_response:
                        graceful_err = self._parse_error(auth_response.get('error_description', ''))
                        logger.error(f"Delegated Auth Failed in browser: {auth_response['error']} - {graceful_err}")
                        raise Exception(f"Delegated Auth Failed: {graceful_err}")
                        
                    if "code" not in auth_response:
                        raise Exception("Delegated Auth Failed: No authorization code received.")
                        
                    auth_code = auth_response["code"]
                        
                    result = self.app.acquire_token_by_authorization_code(
                        auth_code,
                        scopes=scopes,
                        redirect_uri=redirect_uri
                    )
                                
                if "access_token" in result:
                    return result["access_token"]
                else:
                    graceful_err = self._parse_error(result.get('error_description', ''))
                    logger.error(f"Failed to acquire delegated token: {result.get('error')} - {graceful_err}")
                    raise Exception(f"Delegated Auth Failed: {graceful_err}")
        except Exception as e:
            err_msg = str(e)
            if "Unable to get authority configuration for" in err_msg:
                raise Exception("Incorrect Tenant ID.")
            raise e
