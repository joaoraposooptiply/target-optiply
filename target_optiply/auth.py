"""Optiply authentication module."""

from __future__ import annotations

import json
import logging
import os
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class OptiplyAuthenticator:
    """API Authenticator for OAuth 2.0 password flow."""

    def __init__(
        self,
        config: Dict[str, Any],
        auth_endpoint: Optional[str] = None,
    ) -> None:
        """Initialize authenticator.

        Args:
            config: Configuration dictionary containing credentials.
            auth_endpoint: Optional custom auth endpoint.
        """
        self._config = config
        
        # Validate required config fields
        required_fields = ["username", "password", "client_id", "client_secret"]
        missing_fields = [field for field in required_fields if field not in config or not config[field]]
        if missing_fields:
            raise ValueError(f"Missing required configuration fields: {missing_fields}")
        
        self._auth_endpoint = auth_endpoint or os.environ.get(
            "optiply_dashboard_url", "https://dashboard.optiply.nl/api"
        ) + "/auth/oauth/token"
        self._access_token = None
        self._token_expires_at = None
        self._refresh_token = None
        
        logger.info(f"Initialized authenticator with endpoint: {self._auth_endpoint}")
        logger.info(f"Config keys available: {list(config.keys())}")

    @property
    def auth_headers(self) -> Dict[str, str]:
        """Get authentication headers.

        Returns:
            Dictionary containing Authorization header with Bearer token.
        """
        if not self.is_token_valid():
            logger.info("Token is not valid, updating access token")
            self.update_access_token()
        else:
            logger.debug("Using existing valid token")
        
        if not self._access_token:
            raise Exception("Failed to obtain access token")
        
        return {
            "Authorization": f"Bearer {self._access_token}"
        }

    @property
    def oauth_request_body(self) -> Dict[str, str]:
        """Get OAuth request body for password flow.

        Returns:
            Dictionary containing OAuth request parameters.
        """
        return {
            "grant_type": "password",
            "username": self._config["username"],
            "password": self._config["password"]
        }

    def is_token_valid(self) -> bool:
        """Check if the current access token is still valid.

        Returns:
            True if token is valid, False otherwise.
        """
        if not self._access_token:
            logger.debug("No access token available")
            return False
        
        if not self._token_expires_at:
            logger.debug("No token expiration time available")
            return False
        
        # Check if token expires within the next 2 minutes
        now = datetime.utcnow()
        is_valid = self._token_expires_at > (now + timedelta(minutes=2))
        
        if is_valid:
            logger.debug(f"Token is valid, expires at {self._token_expires_at}")
        else:
            logger.debug(f"Token is expired or will expire soon. Expires at {self._token_expires_at}, current time {now}")
        
        return is_valid

    def update_access_token(self) -> None:
        """Update the access token by making a request to the auth endpoint."""
        logger.info("Starting token refresh process")
        
        try:
            # Prepare Basic Auth headers
            client_id = self._config["client_id"]
            client_secret = self._config["client_secret"]
            import base64
            basic_auth = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
            
            headers = {
                "Authorization": f"Basic {basic_auth}",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            
            logger.info(f"Making token request to: {self._auth_endpoint}")
            logger.info(f"Request body: {self.oauth_request_body}")
            logger.info(f"Headers (excluding Authorization): {dict((k, v) for k, v in headers.items() if k != 'Authorization')}")
            
            # Make the token request
            response = requests.post(
                self._auth_endpoint,
                data=self.oauth_request_body,
                headers=headers,
                timeout=30
            )
            
            logger.info(f"Token request response status: {response.status_code}")
            logger.info(f"Token request response headers: {dict(response.headers)}")
            
            if response.status_code != 200:
                logger.error(f"Token request failed with status {response.status_code}")
                logger.error(f"Response text: {response.text}")
                raise Exception(f"Token request failed with status {response.status_code}: {response.text}")
            
            token_data = response.json()
            logger.info(f"Token response keys: {list(token_data.keys())}")
            
            # Update token information
            if "access_token" not in token_data:
                raise Exception(f"Access token not found in response: {token_data}")
                
            self._access_token = token_data["access_token"]
            self._refresh_token = token_data.get("refresh_token")
            
            # Calculate expiration time
            expires_in = token_data.get("expires_in", 3600)  # Default to 1 hour
            self._token_expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
            
            logger.info(f"Successfully updated access token. Expires in {expires_in} seconds")
            logger.info(f"Token expires at: {self._token_expires_at}")
            
        except Exception as e:
            logger.error(f"Failed to update access token: {str(e)}")
            raise

    def force_refresh(self) -> None:
        """Force a token refresh regardless of current token validity."""
        self._access_token = None
        self._token_expires_at = None
        self.update_access_token()
