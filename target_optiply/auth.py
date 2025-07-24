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
        self._config = config["importCredentials"]
        self._auth_endpoint = auth_endpoint or os.environ.get(
            "optiply_dashboard_url", "https://dashboard.acceptance.optiply.com/api"
        ) + "/auth/oauth/token"
        # Use existing access_token if provided in config
        self._access_token = self._config.get("access_token")
        self._token_expires_at = None
        self._refresh_token = None
        
        # If we have an access_token, assume it's valid for now
        if self._access_token:
            # Set expiration to 1 hour from now as a reasonable default
            self._token_expires_at = datetime.utcnow() + timedelta(hours=1)

    @property
    def auth_headers(self) -> Dict[str, str]:
        """Get authentication headers.

        Returns:
            Dictionary containing Authorization header with Bearer token.
        """
        if not self.is_token_valid():
            self.update_access_token()
        
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
            "password": self._config["password"],
            "client_id": self._config["client_id"],
            "client_secret": self._config["client_secret"]
        }

    def is_token_valid(self) -> bool:
        """Check if the current access token is still valid.

        Returns:
            True if token is valid, False otherwise.
        """
        if not self._access_token:
            return False
        
        if not self._token_expires_at:
            return False
        
        # Check if token expires within the next 2 minutes
        now = datetime.utcnow()
        return self._token_expires_at > (now + timedelta(minutes=2))

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
            
            # Log the auth request details (mask sensitive data)
            logger.info(f"Making token request to: {self._auth_endpoint}")
            logger.info(f"Client ID: {client_id}")
            logger.info(f"Client Secret: {client_secret[:8]}...{client_secret[-4:] if len(client_secret) > 12 else '***'}")
            logger.info(f"Username: {self._config['username']}")
            logger.info(f"Password: {self._config['password'][:4]}...{self._config['password'][-2:] if len(self._config['password']) > 6 else '***'}")
            logger.info(f"Basic Auth Header: Basic {basic_auth[:20]}...{basic_auth[-10:] if len(basic_auth) > 30 else '***'}")
            logger.info(f"Request Headers: {headers}")
            logger.info(f"Request Body: {self.oauth_request_body}")
            
            # Make the token request
            response = requests.post(
                self._auth_endpoint,
                data=self.oauth_request_body,
                headers=headers,
                timeout=30
            )
            
            logger.info(f"Auth Response Status: {response.status_code}")
            logger.info(f"Auth Response Headers: {dict(response.headers)}")
            
            if response.status_code != 200:
                logger.error(f"Auth Response Body: {response.text}")
                raise Exception(f"Token request failed with status {response.status_code}: {response.text}")
            
            token_data = response.json()
            logger.info(f"Auth Response Body: {token_data}")
            
            # Update token information
            self._access_token = token_data["access_token"]
            self._refresh_token = token_data.get("refresh_token")
            
            # Calculate expiration time
            expires_in = token_data.get("expires_in", 3600)  # Default to 1 hour
            self._token_expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
            
            logger.info("Successfully updated access token")
            logger.info(f"Token expires at: {self._token_expires_at}")
            
        except Exception as e:
            logger.error(f"Failed to update access token: {str(e)}")
            raise

    def force_refresh(self) -> None:
        """Force a token refresh regardless of current token validity."""
        self._access_token = None
        self._token_expires_at = None
        self.update_access_token()