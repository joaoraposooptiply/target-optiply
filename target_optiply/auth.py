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
        target,
        auth_endpoint: Optional[str] = None,
    ) -> None:
        """Initialize authenticator.

        Args:
            target: The target instance.
            auth_endpoint: Optional custom auth endpoint.
        """
        self._config: Dict[str, Any] = target._config
        self._target = target  # Store reference to target for config file access
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
        auth_config = self._get_auth_config()
        
        return {
            "grant_type": "password",
            "username": auth_config["username"],
            "password": auth_config["password"],
            "client_id": auth_config["client_id"],
            "client_secret": auth_config["client_secret"]
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
            auth_config = self._get_auth_config()
            client_id = auth_config["client_id"]
            client_secret = auth_config["client_secret"]
            import base64
            basic_auth = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
            
            headers = {
                "Authorization": f"Basic {basic_auth}",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            
            logger.info(f"Making token request to: {self._auth_endpoint}")
            
            # Make the token request
            response = requests.post(
                self._auth_endpoint,
                data=self.oauth_request_body,
                headers=headers,
                timeout=30
            )
            
            if response.status_code != 200:
                raise Exception(f"Token request failed with status {response.status_code}: {response.text}")
            
            token_data = response.json()
            
            # Update token information
            self._access_token = token_data["access_token"]
            self._refresh_token = token_data.get("refresh_token")
            
            # Calculate expiration time
            expires_in = token_data.get("expires_in", 3600)  # Default to 1 hour
            self._token_expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
            
            logger.info("Successfully updated access token")
            
            # Save the new token to the config file
            self._save_token_to_config()
            
        except Exception as e:
            logger.error(f"Failed to update access token: {str(e)}")
            raise

    def force_refresh(self) -> None:
        """Force a token refresh regardless of current token validity."""
        self._access_token = None
        self._token_expires_at = None
        self.update_access_token()

    def _get_auth_config(self) -> Dict[str, Any]:
        """Get the authentication config from top-level config."""
        return {
            "client_id": self._config.get("client_id"),
            "client_secret": self._config.get("client_secret"),
            "username": self._config.get("username"),
            "password": self._config.get("password"),
            "access_token": self._config.get("access_token"),
            "coupling_id": self._config.get("coupling_id") or self._config.get("couplingId")
        }

    def _save_token_to_config(self) -> None:
        """Save the current access token back to the config file."""
        try:
            if not hasattr(self._target, 'config_file') or not self._target.config_file:
                logger.warning("No config file path available, skipping token persistence")
                return
            
            config_file_path = self._target.config_file
            
            # Read the current config file
            with open(config_file_path, 'r') as f:
                config_data = json.load(f)
            
            # Update the access_token at top level
            config_data["access_token"] = self._access_token
            logger.info("Updated access_token in config")
            
            # Write the updated config back to file
            with open(config_file_path, 'w') as f:
                json.dump(config_data, f, indent=2)
            
            logger.info(f"Successfully saved new access token to config file: {config_file_path}")
            
        except Exception as e:
            logger.error(f"Failed to save token to config file: {str(e)}")
            # Don't raise the exception - token refresh was successful, 
            # we just couldn't persist it