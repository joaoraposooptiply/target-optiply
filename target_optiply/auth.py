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
        self._auth_endpoint = auth_endpoint or os.environ.get(
            "optiply_dashboard_url", "https://dashboard.acceptance.optiply.com/api"
        ) + "/auth/oauth/token"
        
        # Log environment variables that might be used
        logger.info("🔍 Checking environment variables:")
        logger.info(f"optiply_dashboard_url: {os.environ.get('optiply_dashboard_url', 'NOT_SET')}")
        logger.info(f"OPTIPLY_CLIENT_ID: {os.environ.get('OPTIPLY_CLIENT_ID', 'NOT_SET')}")
        logger.info(f"OPTIPLY_CLIENT_SECRET: {os.environ.get('OPTIPLY_CLIENT_SECRET', 'NOT_SET')}")
        logger.info(f"OPTIPLY_USERNAME: {os.environ.get('OPTIPLY_USERNAME', 'NOT_SET')}")
        logger.info(f"OPTIPLY_PASSWORD: {os.environ.get('OPTIPLY_PASSWORD', 'NOT_SET')}")
        
        # Check if .env file exists
        import pathlib
        env_file = pathlib.Path(".env")
        if env_file.exists():
            logger.info("✅ .env file found in working directory")
            try:
                with open(env_file, 'r') as f:
                    env_contents = f.read()
                logger.info("📄 .env file contents:")
                # Log each line (mask sensitive values)
                for line in env_contents.split('\n'):
                    if line.strip() and not line.startswith('#'):
                        if 'PASSWORD' in line or 'SECRET' in line or 'TOKEN' in line:
                            # Mask sensitive values
                            key, value = line.split('=', 1) if '=' in line else (line, '')
                            masked_value = value[:4] + '...' + value[-2:] if len(value) > 6 else '***'
                            logger.info(f"  {key}={masked_value}")
                        else:
                            logger.info(f"  {line}")
            except Exception as e:
                logger.error(f"❌ Error reading .env file: {e}")
        else:
            logger.info("❌ .env file not found in working directory")
        
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
        
        # Log the credentials being used (masked)
        logger.info(f"Auth config client_id: {auth_config.get('client_id', 'NOT_FOUND')}")
        logger.info(f"Auth config username: {auth_config.get('username', 'NOT_FOUND')}")
        logger.info(f"Auth config password: {auth_config.get('password', 'NOT_FOUND')[:4]}...{auth_config.get('password', 'NOT_FOUND')[-2:] if len(auth_config.get('password', '')) > 6 else '***'}")
        
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

    def _get_auth_config(self) -> Dict[str, Any]:
        """Get the authentication config from the appropriate section."""
        # Check for nested credential sections first
        if "importCredentials" in self._config:
            auth_config = self._config["importCredentials"]
            logger.info("✅ Using importCredentials section")
            logger.info(f"importCredentials keys: {list(auth_config.keys())}")
            return auth_config
        elif "apiCredentials" in self._config:
            auth_config = self._config["apiCredentials"]
            logger.info("✅ Using apiCredentials section")
            logger.info(f"apiCredentials keys: {list(auth_config.keys())}")
            return auth_config
        else:
            # Use top-level config (for deployed environment)
            logger.info("✅ Using top-level config")
            logger.info(f"Top-level config keys: {list(self._config.keys())}")
            return self._config