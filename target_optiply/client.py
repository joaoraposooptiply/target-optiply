"""Optiply target sink class, which handles writing streams."""

from __future__ import annotations

import backoff
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
import requests
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from target_hotglue.client import HotglueSink
from singer_sdk.plugin_base import PluginBase

from target_optiply.auth import OptiplyAuthenticator

logger = logging.getLogger(__name__)

class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder for datetime objects."""

    def default(self, obj):
        """Encode datetime objects."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class OptiplySink(HotglueSink):
    """Optiply target sink class."""

    base_url = os.environ.get("optiply_base_url", "https://api.acceptance.optiply.com/v1")

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]] = None,
    ) -> None:
        """Initialize the sink.

        Args:
            target: The target instance.
            stream_name: The name of the stream.
            schema: The schema for the stream.
            key_properties: The key properties for the stream.
        """
        self._target = target
        super().__init__(target, stream_name, schema, key_properties)
        self._authenticator = None
        self._session = None
        self._access_token = None
        self._token_expires_at = None

    @property
    def authenticator(self) -> OptiplyAuthenticator:
        """Get the authenticator instance.

        Returns:
            The authenticator instance.
        """
        if self._authenticator is None:
            # Use the target's config to ensure we get the full config structure
            self._authenticator = OptiplyAuthenticator(self._target.config)
        return self._authenticator

    def http_headers(self) -> Dict[str, str]:
        """Get the HTTP headers for the request.

        Returns:
            The HTTP headers.
        """
        headers = {}
        headers.update(self.authenticator.auth_headers or {})
        headers.update({
            "Content-Type": "application/vnd.api+json",
            "Accept": "application/vnd.api+json"
        })
        return headers

    def validate_response(self, response: requests.Response) -> None:
        """Validate the response from the API.

        Args:
            response: The response to validate.

        Raises:
            FatalAPIError: If the response indicates a fatal error.
            RetriableAPIError: If the response indicates a retriable error.
        """
        if response.status_code >= 500:
            raise RetriableAPIError(f"Server error: {response.text}")
        elif response.status_code == 404:
            logger.warning(f"Resource not found (404): {response.url}")
            return
        elif response.status_code == 401:
            # 401 errors are handled in _request method with token refresh
            raise FatalAPIError(f"Authentication failed after token refresh: {response.text}")
        elif response.status_code >= 400:
            raise FatalAPIError(f"Client error: {response.text}")

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        max_tries=5,
        factor=2,
    )
    def _request(
        self, http_method, endpoint, params=None, request_data=None, headers=None
    ) -> requests.Response:
        """Make a request with automatic token refresh on 401 errors."""
        url = self.url(endpoint)
        headers = self.http_headers()

        # First attempt
        response = requests.request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data
        )
        
        # Handle 401 errors by refreshing token and retrying
        if response.status_code == 401:
            logger.info("Received 401 error, attempting to refresh token and retry")
            try:
                # Force token refresh using the authenticator method
                self._authenticator.force_refresh()
                
                # Get fresh headers with new token
                headers = self.http_headers()
                
                # Retry the request with new token
                response = requests.request(
                    method=http_method,
                    url=url,
                    params=params,
                    headers=headers,
                    json=request_data
                )
                logger.info("Successfully retried request after token refresh")
                
                # If we still get 401 after refresh, it's a fatal error
                if response.status_code == 401:
                    logger.error("Still getting 401 after token refresh - authentication failed")
                    raise FatalAPIError(f"Authentication failed after token refresh: {response.text}")
                    
            except Exception as e:
                logger.error(f"Failed to refresh token and retry: {str(e)}")
                raise
        
        self.validate_response(response)
        return response

    def url(self, endpoint: str = "") -> str:
        """Get the URL for the given endpoint.

        Args:
            endpoint: The endpoint to get the URL for.

        Returns:
            The URL for the endpoint.
        """
        # Add accountId and couplingId as query parameters if they exist
        params = {}
        if "account_id" in self.config:
            params["accountId"] = self.config["account_id"]
        if "coupling_id" in self.config:
            params["couplingId"] = self.config["coupling_id"]
        
        url = f"{self.base_url}/{endpoint}"
        if params:
            query_string = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"{url}?{query_string}"
        return url

    def request_api(self, http_method, endpoint=None, params={}, request_data=None, headers={}):
        """Request records from REST endpoint(s), returning response records."""
        self.logger.info(f"REQUEST - endpoint: {endpoint}, request_body: {request_data}")
        resp = self._request(http_method, endpoint, params, request_data, headers)
        return resp