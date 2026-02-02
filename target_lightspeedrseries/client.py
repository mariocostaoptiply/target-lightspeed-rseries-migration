"""LightspeedRSeries target sink class, which handles writing streams."""

from __future__ import annotations

import json
import time
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Dict, List, Optional

import backoff
import requests
from singer_sdk.exceptions import RetriableAPIError
from singer_sdk.plugin_base import PluginBase
from singer_sdk.sinks import RecordSink
from target_hotglue.client import HotglueSink
from target_lightspeedrseries.auth import LightspeedRSeriesAuthenticator


class LightspeedRSeriesSink(HotglueSink, RecordSink):
    """LightspeedRSeries target sink class."""

    # Rate limiting constants
    RATE_LIMIT_REQUESTS_PER_SECOND = 3
    MIN_REQUEST_INTERVAL = 0.5  # Conservative: 0.5s = 2 req/s (well below 3 req/s limit)
    DEFAULT_RETRY_AFTER = 60  # Default wait time in seconds for 429 responses
    MAX_RETRY_TIME = 300  # Max 5 minutes total retry time
    BACKOFF_FACTOR = 2
    MAX_RETRY_TRIES = 5

    # Default URLs
    DEFAULT_BASE_URL = "https://api.lightspeedapp.com"
    DEFAULT_AUTH_ENDPOINT = "https://cloud.lightspeedapp.com/auth/oauth/token"

    # Class-level rate limiting state (shared across all instances)
    _last_request_time: float = 0.0
    _min_request_interval: float = 0.5

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)
        self._target = target
        self.target_name = "lightspeedrseries"
        self._initialize_rate_limiting()

    def _initialize_rate_limiting(self) -> None:
        """Initialize class-level rate limiting state."""
        if not hasattr(LightspeedRSeriesSink, '_last_request_time'):
            LightspeedRSeriesSink._last_request_time = 0.0
        if not hasattr(LightspeedRSeriesSink, '_min_request_interval'):
            LightspeedRSeriesSink._min_request_interval = self.MIN_REQUEST_INTERVAL

    @property
    def base_url(self) -> str:
        """Get the base URL for API requests."""
        base_url = self.config.get("full_url") or self.DEFAULT_BASE_URL
        return base_url.rstrip("/")

    @property
    def url(self) -> str:
        """Get the base API URL for the configured account."""
        return f"{self.base_url}/API/V3/Account/{self.config['account_ids']}"

    @property
    def http_headers(self) -> dict:
        """Get HTTP headers for API requests."""
        headers = {}
        
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        
        auth_headers = self.authenticator.auth_headers
        headers.update(auth_headers)
        
        return headers

    def _rate_limit(self) -> None:
        """Enforce rate limiting: max 3 requests per second."""
        current_time = time.time()
        time_since_last_request = current_time - LightspeedRSeriesSink._last_request_time
        
        if time_since_last_request < LightspeedRSeriesSink._min_request_interval:
            sleep_time = LightspeedRSeriesSink._min_request_interval - time_since_last_request
            self.logger.info(
                f"Rate limiting: sleeping {sleep_time:.3f}s to maintain < {self.RATE_LIMIT_REQUESTS_PER_SECOND} req/s "
                f"(last request was {time_since_last_request:.3f}s ago, need {LightspeedRSeriesSink._min_request_interval}s)"
            )
            time.sleep(sleep_time)
        
        # Note: _last_request_time is updated AFTER the request completes in _request()
    
    def _parse_retry_after(self, retry_after: Optional[str]) -> int:
        if not retry_after:
            return self.DEFAULT_RETRY_AFTER
        
        try:
            # Try parsing as integer seconds
            return int(retry_after)
        except ValueError:
            # Try parsing as HTTP date
            try:
                retry_datetime = parsedate_to_datetime(retry_after)
                wait_seconds = max(
                    self.DEFAULT_RETRY_AFTER,
                    (retry_datetime - datetime.utcnow()).total_seconds()
                )
                return int(wait_seconds)
            except Exception:
                return self.DEFAULT_RETRY_AFTER

    def _handle_429_response(self, response: requests.Response) -> None:
        """Handle 429 Rate Limit errors with proper backoff."""
        retry_after = response.headers.get("Retry-After")
        wait_time = self._parse_retry_after(retry_after)
        
        self.logger.warning(
            f"Rate limit exceeded (429). Waiting {wait_time}s before retry. "
            f"Retry-After header: {retry_after}"
        )
        time.sleep(wait_time)
        raise RetriableAPIError(
            f"Rate limit exceeded. Retry after {wait_time}s",
            response=response
        )
    
    def validate_response(self, response: requests.Response) -> None:
        """Validate response and handle rate limiting."""
        if response.status_code == 429:
            self._handle_429_response(response)
        
        super().validate_response(response)

    def _build_request_url(self, endpoint: str) -> str:
        """Build the full URL for an API request."""
        if endpoint.startswith("/"):
            return f"{self.url}{endpoint}"
        return f"{self.url}/{endpoint}"

    def _log_request(
        self,
        http_method: str,
        endpoint: str,
        url: str,
        params: Optional[Dict] = None,
        request_data: Optional[Dict] = None,
    ) -> None:
        """Log request details."""
        self.logger.info(
            f"Making request to endpoint='{endpoint}' with method: '{http_method}' "
            f"and url='{url}'"
        )
        if request_data:
            self.logger.info(f"Request payload: {request_data}")
        if params:
            self.logger.info(f"Request params: {params}")

    def _log_response(self, response: requests.Response) -> None:
        """Log response details."""
        self.logger.info(f"Response status: {response.status_code}")
        
        if response.status_code >= 400:
            self._log_error_response(response)
        elif response.status_code < 300:
            self._log_success_response(response)

    def _log_error_response(self, response: requests.Response) -> None:
        """Log error response details."""
        self.logger.error("=" * 80)
        self.logger.error(f"HTTP ERROR {response.status_code}")
        self.logger.error("=" * 80)
        try:
            error_response = response.json()
            self.logger.error(f"Error response (JSON): {json.dumps(error_response, indent=2)}")
        except Exception:
            self.logger.error(f"Error response (text, first 2000 chars): {response.text[:2000]}")
        self.logger.error(f"Response headers: {dict(response.headers)}")
        self.logger.error("=" * 80)

    def _log_success_response(self, response: requests.Response) -> None:
        """Log successful response details."""
        try:
            response_data = response.json()
            self.logger.debug(f"Response data: {str(response_data)[:200]}")
        except Exception:
            self.logger.debug(f"Response text: {response.text[:200]}")
    
    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        max_tries=5,
        factor=2,
        max_time=300,
    )
    def _request(
        self,
        http_method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        request_data: Optional[Dict] = None,
        headers: Optional[Dict] = None,
    ) -> requests.Response:
        """Make an HTTP request with rate limiting and error handling."""
        # Enforce rate limiting before making request
        self._rate_limit()
        
        url = self._build_request_url(endpoint)
        request_headers = self.http_headers.copy()
        if headers:
            request_headers.update(headers)

        self._log_request(http_method, endpoint, url, params, request_data)

        response = requests.request(
            method=http_method,
            url=url,
            params=params,
            headers=request_headers,
            json=request_data,
        )
        
        # Update timestamp AFTER the request completes
        # This ensures we track when we actually made the request, accounting for request duration
        LightspeedRSeriesSink._last_request_time = time.time()
        
        self._log_response(response)
        self.validate_response(response)
        return response
    
    def request_api(
        self,
        http_method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        request_data: Optional[Dict] = None,
        headers: Optional[Dict] = None,
    ) -> requests.Response:
        return self._request(http_method, endpoint, params, request_data, headers)

    @property
    def authenticator(self) -> LightspeedRSeriesAuthenticator:
        """Get the authenticator instance for API requests."""
        auth_state = getattr(self, "auth_state", {})
        return LightspeedRSeriesAuthenticator(
            self._target,
            auth_state,
            auth_endpoint=self.DEFAULT_AUTH_ENDPOINT,
        )