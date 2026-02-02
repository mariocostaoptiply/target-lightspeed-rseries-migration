"""LightspeedRSeries authentication module."""

from __future__ import annotations

import json
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Optional

import backoff
import requests


class RateLimitError(Exception):
    """Exception raised when rate limit (429) is encountered."""
    pass


class Authenticator(ABC):
    """Base authenticator class."""

    def __init__(self, target, state: Dict[str, Any] = None):
        if state is None:
            state = {}
        self.target_name: str = target.name
        self._config: Dict[str, Any] = target._config
        self._auth_headers: Dict[str, Any] = {}
        self._auth_params: Dict[str, Any] = {}
        self.logger: logging.Logger = target.logger
        self._config_file_path = target._config_file_path
        self._target = target
        self.state = state

    @property
    @abstractmethod
    def auth_headers(self) -> dict:
        """Return authentication headers."""
        raise NotImplementedError()


class LightspeedRSeriesAuthenticator(Authenticator):
    """API Authenticator for OAuth 2.0 flows."""

    # Constants
    DEFAULT_AUTH_ENDPOINT = "https://cloud.lightspeedapp.com/auth/oauth/token"
    TOKEN_BUFFER_SECONDS = 120  # Refresh token if it expires within this many seconds
    DEFAULT_WAIT_TIME = 60.0  # Default wait time for rate limiting
    DEFAULT_TOKEN_EXPIRATION = 3600  # Default token expiration in seconds

    def __init__(
        self,
        target,
        state,
        auth_endpoint: Optional[str] = None,
    ) -> None:
        super().__init__(target, state)
        self._auth_endpoint = auth_endpoint or self.DEFAULT_AUTH_ENDPOINT

    @property
    def auth_headers(self) -> dict:
        if not self.is_token_valid():
            self.update_access_token()
        return {
            "Authorization": f"Bearer {self._config.get('access_token')}"
        }


    @property
    def oauth_request_body(self) -> dict:
        return {
            "refresh_token": self._config["refresh_token"],
            "grant_type": "refresh_token",
            "client_id": self._config["client_id"],
            "client_secret": self._config["client_secret"],
        }

    def is_token_valid(self) -> bool:
        access_token = self._config.get("access_token")
        if not access_token:
            return False

        expires_in = self._config.get("expires_in")
        if expires_in is None:
            return False

        now = round(datetime.utcnow().timestamp())
        expires_in = int(expires_in)
        return (expires_in - now) >= self.TOKEN_BUFFER_SECONDS

    def _calculate_wait_time(self, retry_after: Optional[str]) -> float:
        if not retry_after:
            return self.DEFAULT_WAIT_TIME

        try:
            return float(retry_after)
        except ValueError:
            try:
                retry_datetime = parsedate_to_datetime(retry_after)
                wait_seconds = (retry_datetime - datetime.utcnow()).total_seconds()
                return max(self.DEFAULT_WAIT_TIME, wait_seconds)
            except (ValueError, TypeError, OverflowError):
                return self.DEFAULT_WAIT_TIME

    @backoff.on_exception(backoff.expo, RateLimitError, max_tries=10, factor=2, max_time=300)
    def _make_token_request(self, auth_request_payload: dict, headers: dict) -> requests.Response:
        token_response = requests.post(
            self._auth_endpoint,
            data=auth_request_payload,
            headers=headers
        )
        
        if token_response.status_code == 429:
            retry_after = token_response.headers.get("Retry-After")
            wait_time = self._calculate_wait_time(retry_after)
            
            self.logger.warning(
                f"Rate limited (429). Waiting {wait_time:.1f} seconds. "
                f"Retry-After header: {retry_after}"
            )
            time.sleep(wait_time)
            raise RateLimitError(f"Rate limit exceeded. Retry after {wait_time}s")
        
        return token_response

    def _log_token_refresh_attempt(self, refresh_token: str) -> None:
        """Log token refresh attempt with separator."""
        self.logger.info("=" * 80)
        self.logger.info(f"ATTEMPTING TOKEN REFRESH - Using refresh_token:{refresh_token}")
        self.logger.info("=" * 80)

    def _log_token_received(self, token_json: dict, expires_in: int) -> None:
        """Log received token details with separator."""
        refresh_token_to_log = token_json.get("refresh_token") or self._config.get("refresh_token", "N/A")
        self.logger.info("=" * 80)
        self.logger.info("NEW TOKEN RECEIVED - Full token details:")
        self.logger.info(f"access_token: {token_json['access_token']}")
        self.logger.info(f"refresh_token: {refresh_token_to_log}")
        self.logger.info(f"expires_in: {expires_in} seconds")
        self.logger.info("=" * 80)

    def _log_token_refresh_error(self, refresh_token: str, status_code: int, error_response: dict) -> None:
        """Log token refresh error with separator."""
        self.logger.error("=" * 80)
        self.logger.error(f"TOKEN REFRESH FAILED: {refresh_token}")
        self.logger.error(f"Error response: {status_code} | {error_response}")
        self.logger.error("=" * 80)

    def _parse_error_response(self, response: requests.Response) -> dict:
        """Parse error response from token request."""
        try:
            return response.json()
        except Exception:
            return {
                "error": "Could not parse error response",
                "text": response.text[:200]
            }

    def update_access_token(self) -> None:
        self.logger.info("Requesting new token from OAuth endpoint...")
        request_time = datetime.utcnow()

        current_refresh_token = self._config.get("refresh_token")
        if not current_refresh_token:
            raise RuntimeError("No refresh_token found in config. Cannot refresh access token.")

        auth_request_payload = self.oauth_request_body
        self._log_token_refresh_attempt(current_refresh_token)

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        token_response = self._make_token_request(auth_request_payload, headers)

        if (
            token_response.status_code == 200
            and token_response.json().get("error_description")
            == "Rate limit exceeded: access_token not expired"
        ):
            self.logger.warning("Rate limit exceeded: access_token not expired")
            return

        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            error_response = self._parse_error_response(token_response)
            refresh_token_used = auth_request_payload.get("refresh_token", "N/A")
            self._log_token_refresh_error(refresh_token_used, token_response.status_code, error_response)
            self.state.update({"auth_error_response": error_response})
            raise RuntimeError(
                f"Failed OAuth login, response was '{error_response}'. {ex}"
            ) from ex

        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        expires_in = token_json.get("expires_in", self.DEFAULT_TOKEN_EXPIRATION)

        if expires_in is None:
            self.logger.debug("No expires_in received in OAuth response and no default_expiration set. Token will be treated as if it never expires.")

        self._log_token_received(token_json, expires_in)

        refresh_token_updated = self._update_config_tokens(token_json, request_time, expires_in)
        self._save_config_to_file()
        self._log_token_save_status(refresh_token_updated)

    def _update_config_tokens(self, token_json: dict, request_time: datetime, expires_in: int) -> bool:
        """Update config with new tokens and return whether refresh token was updated."""
        self._config["access_token"] = token_json["access_token"]
        refresh_token_updated = False

        if "refresh_token" in token_json:
            self._config["refresh_token"] = token_json["refresh_token"]
            refresh_token_updated = True
        else:
            self.logger.debug("No refresh_token in response, keeping existing one")

        now = round(request_time.timestamp())
        expires_timestamp = now + int(expires_in)
        self._config["expires_in"] = expires_timestamp

        expires_datetime = datetime.fromtimestamp(expires_timestamp)
        self.logger.info(f"Token expires at: {expires_datetime.isoformat()}")

        return refresh_token_updated

    def _save_config_to_file(self) -> None:
        """Save current config to file."""
        with open(self._config_file_path, "w") as outfile:
            json.dump(self._config, outfile, indent=4)

    def _log_token_save_status(self, refresh_token_updated: bool) -> None:
        """Log token save status."""
        self.logger.info(
            f"Tokens saved to config file: {self._config_file_path}. "
            f"Access token: updated, "
            f"Refresh token: {'updated' if refresh_token_updated else 'unchanged'}"
        )