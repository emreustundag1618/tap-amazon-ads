"""TapAmazonAds Authentication."""

from __future__ import annotations

import requests
from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class TapAmazonAdsAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for Amazon Ads API using Login with Amazon (LWA) OAuth2."""

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for refreshing Amazon Ads API access tokens.

        Returns:
            A dict with the request body for token refresh
        """
        return {
            "grant_type": "refresh_token",
            "refresh_token": self.config["refresh_token"],
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
        }

    @property
    def oauth_request_payload(self) -> dict:
        """Return request payload for OAuth request.
        
        Returns:
            A dict with the OAuth request payload
        """
        return self.oauth_request_body

    @classmethod
    def create_for_stream(cls, stream) -> TapAmazonAdsAuthenticator:  # noqa: ANN001
        """Instantiate an authenticator for a specific Singer stream.

        Args:
            stream: The Singer stream instance.

        Returns:
            A new authenticator.
        """
        return cls(
            stream=stream,
            auth_endpoint=stream.config.get("auth_endpoint", "https://api.amazon.com/auth/o2/token"),
            oauth_scopes=stream.config.get("permission_scope", "advertising::campaign_management"),
        )

    def update_access_token(self) -> None:
        """Update `access_token` along with: `last_refreshed` and `expires_in`.
        
        Raises:
            RuntimeError: When OAuth login fails.
        """
        import datetime
        request_time = datetime.datetime.now(datetime.timezone.utc)
        
        auth_request_payload = self.oauth_request_payload
        
        token_response = requests.post(
            self.auth_endpoint,
            data=auth_request_payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=60,
        )
        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization was successful.")
        except Exception as ex:
            self.logger.error(f"OAuth failed with response: {token_response.text}")
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.json()}'. {ex}"
            ) from ex
        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        self.expires_in = token_json.get("expires_in")
        if self.expires_in is None:
            self.logger.info(
                "No expires_in receied in OAuth response and no "
                "default_expiration set. Token will be treated as if it never "
                "expires."
            )
        self.last_refreshed = request_time
