"""TapAmazonAds tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_amazon_ads import streams


class TapTapAmazonAds(Tap):
    """TapAmazonAds tap class."""

    name = "tap-amazon-ads"

    # Amazon Ads API configuration schema
    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType(nullable=False),
            required=True,
            secret=True,
            title="Client ID",
            description="Login with Amazon (LWA) Client ID from Security Profile",
        ),
        th.Property(
            "client_secret",
            th.StringType(nullable=False),
            required=True,
            secret=True,
            title="Client Secret",
            description="Login with Amazon (LWA) Client Secret from Security Profile",
        ),
        th.Property(
            "refresh_token",
            th.StringType(nullable=False),
            required=True,
            secret=True,
            title="Refresh Token",
            description="OAuth2 refresh token obtained from authorization flow",
        ),
        th.Property(
            "profile_ids",
            th.ArrayType(th.StringType),
            required=True,
            title="Profile IDs",
            description="List of Amazon Ads profile IDs to extract data from",
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://advertising-api.amazon.com",
            title="API Base URL",
            description="Amazon Ads API base URL",
        ),
        th.Property(
            "auth_endpoint",
            th.StringType,
            default="https://api.amazon.com/auth/o2/token",
            title="OAuth Token Endpoint",
            description="Login with Amazon OAuth2 token endpoint",
        ),
        th.Property(
            "permission_scope",
            th.StringType,
            default="advertising::campaign_management",
            title="OAuth Permission Scope",
            description="OAuth2 permission scope for Amazon Ads API",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            title="Start Date",
            description="Start date for incremental replication",
        ),
        th.Property(
            "user_agent",
            th.StringType,
            default="tap-amazon-ads/1.0.0",
            title="User Agent",
            description="User agent string for API requests",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.TapAmazonAdsStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.CampaignsStream(self),
            streams.AdGroupsStream(self),
            streams.KeywordsStream(self),
            streams.ProductAdsStream(self),
            streams.TargetsStream(self),
            streams.NegativeKeywordsStream(self),
        ]


if __name__ == "__main__":
    TapTapAmazonAds.cli()
