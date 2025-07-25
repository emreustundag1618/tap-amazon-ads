"""CampaignsStream module.

Contains the concrete implementation of `CampaignsStream`, moved from
`tap_amazon_ads.streams` to this dedicated module under the
`tap_amazon_ads.streams` package.
"""

from __future__ import annotations
import typing as t

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_amazon_ads.client import TapAmazonAdsStream


class CampaignsStream(TapAmazonAdsStream):
    """Stream for Sponsored Products Campaigns."""

    name = "campaigns"
    path = "/sp/campaigns/list"
    primary_keys: t.ClassVar[list[str]] = ["campaignId"]
    replication_key = None
    records_jsonpath = "$.campaigns[*]"
    rest_method = "POST"

    schema = th.PropertiesList(
        th.Property("campaignId", th.StringType, description="The campaign identifier"),
        th.Property("name", th.StringType, description="The campaign name"),
        th.Property(
            "budget",
            th.ObjectType(
                th.Property("budget", th.NumberType),
                th.Property("budgetType", th.StringType),
            ),
            description="Campaign budget information",
        ),
        th.Property(
            "targetingType",
            th.StringType,
            description="Campaign targeting type (manual, auto)",
        ),
        th.Property(
            "state",
            th.StringType,
            description="The campaign state (enabled, paused, archived)",
        ),
        th.Property(
            "startDate", th.StringType, format="date", description="Campaign start date"
        ),
        th.Property(
            "endDate", th.StringType, format="date", description="Campaign end date"
        ),
        th.Property(
            "premiumBidAdjustment",
            th.BooleanType,
            description="Premium bid adjustment enabled",
        ),
        th.Property(
            "dynamicBidding",
            th.ObjectType(
                th.Property("strategy", th.StringType),
                th.Property(
                    "placementBidding",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("placement", th.StringType),
                            th.Property("percentage", th.NumberType),
                        )
                    ),
                ),
            ),
        ),
        th.Property(
            "servingStatus", th.StringType, description="Campaign serving status"
        ),
    ).to_dict()

    def get_child_context(
        self, record: dict, context: t.Optional[t.Dict] = None
    ) -> dict:
        """Return context dictionary for child streams with campaign ID from parent record."""
        return {
            "campaign_id": record["campaignId"],
            "profile_id": context.get("profile_id") if context else None,
        }

    def get_http_headers(self, context: t.Optional[t.Dict] = None) -> dict:
        """Return the http headers needed for Amazon Ads API."""
        headers = super().get_http_headers(context)
        headers["Accept"] = "application/vnd.spCampaign.v3+json"
        headers["Content-Type"] = "application/vnd.spCampaign.v3+json"
        return headers

    def prepare_request_payload(
        self,
        context: t.Optional[t.Dict],
        next_page_token: t.Any | None = None,
    ) -> dict | None:
        """Prepare the request payload for listing campaigns."""
        return {
            "stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]},
            "maxResults": 100,
        }


__all__ = ["CampaignsStream"]
