"""AdGroupsStream module.

Contains concrete implementation of `AdGroupsStream`, migrated from
`tap_amazon_ads.streams`.
"""

from __future__ import annotations
import typing as t

from singer_sdk import typing as th

from tap_amazon_ads.client import TapAmazonAdsStream
from tap_amazon_ads.streams.campaigns_stream import CampaignsStream


class AdGroupsStream(TapAmazonAdsStream):
    """Stream for Sponsored Products Ad Groups."""

    name = "adgroups"
    path = "/sp/adGroups/list"
    primary_keys: t.ClassVar[list[str]] = ["adGroupId"]
    replication_key = None
    records_jsonpath = "$.adGroups[*]"
    rest_method = "POST"

    schema = th.PropertiesList(
        th.Property("adGroupId", th.StringType, description="The ad group identifier"),
        th.Property("campaignId", th.StringType, description="The parent campaign identifier"),
        th.Property("name", th.StringType, description="The ad group name"),
        th.Property("state", th.StringType, description="The ad group state"),
        th.Property("defaultBid", th.NumberType, description="Default bid amount"),
        th.Property(
            "extendedData",
            th.ObjectType(
                th.Property("creationDate", th.NumberType),
                th.Property("creationDateTime", th.StringType, format="date-time"),
                th.Property("lastUpdateDate", th.NumberType),
                th.Property("lastUpdateDateTime", th.StringType, format="date-time"),
                th.Property("servingStatus", th.StringType),
                th.Property("statusReasons", th.ArrayType(th.StringType)),
            ),
            description="Extended data fields",
        ),
    ).to_dict()

    def get_child_context(self, record: dict, context: t.Optional[t.Dict] = None) -> dict:
        """Return context dictionary for child streams with ad group ID from parent record."""
        return {
            "adgroup_id": record["adGroupId"],
            "campaign_id": record["campaignId"],
            "profile_id": context.get("profile_id") if context else None,
        }

    def get_http_headers(self, context: t.Optional[t.Dict] = None) -> dict:
        """Return the http headers needed for Amazon Ads API."""
        headers = super().get_http_headers(context)
        headers["Accept"] = "application/vnd.spAdGroup.v3+json"
        headers["Content-Type"] = "application/vnd.spAdGroup.v3+json"
        return headers

    def prepare_request_payload(self, context: t.Optional[t.Dict], next_page_token: t.Any | None = None) -> dict | None:
        """Prepare the request payload for listing ad groups."""
        return {
            "stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]},
            "includeExtendedDataFields": True,
            "maxResults": 100,
        }


__all__ = ["AdGroupsStream"]
