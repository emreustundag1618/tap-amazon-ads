"""KeywordsStream module.

Concrete implementation of `KeywordsStream` migrated from `tap_amazon_ads.streams`.
"""

from __future__ import annotations
import typing as t

from singer_sdk import typing as th

from tap_amazon_ads.client import TapAmazonAdsStream
from tap_amazon_ads.all_streams.campaigns_stream import CampaignsStream


class KeywordsStream(TapAmazonAdsStream):
    """Stream for Sponsored Products Keywords - Child stream of Campaigns."""

    name = "keywords"
    path = "/sp/keywords/list"
    primary_keys: t.ClassVar[list[str]] = ["keywordId"]
    replication_key = None
    records_jsonpath = "$.keywords[*]"
    rest_method = "POST"
    parent_stream_type = CampaignsStream  # child stream of campaigns

    schema = th.PropertiesList(
        th.Property("keywordId", th.StringType, description="The keyword identifier"),
        th.Property("adGroupId", th.StringType, description="The parent ad group identifier"),
        th.Property("campaignId", th.StringType, description="The parent campaign identifier"),
        th.Property("keywordText", th.StringType, description="The keyword text"),
        th.Property("matchType", th.StringType, description="Keyword match type (exact, phrase, broad)"),
        th.Property("state", th.StringType, description="The keyword state"),
        th.Property("bid", th.NumberType, description="Keyword bid amount"),
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

    def get_http_headers(self, context: t.Optional[t.Dict] = None) -> dict:
        """Return the http headers needed for Amazon Ads API."""
        headers = super().get_http_headers(context)
        headers["Accept"] = "application/vnd.spKeyword.v3+json"
        headers["Content-Type"] = "application/vnd.spKeyword.v3+json"
        return headers

    def prepare_request_payload(self, context: t.Optional[t.Dict], next_page_token: t.Any | None = None) -> dict | None:
        """Prepare the request payload for listing keywords filtered by campaign."""
        if context and context.get("campaign_id"):
            return {
                "campaignIdFilter": {"include": [context["campaign_id"]]},
                "stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]},
                "includeExtendedDataFields": True,
                "maxResults": 100,
            }
        else:
            return {
                "stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]},
                "includeExtendedDataFields": True,
                "maxResults": 100,
            }


__all__ = ["KeywordsStream"]
