"""NegativeKeywordsStream module.

Concrete implementation migrated from `tap_amazon_ads.streams`.
"""

from __future__ import annotations
import typing as t

from singer_sdk import typing as th

from tap_amazon_ads.client import TapAmazonAdsStream
from tap_amazon_ads.streams.campaigns_stream import CampaignsStream


class NegativeKeywordsStream(TapAmazonAdsStream):
    """Stream for Sponsored Products Negative Keywords - Child stream of Campaigns."""

    name = "negative_keywords"
    path = "/sp/negativeKeywords/list"
    primary_keys: t.ClassVar[list[str]] = ["keywordId"]
    replication_key = None
    records_jsonpath = "$.negativeKeywords[*]"
    rest_method = "POST"
    parent_stream_type = CampaignsStream

    schema = th.PropertiesList(
        th.Property("keywordId", th.StringType, description="The negative keyword identifier"),
        th.Property("adGroupId", th.StringType, description="The parent ad group identifier"),
        th.Property("campaignId", th.StringType, description="The parent campaign identifier"),
        th.Property("keywordText", th.StringType, description="The negative keyword text"),
        th.Property("matchType", th.StringType, description="Negative keyword match type"),
        th.Property("state", th.StringType, description="The negative keyword state"),
    ).to_dict()

    def get_http_headers(self, context: t.Optional[t.Dict] = None) -> dict:
        headers = super().get_http_headers(context)
        headers["Accept"] = "application/vnd.spNegativeKeyword.v3+json"
        headers["Content-Type"] = "application/vnd.spNegativeKeyword.v3+json"
        return headers

    def prepare_request_payload(self, context: t.Optional[t.Dict], next_page_token: t.Any | None = None) -> dict | None:
        if context and "campaign_id" in context:
            return {
                "campaignIdFilter": {"include": [context["campaign_id"]]},
                "stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]},
                "maxResults": 100,
            }
        else:
            return {
                "stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]},
                "maxResults": 100,
            }


__all__ = ["NegativeKeywordsStream"]
