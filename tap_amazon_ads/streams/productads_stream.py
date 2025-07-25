"""ProductAdsStream module.

Concrete implementation moved from `tap_amazon_ads.streams`.
"""

from __future__ import annotations
import typing as t

from singer_sdk import typing as th

from tap_amazon_ads.client import TapAmazonAdsStream
from tap_amazon_ads.streams.adgroups_stream import AdGroupsStream


class ProductAdsStream(TapAmazonAdsStream):
    """Stream for Sponsored Products Ads - Child stream of AdGroups."""

    name = "productads"
    path = "/sp/productAds/list"
    primary_keys: t.ClassVar[list[str]] = ["adId"]
    replication_key = None
    records_jsonpath = "$.productAds[*]"
    rest_method = "POST"
    parent_stream_type = AdGroupsStream

    schema = th.PropertiesList(
        th.Property("adId", th.StringType, description="The product ad identifier"),
        th.Property("adGroupId", th.StringType, description="The parent ad group identifier"),
        th.Property("campaignId", th.StringType, description="The parent campaign identifier"),
        th.Property("sku", th.StringType, description="Product SKU"),
        th.Property("asin", th.StringType, description="Product ASIN"),
        th.Property("state", th.StringType, description="The ad state"),
        th.Property(
            "extendedData",
            th.ObjectType(
                th.Property("creationDate", th.NumberType),
                th.Property("lastUpdateDate", th.NumberType),
            ),
            description="Extended data fields",
        ),
    ).to_dict()

    def get_http_headers(self, context: t.Optional[t.Dict] = None) -> dict:
        headers = super().get_http_headers(context)
        headers["Accept"] = "application/vnd.spProductAd.v3+json"
        headers["Content-Type"] = "application/vnd.spProductAd.v3+json"
        return headers

    def prepare_request_payload(self, context: t.Optional[t.Dict], next_page_token: t.Any | None = None) -> dict | None:
        if context and context.get("adgroup_id"):
            return {
                "adGroupIdFilter": {"include": [context["adgroup_id"]]},
                "stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]},
                "maxResults": 100,
            }
        else:
            return {
                "stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]},
                "maxResults": 100,
            }


__all__ = ["ProductAdsStream"]
