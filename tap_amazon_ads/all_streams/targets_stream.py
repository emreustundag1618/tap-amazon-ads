"""TargetsStream module.

Concrete implementation of `TargetsStream`, migrated from `tap_amazon_ads.streams`.
"""

from __future__ import annotations
import typing as t

from singer_sdk import typing as th

from tap_amazon_ads.client import TapAmazonAdsStream
from tap_amazon_ads.all_streams.campaigns_stream import CampaignsStream


class TargetsStream(TapAmazonAdsStream):
    """Stream for Sponsored Products Targets (Product Targeting) - Child stream of Campaigns."""

    name = "targets"
    path = "/sp/targets/list"
    primary_keys: t.ClassVar[list[str]] = ["targetId"]
    replication_key = None
    records_jsonpath = "$.targetingClauses[*]"
    rest_method = "POST"
    parent_stream_type = CampaignsStream

    schema = th.PropertiesList(
        th.Property("targetId", th.StringType, description="The target identifier"),
        th.Property("adGroupId", th.StringType, description="The parent ad group identifier"),
        th.Property("campaignId", th.StringType, description="The parent campaign identifier"),
        th.Property("state", th.StringType, description="The target state"),
        th.Property("expressionType", th.StringType, description="Target expression type (AUTO, MANUAL)"),
        th.Property("expression", th.ArrayType(th.ObjectType()), description="Target expression details"),
        th.Property("resolvedExpression", th.ArrayType(th.ObjectType()), description="Resolved target expression details"),
        th.Property("bid", th.NumberType, description="Target bid amount"),
    ).to_dict()

    def get_http_headers(self, context: t.Optional[t.Dict] = None) -> dict:
        headers = super().get_http_headers(context)
        headers["Accept"] = "application/vnd.spTargetingClause.v3+json"
        headers["Content-Type"] = "application/vnd.spTargetingClause.v3+json"
        return headers

    def prepare_request_payload(self, context: t.Optional[t.Dict], next_page_token: t.Any | None = None) -> dict | None:
        if context and context.get("campaign_id"):
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


__all__ = ["TargetsStream"]
