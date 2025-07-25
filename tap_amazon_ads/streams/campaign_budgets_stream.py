"""CampaignBudgetsStream module.

Concrete implementation migrated from `tap_amazon_ads.streams`.
"""

from __future__ import annotations
import typing as t

from singer_sdk import typing as th

from tap_amazon_ads.client import TapAmazonAdsStream
from tap_amazon_ads.streams.campaigns_stream import CampaignsStream


class CampaignBudgetsStream(TapAmazonAdsStream):
    """Stream for Sponsored Products Campaign Budgets - Child stream of Campaigns."""

    name = "campaign_budgets"
    path = "/sp/campaigns/budget/usage"
    primary_keys: t.ClassVar[list[str]] = ["campaignId"]
    replication_key = None
    records_jsonpath = "$.success[*]"
    rest_method = "POST"
    parent_stream_type = CampaignsStream

    schema = th.PropertiesList(
        th.Property("campaignId", th.StringType, description="The campaign identifier"),
        th.Property("budget", th.NumberType, description="The campaign budget amount"),
        th.Property("budgetUsagePercent", th.NumberType, description="Percentage of budget used"),
        th.Property(
            "usageUpdatedTimestamp",
            th.StringType,
            format="date-time",
            description="When the budget usage was last updated",
        ),
        th.Property("index", th.IntegerType, description="Index of the campaign in the response"),
    ).to_dict()

    def get_http_headers(self, context: t.Optional[t.Dict] = None) -> dict:
        headers = super().get_http_headers(context)
        headers["Accept"] = "application/vnd.spCampaignBudget.v3+json"
        headers["Content-Type"] = "application/vnd.spCampaignBudget.v3+json"
        return headers

    def prepare_request_payload(self, context: t.Optional[t.Dict], next_page_token: t.Any | None = None) -> dict | None:
        campaign_id = context["campaign_id"] if context else None
        return {"campaignIds": [campaign_id]} if campaign_id else None

    def get_child_context(self, record: dict, context: t.Optional[t.Dict] = None) -> dict:
        return {
            "campaign_id": record["campaignId"],
            "profile_id": context.get("profile_id") if context else None,
        }

    def parse_response(self, response) -> t.Iterable[dict]:
        """Parse the response and yield result rows."""
        try:
            data = response.json()
            self.logger.info("Received budget data", extra={"data": data})

            for record in data.get("success", []):
                yield record

            for error in data.get("error", []):
                self.logger.warning("Error in budget response", extra={"error": error})
        except Exception as exc:
            self.logger.error("Error parsing budget response", exc_info=exc)
            raise


__all__ = ["CampaignBudgetsStream"]
