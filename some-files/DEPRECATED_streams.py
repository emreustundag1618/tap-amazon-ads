"""Stream type classes for tap-amazon-ads."""

from __future__ import annotations
import json
import typing as t
from importlib import resources
from datetime import datetime, timedelta
import time
import requests
import random
import re
import io
import gzip

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_amazon_ads.client import TapAmazonAdsStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = resources.files(__package__) / "schemas"


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
        th.Property(
            "campaignId", th.StringType, description="The parent campaign identifier"
        ),
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

    def get_child_context(
        self, record: dict, context: t.Optional[t.Dict] = None
    ) -> dict:
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

    def prepare_request_payload(
        self,
        context: t.Optional[t.Dict],
        next_page_token: t.Any | None = None,
    ) -> dict | None:
        """Prepare the request payload for listing ad groups."""
        return {
            "stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]},
            "includeExtendedDataFields": True,
            "maxResults": 100,
        }


class KeywordsStream(TapAmazonAdsStream):
    """Stream for Sponsored Products Keywords - Child stream of Campaigns."""

    name = "keywords"
    path = "/sp/keywords/list"
    primary_keys: t.ClassVar[list[str]] = ["keywordId"]
    replication_key = None
    records_jsonpath = "$.keywords[*]"
    rest_method = "POST"
    parent_stream_type = CampaignsStream  # Make this a child stream of campaigns

    schema = th.PropertiesList(
        th.Property("keywordId", th.StringType, description="The keyword identifier"),
        th.Property(
            "adGroupId", th.StringType, description="The parent ad group identifier"
        ),
        th.Property(
            "campaignId", th.StringType, description="The parent campaign identifier"
        ),
        th.Property("keywordText", th.StringType, description="The keyword text"),
        th.Property(
            "matchType",
            th.StringType,
            description="Keyword match type (exact, phrase, broad)",
        ),
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

    def prepare_request_payload(
        self,
        context: t.Optional[t.Dict],
        next_page_token: t.Any | None = None,
    ) -> dict | None:
        """Prepare the request payload for listing keywords filtered by campaign."""
        # If we have a campaign_id from parent context, filter by it
        if context and context.get("campaign_id"):
            return {
                "campaignIdFilter": {"include": [context["campaign_id"]]},
                "stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]},
                "includeExtendedDataFields": True,
                "maxResults": 100,
            }
        else:
            # Fallback to no filter (but this shouldn't happen in child stream)
            return {
                "stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]},
                "includeExtendedDataFields": True,
                "maxResults": 100,
            }


class ProductAdsStream(TapAmazonAdsStream):
    """Stream for Sponsored Products Ads - Child stream of AdGroups."""

    name = "productads"
    path = "/sp/productAds/list"
    primary_keys: t.ClassVar[list[str]] = ["adId"]
    replication_key = None
    records_jsonpath = "$.productAds[*]"
    rest_method = "POST"
    parent_stream_type = AdGroupsStream  # Make this a child stream of ad groups

    schema = th.PropertiesList(
        th.Property("adId", th.StringType, description="The product ad identifier"),
        th.Property(
            "adGroupId", th.StringType, description="The parent ad group identifier"
        ),
        th.Property(
            "campaignId", th.StringType, description="The parent campaign identifier"
        ),
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
        """Return the http headers needed for Amazon Ads API."""
        headers = super().get_http_headers(context)
        headers["Accept"] = "application/vnd.spProductAd.v3+json"
        headers["Content-Type"] = "application/vnd.spProductAd.v3+json"
        return headers

    def prepare_request_payload(
        self,
        context: t.Optional[t.Dict],
        next_page_token: t.Any | None = None,
    ) -> dict | None:
        """Prepare the request payload for listing product ads filtered by ad group."""
        # If we have an adgroup_id from parent context, filter by it
        if context and context.get("adgroup_id"):
            return {
                "adGroupIdFilter": {"include": [context["adgroup_id"]]},
                "stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]},
                "maxResults": 100,
            }
        else:
            # Fallback to no filter (but this shouldn't happen in child stream)
            return {
                "stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]},
                "maxResults": 100,
            }


class TargetsStream(TapAmazonAdsStream):
    """Stream for Sponsored Products Targets (Product Targeting) - Child stream of Campaigns."""

    name = "targets"
    path = "/sp/targets/list"
    primary_keys: t.ClassVar[list[str]] = ["targetId"]
    replication_key = None
    records_jsonpath = "$.targetingClauses[*]"
    rest_method = "POST"
    parent_stream_type = CampaignsStream  # Make this a child stream of campaigns

    schema = th.PropertiesList(
        th.Property("targetId", th.StringType, description="The target identifier"),
        th.Property(
            "adGroupId", th.StringType, description="The parent ad group identifier"
        ),
        th.Property(
            "campaignId", th.StringType, description="The parent campaign identifier"
        ),
        th.Property("state", th.StringType, description="The target state"),
        th.Property(
            "expressionType",
            th.StringType,
            description="Target expression type (AUTO, MANUAL)",
        ),
        th.Property(
            "expression",
            th.ArrayType(th.ObjectType()),
            description="Target expression details",
        ),
        th.Property(
            "resolvedExpression",
            th.ArrayType(th.ObjectType()),
            description="Resolved target expression details",
        ),
        th.Property("bid", th.NumberType, description="Target bid amount"),
    ).to_dict()

    def get_http_headers(self, context: t.Optional[t.Dict] = None) -> dict:
        """Return the http headers needed for Amazon Ads API."""
        headers = super().get_http_headers(context)
        headers["Accept"] = "application/vnd.spTargetingClause.v3+json"
        headers["Content-Type"] = "application/vnd.spTargetingClause.v3+json"
        return headers

    def prepare_request_payload(
        self,
        context: t.Optional[t.Dict],
        next_page_token: t.Any | None = None,
    ) -> dict | None:
        """Prepare the request payload for listing targets filtered by campaign."""
        # If we have a campaign_id from parent context, filter by it
        if context and context.get("campaign_id"):
            return {
                "campaignIdFilter": {"include": [context["campaign_id"]]},
                "stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]},
                "maxResults": 100,
            }
        else:
            # Fallback to no filter (but this shouldn't happen in child stream)
            return {
                "stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]},
                "maxResults": 100,
            }


class CampaignBudgetsStream(TapAmazonAdsStream):
    """Stream for Sponsored Products Campaign Budgets - Child stream of Campaigns."""

    name = "campaign_budgets"
    path = "/sp/campaigns/budget/usage"
    primary_keys: t.ClassVar[list[str]] = ["campaignId"]
    replication_key = None
    records_jsonpath = "$.success[*]"  # Changed from "$[*]"
    rest_method = "POST"
    parent_stream_type = CampaignsStream  # Child stream of campaigns

    schema = th.PropertiesList(
        th.Property("campaignId", th.StringType, description="The campaign identifier"),
        th.Property("budget", th.NumberType, description="The campaign budget amount"),
        th.Property(
            "budgetUsagePercent", th.NumberType, description="Percentage of budget used"
        ),
        th.Property(
            "usageUpdatedTimestamp",
            th.StringType,
            format="date-time",
            description="When the budget usage was last updated",
        ),
        th.Property(
            "index", th.IntegerType, description="Index of the campaign in the response"
        ),
    ).to_dict()

    def get_http_headers(self, context: t.Optional[t.Dict] = None) -> dict:
        """Return the http headers needed for Amazon Ads API."""
        headers = super().get_http_headers(context)
        headers["Accept"] = "application/vnd.spCampaignBudget.v3+json"
        headers["Content-Type"] = "application/vnd.spCampaignBudget.v3+json"
        return headers

    def prepare_request_payload(
        self,
        context: t.Optional[t.Dict],
        next_page_token: t.Any | None = None,
    ) -> dict | None:
        """Prepare the request payload for getting campaign budgets."""
        campaign_id = context["campaign_id"]
        return {"campaignIds": [campaign_id]}

    def get_child_context(
        self, record: dict, context: t.Optional[t.Dict] = None
    ) -> dict:
        """Return context dictionary for child streams with campaign ID from parent record."""
        return {
            "campaign_id": record["campaignId"],
            "profile_id": context.get("profile_id") if context else None,
        }

    def parse_response(self, response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        try:
            data = response.json()
            self.logger.info(f"Received budget data: {data}")

            # Process successful responses
            for record in data.get("success", []):
                # Add any additional processing if needed
                yield record

            # Log any errors
            for error in data.get("error", []):
                self.logger.warning(f"Error in budget response: {error}")

        except Exception as e:
            self.logger.error(f"Error parsing response: {e}")
            self.logger.error(f"Response content: {response.text}")
            raise


class CampaignPerformanceReportStream(TapAmazonAdsStream):
    """Stream for Sponsored Products Campaign Performance Reports."""

    name = "campaign_performance_report"

    # Regex for extracting duplicate report ID from 409 responses
    _duplicate_id_regex = re.compile(
        r"duplicate\s+of\s*:\s*([a-f0-9\-]{36})", re.IGNORECASE
    )
    path = "/reporting/reports"
    primary_keys: t.ClassVar[list[str]] = ["campaignId", "date"]
    replication_key = "date"
    rest_method = "POST"

    # Report configuration
    report_type = "spCampaigns"
    report_format = "GZIP_JSON"  # or "GZIP_CSV"
    report_version = "v3"

    # Report fields we want to include
    # Comprehensive list of metrics/columns to include in the campaign performance report
    # Amazon Ads documentation may evolve, keep this list in sync with the latest API release notes.
    report_metrics = [
        "impressions",
        "clicks",
        "cost",
        "purchases1d",
        "purchases7d",
        "purchases14d",
        "purchases30d",
        "purchasesSameSku1d",
        "purchasesSameSku7d",
        "purchasesSameSku14d",
        "purchasesSameSku30d",
        "unitsSoldClicks1d",
        "unitsSoldClicks7d",
        "unitsSoldClicks14d",
        "unitsSoldClicks30d",
        "sales1d",
        "sales7d",
        "sales14d",
        "sales30d",
        "attributedSalesSameSku1d",
        "attributedSalesSameSku7d",
        "attributedSalesSameSku14d",
        "attributedSalesSameSku30d",
        "unitsSoldSameSku1d",
        "unitsSoldSameSku7d",
        "unitsSoldSameSku14d",
        "unitsSoldSameSku30d",
        "kindleEditionNormalizedPagesRead14d",
        "kindleEditionNormalizedPagesRoyalties14d",
        "qualifiedBorrows",
        "royaltyQualifiedBorrows",
        "addToList",
        "date",

        "campaignBiddingStrategy",
        "costPerClick",
        "clickThroughRate",
        "spend",
        "acosClicks14d",
        "roasClicks14d",
        "retailer",
        "campaignName",
        "campaignId",
        "campaignStatus",
        "campaignBudgetAmount",
        "campaignBudgetType",
        "campaignRuleBasedBudgetAmount",
        "campaignApplicableBudgetRuleId",
        "campaignApplicableBudgetRuleName",
        "campaignBudgetCurrencyCode",
        "topOfSearchImpressionShare"
    ]

    # Report time unit (daily, weekly, monthly)
    time_unit = "DAILY"

    # Number of days to look back for initial sync
    lookback_days = 60

    schema = {
        "type": "object",
        "properties": {
            "campaignId": {
                "type": ["string", "null"],
                "description": "The campaign identifier",
            },
            "campaignName": {
                "type": ["string", "null"],
                "description": "The campaign name",
            },
            "date": {
                "type": ["string", "null"],
                "format": "date",
                "description": "The report date",
            },
            "impressions": {
                "type": ["integer", "null"],
                "description": "Number of ad impressions",
            },
            "clicks": {"type": ["integer", "null"], "description": "Number of clicks"},
            "cost": {"type": ["number", "null"], "description": "Total spend amount"},
            "attributedSales14d": {
                "type": ["number", "null"],
                "description": "Attributed sales amount (14-day attribution window)",
            },
            "attributedUnitsOrdered14d": {
                "type": ["integer", "null"],
                "description": "Number of units ordered (14-day attribution window)",
            },
            "attributedSales14dSameSKU": {
                "type": ["number", "null"],
                "description": "Attributed sales amount for same SKU (14-day attribution window)",
            },
            "attributedUnitsOrdered14dSameSKU": {
                "type": ["integer", "null"],
                "description": "Number of same SKU units ordered (14-day attribution window)",
            },
            "attributedConversions14d": {
                "type": ["integer", "null"],
                "description": "Number of conversions (14-day attribution window)",
            },
            "attributedConversions14dSameSKU": {
                "type": ["integer", "null"],
                "description": "Number of same SKU conversions (14-day attribution window)",
            },
            "campaignBudget": {
                "type": ["number", "null"],
                "description": "Campaign budget amount",
            },
            "campaignBudgetType": {
                "type": ["string", "null"],
                "description": "Campaign budget type (daily, lifetime)",
            },
            "campaignStatus": {
                "type": ["string", "null"],
                "description": "Campaign status (enabled, paused, archived)",
            },
            "campaignBudgetAmount": {
                "type": ["number", "null"],
                "description": "Campaign budget amount",
            },
            "campaignRuleBasedBudgetAmount": {
                "type": ["number", "null"],
                "description": "Rule-based budget amount for campaigns using rule-based bidding",
            },
            "campaignApplicableBudgetRuleId": {
                "type": ["string", "null"],
                "description": "Identifier of the applicable budget rule",
            },
            "campaignApplicableBudgetRuleName": {
                "type": ["string", "null"],
                "description": "Name of the applicable budget rule",
            },
            "campaignBudgetCurrencyCode": {
                "type": ["string", "null"],
                "description": "Currency code for the campaign budget",
            },
            "campaignBiddingStrategy": {
                "type": ["string", "null"],
                "description": "Campaign bidding strategy",
            },
            "costPerClick": {
                "type": ["number", "null"],
                "description": "Average cost per click",
            },
            "clickThroughRate": {
                "type": ["number", "null"],
                "description": "Click-through rate",
            },
            "spend": {
                "type": ["number", "null"],
                "description": "Spend amount (alias of cost)",
            },
            "acosClicks14d": {
                "type": ["number", "null"],
                "description": "Advertising cost of sale (ACOS) for clicks within 14 days",
            },
            "roasClicks14d": {
                "type": ["number", "null"],
                "description": "Return on ad spend (ROAS) for clicks within 14 days",
            },
            "topOfSearchImpressionShare": {
                "type": ["number", "null"],
                "description": "Top of search impression share",
            },
            "retailer": {
                "type": ["string", "null"],
                "description": "Retailer name",
            },
            "purchases1d": {
                "type": ["integer", "null"],
                "description": "Number of purchases within 1 day",
            },
            "purchases7d": {
                "type": ["integer", "null"],
                "description": "Number of purchases within 7 days",
            },
            "purchases14d": {
                "type": ["integer", "null"],
                "description": "Number of purchases within 14 days",
            },
            "purchases30d": {
                "type": ["integer", "null"],
                "description": "Number of purchases within 30 days",
            },
            "purchasesSameSku1d": {
                "type": ["integer", "null"],
                "description": "Number of same SKU purchases within 1 day",
            },
            "purchasesSameSku7d": {
                "type": ["integer", "null"],
                "description": "Number of same SKU purchases within 7 days",
            },
            "purchasesSameSku14d": {
                "type": ["integer", "null"],
                "description": "Number of same SKU purchases within 14 days",
            },
            "purchasesSameSku30d": {
                "type": ["integer", "null"],
                "description": "Number of same SKU purchases within 30 days",
            },
            "unitsSoldClicks1d": {
                "type": ["integer", "null"],
                "description": "Units sold from clicks within 1 day",
            },
            "unitsSoldClicks7d": {
                "type": ["integer", "null"],
                "description": "Units sold from clicks within 7 days",
            },
            "unitsSoldClicks14d": {
                "type": ["integer", "null"],
                "description": "Units sold from clicks within 14 days",
            },
            "unitsSoldClicks30d": {
                "type": ["integer", "null"],
                "description": "Units sold from clicks within 30 days",
            },
            "sales1d": {
                "type": ["number", "null"],
                "description": "Sales amount within 1 day",
            },
            "sales7d": {
                "type": ["number", "null"],
                "description": "Sales amount within 7 days",
            },
            "sales14d": {
                "type": ["number", "null"],
                "description": "Sales amount within 14 days",
            },
            "sales30d": {
                "type": ["number", "null"],
                "description": "Sales amount within 30 days",
            },
            "attributedSalesSameSku1d": {
                "type": ["number", "null"],
                "description": "Attributed same SKU sales within 1 day",
            },
            "attributedSalesSameSku7d": {
                "type": ["number", "null"],
                "description": "Attributed same SKU sales within 7 days",
            },
            "attributedSalesSameSku14d": {
                "type": ["number", "null"],
                "description": "Attributed same SKU sales within 14 days",
            },
            "attributedSalesSameSku30d": {
                "type": ["number", "null"],
                "description": "Attributed same SKU sales within 30 days",
            },
            "unitsSoldSameSku1d": {
                "type": ["integer", "null"],
                "description": "Units of same SKU sold within 1 day",
            },
            "unitsSoldSameSku7d": {
                "type": ["integer", "null"],
                "description": "Units of same SKU sold within 7 days",
            },
            "unitsSoldSameSku14d": {
                "type": ["integer", "null"],
                "description": "Units of same SKU sold within 14 days",
            },
            "unitsSoldSameSku30d": {
                "type": ["integer", "null"],
                "description": "Units of same SKU sold within 30 days",
            },
            "kindleEditionNormalizedPagesRead14d": {
                "type": ["number", "null"],
                "description": "KENP read within 14 days",
            },
            "kindleEditionNormalizedPagesRoyalties14d": {
                "type": ["number", "null"],
                "description": "KENP royalties within 14 days",
            },
            "qualifiedBorrows": {
                "type": ["integer", "null"],
                "description": "Qualified borrows",
            },
            "royaltyQualifiedBorrows": {
                "type": ["integer", "null"],
                "description": "Qualified borrows royalties",
            },
            "addToList": {
                "type": ["integer", "null"],
                "description": "Add-to-list actions",
            },
        },
        "required": ["campaignId", "date"],
    }

    def get_http_headers(self, context: t.Optional[t.Dict] = None) -> dict:
        """Return the http headers needed for Amazon Ads API."""
        headers = super().get_http_headers(context)
        headers["Amazon-Ads-API-Version"] = self.report_version
        headers["Content-Type"] = "application/vnd.createasyncreportrequest.v3+json"
        headers["Accept"] = "application/vnd.createasyncreportrequest.v3+json"
        return headers

    def prepare_request_payload(
        self,
        context: t.Optional[t.Dict],
        next_page_token: t.Any | None = None,
    ) -> dict | None:
        import uuid

        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=self.lookback_days)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        # Generate a unique report name with a timestamp and UUID
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]  # Take first 8 chars of UUID
        report_name = f"Campaign_Performance_{start_date_str}_to_{end_date_str}_{timestamp}_{unique_id}"

        self.logger.info(
            f"Requesting report for date range: {start_date_str} to {end_date_str}"
        )
        self.logger.info(f"Using report name: {report_name}")

        # Re-use the comprehensive metric list defined at class level
        report_metrics = self.report_metrics

        payload = {
            "name": report_name,
            "startDate": start_date_str,
            "endDate": end_date_str,
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": ["campaign"],
                "columns": report_metrics,
                "reportTypeId": "spCampaigns",
                "timeUnit": "DAILY",
                "format": "GZIP_JSON",
            },
        }

        self.logger.debug(f"Report request payload: {json.dumps(payload, indent=2)}")
        return payload

    def validate_response(self, response):
        """Override default validation to allow 409 Duplicate report errors."""
        # Allow duplicate-report responses (409 or sometimes 425 from Amazon Ads)
        if response.status_code in (409, 425):
            self.logger.info(
                f"HTTP {response.status_code} received – treating as non-fatal duplicate report response"
            )
            return  # Do not raise an error
        # Delegate all other statuses to the parent implementation
        super().validate_response(response)

    def _extract_duplicate_report_id(self, message: str | None) -> str | None:
        """Extract the existing reportId from a duplicate report error message.

        The Ads API usually embeds the existing `reportId` in the error message
        when returning HTTP 409. Example message:
            "Report is a duplicate of: 12345678-1234-1234-1234-1234567890ab"
        """
        if not message:
            return None
        match = self._duplicate_id_regex.search(message)
        if match:
            return match.group(1)
        return None

    def parse_response(self, response) -> t.Iterable[dict]:
        """Handle the report lifecycle: create, poll, and download."""
        try:
            # Attempt to parse JSON but fall back to text for non-JSON error responses
            try:
                response_data = response.json()
            except ValueError:
                response_data = {}
            self.logger.info(
                f"Report creation response status={response.status_code}: {response.text}"
            )

            # Standard 202/200 success path
            report_id = (
                response_data.get("reportId")
                if isinstance(response_data, dict)
                else None
            )

            # Handle duplicate-report scenario (Amazon may return 409 or 425)
            if response.status_code in (409, 425) and not report_id:
                duplicate_report_id = self._extract_duplicate_report_id(response.text)
                if duplicate_report_id:
                    self.logger.info(
                        f"Duplicate report detected. Reusing existing report ID: {duplicate_report_id}"
                    )
                    report_id = duplicate_report_id
                else:
                    self.logger.error(
                        "Duplicate report response did not contain an existing report ID – aborting."
                    )
                    return []

            if not report_id:
                error_message = (
                    response_data.get("details")
                    if isinstance(response_data, dict)
                    else response.text
                )
                self.logger.error(f"Failed to create report. Response: {error_message}")
                return []

            self.logger.info(f"Created report with ID: {report_id}")

            # Add a small delay to prevent duplicate request errors
            import time

            time.sleep(2)

            # Process the report and yield records
            for record in self._process_report(report_id):
                yield record

        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON response: {response.text}")
        except Exception as e:
            self.logger.error(f"Error in parse_response: {str(e)}")
            if hasattr(e, "__traceback__"):
                import traceback

                self.logger.error(traceback.format_exc())

            # Add a delay before retrying to avoid rate limiting
            time.sleep(5)

    def _process_report(self, report_id: str) -> t.Iterable[dict]:
        """Process the report by polling for status and downloading results."""
        # Poll for report status and get the download URL if successful
        status, report_url = self._poll_report_status(report_id)

        if status == "COMPLETED" and report_url:
            try:
                # Download the report data from the pre-signed S3 URL
                report_data = self._download_report(report_url)
                if report_data and "reports" in report_data and report_data["reports"]:
                    # Parse and yield records
                    self.logger.info(
                        f"Successfully parsed {len(report_data['reports'])} records from report {report_id}"
                    )
                    yield from self._parse_report_data(report_data)
                else:
                    self.logger.error(
                        f"No report data found in the downloaded content for report {report_id}"
                    )
            except Exception as e:
                self.logger.error(f"Error processing report {report_id}: {str(e)}")
                if hasattr(e, "__traceback__"):
                    import traceback

                    self.logger.error(traceback.format_exc())
        else:
            self.logger.error(
                f"Report generation failed or was cancelled with status: {status}"
            )
            if not report_url and status == "COMPLETED":
                self.logger.error(
                    "Report marked as completed but no download URL was provided"
                )

    def _poll_report_status(
        self, report_id: str, max_attempts: int = 200
    ) -> tuple[str, str]:
        """Poll for report status and return status and download URL."""
        status_endpoint = f"/reporting/reports/{report_id}"
        self.logger.info(f"Polling for report status: {report_id}")

        for attempt in range(1, max_attempts + 1):
            try:
                self.logger.info(
                    f"Polling attempt {attempt}/{max_attempts} for report {report_id}"
                )
                # --- Build request headers fresh each iteration ---
                # Refresh the OAuth token proactively if it's within 5 min of expiry
                if hasattr(self, "authenticator"):
                    try:
                        auth = self.authenticator
                        if getattr(auth, "expires_in", None) and getattr(
                            auth, "last_refreshed", None
                        ):
                            import datetime

                            if datetime.datetime.now(
                                datetime.timezone.utc
                            ) >= auth.last_refreshed + datetime.timedelta(
                                seconds=auth.expires_in - 300
                            ):
                                self.logger.info(
                                    "Access token nearing expiry – refreshing."
                                )
                                auth.update_access_token()
                    except Exception as refresh_check_err:
                        self.logger.warning(
                            f"Access-token pre-check failed: {refresh_check_err}"
                        )

                status_headers = self.get_http_headers()
                status_headers["Accept"] = "*/*"  # Amazon Ads spec for status endpoint
                status_headers["Content-Type"] = (
                    "application/vnd.createasyncreportrequest.v3+json"  # kept for parity with Postman test
                )

                # Always attach the most recent bearer token
                token = (
                    getattr(self.authenticator, "access_token", None)
                    if hasattr(self, "authenticator")
                    else None
                )
                if token:
                    status_headers["Authorization"] = f"Bearer {token}"

                response = self.requests_session.get(
                    self.url_base + status_endpoint, headers=status_headers, timeout=30
                )
                response.raise_for_status()

                status_data = response.json()
                self.logger.info(
                    f"Status response: {json.dumps(status_data, indent=2)}"
                )

                status = status_data.get("status")
                if not status:
                    self.logger.error(f"Invalid status response: {status_data}")
                    return "FAILED", ""

                if status == "COMPLETED":
                    report_url = status_data.get("url")
                    if not report_url:
                        self.logger.error(
                            f"Report completed but no download URL provided: {status_data}"
                        )
                        return "FAILED", ""
                    return status, report_url

                elif status in ["FAILURE", "CANCELLED"]:
                    failure_reason = status_data.get(
                        "failureReason", "No details provided"
                    )
                    self.logger.error(
                        f"Report generation {status.lower()}: {failure_reason}"
                    )
                    return status, ""

                # Exponential backoff with jitter
                sleep_time = min(2**attempt, 60) + (random.random() * 2)  # Add jitter
                self.logger.info(
                    f"Report status: {status}. Waiting {sleep_time:.2f} seconds before next poll..."
                )
                time.sleep(sleep_time)

            except requests.exceptions.HTTPError as http_err:
                status_code = (
                    http_err.response.status_code
                    if getattr(http_err, "response", None) is not None
                    else None
                )
                if status_code == 401:
                    self.logger.warning(
                        "401 Unauthorized received – refreshing access token and retrying."
                    )
                    try:
                        if hasattr(self, "authenticator"):
                            self.authenticator.update_access_token()
                    except Exception as refresh_err:
                        self.logger.error(
                            f"Failed to refresh access token: {refresh_err}"
                        )
                        if attempt == max_attempts:
                            return "FAILED", ""
                    time.sleep(2)
                    continue  # Retry immediately after refreshing token

                if status_code == 429:
                    retry_after = (
                        int(http_err.response.headers.get("Retry-After", 60))
                        if http_err.response
                        else 60
                    )
                    self.logger.info(
                        f"429 Too Many Requests – sleeping {retry_after}s before retry."
                    )
                    time.sleep(retry_after)
                    continue  # Retry after waiting

                # Other HTTP errors
                self.logger.error(
                    f"HTTP error polling report status (attempt {attempt}): {str(http_err)}"
                )
                if attempt == max_attempts:
                    self.logger.error("Max polling attempts reached")
                    return "FAILED", ""
                time.sleep(5)

            except requests.exceptions.RequestException as e:
                self.logger.error(
                    f"Request error polling report status (attempt {attempt}): {str(e)}"
                )
                if attempt == max_attempts:
                    self.logger.error("Max polling attempts reached")
                    return "FAILED", ""
                time.sleep(5)

        self.logger.error(
            f"Max polling attempts ({max_attempts}) reached without completion"
        )
        return "TIMEOUT", ""

    def _download_report(self, report_url: str) -> dict:
        """
        Download the completed report from the provided S3 pre-signed URL.

        Args:
            report_url: The pre-signed S3 URL to download the report from

        Returns:
            dict: The parsed report data
        """
        self.logger.info(f"Downloading report from S3 URL: {report_url}")

        try:
            # Download the report from the S3 pre-signed URL
            response = self.requests_session.get(
                report_url, timeout=60  # Increased timeout for potentially large files
            )
            response.raise_for_status()

            # The report is a gzipped JSON file
            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz_file:
                report_data = json.load(gz_file)

            self.logger.info(f"Successfully downloaded and decompressed report")

            # Log a sample of the report data for debugging
            if isinstance(report_data, list) and len(report_data) > 0:
                sample = report_data[0] if len(report_data) > 0 else {}
                self.logger.debug(f"Report data sample: {json.dumps(sample, indent=2)}")
            else:
                self.logger.warning("Report data is empty or in unexpected format")
                self.logger.debug(f"Raw report data: {report_data}")

            # Return the report data in the expected format
            return {
                "reports": (
                    report_data if isinstance(report_data, list) else [report_data]
                )
            }

        except Exception as e:
            self.logger.error(f"Error downloading report: {str(e)}")
            if hasattr(e, "response") and e.response is not None:
                try:
                    error_details = e.response.json()
                    self.logger.error(f"Error details: {error_details}")
                except (ValueError, AttributeError):
                    self.logger.error(
                        f"Raw error response: {e.response.text if hasattr(e.response, 'text') else 'No response text'}"
                    )
            raise

    def _parse_report_data(self, report_data: dict) -> t.Iterable[dict]:
        """Parse the report data into records."""
        # The report data is a list of records in the "reports" key
        for record in report_data.get("reports", []):
            parsed_record = {}

            # Map the record fields to our schema
            for field in self.schema["properties"].keys():
                value = record.get(field)

                # --- Dynamic numeric conversion based on schema definitions ---
                if value is not None:
                    prop_types = self.schema["properties"].get(field, {}).get("type", [])
                    # prop_types could be a list or a single string
                    if isinstance(prop_types, str):
                        prop_types = [prop_types]
                    try:
                        if "integer" in prop_types:
                            # Some integer metrics are returned as floats like "0.0"; cast safely
                            value = int(float(value))
                        elif "number" in prop_types:
                            value = float(value)
                    except (ValueError, TypeError):
                        value = None

                parsed_record[field] = value

            self.logger.debug(f"Parsed record: {parsed_record}")
            yield parsed_record


class NegativeKeywordsStream(TapAmazonAdsStream):
    """Stream for Sponsored Products Negative Keywords - Child stream of Campaigns."""

    name = "negative_keywords"
    path = "/sp/negativeKeywords/list"
    primary_keys: t.ClassVar[list[str]] = ["keywordId"]
    replication_key = None
    records_jsonpath = "$.negativeKeywords[*]"
    rest_method = "POST"
    parent_stream_type = CampaignsStream  # Make this a child stream of campaigns

    schema = th.PropertiesList(
        th.Property(
            "keywordId", th.StringType, description="The negative keyword identifier"
        ),
        th.Property(
            "adGroupId", th.StringType, description="The parent ad group identifier"
        ),
        th.Property(
            "campaignId", th.StringType, description="The parent campaign identifier"
        ),
        th.Property(
            "keywordText", th.StringType, description="The negative keyword text"
        ),
        th.Property(
            "matchType", th.StringType, description="Negative keyword match type"
        ),
        th.Property("state", th.StringType, description="The negative keyword state"),
    ).to_dict()

    def get_http_headers(self, context: t.Optional[t.Dict] = None) -> dict:
        """Return the http headers needed for Amazon Ads API."""
        headers = super().get_http_headers(context)
        headers["Accept"] = "application/vnd.spNegativeKeyword.v3+json"
        headers["Content-Type"] = "application/vnd.spNegativeKeyword.v3+json"
        return headers

    def prepare_request_payload(
        self,
        context: t.Optional[t.Dict],
        next_page_token: t.Any | None = None,
    ) -> dict | None:
        """Prepare the request payload for listing negative keywords filtered by campaign."""
        # If we have a campaign_id from parent context, filter by it
        if context and "campaign_id" in context:
            return {
                "campaignIdFilter": {"include": [context["campaign_id"]]},
                "stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]},
                "maxResults": 100,
            }
        else:
            # Fallback to no filter (but this shouldn't happen in child stream)
            return {
                "stateFilter": {"include": ["ENABLED", "PAUSED", "ARCHIVED"]},
                "maxResults": 100,
            }
