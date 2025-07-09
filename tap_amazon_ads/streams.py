"""Stream type classes for tap-amazon-ads."""

from __future__ import annotations

import typing as t
from importlib import resources

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
        th.Property("budget", th.ObjectType(
            th.Property("budget", th.NumberType),
            th.Property("budgetType", th.StringType),
        ), description="Campaign budget information"),
        th.Property("targetingType", th.StringType, description="Campaign targeting type (manual, auto)"),
        th.Property("state", th.StringType, description="The campaign state (enabled, paused, archived)"),
        th.Property("startDate", th.StringType, format="date", description="Campaign start date"),
        th.Property("endDate", th.StringType, format="date", description="Campaign end date"),
        th.Property("premiumBidAdjustment", th.BooleanType, description="Premium bid adjustment enabled"),
        th.Property(
            "dynamicBidding",
            th.ObjectType(
                th.Property("strategy", th.StringType),
                th.Property("placementBidding", th.ArrayType(th.ObjectType(
                    th.Property("placement", th.StringType),
                    th.Property("percentage", th.NumberType),
                ))),
            ),
        ),
        th.Property("servingStatus", th.StringType, description="Campaign serving status"),
    ).to_dict()

    def get_child_context(self, record: dict, context: t.Optional[t.Dict] = None) -> dict:
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
            "stateFilter": {
                "include": ["ENABLED", "PAUSED", "ARCHIVED"]
            },
            "maxResults": 100
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
            description="Extended data fields"
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

    def prepare_request_payload(
        self,
        context: t.Optional[t.Dict],
        next_page_token: t.Any | None = None,
    ) -> dict | None:
        """Prepare the request payload for listing ad groups."""
        return {
            "stateFilter": {
                "include": ["ENABLED", "PAUSED", "ARCHIVED"]
            },
            "includeExtendedDataFields": True,
            "maxResults": 100
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
            description="Extended data fields"
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
                "campaignIdFilter": {
                    "include": [context["campaign_id"]]
                },
                "stateFilter": {
                    "include": ["ENABLED", "PAUSED", "ARCHIVED"]
                },
                "includeExtendedDataFields": True,
                "maxResults": 100
            }
        else:
            # Fallback to no filter (but this shouldn't happen in child stream)
            return {
                "stateFilter": {
                    "include": ["ENABLED", "PAUSED", "ARCHIVED"]
                },
                "includeExtendedDataFields": True,
                "maxResults": 100
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
            description="Extended data fields"
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
                "adGroupIdFilter": {
                    "include": [context["adgroup_id"]]
                },
                "stateFilter": {
                    "include": ["ENABLED", "PAUSED", "ARCHIVED"]
                },
                "maxResults": 100
            }
        else:
            # Fallback to no filter (but this shouldn't happen in child stream)
            return {
                "stateFilter": {
                    "include": ["ENABLED", "PAUSED", "ARCHIVED"]
                },
                "maxResults": 100
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
        th.Property("adGroupId", th.StringType, description="The parent ad group identifier"),
        th.Property("campaignId", th.StringType, description="The parent campaign identifier"),
        th.Property("state", th.StringType, description="The target state"),
        th.Property("expressionType", th.StringType, description="Target expression type (AUTO, MANUAL)"),
        th.Property(
            "expression",
            th.ArrayType(th.ObjectType()),
            description="Target expression details"
        ),
        th.Property(
            "resolvedExpression",
            th.ArrayType(th.ObjectType()),
            description="Resolved target expression details"
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
                "campaignIdFilter": {
                    "include": [context["campaign_id"]]
                },
                "stateFilter": {
                    "include": ["ENABLED", "PAUSED", "ARCHIVED"]
                },
                "maxResults": 100
            }
        else:
            # Fallback to no filter (but this shouldn't happen in child stream)
            return {
                "stateFilter": {
                    "include": ["ENABLED", "PAUSED", "ARCHIVED"]
                },
                "maxResults": 100
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
        th.Property("budgetUsagePercent", th.NumberType, description="Percentage of budget used"),
        th.Property("usageUpdatedTimestamp", th.StringType, format="date-time", 
                   description="When the budget usage was last updated"),
        th.Property("index", th.IntegerType, description="Index of the campaign in the response"),
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
        return {
            "campaignIds": [campaign_id]
        }

    def get_child_context(self, record: dict, context: t.Optional[t.Dict] = None) -> dict:
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
        th.Property("keywordId", th.StringType, description="The negative keyword identifier"),
        th.Property("adGroupId", th.StringType, description="The parent ad group identifier"),
        th.Property("campaignId", th.StringType, description="The parent campaign identifier"),
        th.Property("keywordText", th.StringType, description="The negative keyword text"),
        th.Property("matchType", th.StringType, description="Negative keyword match type"),
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
                "campaignIdFilter": {
                    "include": [context["campaign_id"]]
                },
                "stateFilter": {
                    "include": ["ENABLED", "PAUSED", "ARCHIVED"]
                },
                "maxResults": 100
            }
        else:
            # Fallback to no filter (but this shouldn't happen in child stream)
            return {
                "stateFilter": {
                    "include": ["ENABLED", "PAUSED", "ARCHIVED"]
                },
                "maxResults": 100
            }
