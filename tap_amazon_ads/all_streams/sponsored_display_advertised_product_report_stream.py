"""Sponsored Display Advertised Product Daily Report stream.

Concrete implementation migrated into the `all_streams` package to avoid the
circular import that occurs when referencing the aggregation module.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
import typing as t

from .campaign_performance_report_stream import CampaignPerformanceReportStream


class SponsoredDisplayAdvertisedProductReportStream(CampaignPerformanceReportStream):
    """Stream for Sponsored Display Advertised Product (Daily) reports."""

    name = "sd_advertised_product_report"

    # Report API specifics
    report_type = "sdAdvertisedProduct"
    report_format = "GZIP_JSON"
    report_version = "v3"

    # Columns (metrics) requested – based on provided payload example
    report_metrics = [
        "date",
        "adGroupId",
        "adGroupName",
        "adId",
        "promotedAsin",
        "promotedSku",
        "purchasesClicks",
        "purchasesPromotedClicks",
        "detailPageViewsClicks",
        "newToBrandPurchasesClicks",
        "salesClicks",
        "salesPromotedClicks",
        "newToBrandSalesClicks",
        "unitsSoldClicks",
        "newToBrandUnitsSoldClicks",
        "bidOptimization",
        "campaignId",
        "campaignName",
        "clicks",
        "cost",
        "campaignBudgetCurrencyCode",
        "impressions",
        "purchases",
        "impressionsViews",
        "detailPageViews",
        "sales",
        "unitsSold",
        "newToBrandPurchases",
        "newToBrandSales",
        "newToBrandUnitsSold",
        "brandedSearchesClicks",
        "brandedSearches",
        "brandedSearchesViews",
        "brandedSearchRate",
        "eCPBrandSearch",
        "videoCompleteViews",
        "videoFirstQuartileViews",
        "videoMidpointViews",
        "videoThirdQuartileViews",
        "videoUnmutes",
        "viewabilityRate",
        "viewClickThroughRate",
        "impressionsFrequencyAverage",
        "cumulativeReach",
        "newToBrandDetailPageViews",
        "newToBrandDetailPageViewViews",
        "newToBrandDetailPageViewClicks",
        "addToCart",
        "addToCartViews",
        "addToCartClicks",
        "addToCartRate",
        "eCPAddToCart",
        "newToBrandDetailPageViewRate",
        "newToBrandECPDetailPageView",
    ]

    # Standard settings – reuse parent defaults
    time_unit = "DAILY"
    lookback_days = 30

    # Build JSON schema
    schema: dict = {
        "type": "object",
        "properties": {
            "date": {"type": ["string", "null"], "format": "date"},
            "adGroupId": {"type": ["string", "null"]},
            "adGroupName": {"type": ["string", "null"]},
            "adId": {"type": ["string", "null"]},
            "promotedAsin": {"type": ["string", "null"]},
            "promotedSku": {"type": ["string", "null"]},
            "bidOptimization": {"type": ["string", "null"]},
            "campaignId": {"type": ["string", "null"]},
            "campaignName": {"type": ["string", "null"]},
            "campaignBudgetCurrencyCode": {"type": ["string", "null"]},
            # Integer count metrics
            "purchasesClicks": {"type": ["integer", "null"]},
            "purchasesPromotedClicks": {"type": ["integer", "null"]},
            "detailPageViewsClicks": {"type": ["integer", "null"]},
            "newToBrandPurchasesClicks": {"type": ["integer", "null"]},
            "salesClicks": {"type": ["number", "null"]},
            "salesPromotedClicks": {"type": ["number", "null"]},
            "newToBrandSalesClicks": {"type": ["number", "null"]},
            "unitsSoldClicks": {"type": ["integer", "null"]},
            "newToBrandUnitsSoldClicks": {"type": ["integer", "null"]},

            "clicks": {"type": ["integer", "null"]},
            "impressions": {"type": ["integer", "null"]},
            "purchases": {"type": ["integer", "null"]},
            "impressionsViews": {"type": ["integer", "null"]},
            "detailPageViews": {"type": ["integer", "null"]},
            "sales": {"type": ["number", "null"]},
            "unitsSold": {"type": ["integer", "null"]},
            "newToBrandPurchases": {"type": ["integer", "null"]},
            "newToBrandSales": {"type": ["number", "null"]},
            "newToBrandUnitsSold": {"type": ["integer", "null"]},

            "brandedSearchesClicks": {"type": ["integer", "null"]},
            "brandedSearches": {"type": ["integer", "null"]},
            "brandedSearchesViews": {"type": ["integer", "null"]},
            "brandedSearchRate": {"type": ["number", "null"]},
            "eCPBrandSearch": {"type": ["number", "null"]},

            "videoCompleteViews": {"type": ["integer", "null"]},
            "videoFirstQuartileViews": {"type": ["integer", "null"]},
            "videoMidpointViews": {"type": ["integer", "null"]},
            "videoThirdQuartileViews": {"type": ["integer", "null"]},
            "videoUnmutes": {"type": ["integer", "null"]},

            "viewabilityRate": {"type": ["number", "null"]},
            "viewClickThroughRate": {"type": ["number", "null"]},
            "impressionsFrequencyAverage": {"type": ["number", "null"]},
            "cumulativeReach": {"type": ["integer", "null"]},

            "newToBrandDetailPageViews": {"type": ["integer", "null"]},
            "newToBrandDetailPageViewViews": {"type": ["integer", "null"]},
            "newToBrandDetailPageViewClicks": {"type": ["integer", "null"]},

            "addToCart": {"type": ["integer", "null"]},
            "addToCartViews": {"type": ["integer", "null"]},
            "addToCartClicks": {"type": ["integer", "null"]},
            "addToCartRate": {"type": ["number", "null"]},
            "eCPAddToCart": {"type": ["number", "null"]},

            "newToBrandDetailPageViewRate": {"type": ["number", "null"]},
            "newToBrandECPDetailPageView": {"type": ["number", "null"]},

            "cost": {"type": ["number", "null"]},
        },
        "required": ["campaignId", "date"],
    }

    # ------------------------------------------------------------------
    # Payload builder
    # ------------------------------------------------------------------
    def prepare_request_payload(
        self, context: t.Optional[t.Dict], next_page_token: t.Any | None = None
    ) -> dict | None:
        """Build request payload for Sponsored Display advertised product report."""
        import uuid

        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=self.lookback_days)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        report_name = (
            f"SponsoredDisplayAdvertisedProductDailyReport_{start_date_str}_to_{end_date_str}_{timestamp}_{unique_id}"
        )

        payload = {
            "name": report_name,
            "startDate": start_date_str,
            "endDate": end_date_str,
            "configuration": {
                "adProduct": "SPONSORED_DISPLAY",
                "groupBy": ["advertiser"],
                "columns": self.report_metrics,
                "reportTypeId": self.report_type,
                "timeUnit": self.time_unit,
                "format": self.report_format,
            },
        }

        self.logger.debug(
            f"Sponsored Display advertised product payload: {json.dumps(payload, indent=2)}"
        )
        return payload


__all__ = ["SponsoredDisplayAdvertisedProductReportStream"]
