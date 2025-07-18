"""Sponsored Products Keywords & Targeting Summary Report stream.

Concrete implementation migrated into the `all_streams` package for cleaner
stream modularization.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
import typing as t

from .campaign_performance_report_stream import CampaignPerformanceReportStream


class KeywordsTargetingSummaryReportStream(CampaignPerformanceReportStream):
    """Stream for Sponsored Products Keywords and Targeting (SUMMARY) reports."""

    name = "keywords_targeting_summary_report"

    # Incremental replication based on daily date column
    replication_key = "date"

    # Use campaignId and targeting as composite primary keys (no date field)
    primary_keys: t.ClassVar[list[str]] = ["date", "campaignId", "targeting"]

    # Report API specifics
    report_type = "spTargeting"
    report_format = "GZIP_JSON"
    report_version = "v3"

    # Columns requested (per payload example)
    report_metrics = [
        "date",
        # Performance metrics
        "impressions",
        "clicks",
        "costPerClick",
        "clickThroughRate",
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
        "salesOtherSku7d",
        "unitsSoldOtherSku7d",
        "acosClicks7d",
        "acosClicks14d",
        "roasClicks7d",
        "roasClicks14d",
        # Entity metadata
        "keywordId",
        "keyword",
        "campaignBudgetCurrencyCode",
        "portfolioId",
        "campaignName",
        "campaignId",
        "campaignBudgetType",
        "campaignBudgetAmount",
        "campaignStatus",
        "keywordBid",
        "adGroupName",
        "adGroupId",
        "keywordType",
        "matchType",
        "targeting",
        "adKeywordStatus",
    ]

    # Time unit set to DAILY to get per-day records
    time_unit = "DAILY"
    lookback_days = 30

    # Build JSON schema with type inference
    schema: dict = {
        "type": "object",
        "properties": {
            # Numeric metrics (integer or number depending on monetary)
            "date": {"type": ["string", "null"], "format": "date"},
            "impressions": {"type": ["integer", "null"]},
            "clicks": {"type": ["integer", "null"]},
            "costPerClick": {"type": ["number", "null"]},
            "clickThroughRate": {"type": ["number", "null"]},
            "cost": {"type": ["number", "null"]},
            "purchases1d": {"type": ["integer", "null"]},
            "purchases7d": {"type": ["integer", "null"]},
            "purchases14d": {"type": ["integer", "null"]},
            "purchases30d": {"type": ["integer", "null"]},
            "purchasesSameSku1d": {"type": ["integer", "null"]},
            "purchasesSameSku7d": {"type": ["integer", "null"]},
            "purchasesSameSku14d": {"type": ["integer", "null"]},
            "purchasesSameSku30d": {"type": ["integer", "null"]},
            "unitsSoldClicks1d": {"type": ["integer", "null"]},
            "unitsSoldClicks7d": {"type": ["integer", "null"]},
            "unitsSoldClicks14d": {"type": ["integer", "null"]},
            "unitsSoldClicks30d": {"type": ["integer", "null"]},
            "sales1d": {"type": ["number", "null"]},
            "sales7d": {"type": ["number", "null"]},
            "sales14d": {"type": ["number", "null"]},
            "sales30d": {"type": ["number", "null"]},
            "attributedSalesSameSku1d": {"type": ["number", "null"]},
            "attributedSalesSameSku7d": {"type": ["number", "null"]},
            "attributedSalesSameSku14d": {"type": ["number", "null"]},
            "attributedSalesSameSku30d": {"type": ["number", "null"]},
            "unitsSoldSameSku1d": {"type": ["integer", "null"]},
            "unitsSoldSameSku7d": {"type": ["integer", "null"]},
            "unitsSoldSameSku14d": {"type": ["integer", "null"]},
            "unitsSoldSameSku30d": {"type": ["integer", "null"]},
            "kindleEditionNormalizedPagesRead14d": {"type": ["number", "null"]},
            "kindleEditionNormalizedPagesRoyalties14d": {"type": ["number", "null"]},
            "salesOtherSku7d": {"type": ["number", "null"]},
            "unitsSoldOtherSku7d": {"type": ["integer", "null"]},
            "acosClicks7d": {"type": ["number", "null"]},
            "acosClicks14d": {"type": ["number", "null"]},
            "roasClicks7d": {"type": ["number", "null"]},
            "roasClicks14d": {"type": ["number", "null"]},
            # Strings & metadata
            "keywordId": {"type": ["string", "null"]},
            "keyword": {"type": ["string", "null"]},
            "campaignBudgetCurrencyCode": {"type": ["string", "null"]},
            "startDate": {"type": ["string", "null"], "format": "date"},
            "endDate": {"type": ["string", "null"], "format": "date"},
            "portfolioId": {"type": ["string", "null"]},
            "campaignName": {"type": ["string", "null"]},
            "campaignId": {"type": ["string", "null"]},
            "campaignBudgetType": {"type": ["string", "null"]},
            "campaignBudgetAmount": {"type": ["number", "null"]},
            "campaignStatus": {"type": ["string", "null"]},
            "keywordBid": {"type": ["number", "null"]},
            "adGroupName": {"type": ["string", "null"]},
            "adGroupId": {"type": ["string", "null"]},
            "keywordType": {"type": ["string", "null"]},
            "matchType": {"type": ["string", "null"]},
            "targeting": {"type": ["string", "null"]},
            "adKeywordStatus": {"type": ["string", "null"]},
        },
        "required": ["campaignId", "targeting", "date"],
    }

    # ------------------------------------------------------------
    # Payload builder
    # ------------------------------------------------------------
    def prepare_request_payload(
        self, context: t.Optional[t.Dict], next_page_token: t.Any | None = None
    ) -> dict | None:
        """Construct request payload for Keywords & Targeting summary report."""
        import uuid

        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=self.lookback_days)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        report_name = (
            f"SponsoredProductsKeywordsAndTargetingSummaryReport_{start_date_str}_to_{end_date_str}_{timestamp}_{unique_id}"
        )

        payload = {
            "name": report_name,
            "startDate": start_date_str,
            "endDate": end_date_str,
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": ["targeting"],
                "columns": self.report_metrics,
                "reportTypeId": self.report_type,
                "timeUnit": self.time_unit,
                "format": self.report_format,
                "filters": [
                    {
                        "field": "keywordType",
                        "values": [
                            "BROAD",
                            "PHRASE",
                            "EXACT",
                            "TARGETING_EXPRESSION",
                            "TARGETING_EXPRESSION_PREDEFINED",
                        ],
                    },
                    {
                        "field": "adKeywordStatus",
                        "values": ["ENABLED", "PAUSED", "ARCHIVED"],
                    },
                ],
            },
        }

        self.logger.debug(
            f"Keywords & Targeting summary payload: {json.dumps(payload, indent=2)}"
        )
        return payload


__all__ = ["KeywordsTargetingSummaryReportStream"]
