"""Sponsored Products Advertised Product Daily Report stream.

Concrete implementation migrated from the top-level module into
`tap_amazon_ads.streams` for better modularity.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
import typing as t

from .campaign_performance_report_stream import CampaignPerformanceReportStream


class AdvertisedProductReportStream(CampaignPerformanceReportStream):
    """Stream for Sponsored Products Advertised Product (Daily) reports."""

    name = "advertised_product_report"
    primary_keys: t.ClassVar[list[str]] = ["date", "campaignId", "adGroupId", "adId", "advertisedAsin"]
    replication_key = "date"
    # Report API specifics
    report_type = "spAdvertisedProduct"
    report_format = "GZIP_JSON"
    report_version = "v3"

    # Columns (metrics) requested – taken from the user's payload example
    report_metrics = [
        "date",
        "campaignName",
        "campaignId",
        "adGroupName",
        "adGroupId",
        "adId",
        "portfolioId",
        "impressions",
        "clicks",
        "costPerClick",
        "clickThroughRate",
        "cost",
        "spend",
        "campaignBudgetCurrencyCode",
        "campaignBudgetAmount",
        "campaignBudgetType",
        "campaignStatus",
        "advertisedAsin",
        "advertisedSku",
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
        "salesOtherSku7d",
        "unitsSoldSameSku1d",
        "unitsSoldSameSku7d",
        "unitsSoldSameSku14d",
        "unitsSoldSameSku30d",
        "unitsSoldOtherSku7d",
        "kindleEditionNormalizedPagesRead14d",
        "kindleEditionNormalizedPagesRoyalties14d",
        "acosClicks7d",
        "acosClicks14d",
        "roasClicks7d",
        "roasClicks14d",
    ]

    # Standard settings – reuse parent defaults
    time_unit = "DAILY"
    lookback_days = 30

    # Build JSON schema for the metrics above (types inferred)
    schema: dict = {
        "type": "object",
        "properties": {
            "date": {"type": ["string", "null"], "format": "date"},
            "campaignName": {"type": ["string", "null"]},
            "campaignId": {"type": ["string", "null"]},
            "adGroupName": {"type": ["string", "null"]},
            "adGroupId": {"type": ["string", "null"]},
            "adId": {"type": ["string", "null"]},
            "portfolioId": {"type": ["string", "null"]},
            "advertisedAsin": {"type": ["string", "null"]},
            "advertisedSku": {"type": ["string", "null"]},

            "impressions": {"type": ["integer", "null"]},
            "clicks": {"type": ["integer", "null"]},
            "cost": {"type": ["number", "null"]},
            "spend": {"type": ["number", "null"]},
            "costPerClick": {"type": ["number", "null"]},
            "clickThroughRate": {"type": ["number", "null"]},

            "campaignBudgetCurrencyCode": {"type": ["string", "null"]},
            "campaignBudgetAmount": {"type": ["number", "null"]},
            "campaignBudgetType": {"type": ["string", "null"]},
            "campaignStatus": {"type": ["string", "null"]},

            # Purchases & sales (integers vs decimals)
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

            "salesOtherSku7d": {"type": ["number", "null"]},

            "unitsSoldSameSku1d": {"type": ["integer", "null"]},
            "unitsSoldSameSku7d": {"type": ["integer", "null"]},
            "unitsSoldSameSku14d": {"type": ["integer", "null"]},
            "unitsSoldSameSku30d": {"type": ["integer", "null"]},
            "unitsSoldOtherSku7d": {"type": ["integer", "null"]},

            "kindleEditionNormalizedPagesRead14d": {"type": ["number", "null"]},
            "kindleEditionNormalizedPagesRoyalties14d": {"type": ["number", "null"]},

            "acosClicks7d": {"type": ["number", "null"]},
            "acosClicks14d": {"type": ["number", "null"]},
            "roasClicks7d": {"type": ["number", "null"]},
            "roasClicks14d": {"type": ["number", "null"]},
        },
        "required": ["campaignId", "date"],
    }

    # ---------------------------------------------------------------------
    # Payload builder
    # ---------------------------------------------------------------------
    def prepare_request_payload(
        self, context: t.Optional[t.Dict], next_page_token: t.Any | None = None
    ) -> dict | None:
        """Build the request body for the advertised product report."""
        import uuid

        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=self.lookback_days)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        report_name = (
            f"SponsoredProductsAdvertisedProductDailyReport_{start_date_str}_to_{end_date_str}_{timestamp}_{unique_id}"
        )

        payload = {
            "name": report_name,
            "startDate": start_date_str,
            "endDate": end_date_str,
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": ["advertiser"],
                "columns": self.report_metrics,
                "reportTypeId": self.report_type,
                "timeUnit": self.time_unit,
                "format": self.report_format,
                "filters": [
                    {
                        "field": "adCreativeStatus",
                        "values": ["ENABLED", "PAUSED", "ARCHIVED"],
                    }
                ],
            },
        }

        self.logger.debug("Advertised product report payload", extra={"payload": json.dumps(payload)})
        return payload


__all__ = ["AdvertisedProductReportStream"]
