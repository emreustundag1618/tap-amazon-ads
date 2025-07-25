"""Aggregation module exposing all concrete Amazon Ads stream classes.

Having a dedicated package of concrete stream classes keeps the base
framework (`streams.py`) separate from individual report implementations.
This avoids the Python namespace collision that would occur if we tried to
turn `streams.py` itself into a package directory while preserving the same
import path.

Concrete stream classes are simply re-exported here so callers can import
cleanly with, e.g.::

    from tap_amazon_ads.streams import AdvertisedProductReportStream

Add new stream modules to the import list below as they are created.
"""

from __future__ import annotations

# Report streams implemented as separate modules already
from .advertised_product_report_stream import AdvertisedProductReportStream
from .sponsored_display_advertised_product_report_stream import (
    SponsoredDisplayAdvertisedProductReportStream,
)
from .keywords_targeting_summary_report_stream import (
    KeywordsTargetingSummaryReportStream,
)

# Proxy re-export modules for base/entity streams
from .campaigns_stream import CampaignsStream
from .adgroups_stream import AdGroupsStream
from .keywords_stream import KeywordsStream
from .targets_stream import TargetsStream
from .negative_keywords_stream import NegativeKeywordsStream
from .productads_stream import ProductAdsStream
from .campaign_budgets_stream import CampaignBudgetsStream
from .campaign_performance_report_stream import CampaignPerformanceReportStream
from .search_terms_report_stream import SearchTermsReportStream

__all__ = [
    # Entity/base streams
    "CampaignsStream",
    "AdGroupsStream",
    "KeywordsStream",
    "TargetsStream",
    "NegativeKeywordsStream",
    "ProductAdsStream",
    "CampaignBudgetsStream",
    # Report streams
    "CampaignPerformanceReportStream",
    "SearchTermsReportStream",
    "AdvertisedProductReportStream",
    "SponsoredDisplayAdvertisedProductReportStream",
    "KeywordsTargetingSummaryReportStream",
]

