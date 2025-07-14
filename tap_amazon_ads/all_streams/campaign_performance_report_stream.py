"""CampaignPerformanceReportStream module.

Concrete implementation migrated from `tap_amazon_ads.streams`.
"""

from __future__ import annotations

import gzip
import io
import json
import random
import re
import time
from datetime import datetime, timedelta
import typing as t

import requests
from singer_sdk import typing as th

from tap_amazon_ads.client import TapAmazonAdsStream


class CampaignPerformanceReportStream(TapAmazonAdsStream):
    """Stream for Sponsored Products Campaign Performance Reports."""

    name = "campaign_performance_report"

    _duplicate_id_regex = re.compile(r"duplicate\s+of\s*:\s*([a-f0-9\-]{36})", re.IGNORECASE)
    path = "/reporting/reports"
    primary_keys: t.ClassVar[list[str]] = ["campaignId", "date"]
    replication_key = "date"
    rest_method = "POST"

    report_type = "spCampaigns"
    report_format = "GZIP_JSON"
    report_version = "v3"

    # Comprehensive set of metrics requested in the report
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
        "topOfSearchImpressionShare",
    ]

    time_unit = "DAILY"
    lookback_days = 30

    # --- Schema definition (same as original) ---
    schema: dict = {
        "type": "object",
        "properties": {
            "campaignId": {"type": ["string", "null"], "description": "Campaign ID"},
            "campaignName": {"type": ["string", "null"], "description": "Campaign name"},
            "date": {"type": ["string", "null"], "format": "date"},
            "impressions": {"type": ["integer", "null"]},
            "clicks": {"type": ["integer", "null"]},
            "cost": {"type": ["number", "null"]},
            "attributedSales14d": {"type": ["number", "null"]},
            "attributedUnitsOrdered14d": {"type": ["integer", "null"]},
            "attributedSales14dSameSKU": {"type": ["number", "null"]},
            "attributedUnitsOrdered14dSameSKU": {"type": ["integer", "null"]},
            "attributedConversions14d": {"type": ["integer", "null"]},
            "attributedConversions14dSameSKU": {"type": ["integer", "null"]},
            "campaignBudget": {"type": ["number", "null"]},
            "campaignBudgetType": {"type": ["string", "null"]},
            "campaignStatus": {"type": ["string", "null"]},
            "campaignBudgetAmount": {"type": ["number", "null"]},
            "campaignRuleBasedBudgetAmount": {"type": ["number", "null"]},
            "campaignApplicableBudgetRuleId": {"type": ["string", "null"]},
            "campaignApplicableBudgetRuleName": {"type": ["string", "null"]},
            "campaignBudgetCurrencyCode": {"type": ["string", "null"]},
            "campaignBiddingStrategy": {"type": ["string", "null"]},
            "costPerClick": {"type": ["number", "null"]},
            "clickThroughRate": {"type": ["number", "null"]},
            "spend": {"type": ["number", "null"]},
            "acosClicks14d": {"type": ["number", "null"]},
            "roasClicks14d": {"type": ["number", "null"]},
            "topOfSearchImpressionShare": {"type": ["number", "null"]},
            "retailer": {"type": ["string", "null"]},
            # Additional metric fields trimmed for brevity
        },
        "required": ["campaignId", "date"],
    }

    # --------------------------- Helpers ---------------------------
    def get_http_headers(self, context: t.Optional[t.Dict] = None) -> dict:  # noqa: D401
        headers = super().get_http_headers(context)
        headers["Amazon-Ads-API-Version"] = self.report_version
        headers["Content-Type"] = "application/vnd.createasyncreportrequest.v3+json"
        headers["Accept"] = "application/vnd.createasyncreportrequest.v3+json"
        return headers

    # --------------------------- Payload ---------------------------
    def prepare_request_payload(self, context: t.Optional[t.Dict], next_page_token: t.Any | None = None) -> dict | None:
        import uuid

        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=self.lookback_days)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        report_name = f"Campaign_Performance_{start_date_str}_to_{end_date_str}_{timestamp}_{unique_id}"

        payload = {
            "name": report_name,
            "startDate": start_date_str,
            "endDate": end_date_str,
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": ["campaign"],
                "columns": self.report_metrics,
                "reportTypeId": self.report_type,
                "timeUnit": self.time_unit,
                "format": self.report_format,
            },
        }
        self.logger.debug("Report request payload", extra={"payload": payload})
        return payload

    # ----------------- Response Handling & Polling ----------------
    def validate_response(self, response):
        if response.status_code in (409, 425):
            self.logger.info(f"HTTP {response.status_code} treated as duplicate report response")
            return
        super().validate_response(response)

    def _extract_duplicate_report_id(self, message: str | None) -> str | None:
        if not message:
            return None
        match = self._duplicate_id_regex.search(message)
        return match.group(1) if match else None

    def parse_response(self, response) -> t.Iterable[dict]:  # noqa: C901
        try:
            try:
                response_data = response.json()
            except ValueError:
                response_data = {}
            self.logger.info("Report creation response", extra={"status": response.status_code, "text": response.text})

            report_id = response_data.get("reportId") if isinstance(response_data, dict) else None
            if response.status_code in (409, 425) and not report_id:
                report_id = self._extract_duplicate_report_id(response.text)
                if not report_id:
                    self.logger.error("Duplicate report response without report ID – aborting")
                    return []

            if not report_id:
                self.logger.error("Failed to create report", extra={"response": response_data or response.text})
                return []

            self.logger.info("Created report", extra={"report_id": report_id})
            time.sleep(2)  # Small delay to avoid duplicate errors

            yield from self._process_report(report_id)
        except Exception as exc:
            self.logger.error("Error in parse_response", exc_info=exc)

    # --------------------------- Helpers ---------------------------
    def _process_report(self, report_id: str) -> t.Iterable[dict]:
        status, url = self._poll_report_status(report_id)
        if status == "COMPLETED" and url:
            try:
                yield from self._parse_report_data(self._download_report(url))
            except Exception as exc:
                self.logger.error("Error processing report", exc_info=exc)
        else:
            self.logger.error("Report generation failed", extra={"status": status})

    def _poll_report_status(self, report_id: str, max_attempts: int = 200) -> tuple[str, str]:
        status_endpoint = f"/reporting/reports/{report_id}"
        self.logger.info("Polling report status", extra={"report_id": report_id})

        for attempt in range(1, max_attempts + 1):
            try:
                if hasattr(self, "authenticator"):
                    try:
                        auth = self.authenticator
                        if getattr(auth, "expires_in", None) and getattr(auth, "last_refreshed", None):
                            if datetime.now(datetime.timezone.utc) >= auth.last_refreshed + timedelta(seconds=auth.expires_in - 300):
                                self.logger.info("Refreshing access token before expiry")
                                auth.update_access_token()
                    except Exception as token_exc:
                        self.logger.warning("Access-token refresh pre-check failed", exc_info=token_exc)

                headers = self.get_http_headers()
                headers["Accept"] = "*/*"
                headers["Content-Type"] = "application/vnd.createasyncreportrequest.v3+json"
                if hasattr(self, "authenticator") and getattr(self.authenticator, "access_token", None):
                    headers["Authorization"] = f"Bearer {self.authenticator.access_token}"

                resp = self.requests_session.get(self.url_base + status_endpoint, headers=headers, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                status = data.get("status")
                self.logger.info("Status response", extra={"data": data})

                if status == "COMPLETED":
                    url = data.get("url")
                    if url:
                        return status, url
                    self.logger.error("Completed report missing URL", extra={"data": data})
                    return "FAILED", ""
                if status in ("FAILURE", "CANCELLED"):
                    self.logger.error("Report failed", extra={"status": status, "reason": data.get("failureReason")})
                    return status, ""

                sleep = min(2 ** attempt, 60) + random.random() * 2
                self.logger.info("Report status %s – sleeping %.2fs", status, sleep)
                time.sleep(sleep)
            except requests.exceptions.HTTPError as http_err:
                code = http_err.response.status_code if http_err.response else None
                if code == 401:
                    self.logger.warning("401 Unauthorized – refreshing token and retrying")
                    if hasattr(self, "authenticator"):
                        try:
                            self.authenticator.update_access_token()
                        except Exception:
                            if attempt == max_attempts:
                                return "FAILED", ""
                    time.sleep(2)
                    continue
                if code == 429:
                    retry_after = int(http_err.response.headers.get("Retry-After", 60)) if http_err.response else 60
                    self.logger.info("429 Too Many Requests – waiting %ss", retry_after)
                    time.sleep(retry_after)
                    continue
                self.logger.error("HTTP error polling report", exc_info=http_err)
                if attempt == max_attempts:
                    return "FAILED", ""
                time.sleep(5)
            except requests.exceptions.RequestException as req_exc:
                self.logger.error("Request error polling report", exc_info=req_exc)
                if attempt == max_attempts:
                    return "FAILED", ""
                time.sleep(5)
        return "TIMEOUT", ""

    def _download_report(self, url: str) -> dict:
        self.logger.info("Downloading report", extra={"url": url})
        try:
            resp = self.requests_session.get(url, timeout=60)
            resp.raise_for_status()
            with gzip.GzipFile(fileobj=io.BytesIO(resp.content)) as gz:
                data = json.load(gz)
            self.logger.info("Downloaded and decompressed report")
            return {"reports": data if isinstance(data, list) else [data]}
        except Exception as exc:
            self.logger.error("Error downloading report", exc_info=exc)
            raise

    def _parse_report_data(self, report_data: dict) -> t.Iterable[dict]:
        """Parse the downloaded report JSON into stream records."""
        for record in report_data.get("reports", []):
            parsed_record: dict = {}
            for field, prop in self.schema["properties"].items():
                value = record.get(field)
                if value is not None:
                    prop_types = prop.get("type", [])
                    if isinstance(prop_types, str):
                        prop_types = [prop_types]
                    try:
                        if "integer" in prop_types:
                            value = int(float(value))
                        elif "number" in prop_types:
                            value = float(value)
                    except (ValueError, TypeError):
                        value = None
                parsed_record[field] = value
            yield parsed_record


__all__ = ["CampaignPerformanceReportStream"]
