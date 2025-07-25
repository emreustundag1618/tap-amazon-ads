"""SearchTermsReportStream module.

Concrete implementation for Sponsored Products Search Term reports.
"""

from __future__ import annotations

import gzip
import io
import json
import random
import re
import time
from datetime import datetime, timedelta, timezone
import typing as t

import requests
from singer_sdk import typing as th

from .campaign_performance_report_stream import CampaignPerformanceReportStream


class SearchTermsReportStream(CampaignPerformanceReportStream):
    """Stream for Sponsored Products Search Term Reports."""

    name = "search_terms_report"
    
    # Primary keys for search terms report
    primary_keys: t.ClassVar[list[str]] = ["campaignId", "searchTerm", "keywordId", "date"]

    # Report API specifics
    report_type = "spSearchTerm"
    report_format = "GZIP_JSON"
    report_version = "v3"

    # Columns from official Amazon Ads API example
    report_metrics = [
        "adGroupId",
        "campaignId", 
        "keywordId",
        "targeting",
        "searchTerm",
        "impressions", 
        "clicks", 
        "cost", 
        "purchases1d", 
        "purchases7d", 
        "purchases14d", 
        "purchases30d",
        "date"
    ]

    time_unit = "DAILY"
    lookback_days = 30

    # Schema definition based on the payload columns
    schema = th.PropertiesList(
        th.Property("campaignId", th.StringType, description="Campaign ID"),
        th.Property("campaignName", th.StringType, description="Campaign name"),
        th.Property("campaignStatus", th.StringType, description="Campaign status"),
        th.Property("campaignBudgetType", th.StringType, description="Campaign budget type"),
        th.Property("campaignBudgetAmount", th.NumberType, description="Campaign budget amount"),
        th.Property("campaignBudgetCurrencyCode", th.StringType, description="Campaign budget currency"),
        th.Property("portfolioId", th.StringType, description="Portfolio ID"),
        th.Property("adGroupId", th.StringType, description="Ad group ID"),
        th.Property("adGroupName", th.StringType, description="Ad group name"),
        th.Property("keywordId", th.StringType, description="Keyword ID"),
        th.Property("keyword", th.StringType, description="Keyword text"),
        th.Property("keywordType", th.StringType, description="Keyword type"),
        th.Property("matchType", th.StringType, description="Match type"),
        th.Property("keywordBid", th.NumberType, description="Keyword bid"),
        th.Property("adKeywordStatus", th.StringType, description="Ad keyword status"),
        th.Property("targeting", th.StringType, description="Targeting type"),
        th.Property("searchTerm", th.StringType, description="Search term"),
        th.Property("date", th.StringType, format="date", description="Report date"),
        th.Property("startDate", th.StringType, format="date", description="Start date"),
        th.Property("endDate", th.StringType, format="date", description="End date"),
        th.Property("impressions", th.IntegerType, description="Impressions"),
        th.Property("clicks", th.IntegerType, description="Clicks"),
        th.Property("cost", th.NumberType, description="Cost"),
        th.Property("costPerClick", th.NumberType, description="Cost per click"),
        th.Property("clickThroughRate", th.NumberType, description="Click through rate"),
        th.Property("purchases1d", th.IntegerType, description="Purchases 1 day"),
        th.Property("purchases7d", th.IntegerType, description="Purchases 7 days"),
        th.Property("purchases14d", th.IntegerType, description="Purchases 14 days"),
        th.Property("purchases30d", th.IntegerType, description="Purchases 30 days"),
        th.Property("purchasesSameSku1d", th.IntegerType, description="Purchases same SKU 1 day"),
        th.Property("purchasesSameSku7d", th.IntegerType, description="Purchases same SKU 7 days"),
        th.Property("purchasesSameSku14d", th.IntegerType, description="Purchases same SKU 14 days"),
        th.Property("purchasesSameSku30d", th.IntegerType, description="Purchases same SKU 30 days"),
        th.Property("unitsSoldClicks1d", th.IntegerType, description="Units sold clicks 1 day"),
        th.Property("unitsSoldClicks7d", th.IntegerType, description="Units sold clicks 7 days"),
        th.Property("unitsSoldClicks14d", th.IntegerType, description="Units sold clicks 14 days"),
        th.Property("unitsSoldClicks30d", th.IntegerType, description="Units sold clicks 30 days"),
        th.Property("sales1d", th.NumberType, description="Sales 1 day"),
        th.Property("sales7d", th.NumberType, description="Sales 7 days"),
        th.Property("sales14d", th.NumberType, description="Sales 14 days"),
        th.Property("sales30d", th.NumberType, description="Sales 30 days"),
        th.Property("attributedSalesSameSku1d", th.NumberType, description="Attributed sales same SKU 1 day"),
        th.Property("attributedSalesSameSku7d", th.NumberType, description="Attributed sales same SKU 7 days"),
        th.Property("attributedSalesSameSku14d", th.NumberType, description="Attributed sales same SKU 14 days"),
        th.Property("attributedSalesSameSku30d", th.NumberType, description="Attributed sales same SKU 30 days"),
        th.Property("unitsSoldSameSku1d", th.IntegerType, description="Units sold same SKU 1 day"),
        th.Property("unitsSoldSameSku7d", th.IntegerType, description="Units sold same SKU 7 days"),
        th.Property("unitsSoldSameSku14d", th.IntegerType, description="Units sold same SKU 14 days"),
        th.Property("unitsSoldSameSku30d", th.IntegerType, description="Units sold same SKU 30 days"),
        th.Property("kindleEditionNormalizedPagesRead14d", th.IntegerType, description="Kindle pages read 14 days"),
        th.Property("kindleEditionNormalizedPagesRoyalties14d", th.NumberType, description="Kindle royalties 14 days"),
        th.Property("salesOtherSku7d", th.NumberType, description="Sales other SKU 7 days"),
        th.Property("unitsSoldOtherSku7d", th.IntegerType, description="Units sold other SKU 7 days"),
        th.Property("acosClicks7d", th.NumberType, description="ACOS clicks 7 days"),
        th.Property("acosClicks14d", th.NumberType, description="ACOS clicks 14 days"),
        th.Property("roasClicks7d", th.NumberType, description="ROAS clicks 7 days"),
        th.Property("roasClicks14d", th.NumberType, description="ROAS clicks 14 days"),
    ).to_dict()

    def get_http_headers(self, context: t.Optional[t.Dict] = None) -> dict:
        """Return the http headers needed for Amazon Ads API."""
        headers = super().get_http_headers(context)
        headers["Amazon-Ads-API-Version"] = self.report_version
        headers["Accept"] = "application/vnd.createasyncreportrequest.v3+json"
        headers["Content-Type"] = "application/vnd.createasyncreportrequest.v3+json"
        return headers
    


    def prepare_request_payload(
        self, context: t.Optional[t.Dict], next_page_token: t.Any | None = None
    ) -> dict | None:
        """Construct request payload for Sponsored Products Search Term report."""
        end_date = datetime.utcnow().date()
        start_date = end_date - timedelta(days=self.lookback_days)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        # Simple report name matching official example pattern
        report_name = f"SP search term report {start_date_str}/{end_date_str}"

        # Payload structure matching official Amazon Ads API example exactly
        payload = {
            "name": report_name,
            "startDate": start_date_str,
            "endDate": end_date_str,
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": ["searchTerm"],
                "columns": self.report_metrics,
                "reportTypeId": self.report_type,
                "timeUnit": self.time_unit,
                "format": self.report_format
            }
        }

        self.logger.debug(
            f"Search Terms report payload: {json.dumps(payload, indent=2)}"
        )
        return payload

    def validate_response(self, response) -> None:
        """Validate the response from the API with detailed error logging."""
        try:
            response.raise_for_status()
            self.logger.debug("API request successful")
        except Exception as e:
            # Log detailed error information
            self.logger.error("=" * 80)
            self.logger.error("API REQUEST FAILED")
            self.logger.error("=" * 80)
            
            # Request details
            self.logger.error("\n=== REQUEST DETAILS ===")
            self.logger.error("URL: %s", response.request.url)
            self.logger.error("Method: %s", response.request.method)
            
            # Request headers
            self.logger.error("\n=== REQUEST HEADERS ===")
            for k, v in response.request.headers.items():
                self.logger.error("%s: %s", k, v)
            
            # Request body (if any)
            if hasattr(response.request, 'body') and response.request.body:
                self.logger.error("\n=== REQUEST BODY ===")
                try:
                    self.logger.error(json.dumps(json.loads(response.request.body), indent=2))
                except:
                    self.logger.error(response.request.body)
            
            # Response details
            self.logger.error("\n=== RESPONSE STATUS ===")
            self.logger.error("Status Code: %s %s", response.status_code, response.reason)
            
            # Response headers
            self.logger.error("\n=== RESPONSE HEADERS ===")
            for k, v in response.headers.items():
                self.logger.error("%s: %s", k, v)
            
            # Response body (if any)
            self.logger.error("\n=== RESPONSE BODY ===")
            try:
                self.logger.error(json.dumps(response.json(), indent=2))
            except:
                self.logger.error(response.text or "No response body")
            
            self.logger.error("=" * 80)
            self.logger.error("END OF ERROR DETAILS")
            self.logger.error("=" * 80)
            
            # Re-raise the exception
            raise

    def _extract_duplicate_report_id(self, message: str | None) -> str | None:
        """Extract duplicate report ID from error message."""
        if not message:
            return None
        match = self._duplicate_id_regex.search(message)
        return match.group(1) if match else None

    def parse_response(self, response) -> t.Iterable[dict]:
        """Parse the initial response and process the report."""
        try:
            response_data = response.json()
            report_id = response_data.get("reportId")
            
            if not report_id:
                self.logger.error("No reportId in response: %s", response_data)
                return
                
            self.logger.info("Report created with ID: %s", report_id)
            yield from self._process_report(report_id)
            
        except Exception as exc:
            # Check for duplicate report error
            error_msg = getattr(exc, 'response', {}).get('text', str(exc)) if hasattr(exc, 'response') else str(exc)
            duplicate_id = self._extract_duplicate_report_id(error_msg)
            
            if duplicate_id:
                self.logger.info("Using duplicate report ID: %s", duplicate_id)
                yield from self._process_report(duplicate_id)
            else:
                self.logger.error("Error parsing response", exc_info=exc)
                raise

    def _process_report(self, report_id: str) -> t.Iterable[dict]:
        """Process a report by polling for completion and downloading data."""
        status, download_url = self._poll_report_status(report_id)
        
        if status == "COMPLETED" and download_url:
            report_data = self._download_report(download_url)
            yield from self._parse_report_data(report_data)
        else:
            self.logger.error("Report processing failed with status: %s", status)

    def _poll_report_status(self, report_id: str, max_attempts: int = 200) -> tuple[str, str]:
        """Poll report status until completion."""
        status_endpoint = f"/reporting/reports/{report_id}"
        self.logger.info("Polling report status", extra={"report_id": report_id})

        for attempt in range(1, max_attempts + 1):
            try:
                if hasattr(self, "authenticator"):
                    try:
                        auth = self.authenticator
                        if getattr(auth, "expires_in", None) and getattr(auth, "last_refreshed", None):
                            if datetime.now(timezone.utc) >= auth.last_refreshed + timedelta(seconds=auth.expires_in - 300):
                                self.logger.info("Refreshing access token before expiry")
                                auth.update_access_token()
                    except Exception as token_exc:
                        self.logger.warning("Access-token refresh pre-check failed", exc_info=token_exc)

                headers = self.get_http_headers()
                headers["Accept"] = "application/vnd.getasyncreportresponse.v3+json"
                headers["Content-Type"] = "application/vnd.getasyncreportresponse.v3+json"
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
        """Download and decompress the report data."""
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
        # Obtain the current bookmark (may be None on first run)
        bookmark_value: str | None = self.get_starting_replication_key_value(None)
        bookmark_date: datetime.date | None = None
        if bookmark_value:
            try:
                bookmark_date = datetime.strptime(bookmark_value, "%Y-%m-%d").date()
            except Exception:
                self.logger.debug("Unable to parse bookmark value '%s'", bookmark_value)

        for record in report_data.get("reports", []):
            # Skip rows that are not newer than the bookmark
            record_date_str = record.get("date")
            if bookmark_date and record_date_str:
                try:
                    record_date = datetime.strptime(record_date_str, "%Y-%m-%d").date()
                    if record_date <= bookmark_date:
                        continue  # Duplicate or already processed
                except Exception:
                    # If the date cannot be parsed, fall through and process the row
                    pass
                    
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


    def get_url_params(
        self,
        context: t.Optional[dict],  # noqa: ARG002
        next_page_token: t.Any | None,
    ) -> dict:
        """Return URL parameters (only nextToken if provided)."""
        params: dict = {}
        if next_page_token:
            params["nextToken"] = next_page_token
        return params


__all__ = ["SearchTermsReportStream"]
