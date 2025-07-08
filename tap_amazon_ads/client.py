"""REST client handling, including TapAmazonAdsStream base class."""

from __future__ import annotations

import decimal
import typing as t
from copy import copy
from functools import cached_property
from importlib import resources

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TC002
from singer_sdk.streams import RESTStream

from tap_amazon_ads.auth import TapAmazonAdsAuthenticator

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Auth, Context

import requests


# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = resources.files(__package__) / "schemas"


class TapAmazonAdsStream(RESTStream):
    """Amazon Ads API stream class."""

    # Update this value if necessary or override `parse_response`.
    records_jsonpath = "$[*]"

    # Update this value if necessary or override `get_new_paginator`.
    next_page_token_jsonpath = "$.nextToken"  # noqa: S105

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config.get("api_url", "https://advertising-api.amazon.com")

    @cached_property
    def authenticator(self) -> Auth:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return TapAmazonAdsAuthenticator.create_for_stream(self)

    def get_http_headers(self, context: Context | None = None) -> dict:
        """Return the http headers needed for Amazon Ads API.

        Args:
            context: Stream context that may contain profile_id.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {
            "Amazon-Advertising-API-ClientId": self.config["client_id"],
            "Accept": "application/json",
        }
        
        # Add profile scope header - required for most Amazon Ads API endpoints
        profile_id = None
        if context and "profile_id" in context:
            profile_id = context["profile_id"]
        else:
            profile_id = getattr(self, 'current_profile_id', None)
            
        if profile_id:
            headers["Amazon-Advertising-API-Scope"] = str(profile_id)
            
        return headers

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed for Amazon Ads API.

        Returns:
            A dictionary of HTTP headers.
        """
        return self.get_http_headers()

    def get_new_paginator(self) -> BaseAPIPaginator | None:
        """Create a new pagination helper instance.

        Amazon Ads API uses nextToken-based pagination.

        Returns:
            A pagination helper instance, or ``None`` to indicate pagination
            is not supported.
        """
        return super().get_new_paginator()

    def get_url_params(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            params["nextToken"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def prepare_request_payload(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: t.Any | None,  # noqa: ARG002, ANN401
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        For Amazon Ads API, many endpoints use POST requests with JSON payloads
        even for listing operations.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary with the JSON body for a POST requests.
        """
        # Most Amazon Ads API list endpoints use POST with empty body or filters
        return {}

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        # This method is only called on successful responses (200-level)
        # Error responses are handled by validate_response before we get here
        yield from extract_jsonpath(
            self.records_jsonpath,
            input=response.json(parse_float=decimal.Decimal),
        )

    def post_process(
        self,
        row: dict,
        context: Context | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        return row

    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        Amazon Ads API requires profile_id scope for most operations.
        This method iterates through all configured profile_ids.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            Each record from the source.
        """
        profile_ids = self.config.get("profile_ids", [])
        
        for profile_id in profile_ids:
            # Create context with profile_id
            profile_context = dict(context) if context else {}
            profile_context["profile_id"] = profile_id
            
            # Set current profile for fallback
            self.current_profile_id = profile_id
            self.logger.info(f"Fetching data for profile: {profile_id}")
            
            # Call parent get_records for this profile with updated context
            yield from super().get_records(profile_context)

    def prepare_request(
        self,
        context: Context | None,
        next_page_token: t.Any | None = None,
    ) -> requests.PreparedRequest:
        """Prepare a request object for Amazon Ads API with proper headers.
        
        Args:
            context: Stream context containing profile_id.
            next_page_token: Pagination token.
            
        Returns:
            A prepared request object.
        """
        http_method = getattr(self, 'rest_method', 'GET')  # Default to GET
        url: str = self.get_url(context)
        params: dict = self.get_url_params(context, next_page_token)
        request_data = self.prepare_request_payload(context, next_page_token)
        headers = self.get_http_headers(context)  # Use context-aware headers

        request = requests.Request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data,
        )
        prepared_request = self._requests_session.prepare_request(request)
        
        # Apply authentication to the prepared request
        authenticator = self.authenticator
        if authenticator:
            authenticator(prepared_request)
            
            # Remove Content-Type and Content-Length headers for GET requests
            # Amazon Ads API doesn't like these on GET requests
            if http_method == 'GET':
                prepared_request.headers.pop('Content-Type', None)
                prepared_request.headers.pop('Content-Length', None)
        else:
            self.logger.warning("No authenticator found!")
            
        return prepared_request

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response.
        
        Args:
            response: The HTTP response object.
            
        Raises:
            FatalAPIError: If the response indicates an error.
        """
        # Call the parent validation logic
        super().validate_response(response)
