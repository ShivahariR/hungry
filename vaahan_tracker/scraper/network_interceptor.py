"""
Network interceptor for discovering Vaahan dashboard's underlying XHR/API endpoints.

The Vaahan dashboard (vahan.parivahan.gov.in) is built on JSF/PrimeFaces,
which uses AJAX POST requests under the hood. This module intercepts those
requests to discover usable API endpoints that can be hit directly with
requests, avoiding the need for full browser automation.
"""

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

try:
    from playwright.async_api import Page, Request, Response
except ImportError:
    Page = Request = Response = None

logger = logging.getLogger(__name__)


@dataclass
class InterceptedEndpoint:
    url: str
    method: str
    headers: dict
    post_data: str | None
    response_status: int | None = None
    response_body: str | None = None
    content_type: str | None = None


@dataclass
class NetworkInterceptor:
    """Captures XHR/API requests made by the Vaahan dashboard."""

    captured_requests: list[InterceptedEndpoint] = field(default_factory=list)
    _interesting_patterns: list[str] = field(default_factory=lambda: [
        "vahan4dashboard",
        "reportview",
        "groupTable",
        "javax.faces",
        "getDataByFilter",
        "getChartData",
        "getDashboardData",
    ])

    def _is_interesting(self, url: str) -> bool:
        return any(pattern in url for pattern in self._interesting_patterns)

    async def on_request(self, request: Request) -> None:
        if request.resource_type in ("xhr", "fetch", "document") and self._is_interesting(request.url):
            endpoint = InterceptedEndpoint(
                url=request.url,
                method=request.method,
                headers=dict(request.headers),
                post_data=request.post_data,
            )
            self.captured_requests.append(endpoint)
            logger.debug(f"Captured {request.method} {request.url}")
            if request.post_data:
                logger.debug(f"  POST data: {request.post_data[:500]}")

    async def on_response(self, response: Response) -> None:
        if not self._is_interesting(response.url):
            return
        for endpoint in self.captured_requests:
            if endpoint.url == response.url and endpoint.response_status is None:
                endpoint.response_status = response.status
                endpoint.content_type = response.headers.get("content-type", "")
                try:
                    endpoint.response_body = await response.text()
                except Exception:
                    endpoint.response_body = None
                break

    async def attach(self, page: Page) -> None:
        page.on("request", self.on_request)
        page.on("response", self.on_response)
        logger.info("Network interceptor attached to page")

    def get_api_endpoints(self) -> list[InterceptedEndpoint]:
        """Return endpoints that returned successful JSON/XML responses."""
        return [
            ep for ep in self.captured_requests
            if ep.response_status and ep.response_status < 400
            and ep.content_type
            and any(t in ep.content_type for t in ["json", "xml", "javascript"])
        ]

    def get_form_post_endpoints(self) -> list[InterceptedEndpoint]:
        """Return JSF form POST endpoints (PrimeFaces AJAX)."""
        return [
            ep for ep in self.captured_requests
            if ep.method == "POST"
            and ep.post_data
            and "javax.faces" in (ep.post_data or "")
        ]

    def export_captured(self, output_path: Path) -> None:
        """Export captured endpoints to JSON for analysis."""
        data = []
        for ep in self.captured_requests:
            data.append({
                "url": ep.url,
                "method": ep.method,
                "post_data": ep.post_data[:2000] if ep.post_data else None,
                "response_status": ep.response_status,
                "content_type": ep.content_type,
                "response_preview": ep.response_body[:1000] if ep.response_body else None,
            })
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(data, indent=2))
        logger.info(f"Exported {len(data)} captured endpoints to {output_path}")
