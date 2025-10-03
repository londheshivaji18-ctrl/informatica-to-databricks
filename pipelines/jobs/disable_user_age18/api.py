from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from typing import Any, Mapping, Protocol
from urllib.parse import quote

try:  # requests is available on Databricks; fallback keeps tests lightweight
    import requests
except Exception:  # pragma: no cover - requests should exist in production runtime
    requests = None  # type: ignore

log = logging.getLogger(__name__)


class HttpClient(Protocol):
    def delete(self, url: str, headers: Mapping[str, str], timeout: float | None = None) -> Any:  # noqa: D401
        ...


@dataclass
class DisableApiResponse:
    user_id: str
    email: str
    request_id: str
    status_code: int
    item_output: str
    system_output: str


class DisableApiClient:
    """Best-effort HTTP DELETE client used to disable accounts."""

    def __init__(
        self,
        base_url_prefix: str,
        base_url_suffix: str,
        http_client: HttpClient | None = None,
        *,
        timeout_seconds: float = 10.0,
    ) -> None:
        self.base_url_prefix = base_url_prefix.rstrip("/")
        self.base_url_suffix = base_url_suffix
        if http_client is not None:
            self.http_client = http_client
        else:
            if requests is None:  # pragma: no cover - exercised only in constrained envs
                raise RuntimeError("requests library is required for DisableApiClient")
            self.http_client = requests.Session()
        self.timeout_seconds = timeout_seconds

    def build_url(self, email: str) -> str:
        encoded_email = quote(email or "", safe="")
        return f"{self.base_url_prefix}/{encoded_email}{self.base_url_suffix}"

    def invoke(self, payload: Mapping[str, Any]) -> DisableApiResponse:
        email = str(payload.get("email", ""))
        request_id = str(payload.get("request_id", ""))
        headers = {
            key: str(value)
            for key, value in payload.items()
            if key
            in {
                "accept",
                "x_ibm_client_id",
                "x_ibm_client_secret",
                "x_request_id",
                "authorization",
                "x_customer_id",
                "x_aai_ope_id",
                "x_token_owner_type",
            }
        }
        url = self.build_url(email)
        hashed_email = hashlib.sha256(email.encode("utf-8")).hexdigest() if email else ""
        log.debug(
            "disable_api_request_prepared",
            extra={
                "event": "disable_api_request_prepared",
                "url": url,
                "email_hash": hashed_email,
                "request_id": request_id,
            },
        )

        try:
            response = self.http_client.delete(url, headers=headers, timeout=self.timeout_seconds)
            status_code = int(getattr(response, "status_code", 0))
            body_text = getattr(response, "text", "")
            try:
                body_json = response.json()
            except Exception:  # tolerate non-JSON payloads
                body_json = None

            system_output: str
            item_output: str
            if isinstance(body_json, Mapping):
                system_output = str(body_json.get("systemMessage") or body_json.get("message") or body_json)
                item_output = str(body_json.get("itemMessage") or body_json.get("detail") or body_json)
            else:
                system_output = body_text
                item_output = body_text

            log.info(
                "disable_api_called",
                extra={
                    "event": "disable_api_called",
                    "status_code": status_code,
                    "email_hash": hashed_email,
                    "request_id": request_id,
                },
            )
        except Exception as exc:  # pragma: no cover - error path validated via unit tests
            status_code = -1
            system_output = f"error: {type(exc).__name__}"
            item_output = str(exc)
            log.error(
                "disable_api_failed",
                extra={
                    "event": "disable_api_failed",
                    "email_hash": hashed_email,
                    "request_id": request_id,
                    "error": type(exc).__name__,
                },
            )

        return DisableApiResponse(
            user_id=str(payload.get("user_id", "")),
            email=email,
            request_id=request_id,
            status_code=status_code,
            item_output=item_output,
            system_output=system_output,
        )


__all__ = ["DisableApiClient", "DisableApiResponse", "HttpClient"]
