# File: profile_service/schemas.py
"""Pydantic v2 models for the user risk-profile service."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class RiskTier(str, Enum):
    """Coarse risk classification maintained by the offline scoring batch."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class UserProfile(BaseModel):
    """User Risk Profile — the unit cached in Redis and served by the API."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "user_id": "u_0007",
                "risk_tier": "medium",
                "lifetime_amount": 18342.55,
                "avg_txn_amount": 62.10,
                "country": "US",
                "updated_at": "2026-08-20T09:15:00Z",
            }
        }
    )

    user_id: str = Field(
        pattern=r"^[A-Za-z0-9_-]{3,64}$",
        description="Stable external user identifier.",
        examples=["u_0007"],
    )
    risk_tier: RiskTier = Field(default=RiskTier.LOW)
    lifetime_amount: float = Field(default=0.0, ge=0.0)
    avg_txn_amount: float = Field(default=0.0, ge=0.0)
    country: str = Field(min_length=2, max_length=2, description="ISO-3166 alpha-2 home country")
    updated_at: datetime = Field(default_factory=_utc_now)

    def normalized(self) -> "UserProfile":
        """Return a copy with country upper-cased (canonical cache form)."""
        data = self.model_dump()
        data["country"] = self.country.upper()
        return UserProfile.model_validate(data)


class HealthResponse(BaseModel):
    """Liveness/readiness payload for GET /health."""

    status: str = Field(description="'healthy', 'degraded' or 'unavailable'")
    redis_status: str = Field(description="'up' or 'down'")
    fallback_available: bool = Field(description="True when the in-memory fallback can serve traffic")
    version: str
    uptime_seconds: float = Field(ge=0.0)
