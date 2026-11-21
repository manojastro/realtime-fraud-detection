# File: profile_service/main.py
"""FastAPI service exposing user risk profiles with cache-aside reads.

Endpoints
---------
* ``GET    /profiles/{user_id}``  cache-aside read (Redis TTL 300s, stampede
  lock, fallback to the authoritative store on miss/outage)
* ``PUT    /profiles/{user_id}``  invalidate + update (write-through both layers)
* ``DELETE /profiles/{user_id}``  evict from cache and authoritative store
* ``GET    /health``              liveness + Redis/degradation status

Run locally::

    uvicorn profile_service.main:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator

import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response, status
from pydantic import ValidationError

try:  # package-style
    from profile_service.schemas import HealthResponse, UserProfile
    from profile_service.store import (
        DEFAULT_TTL_SECONDS,
        InMemoryProfileStore,
        ResilientProfileStore,
        RedisProfileStore,
        StoreUnavailableError,
    )
except ImportError:  # script-style
    from schemas import HealthResponse, UserProfile  # type: ignore[no-redef]
    from store import (  # type: ignore[no-redef]
        DEFAULT_TTL_SECONDS,
        InMemoryProfileStore,
        ResilientProfileStore,
        RedisProfileStore,
        StoreUnavailableError,
    )

LOGGER = logging.getLogger("profile_service")

SERVICE_VERSION = "1.0.0"
_request_id_ctx: ContextVar[str] = ContextVar("request_id", default="-")


class RequestIdLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = _request_id_ctx.get()
        return True


def configure_logging(level: str) -> None:
    class JsonFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            payload = {
                "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                "level": record.levelname,
                "logger": record.name,
                "msg": record.getMessage(),
                "request_id": getattr(record, "request_id", "-"),
            }
            if record.exc_info:
                payload["exc"] = self.formatException(record.exc_info)
            return json.dumps(payload, separators=(",", ":"))

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    handler.addFilter(RequestIdLogFilter())
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO),
                        handlers=[handler], force=True)


@dataclass(frozen=True)
class Settings:
    redis_url: str = "redis://localhost:6379/0"
    profile_ttl_seconds: int = DEFAULT_TTL_SECONDS
    seed_path: str = "data/risk_profiles.json"
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            profile_ttl_seconds=int(os.getenv("PROFILE_TTL_SECONDS", str(DEFAULT_TTL_SECONDS))),
            seed_path=os.getenv("PROFILES_SEED_PATH", "data/risk_profiles.json"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
        )


async def _seed_from_file(store: ResilientProfileStore, path: str) -> int:
    """Best-effort warm-up of the authoritative store from the sample dataset."""
    resolved = Path(path)
    if not resolved.exists():
        LOGGER.warning("seed file not found; starting empty", extra={"extra_fields": {"path": path}})
        return 0
    raw_items = json.loads(resolved.read_text(encoding="utf-8"))
    profiles: list[UserProfile] = []
    for item in raw_items:
        try:
            profiles.append(UserProfile.model_validate(item))
        except ValidationError as exc:
            LOGGER.warning("skipping invalid seed record: %s", exc.errors()[:2])
    count = await store.seed(profiles)
    LOGGER.info("seeded profile store", extra={"extra_fields": {"path": path, "rows": count}})
    return count


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = Settings.from_env()
    configure_logging(settings.log_level)

    fallback = InMemoryProfileStore(ttl_seconds=None)  # Postgres-mock (persistent)
    primary: RedisProfileStore | None = None
    try:
        candidate = RedisProfileStore(settings.redis_url, ttl_seconds=settings.profile_ttl_seconds)
        if await candidate.ping():
            primary = candidate
            LOGGER.info("redis connected", extra={"extra_fields": {"url": settings.redis_url}})
        else:
            await candidate.aclose()
    except Exception as exc:  # noqa: BLE001 - any startup failure degrades gracefully
        LOGGER.warning("redis unreachable at startup (%s) — using in-memory fallback only", exc)

    store = ResilientProfileStore(primary=primary, fallback=fallback)

    async def load_from_authoritative(user_id: str) -> UserProfile | None:
        # In production this would query Postgres; swap the InMemoryProfileStore
        # for a PostgresProfileStore implementing the same ProfileStore protocol.
        return await fallback.get(user_id)

    app.state.settings = settings
    app.state.store = store
    app.state.fallback = fallback
    app.state.loader = load_from_authoritative
    app.state.started_at = time.monotonic()

    try:
        await _seed_from_file(store, settings.seed_path)
        yield
    finally:
        await store.aclose()
        LOGGER.info("profile service shut down cleanly")


app = FastAPI(
    title="Fraud Detection — User Risk Profile Service",
    version=SERVICE_VERSION,
    description="Cache-aside user risk profiles backed by Redis with graceful degradation.",
    lifespan=lifespan,
)


@app.middleware("http")
async def request_context_middleware(request: Request, call_next):
    """Attach a request ID to every request/response and emit an access log."""
    request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
    token = _request_id_ctx.set(request_id)
    started = time.perf_counter()
    response_status = 500
    try:
        response = await call_next(request)
        response_status = response.status_code
        response.headers["X-Request-ID"] = request_id
        return response
    finally:
        duration_ms = round((time.perf_counter() - started) * 1000, 2)
        LOGGER.info("access", extra={"extra_fields": {
            "method": request.method,
            "path": request.url.path,
            "status": response_status,
            "duration_ms": duration_ms,
        }})
        _request_id_ctx.reset(token)


@app.exception_handler(StoreUnavailableError)
async def store_unavailable_handler(_: Request, exc: StoreUnavailableError):
    LOGGER.error("store unavailable: %s", exc)
    return Response(
        content='{"detail":"profile store temporarily unavailable"}',
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        media_type="application/json",
    )


# --- endpoints -----------------------------------------------------------------


@app.get("/health", response_model=HealthResponse)
async def health(response: Response) -> HealthResponse:
    store: ResilientProfileStore = app.state.store
    redis_up = await store.ping()

    if redis_up:
        health_status, code = "healthy", status.HTTP_200_OK
    elif app.state.fallback is not None:
        health_status, code = "degraded", status.HTTP_200_OK  # serving via fallback
    else:
        health_status, code = "unavailable", status.HTTP_503_SERVICE_UNAVAILABLE

    if code != status.HTTP_200_OK:
        response.status_code = code

    return HealthResponse(
        status=health_status,
        redis_status="up" if redis_up else "down",
        fallback_available=True,
        version=SERVICE_VERSION,
        uptime_seconds=round(time.monotonic() - app.state.started_at, 3),
    )


@app.get("/profiles/{user_id}", response_model=UserProfile)
async def get_profile(user_id: str, response: Response) -> UserProfile:
    store: ResilientProfileStore = app.state.store
    profile, cache_hit = await store.get_or_load(user_id, app.state.loader)
    if profile is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"profile '{user_id}' not found")
    response.headers["X-Cache"] = "HIT" if cache_hit else "MISS"
    return profile


@app.put("/profiles/{user_id}", response_model=UserProfile)
async def upsert_profile(user_id: str, profile: UserProfile) -> UserProfile:
    if profile.user_id != user_id:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="path user_id does not match body user_id")
    updated = profile.normalized()
    updated.updated_at = datetime.now(timezone.utc)
    # Write-through invalidates+refreshes the cache in one step.
    await app.state.store.put(updated)
    LOGGER.info("profile upserted", extra={"extra_fields": {"user_id": user_id}})
    return updated


@app.delete("/profiles/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def evict_profile(user_id: str) -> Response:
    await app.state.store.delete(user_id)
    LOGGER.info("profile evicted", extra={"extra_fields": {"user_id": user_id}})
    return Response(status_code=status.HTTP_204_NO_CONTENT)


if __name__ == "__main__":  # pragma: no cover
    uvicorn.run(
        "profile_service.main:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PROFILE_SERVICE_PORT", "8000")),
        log_config=None,  # structured logging configured above
    )
