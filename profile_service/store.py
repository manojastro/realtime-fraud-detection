# File: profile_service/store.py
"""Profile store abstractions.

Layering
--------
* ``ProfileStore``        – async protocol both backends implement.
* ``RedisProfileStore``   – TTL cache in front of the authoritative source.
                            Implements cache-aside ``get_or_load`` with an
                            atomic ``SET NX EX`` stampede lock (only one
                            request repopulates a cold key; losers spin on the
                            cache briefly, then fall through to the loader).
* ``InMemoryProfileStore``– asyncio-safe dict with optional TTL. Doubles as
                            the Postgres-mock (authoritative store) and as the
                            full fallback when Redis is unreachable.
* ``ResilientProfileStore`` - composition root used by the app: prefers Redis,
                            degrades transparently to the fallback on any
                            Redis error, and mirrors writes so the fallback
                            stays warm.

All Redis failures surface as ``StoreUnavailableError`` from the pure Redis
store; resilience policy lives exclusively in ``ResilientProfileStore``.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from contextlib import suppress
from typing import Awaitable, Callable, Protocol, runtime_checkable

from redis.asyncio import ConnectionPool, Redis
from redis.exceptions import RedisError

try:  # package-style
    from profile_service.schemas import UserProfile
except ImportError:  # script-style
    from schemas import UserProfile  # type: ignore[no-redef]

LOGGER = logging.getLogger("profile_service.store")

DEFAULT_TTL_SECONDS = 300  # 5-minute cache freshness window
LOCK_TTL_SECONDS = 10  # max time a single loader may hold the stampede lock
LOCK_POLL_INTERVAL_S = 0.05
LOCK_POLL_ATTEMPTS = 20  # ~1s of spinning for cache-hit-after-lock-lost

KEY_NAMESPACE = "profile:v1"
LOCK_NAMESPACE = "lock:profile:v1"

# Atomic compare-and-delete so we never release someone else's lock.
_RELEASE_LOCK_LUA = (
    "if redis.call('get', KEYS[1]) == ARGV[1] then "
    "return redis.call('del', KEYS[1]) else return 0 end"
)


class StoreUnavailableError(RuntimeError):
    """Raised when Redis cannot be reached; callers should degrade gracefully."""


LoaderFn = Callable[[str], Awaitable[UserProfile | None]]
CacheResult = tuple[UserProfile | None, bool]  # (profile, cache_hit)


def _cache_key(user_id: str) -> str:
    return f"{KEY_NAMESPACE}:{user_id}"


def _lock_key(user_id: str) -> str:
    return f"{LOCK_NAMESPACE}:{user_id}"


@runtime_checkable
class ProfileStore(Protocol):
    """Async storage contract shared by every backend."""

    async def get(self, user_id: str) -> UserProfile | None: ...

    async def put(self, profile: UserProfile) -> None: ...

    async def delete(self, user_id: str) -> None: ...

    async def ping(self) -> bool: ...

    async def aclose(self) -> None: ...

    async def seed(self, profiles: list[UserProfile]) -> int:
        """Bulk-load initial data; used by startup seeding."""
        ...


class InMemoryProfileStore:
    """Async dict-backed store with optional TTL expiry.

    * ``ttl_seconds=None`` -> persistent semantics (Postgres-mock).
    * ``ttl_seconds=N``    -> cache-like behaviour (dev fallback without Redis).
    """

    def __init__(self, ttl_seconds: int | None = None) -> None:
        self._ttl_seconds = ttl_seconds
        self._data: dict[str, tuple[UserProfile, float | None]] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _expired(expires_at: float | None) -> bool:
        return expires_at is not None and time.monotonic() >= expires_at

    async def get(self, user_id: str) -> UserProfile | None:
        async with self._lock:
            entry = self._data.get(user_id)
            if entry is None:
                return None
            profile, expires_at = entry
            if self._expired(expires_at):
                del self._data[user_id]
                return None
            return profile

    async def put(self, profile: UserProfile) -> None:
        canonical = profile.normalized()
        expires_at = time.monotonic() + self._ttl_seconds if self._ttl_seconds else None
        async with self._lock:
            self._data[canonical.user_id] = (canonical, expires_at)

    async def delete(self, user_id: str) -> None:
        async with self._lock:
            self._data.pop(user_id, None)

    async def ping(self) -> bool:
        return True

    async def aclose(self) -> None:
        return None

    async def seed(self, profiles: list[UserProfile]) -> int:
        for profile in profiles:
            await self.put(profile)
        return len(profiles)

    async def get_or_load(self, user_id: str, loader: LoaderFn) -> CacheResult:
        # Single-process store: no stampede risk, load straight through.
        profile = await self.get(user_id)
        if profile is not None:
            return profile, True
        loaded = await loader(user_id)
        if loaded is not None:
            await self.put(loaded)
        return loaded, False


class RedisProfileStore:
    """Redis-backed TTL cache implementing cache-aside with stampede guard."""

    def __init__(
        self,
        url: str,
        ttl_seconds: int = DEFAULT_TTL_SECONDS,
        max_connections: int = 20,
    ) -> None:
        self.ttl_seconds = ttl_seconds
        self._pool = ConnectionPool.from_url(
            url,
            max_connections=max_connections,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
            health_check_interval=30,
        )
        self._client = Redis(connection_pool=self._pool)

    # -- raw ops ---------------------------------------------------------------

    async def get(self, user_id: str) -> UserProfile | None:
        try:
            raw = await self._client.get(_cache_key(user_id))
        except RedisError as exc:
            raise StoreUnavailableError(f"redis GET failed: {exc}") from exc
        if raw is None:
            return None
        return UserProfile.model_validate_json(raw)

    async def put(self, profile: UserProfile) -> None:
        canonical = profile.normalized()
        try:
            await self._client.set(
                _cache_key(canonical.user_id),
                canonical.model_dump_json(),
                ex=self.ttl_seconds,
            )
        except RedisError as exc:
            raise StoreUnavailableError(f"redis SET failed: {exc}") from exc

    async def delete(self, user_id: str) -> None:
        try:
            await self._client.delete(_cache_key(user_id), _lock_key(user_id))
        except RedisError as exc:
            raise StoreUnavailableError(f"redis DEL failed: {exc}") from exc

    async def ping(self) -> bool:
        try:
            return bool(await self._client.ping())
        except RedisError:
            return False

    async def aclose(self) -> None:
        await self._client.aclose()
        await self._pool.disconnect(inuse_connections=True)

    async def seed(self, profiles: list[UserProfile]) -> int:
        for profile in profiles:
            await self.put(profile)
        return len(profiles)

    # -- cache-aside orchestration ----------------------------------------------

    async def get_or_load(self, user_id: str, loader: LoaderFn) -> CacheResult:
        """Cache-aside read protected by an atomic SETNX stampede lock."""
        cached = await self.get(user_id)
        if cached is not None:
            return cached, True

        token = uuid.uuid4().hex
        try:
            acquired = await self._client.set(_lock_key(user_id), token, nx=True, ex=LOCK_TTL_SECONDS)
        except RedisError as exc:
            raise StoreUnavailableError(f"redis SETNX failed: {exc}") from exc

        if not acquired:
            # Someone else is loading; spin briefly hoping they populate the cache.
            for _ in range(LOCK_POLL_ATTEMPTS):
                await asyncio.sleep(LOCK_POLL_INTERVAL_S)
                cached = await self.get(user_id)
                if cached is not None:
                    return cached, True
            return None, False  # caller may retry or serve a miss

        try:
            profile = await loader(user_id)
            if profile is not None:
                await self.put(profile)
            return profile, False
        finally:
            # Release only if we still own the lock (atomic Lua CAS).
            with suppress(StoreUnavailableError):
                await self._client.eval(_RELEASE_LOCK_LUA, 1, _lock_key(user_id), token)


class ResilientProfileStore:
    """Prefers Redis; transparently degrades to the fallback when Redis is down.

    Writes are best-effort to both layers so the fallback stays warm and can
    serve reads during a Redis outage. ``degraded`` flips back to False once a
    ``ping()`` succeeds again — surfaced via GET /health.
    """

    def __init__(self, primary: RedisProfileStore | None, fallback: InMemoryProfileStore) -> None:
        self._primary = primary
        self._fallback = fallback
        self.degraded = primary is None

    async def get_or_load(self, user_id: str, loader: LoaderFn) -> CacheResult:
        if self._primary is not None and not self.degraded:
            try:
                result = await self._primary.get_or_load(user_id, loader)
                profile, hit = result
                if profile is not None and not hit:
                    # Mirror-fill the fallback so it can absorb outages.
                    with suppress(Exception):  # noqa: S110 - best effort by design
                        await self._fallback.put(profile)
                return result
            except StoreUnavailableError:
                self.degraded = True
                LOGGER.warning("redis unavailable — degrading to in-memory fallback")
        return await self._fallback.get_or_load(user_id, loader)

    async def put(self, profile: UserProfile) -> None:
        # Fallback write is authoritative (Postgres-mock); cache write is best-effort.
        await self._fallback.put(profile)
        if self._primary is not None:
            try:
                await self._primary.put(profile)
            except StoreUnavailableError:
                self.degraded = True
                LOGGER.warning("redis PUT failed during write-through; serving degraded")

    async def delete(self, user_id: str) -> None:
        await self._fallback.delete(user_id)
        if self._primary is not None:
            try:
                await self._primary.delete(user_id)
            except StoreUnavailableError:
                self.degraded = True
                LOGGER.warning("redis DEL failed during eviction; serving degraded")

    async def ping(self) -> bool:
        if self._primary is None:
            return False
        up = await self._primary.ping()
        if up and self.degraded:
            LOGGER.info("redis recovered — leaving degraded mode")
            self.degraded = False
        return up

    async def aclose(self) -> None:
        if self._primary is not None:
            await self._primary.aclose()

    async def seed(self, profiles: list[UserProfile]) -> int:
        count = await self._fallback.seed(profiles)
        if self._primary is not None:
            try:
                await self._primary.seed(profiles)
            except StoreUnavailableError:
                LOGGER.warning("skipped redis warm-up during seeding (unavailable)")
        return count


__all__ = [
    "DEFAULT_TTL_SECONDS",
    "CacheResult",
    "InMemoryProfileStore",
    "ProfileStore",
    "ResilientProfileStore",
    "RedisProfileStore",
    "StoreUnavailableError",
]
