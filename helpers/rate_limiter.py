from __future__ import annotations

import asyncio


class RateLimiter:
    """
    Enforces a minimum spacing (seconds) between acquire() calls.
    Suitable for global LLM request throttling.
    """

    def __init__(self, *, min_spacing_s: float):
        self._min_spacing_s = float(min_spacing_s)
        self._lock = asyncio.Lock()
        self._next_allowed_time = 0.0

    async def acquire(self) -> float:
        """
        Wait until a request is allowed, then reserve the next slot.
        Returns the amount of time slept (seconds).
        """
        loop = asyncio.get_running_loop()
        async with self._lock:
            now = loop.time()
            sleep_for = max(0.0, self._next_allowed_time - now)
            reserved_start = now if now > self._next_allowed_time else self._next_allowed_time
            self._next_allowed_time = reserved_start + self._min_spacing_s
        if sleep_for > 0:
            await asyncio.sleep(sleep_for)
        return sleep_for

