import asyncio
from unittest.mock import AsyncMock

import pytest

from movici_api_client.cli.handlers.common import gather_safe


@pytest.mark.asyncio
async def test_gather_safe_awaits_coroutines():
    mocks = [AsyncMock(), AsyncMock()]
    await gather_safe(*(m() for m in mocks))
    assert all(m.await_count == 1 for m in mocks)


@pytest.mark.asyncio
async def test_gather_safe_cancels_coroutines_on_error():
    async def raise_error():
        raise ValueError()

    cancelled = False

    async def get_cancelled():
        nonlocal cancelled
        try:
            await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            cancelled = True
            raise

    coros = [raise_error(), get_cancelled()]
    with pytest.raises(ValueError):
        await gather_safe(*coros)
    assert cancelled
