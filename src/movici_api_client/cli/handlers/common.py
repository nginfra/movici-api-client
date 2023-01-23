import asyncio


async def gather_safe(*coros, return_exceptions=False):
    """Like ``asyncio.gather`` but it ensures that in case an exception occurs in some of the
    coroutines, all coroutines that have not completed yet are canceled and awaited.
    """
    if return_exceptions:
        return await asyncio.gather(*coros, return_exceptions=True)

    tasks = [asyncio.create_task(c) for c in coros]

    try:
        return [await t for t in tasks]
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
