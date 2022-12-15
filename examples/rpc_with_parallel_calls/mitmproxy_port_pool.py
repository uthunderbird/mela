import asyncio


class Port:

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    def __repr__(self):
        return f"<Port offset={self.offset}>"

    def __init__(self, offset: int, pool: 'Pool'):
        self.offset = offset
        self._pool = pool

    def acquire(self):
        self._pool.notify_acquired()

    def release(self):
        self._pool.notify_released(self)


class Pool:

    def __init__(self, size=1):
        self._ports: asyncio.Queue[Port] = asyncio.Queue(maxsize=size)
        for port in [Port(x, self) for x in range(size)]:
            self.notify_released(port)

    async def acquire(self) -> Port:
        """
        Get a free port from pool
        """
        return await self._ports.get()

    def notify_acquired(self):
        self._ports.task_done()

    def notify_released(self, port: Port):
        self._ports.put_nowait(port)

    def count_free(self) -> int:
        return self._ports.qsize()
