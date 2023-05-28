import asyncio
import struct
import sys
from types import TracebackType
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Generator,
    Generic,
    Optional,
    Type,
    TypeVar,
    Union,
)

if sys.version_info >= (3, 7, 0):
    __get_running_loop = asyncio.get_running_loop
else:
    def __get_running_loop() -> asyncio.AbstractEventLoop:
        loop = asyncio.get_event_loop()
        if not loop.is_running():
            raise RuntimeError('no running event loop')
        return loop


def get_running_loop() -> asyncio.AbstractEventLoop:
    return __get_running_loop()


def create_completed_future(
        loop: asyncio.AbstractEventLoop
) -> 'asyncio.Future[Any]':
    future = loop.create_future()
    future.set_result(None)
    return future


_TObj = TypeVar("_TObj")
_Release = Callable[[_TObj], Awaitable[None]]


class _ContextManager(Coroutine[Any, None, _TObj], Generic[_TObj]):
    __slots__ = ('_coro', '_obj', '_release', '_release_on_exception')

    def __init__(
            self,
            coro: Coroutine[Any, None, _TObj],
            release: _Release[_TObj],
            release_on_exception: Optional[_Release[_TObj]] = None
    ):
        self._coro = coro
        self._obj: Optional[_TObj] = None
        self._release = release
        self._release_on_exception = (
            release
            if release_on_exception is None
            else release_on_exception
        )

    def send(self, value: Any) -> 'Any':
        return self._coro.send(value)

    def throw(  # type: ignore
            self,
            typ: Type[BaseException],
            val: Optional[Union[BaseException, object]] = None,
            tb: Optional[TracebackType] = None
    ) -> Any:
        if val is None:
            return self._coro.throw(typ)
        if tb is None:
            return self._coro.throw(typ, val)
        return self._coro.throw(typ, val, tb)

    def close(self) -> None:
        self._coro.close()

    def __await__(self) -> Generator[Any, None, _TObj]:
        return self._coro.__await__()

    async def __aenter__(self) -> _TObj:
        self._obj = await self._coro
        assert self._obj
        return self._obj

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc: Optional[BaseException],
            tb: Optional[TracebackType],
    ) -> None:
        if self._obj is None:
            return

        try:
            if exc_type is not None:
                await self._release_on_exception(self._obj)
            else:
                await self._release(self._obj)
        finally:
            self._obj = None


class _IterableContextManager(_ContextManager[_TObj]):
    __slots__ = ()

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

    def __aiter__(self) -> '_IterableContextManager[_TObj]':
        return self

    async def __anext__(self) -> _TObj:
        if self._obj is None:
            self._obj = await self._coro

        try:
            return await self._obj.__anext__()  # type: ignore
        except StopAsyncIteration:
            try:
                await self._release(self._obj)
            finally:
                self._obj = None
            raise


def _pack_int24(n):
    return struct.pack("<I", n)[:3]


def _lenenc_int(i):
    if i < 0:
        raise ValueError(
            "Encoding %d is less than 0 - no representation in LengthEncodedInteger" % i
        )
    elif i < 0xFB:
        return bytes([i])
    elif i < (1 << 16):
        return b"\xfc" + struct.pack("<H", i)
    elif i < (1 << 24):
        return b"\xfd" + struct.pack("<I", i)[:3]
    elif i < (1 << 64):
        return b"\xfe" + struct.pack("<Q", i)
    else:
        raise ValueError(
            "Encoding %x is larger than %x - no representation in LengthEncodedInteger"
            % (i, (1 << 64))
        )
