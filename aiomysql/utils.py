import struct
from types import TracebackType
from typing import (
    Coroutine,
    TypeVar,
    Any,
    Optional,
    Generic, Callable, Awaitable, Type, AsyncGenerator, Generator
)

_Tobj = TypeVar("_Tobj")
_Release = Callable[[_Tobj], Awaitable[None]]


class _ContextManager(Coroutine[Any, None, _Tobj], Generic[_Tobj]):
    __slots__ = ('_coro', '_obj', '_release', '_release_on_exception')

    def __init__(
            self,
            coro: Coroutine[Any, None, _Tobj],
            release: _Release[_Tobj],
            release_on_exception: Optional[_Release[_Tobj]] = None
    ):
        self._coro = coro
        self._obj: Optional[_Tobj] = None
        self._release = release
        self._release_on_exception = (
            release
            if release_on_exception is None
            else release_on_exception
        )

    def send(self, value: Any) -> 'Any':
        return self._coro.send(value)

    def throw(
            self,
            typ: Type[BaseException],
            val: Optional[BaseException] = None,
            tb: Optional[TracebackType] = None
    ) -> Any:
        if val is None:
            return self._coro.throw(typ)
        elif tb is None:
            return self._coro.throw(typ, val)
        else:
            return self._coro.throw(typ, val, tb)

    def close(self) -> None:
        return self._coro.close()

    async def __anext__(self) -> _Tobj:
        try:
            value = self._coro.send(None)
        except StopAsyncIteration:
            self._obj = None
            raise
        else:
            return value

    def __aiter__(self) -> AsyncGenerator[None, _Tobj]:
        return self._obj

    def __await__(self) -> Generator[Any, None, _Tobj]:
        return self._coro.__await__()

    async def __aenter__(self) -> _Tobj:
        self._obj = await self._coro
        assert self._obj
        return self._obj

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc: Optional[BaseException],
            tb: Optional[TracebackType],
    ) -> None:
        try:
            if exc_type is not None and exc is not None and tb is not None:
                await self._release_on_exception(self._obj)
            else:
                await self._release(self._obj)
        finally:
            await self._obj.close()
            self._obj = None


class _IterableContextManager(_ContextManager[_Tobj]):
    __slots__ = ()

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

    def __aiter__(self) -> '_IterableContextManager[_Tobj]':
        return self

    async def __anext__(self) -> _Tobj:
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


def _pack_int24(n: int) -> bytes:
    return struct.pack("<I", n)[:3]


def _lenenc_int(i: int) -> bytes:
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
