from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pipit.dsl2._trace import _Trace
from abc import ABC, abstractmethod


class LocMixin(ABC):
    """
    Mixin class to simplify implementation of the `loc` attribute.

    When `obj.loc[...]` is used, the object's `_locate` method is called with
    the same arguments.
    """

    class _Loc:
        def __init__(self, obj: any) -> None:
            self.obj = obj

        def __getitem__(self, key: any) -> any:
            return self.obj._locate(key)

    @property
    def loc(self) -> _Loc:
        if not hasattr(self, "_loc"):
            self._loc = self._Loc(self)
        return self._loc

    @abstractmethod
    def _locate(self, key: any) -> any:
        pass


def create_trace(backend=None, *args, **kwargs) -> _Trace:
    """
    Creates a new _Trace object using the specified backend,
    or the globally configured backend if none is specified.
    """
    from pipit.util.config import get_option

    backend = backend or get_option("backend")

    if backend == "pandas":
        from pipit.dsl2._pandas import _PandasTrace

        return _PandasTrace(*args, **kwargs)
