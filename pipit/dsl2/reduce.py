from __future__ import annotations

from abc import ABC, abstractmethod
from functools import reduce
from typing_extensions import Literal
from tabulate import tabulate

reducer = Literal["sum", "prod", "max", "min", "mean"]


class Reducible(ABC):
    """
    Mixin class for objects that can be reduced to a single value.
    """

    @abstractmethod
    def reduce(self, func: reducer | callable, initial=None) -> any:
        pass


class DictLike(Reducible):
    def __init__(self, data: dict = None, key_label=None, value_label=None) -> None:
        self.data = data if data is not None else {}
        self.key_label = key_label
        self.value_label = value_label

    def __str__(self) -> str:
        if self.key_label and self.value_label:
            return (
                f"DictLike ({self.key_label} -> "
                + f"{self.value_label}, {len(self.data)} items)"
            )

        return f"DictLike ({len(self.data)} items)"

    def __repr__(self) -> str:
        return str(self)

    def __len__(self) -> int:
        return len(self.data)

    def __getitem__(self, key: any) -> any:
        return self.data[key]

    def __setitem__(self, key: any, value: any) -> None:
        self.data[key] = value

    def __delitem__(self, key: any) -> None:
        del self.data[key]

    def __iter__(self) -> any:
        return iter(self.data)

    def __contains__(self, key: any) -> bool:
        return key in self.data

    def __eq__(self, other: any) -> bool:
        return self.data == other.data

    def __ne__(self, other: any) -> bool:
        return self.data != other.data

    def items(self) -> list[tuple[any, any]]:
        return list(self.data.items())

    def keys(self) -> list[any]:
        return list(self.data.keys())

    def values(self) -> list[any]:
        return list(self.data.values())

    def show(self) -> None:
        """
        Prints the dictionary.
        """

        # Convert dictionary to list of tuples (key, value)
        table = [(k, v) for k, v in self.data.items()]

        # Print table using tabulate
        headers = (
            [self.key_label, self.value_label]
            if self.key_label and self.value_label
            else []
        )

        if len(table) > 20:
            print(
                tabulate(
                    table[:10] + [("...", "...")] + table[-10:],
                    headers=headers,
                    tablefmt="psql",
                )
            )
        else:
            print(tabulate(table, headers=headers, tablefmt="psql"))

        print(self.__str__())

    def reduce(self, func: reducer | callable, initial=None) -> any:
        if func == "sum":
            return sum(self.data.values())
        elif func == "prod":
            return reduce(lambda x, y: x * y, self.data.values())
        elif func == "max":
            return max(self.data.values())
        elif func == "min":
            return min(self.data.values())
        elif func == "mean":
            return sum(self.data.values()) / len(self.data)
        else:
            return reduce(func, self.data.values(), initial)
