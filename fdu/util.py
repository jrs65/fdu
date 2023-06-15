"""Utility functions."""

import os
from collections.abc import Callable, Iterable
from typing import Literal, Protocol, TypeVar

T = TypeVar("T")


class HasChildren(Protocol):
    """A protocol for a tree like type."""

    children: list["HasChildren"]


# Define the common unit definitions
_symbols = ["B", "K", "M", "G", "T", "P"]
_units_quota = {u: 1024 * 1000 ** (i - 1) for i, u in enumerate(_symbols)}
_units_norm = {u: 1024**i for i, u in enumerate(_symbols)}


def formatsize(size: int, unitspec: str, quota: bool = False) -> str:
    """Format a file size.

    Parameters
    ----------
    size
        The size in bytes.
    unitspec
        The units to output in (B, K, M, G, T, P) or use H to determine a human readable
        size automatically.
    quota
        Use Compute Canada quota units, i.e. base-10 multiples of base-2 kilobytes.

    Returns
    -------
    str
        The formatted size.
    """
    if quota:
        limit = 1000
        units = _units_quota
        suffix = "q"
    else:
        limit = 1024
        units = _units_norm
        suffix = ""

    unitspec = unitspec.upper()

    if unitspec == "H":
        for unit, factor in units.items():  # noqa: B007
            if (size / factor) < limit:
                break
        else:
            unit = "P"
    else:
        unit = unitspec if unitspec in units else "B"

    newsize = size / units[unit]
    minprec = 1 if newsize < 10 else 0  # noqa: PLR2004
    return f"{newsize:.{minprec}f}{unit}{suffix}"


def parsesize(strsize: str) -> int:
    """Parse a size string into bytes.

    Parameters
    ----------
    strsize
        The size to parse, e.g. 2T for 2 terabytes, or 17K for 17 kilobytes. Plain
        integers, e.g. 17, are interpreted as bytes. Units are base-2.

    Returns
    -------
    size
        The size in bytes.
    """
    try:
        if (unit := strsize[-1]).isalpha() and unit in _units_norm:
            return int(strsize[:-1]) * _units_norm[unit]
        return int(strsize)
    except ValueError:
        raise ValueError(f'Could not parse "{strsize}" into a size in bytes.')


def walk_tree(
    tree: HasChildren,
    f: Callable[[HasChildren, int], None],
    maxdepth: int | None = None,
    order: Literal["pre", "post", "bfs"] = "pre",
) -> None:
    """Walk the directory tree applying a function to each node.

    Parameters
    ----------
    tree
        Root of the tree to .
    f
        Function to apply.
    depth
        Maximum depth to walk to. If not set, walk directories at all levels.
    order
        Order to walk the tree in, there are two depth first options: `pre` where the
        node is visited before it's children; `post` where it is visited after the
        children; and `bfs` where the nodes are visited in a breadth first order.
    """
    # DFS print
    stack = [(tree, 0)]

    depth = 0

    while stack:
        last_depth = depth
        d, depth = stack.pop(0)

        if maxdepth and depth >= maxdepth:
            continue

        children = [(c, depth + 1) for c in d.children]
        # How exactly we manipulate the stack depends on the exact traversal that we
        # want to do
        if order == "pre":
            stack = children + stack
        elif order == "bfs":
            stack = stack + children
        elif order == "post" and d.children and depth >= last_depth:
            stack = [*children, (d, depth), *stack]
            continue

        # Process the current node
        f(d, depth)


def _skip(xl: Iterable[T | None]) -> list[T]:
    return [x for x in xl if x is not None]


def agg_none(xl: Iterable[T | None], f: Callable[[Iterable[T]], T]) -> T | None:
    """Aggregate a sequence, skipping None values.

    If the list is empty or all None's then None is returned.

    Parameters
    ----------
    xl
        The iterable to aggregate.
    f
        The function to apply, e.g. `sum` or `max`.

    Returns
    -------
    agg
        The aggregated value or `None` if the iterable had no valid elements.
    """
    xl = _skip(xl)
    if len(xl) == 0:
        return None

    return f(xl)


def print_trim(text: str, overwrite: bool = False, **kwargs: dict) -> None:
    """Print text while cleanly trimming to the terminal size.

    Parameters
    ----------
    text
        The text to print.
    overwrite
        Overwrite the current line, e.g. for showing a progress update.
    kwargs
        Arguments passed directly to `print`.
    """
    ts = os.get_terminal_size()

    trimmed_text = text[: ts.columns]

    # \x1b[0K is VT100 erase to *end* of line then \r is move cursor to the start
    end = "\x1b[0K\r" if overwrite else "\n"

    print(trimmed_text, end=end, **kwargs)
