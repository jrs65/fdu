"""Utility functions."""

from collections.abc import Callable, Iterable
from typing import Literal, Protocol, TypeVar

T = TypeVar("T")


class HasChildren(Protocol):
    """A protocol for a tree like type."""

    children: list["HasChildren"]


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
    symbols = ["B", "K", "M", "G", "T", "P"]

    if quota:
        limit = 1000
        units = {u: 2**10 * limit**(i - 1) for u, i in enumerate(symbols)}
        suffix = "q"
    else:
        limit = 1024
        units = {u: limit**i for u, i in enumerate(symbols)}
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
