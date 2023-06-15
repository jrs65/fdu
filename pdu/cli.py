"""Command line interface."""
from pathlib import Path

import click
import peewee as pw

from . import orm, fdu, util


@click.group()
def cli() -> None:
    """Fast parallel disk usage analysis.

    Think du, but it actually works on cedar.
    """


@cli.command()
@click.argument(
    "path",
    type=click.Path(dir_okay=True, file_okay=False, exists=True, path_type=Path),
)
@click.argument(
    "output",
    type=click.Path(dir_okay=False, file_okay=True, writable=True),
)
@click.option(
    "-j",
    "--workers",
    type=int,
    default=1,
    help="Number of parallel workers to use",
)
@click.option(
    "--in-memory",
    is_flag=True,
    default=False,
    help=(
        "If set, use an in memory database and only write out at the very end. "
        "This may be faster, with potential consistency issues."
    ),
)
@click.option(
    "-X", "--exclude",
    type=str,
    multiple=True,
    default=None,
    help=(
        "Exclude directories matching the given regex against their full path. Use "
        "this option multiple times to different patterns. By default two patterns "
        "are used `.*/.git` and `.*/site-packages`"
    ),
)
def scan(
    path: Path, output: str, workers: int, in_memory: bool, exclude: list[str] | None
) -> None:
    """Scan the tree at the given PATH and save the results into OUTPUT."""
    if in_memory:
        orm.database.init(":memory:", pragmas={"foreign_keys": 1})
    else:
        orm.database.init(
            output,
            pragmas={
                "foreign_keys": 1,
                "journal_mode": "wal",
                "synchronous": "off",
                "temp_store": "memory",
                "mmap_size": 2**30,
                "cache_size": -(2**15),
            },
        )
    orm.database.create_tables(orm.BaseModel.__subclasses__())

    if exclude is None:
        exclude = [
            ".*/.git",
            ".*/site-packages",
        ]
    fdu.scan_path(path, workers, exclude_patterns=exclude)

    if in_memory:
        orm.database.execute_sql("VACUUM INTO ?", (output,))

    orm.database.close()


@cli.command()
@click.argument(
    "inputfile",
    type=click.Path(dir_okay=False, file_okay=True, exists=True),
)
@click.option(
    "-d",
    "--depth",
    type=int,
    default=None,
    help="Maximum depth of tree to print.",
)
@click.option(
    "-u",
    "--unit",
    type=click.Choice(["B", "K", "M", "G", "T", "P", "H"]),
    default="K",
    help="The units to output in. Choose H for human readable sizes.",
)
@click.option(
    "-q",
    "--quota",
    is_flag=True,
    type=bool,
    default=False,
    help="Return sizes in Compute Canada quota units.",
)
@click.option(
    "--fields",
    type=str,
    default="S",
    help="Comma separated list of columns to print (e.g. C,S)",
)
@click.option(
    "--subpath",
    type=click.Path(path_type=Path),
    default=None,
    help="Query a subtree within a given dump.",
)
@click.option(
    "--all",
    is_flag=True,
    type=bool,
    default=False,
    help="Print empty directory trees.",
)
@click.option(
    "--user",
    type=str,
    default=None,
    help="Only count files owned by the given user.",
)
@click.option(
    "--min-size",
    type=str,
    default=None,
    help="Only print directories with a total larger than this.",
)
def query(
    inputfile: str,
    depth: int | None,
    unit: str,
    fields: str,
    quota: bool,
    subpath: Path,
    all: bool,
    user: str,
    min_size: str,
) -> None:
    """Query the INPUTFILE to get a du like output of the space usage.

    \b
    Valid column codes:
    - `n`: the number of files in the directory
    - `N`: the number of files in the subtree
    - `s`: the allocated size of all files in the current directory
    - `S`: the allocated size of all files in the subtree
    - `a`: the apparent size of all files in the current directory
    - `A`: the apparent size of all files in the subtree
    - `t`: the latest modification time of the current directory and its files
    - `T`: the latest modification time of the subtree and its files
    """
    orm.database.init(inputfile)
    if user:
        try:
            user = orm.User.get(name=user)
        except pw.DoesNotExist:
            raise ValueError(f"Unknown user {user}")

    root = fdu.build_tree(orm.Directory.query_totals(user=user))

    if subpath:
        root = fdu.extract_subtree(root, subpath)

    if not all:
        fdu.filter_tree(root, lambda d: d.file_count_tree > 0)

    if min_size:
        size = util.parsesize(min_size)
        fdu.filter_tree(root, lambda d: d.allocated_size_tree > size)

    _print = fdu.print_directory_fn(
        columns=fields.split(","),
        unit=unit,
        quota=quota,
    )
    util.walk_tree(root, _print, order="pre", maxdepth=depth)

    orm.database.close()


if __name__ == "__main__":
    cli()
