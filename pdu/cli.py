"""Command line interface."""
from pathlib import Path

import click

from . import orm, pdu, util


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
def scan(path: Path, output: str, workers: int, in_memory: bool) -> None:
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
    pdu.scan_path(path, workers)

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
    "--no-empty",
    is_flag=True,
    type=bool,
    default=False,
    help="Hide empty trees from the output.",
)
def du(
    inputfile: str,
    depth: int | None,
    unit: str,
    fields: str,
    quota: bool,
    subpath: Path,
    no_empty: bool,
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
    root = pdu.build_tree(orm.Directory.query_totals())

    if subpath:
        root = pdu.extract_subtree(root, subpath)

    _print = pdu.print_directory_fn(
        columns=fields.split(","), unit=unit, quota=quota, no_empty=no_empty
    )
    util.walk_tree(root, _print, order="pre", maxdepth=depth)

    orm.database.close()


if __name__ == "__main__":
    cli()
