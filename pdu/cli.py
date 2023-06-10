from pathlib import Path
import click

from . import orm, pdu


@click.group()
def cli():
    pass


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
    "-j", "--workers", type=int, default=1, help="Number of parallel workers to use"
)
def scan(path, output, workers):
    #orm.database.init(":memory:", pragmas={'foreign_keys': 1})

    orm.database.init(
        output,
        pragmas={
            "foreign_keys": 1,
            "journal_mode": "wal",
            #"synchronous": "normal",
            "synchronous": "off",
            "temp_store": "memory",
            "mmap_size": 2**30,
            "cache_size": -2**15,
            #"locking_mode": "EXCLUSIVE"
        },
    )
    orm.database.create_tables(orm.BaseModel.__subclasses__())
    pdu.scan_path(path, workers)

    #orm.database.execute_sql("VACUUM INTO ?", (output,))

    orm.database.close()


if __name__ == "__main__":
    cli()
