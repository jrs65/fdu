import click

from . import orm, pdu


@click.group()
def cli():
    pass


@cli.command()
@click.argument(
    "path", type=click.Path(dir_okay=True, file_okay=False, exists=True),
)
@click.argument(
    "output", type=click.Path(dir_okay=False, file_okay=True, writable=True),
)
@click.option(
    "-j", "--workers", type=int, default=1, help="Number of parallel workers to use"
)
def scan(path, output, workers):
    orm.database.init(":memory:", pragmas={'foreign_keys': 1})
    #database.create_tables(BaseModel.__subclasses__())
    # #database.close()
    # scan_path(path, workers)
    # database.execute_sql("VACUUM INTO ?", (output,))

    # database.init(output, pragmas={
    #     "foreign_keys": 1,
    #     "journal_mode": "wal",
    #     "synchronous": "normal",
    # })
    orm.database.create_tables(orm.BaseModel.__subclasses__())
    #database.close()
    pdu.scan_path(path, workers)
    orm.database.execute_sql("VACUUM INTO ?", (output,))
    # import sqlite3
    # db = sqlite3.connect(output)
    # database.connection().backup(db)
    # db.close()


if __name__ == "__main__":
    cli()
