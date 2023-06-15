"""Database tables."""

import json
from enum import Enum
from typing import Self, TypeVar

import peewee as pw
from peewee import fn

from .util import agg_none

# Sqlite database model
database = pw.SqliteDatabase(None)


E = TypeVar("E", bound=Enum)
JSONType = dict[str, "JSONType"] | list["JSONType"] | str | int | float | bool | None

__schema_version__ = "2023.06"


class EnumField(pw.SmallIntegerField):
    """An Enum like field for Peewee.

    Taken from: https://github.com/coleifer/peewee/issues/630#issuecomment-459404401
    """

    def __init__(self, choices: type[E], *args: tuple, **kwargs: dict):
        super().__init__(*args, **kwargs)
        self.choices = choices

    def db_value(self, value: E) -> int:
        """Convert to the DB type."""
        return value.value

    def python_value(self, value: int) -> E:
        """Convert to the Python type."""
        return self.choices(value)


class BaseModel(pw.Model):
    """Base model class."""

    class Meta:
        """Meta info."""

        database = database


class User(BaseModel):
    """The file owning user.

    Attributes
    ----------
    uid
        The system user id. Used as the primary key.
    name
        The actual user name.
    """

    uid = pw.IntegerField(primary_key=True)
    name = pw.CharField(unique=True)


class Group(BaseModel):
    """The group that owns the file.

    Attributes
    ----------
    gid
        The system group id. Used as the primary key.
    name
        The actual group name.
    """

    gid = pw.IntegerField(primary_key=True)
    name = pw.CharField(unique=True)


class ScanStatus(Enum):
    """An enum type giving the possible outcomes of a scan."""

    NOT_SCANNED = 0
    SUCCESSFUL = 1
    SKIPPED_PERMISSION = 2
    SKIPPED_EXCLUDE = 3


class Directory(BaseModel):
    """Directory info.

    Attributes
    ----------
    name
        The directory name. This is not the full path just the name of the
        current level.
    parent
        A key to the parent directory. This will be null for the root entry.
    mtime
        UTC Unix timestamp of the last modification.
    scan_status
        The status of the directory scan. See `ScanStatus` for the mapping from states
        to the underlying integers.
    children
        List of the sub directories.
    allocated_size_dir, apparent_size_dir, mtime_dir
        The summed sizes, and maximum modification time of all files within the
        directory. Only set directly or by special queries.
    allocated_size_tree, apparent_size_tree, mtime_tree
        The summed sizes, and maximum modification time of all files within the subtree
        starting at this directory. Set by `fdu.build_tree`.
    """

    name = pw.CharField()
    parent = pw.ForeignKeyField("self", null=True)
    mtime = pw.TimestampField(utc=True)

    scan_status = EnumField(choices=ScanStatus, default=ScanStatus.NOT_SCANNED)

    subdirectories: dict[str, Self] | None = None

    file_count_dir: int | None = None
    allocated_size_dir: int | None = None
    apparent_size_dir: int | None = None
    mtime_dir: int | None = None

    file_count_tree: int | None = None
    allocated_size_tree: int | None = None
    apparent_size_tree: int | None = None
    mtime_tree: int | None = None

    @classmethod
    def query_totals(cls, user: User | None = None) -> pw.ModelSelect:
        """Base query to select directories with directory totals.

        Parameters
        ----------
        all
            Return all directories, even if empty.

        Returns
        -------
        query
            An unevaluated select query that will return the directories.
        """
        if user:
            kwargs = {
                "on": (
                    (File.directory_id == Directory.id) & (File.user_id == user.uid)
                ),
            }
        else:
            kwargs = {}

        return (
            cls.select(
                cls,
                fn.Count(File.id).alias("file_count_dir"),
                fn.IfNull(fn.Sum(File.allocated_size), 0).alias("allocated_size_dir"),
                fn.IfNull(fn.Sum(File.apparent_size), 0).alias("apparent_size_dir"),
                fn.IfNull(
                    fn.Max(fn.Max(File.mtime), Directory.mtime),
                    0,
                ).alias("mtime_dir"),
            )
            .join(File, pw.JOIN.LEFT_OUTER, **kwargs)
            .group_by(cls)
        )

    def _sum_subdirs(self) -> None:
        """Sum up information from the subdirectories into the subtree totals."""
        if self.children is None:
            return

        self.file_count_tree = self.file_count_dir
        self.allocated_size_tree = self.allocated_size_dir
        self.apparent_size_tree = self.apparent_size_dir
        self.mtime_tree = self.mtime_dir

        dirs = [self, *self.children]

        self.file_count_tree = agg_none([c.file_count_tree for c in dirs], sum)
        self.allocated_size_tree = agg_none([c.allocated_size_tree for c in dirs], sum)
        self.apparent_size_tree = agg_none([c.apparent_size_tree for c in dirs], sum)
        self.mtime_tree = agg_none([c.mtime_tree for c in dirs], max)

    @property
    def children(self) -> list[Self]:
        """List of subdirectory entries."""
        subdir_names = sorted(self.subdirectories.keys())
        return [self.subdirectories[name] for name in subdir_names]


class File(BaseModel):
    """File info.

    Attributes
    ----------
    name
        The file name.
    directory
        A key to the directory the file is in.
    user, group
        The owning user and group.
    allocated_size, apparent_size
        The actual and apparent file sizes.
    mtime
        UTC Unix timestamp of the last modification.
    num_links
        The number of hard links to the file.
    """

    name = pw.CharField()
    directory = pw.ForeignKeyField(Directory)

    user = pw.ForeignKeyField(User)
    group = pw.ForeignKeyField(Group)

    allocated_size = pw.IntegerField()
    apparent_size = pw.IntegerField()

    mtime = pw.TimestampField(utc=True)

    num_links = pw.SmallIntegerField()

    class Meta:
        """Set an index on this table."""

        indexes = (
            # create a unique on files names within a directory
            (("name", "directory"), True),
            (("user", "directory"), False),
        )


class JSONField(pw.TextField):
    """Very simple JSON field."""

    def db_value(self, value: JSONType) -> str:
        """Serialize the python values for storage in the db."""
        if value is None:
            return None

        return json.dumps(value)

    def python_value(self, value: str) -> JSONType:
        """Deserialize the DB string to JSON."""
        if value is None:
            return None

        return json.loads(value)


class Metadata(BaseModel):
    """Store metadata about the scan."""

    key = pw.CharField(256, primary_key=True)
    value = JSONField()
