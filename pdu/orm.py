import peewee as pw

# Sqlite database model
database = pw.SqliteDatabase(None)


class BaseModel(pw.Model):
    """Base model class."""
    class Meta:
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


class Directory(BaseModel):
    """Directory info.

    Attributes
    ----------
    name
        The directory name. This is not the full path just the name of the
        current level.
    parent
        A key to the parent directory. This will be null for the root entry.
    scanned
        Whether this directory was scanned or not. It may be skipped because of
        exclude filters or permission issues.
    """
    name = pw.CharField()
    parent = pw.ForeignKeyField("self", null=True)
    scanned = pw.BooleanField(default=True)


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
    actual_size, apparent_size
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

    actual_size = pw.IntegerField()
    apparent_size = pw.IntegerField()

    mtime = pw.TimestampField(utc=True)

    num_links = pw.SmallIntegerField()

    class Meta:
        indexes = (
            # create a unique on files names within a directory
            (('name', 'directory'), True),
        )


