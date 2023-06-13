"""Main routines."""

import concurrent.futures
import datetime
import grp
import os
import pwd
import sys
from collections.abc import Callable, Iterable
from pathlib import Path

from .orm import Directory, File, Group, ScanStatus, User, database
from .util import formatsize, walk_tree


def process_dir(
    path: Path,
) -> tuple[Path, None] | tuple[
    Path,
    os.stat_result,
    list[tuple[str, os.stat_result]],
    list[Path],
]:
    """Get the info for the current directory and the names of any subdirectories."""
    subdirs: list[Path] = []
    files: list[tuple[str, os.stat_result]] = []

    try:
        dirstat = path.stat()

        for d in os.scandir(path):
            if d.is_dir(follow_symlinks=False):
                subdirs.append(path / d.name)
            elif d.is_file(follow_symlinks=False):
                s = d.stat()
                files.append((d.name, s))
    except PermissionError:
        return path, None

    return path, dirstat, files, subdirs


def exclude_subdir(path: Path) -> str | None:
    """Determine directories to exclude from scanning."""
    tail = path.parts[-1]

    if tail in [".git", "site-packages"]:
        return tail

    return None


_uid_cache = {}


def _user_from_uid(uid: int) -> User:

    if uid not in _uid_cache:
        u = User.create(uid=uid, name=pwd.getpwuid(uid).pw_name)
        _uid_cache[uid] = u

    return _uid_cache[uid]


_gid_cache = {}


def _group_from_gid(gid: int) -> Group:

    if gid not in _gid_cache:
        g = Group.create(gid=gid, name=grp.getgrgid(gid).gr_name)
        _gid_cache[gid] = g

    return _gid_cache[gid]


_dir_cache = {}


def _dir_from_path(path: Path, base: Path) -> Directory:

    if path not in _dir_cache:

        if path == base:
            parent = None
        elif path.is_relative_to(base):
            parent = _dir_from_path(path.parent, base)
        else:
            raise ValueError(f"{path} is not a descendent of the base path {base}")

        d = Directory.create(name=path.name, parent=parent)
        _dir_cache[path] = d

    return _dir_cache[path]


def scan_path(root_path: Path, workers: int) -> None:
    """Scan the directory tree and add to the database.

    Parameters
    ----------
    root_path
        The root to scan from.
    workers
        Number of parallel workers to use.
    """
    count = 0

    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:

        root_fut = executor.submit(process_dir, root_path)
        waiting = {root_fut}

        while len(waiting) > 0:
            done, waiting = concurrent.futures.wait(
                waiting,
                return_when=concurrent.futures.FIRST_COMPLETED,
            )

            with database.atomic():
                for f in done:
                    r = f.result()
                    path = r[0]
                    info = r[1:]
                    d = _dir_from_path(path, root_path)

                    # No permissions to read the directory...
                    if info[0] is None:
                        d.scan_status = ScanStatus.SKIPPED_PERMISSION
                        d.save()
                        continue

                    dstat, files, subdirs = info
                    d.scan_status = ScanStatus.SUCCESSFUL
                    d.mtime = dstat.st_mtime
                    d.save()

                    files_to_insert = []
                    for filename, s in files:
                        file_row = {
                            "name": filename,
                            "directory": d,
                            "apparent_size": s.st_size,
                            "allocated_size": (s.st_blocks * 512),
                            "mtime": s.st_mtime,
                            "num_links": s.st_nlink,
                            "user": _user_from_uid(s.st_uid),
                            "group": _group_from_gid(s.st_gid),
                        }
                        files_to_insert.append(file_row)

                    File.insert_many(files_to_insert).execute()

                    for sd in subdirs:
                        if exclude_subdir(sd):
                            sdi = _dir_from_path(sd, path)
                            sdi.scan_status = ScanStatus.SKIPPED_EXCLUDE
                            sdi.save()
                            continue

                        waiting.add(executor.submit(process_dir, sd))

                    count += 1
                    print(
                        f"Scanned {count} directories. Currently {path}",
                        file=sys.stderr,
                        # \x1b[0K is VT100 erase to *end* of line
                        # then \r is move cursor to the start
                        end="\x1b[0K\r",
                    )


def build_tree(directories: Iterable[Directory]) -> Directory:
    """Build a directory tree from a complete set of directory entries.

    This will modify the nodes in the iterable:
    - adds a `children` attribute which lists the subdirectories at each level
    - adds a `path` attribute giving the full path of each entry.

    Parameters
    ----------
    directories
        An iterable (e.g. a peewee query) over directory entries. The members should
        form a single complete tree.

    Returns
    -------
    root
        The directory entry at the root of the tree.
    """
    _dir_cache = {}

    # First create a map of IDs to directories
    for d in directories:
        _dir_cache[d.id] = d

        d.children = []

        # Identify the root on this pass
        if d.parent_id is None:
            root = d

    # Do a second pass to set the children of each node
    for d in _dir_cache.values():
        p_id = d.parent_id

        if p_id is None:
            continue

        if p_id not in _dir_cache:
            raise RuntimeError(
                "Iterable input must contain a complete tree, but can not find "
                f"the parent_id {p_id} for {d}",
            )

        _dir_cache[p_id].children.append(d)

    def _add_paths(d: Directory, _: int) -> None:
        # A temporary function to walk the tree and set the paths and sort the children

        if d.parent_id:
            parent = _dir_cache[d.parent_id]
            d.path = parent.path / d.name
        else:
            d.path = Path(d.name)

        d.children.sort(key=lambda d: d.name)

    walk_tree(root, _add_paths, order="pre")
    walk_tree(root, lambda x, _: x._sum_subdirs(), order="post")  # noqa: SLF001

    return root


def print_directory_fn(
    columns: list[str],
    unit: str = "K",
    quota: bool = False,
    isotime: bool = True,
) -> Callable[[Directory, int], None]:
    """Print out the directory tree.

    Parameters
    ----------
    d
        The directory entry to print.
    columns
        The columns to output, given as a list of single character codes.
        - `n`: the number of files in the directory
        - `N`: the number of files in the subtree
        - `s`: the allocated size of all files in the current directory
        - `S`: the allocated size of all files in the subtree
        - `a`: the apparent size of all files in the current directory
        - `A`: the apparent size of all files in the subtree
        - `t`: the latest modification time of the current directory and its files
        - `T`: the latest modification time of the subtree and its files
        Default is just `['S']` to reproduce the output of `du`.
    unit
        The units to output sizes in (B, K, M, G, T, P) or use H to determine a human
        readable size.
    quota
        Use Compute Canada quota units, which are a weird base-10 and base-2 combo.
    isotime
        Output the timestamps in an isoformat.
    """

    def _ptime(time: int):
        return datetime.fromtimestamp(time).isoformat() if isotime else int(time)

    def _psize(size: int):
        return formatsize(size, unit, quota)

    colspec = {
        "c": ["file_count_dir", 10, str],
        "C": ["file_count_tree", 10, str],
        "s": ["allocated_size_dir", 10, _psize],
        "S": ["allocated_size_tree", 10, _psize],
        "a": ["apparent_size_dir", 10, _psize],
        "A": ["apparent_size_tree", 10, _psize],
        "t": ["mtime_dir", 16, _ptime],
        "T": ["mtime_tree", 16, _ptime],
    }

    for c in columns:
        if c not in colspec:
            raise ValueError(f'Unsupported column code "{c}"')

    def _print(d: Directory, _: int):

        output_columns = []

        for c in columns:
            attr, width, fn = colspec[c]
            output_columns.append(f"{fn(getattr(d, attr)):{width}s}")

        output_columns.append(str(d.path))

        print(*output_columns)

    return _print
