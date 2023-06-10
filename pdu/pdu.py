import concurrent.futures
import grp
import pwd
import os
from pathlib import Path
import sys

from .orm import User, Group, Directory, File


def process_dir(path: Path) -> tuple[Path, None] | tuple[
    Path,
    os.stat_result,
    list[tuple[str, os.stat_result]],
    list[Path]
]:
    """Get the info for the current directory and the names of any subdirectories."""

    subdirs: list[Path] = []
    files: list[tuple[str, os.stat_result]] = []

    try:
        dirstat = os.stat(path)

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


def _dir_from_path(path: Path, base: Path):

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

    count = 0

    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:

        root_fut = executor.submit(process_dir, root_path)
        waiting = {root_fut}

        while len(waiting) > 0:
            done, waiting = concurrent.futures.wait(
                waiting, return_when=concurrent.futures.FIRST_COMPLETED
            )

            for f in done:
                r = f.result()
                path = r[0]
                info = r[1:]
                d = _dir_from_path(path, root_path)

                if info[0] is None:
                    d.scanned = False
                    d.save()
                    continue

                dstat, files, subdirs = info

                files_to_insert = []
                for filename, s in files:
                    file_row = {
                        "name": filename,
                        "directory": d,
                        "actual_size": s.st_size,
                        "apparent_size": (s.st_blocks * 512),
                        "mtime": s.st_mtime,
                        "num_links": s.st_nlink,
                        "user": _user_from_uid(s.st_uid),
                        "group": _group_from_gid(s.st_gid),
                    }
                    files_to_insert.append(file_row)

                File.insert_many(files_to_insert).execute()

                for sd in subdirs:
                    if reason := exclude_subdir(sd):
                        d = _dir_from_path(sd, path)
                        d.scanned = False
                        d.save()
                        continue

                    waiting.add(executor.submit(process_dir, sd))

            count += 1
            print(
                f"Scanned {count} directories. Currently {root_path}",
                file=sys.stderr,
                end="\r",
            )
