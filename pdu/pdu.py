import concurrent.futures
import grp
import pwd
import os
import sys

from .orm import User, Group, Directory, File


def process_dir(dirname: str) -> tuple[str, None] | tuple[
    str,
    os.stat_result,
    list[tuple[str, os.stat_result]],
    list[str]
]:
    """Get the info for the current directory and the names of any subdirectories."""

    subdirs: list[str] = []
    files: list[tuple[str, os.stat_result]] = []

    try:
        dirstat = os.stat(dirname)

        for d in os.scandir(dirname):
            if d.is_dir(follow_symlinks=False):
                subdirs.append(f"{dirname}/{d.name}")
            elif d.is_file(follow_symlinks=False):
                s = d.stat()
                files.append((d.name, s))
    except PermissionError:
        return dirname, None

    return dirname, dirstat, files, subdirs


def exclude_subdir(dirname: str) -> str | None:
    tail = os.path.split(dirname)[1]

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


def _dir_from_path(path: str, base: str):

    if path not in _dir_cache:

        stem, name = os.path.split(path)
        if path == base:
            parent = None
        else:
            parent = _dir_from_path(stem, base)

        d = Directory.create(name=name, parent=parent)
        _dir_cache[path] = d

    return _dir_cache[path]


def scan_path(path: str, workers: int) -> None:

    count = 0

    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:

        root = executor.submit(process_dir, path)
        waiting = {root}

        while len(waiting) > 0:
            done, waiting = concurrent.futures.wait(
                waiting, return_when=concurrent.futures.FIRST_COMPLETED
            )

            for f in done:
                r = f.result()
                dirname = r[0]
                info = r[1:]
                d = _dir_from_path(dirname, path)

                if info[0] is None:
                    d.scanned = False
                    d.save()
                    continue

                dstat, files, subdirs = info

                for filename, s in files:
                    File.create(
                        name=filename,
                        directory=d,
                        actual_size=s.st_size,
                        apparent_size=(s.st_blocks * 512),
                        mtime=s.st_mtime,
                        num_links=s.st_nlink,
                        user=_user_from_uid(s.st_uid),
                        group=_group_from_gid(s.st_gid),
                    )

                for sd in subdirs:
                    if reason := exclude_subdir(sd):
                        d = _dir_from_path(sd, path)
                        d.scanned = False
                        d.save()
                        continue

                    waiting.add(executor.submit(process_dir, sd))

            count += 1
            print(
                f"Scanned {count} directories. Currently {dirname}",
                file=sys.stderr,
                end="\r",
            )
