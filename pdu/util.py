
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

    if quota:
        UNITS = {"B": 1, "K": 2**10, "M": 2**10 * 10**3, "G": 2**10 * 10**6, "T": 2**10 * 10**9, "P": 2**10 * 10**12}
        limit = 1000
        suffix = "q"
    else:
        UNITS = {"B": 1, "K": 2**10, "M": 2**20, "G": 2**30, "T": 2**40, "P": 2**50}
        limit = 1024
        suffix = ""

    unitspec = unitspec.upper()

    if unitspec == "H":
        for unit, factor in UNITS.items():
            if (size / factor) < limit:
                break
        else:
            unit = "P"
    else:
        unit = unitspec if unitspec in UNITS else "B"

    newsize = size / UNITS[unit]
    minprec = 1 if newsize < 10 else 0
    return f"{newsize:.{minprec}f}{unit}{suffix}"

