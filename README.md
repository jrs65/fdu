# fdu

A fast parallel du type thing. This tool has two main goals:

- Fast scans on filesystems with slow `stat` calls, e.g. Lustre filesystems
- Separate the filesystem scan from the presentation of the results to allow
  cheap queries from the results of a single slow scan.

The first goal is accomplished by distributing the `stat` calls to multiple
parallel workers. This allows much quicker scans, but may make your sysadmin
unhappy by thrashing the filesystem with metadata queries. Caveat emptor.



