# SCTP record-recovery replay authority

This directory is the immutable input authority for comparing alternative
scenario schedules in the `sctp_parse_notification` fuzz target. It contains
435 inputs: 22 canonical fixtures, 109 maintained seeds, and 304
machine-discovered inputs from the accepted reference replay.

`INPUTS.sha256` records the original logical path and SHA-256 of every input.
Its own SHA-256 is:

```text
174979b477f38c64c138ef7e1f16dbae30483bdd89fc18f39f7c78c661d409f6
```

The files are physically rooted below `inputs/`, but retain their original
logical paths there. This makes the manifest identity equal to the reference
replay identity without consulting ignored corpus state or file timestamps.

The manifest is the complete 435-input inventory. Each entry binds an original
logical path to its content digest, in bytewise lexical order. The authority is
valid only when the manifest has the identity above and `inputs/` contains
exactly those regular files and directories with matching digests; missing,
extra, renamed, linked, or modified entries invalidate it.

Comparison tooling must pass the three `sctp_parse_notification` leaf
directories below `inputs/` as read-only corpus arguments and use a separate
disposable directory as libFuzzer's first, writable corpus argument. Normal
fuzz smoke continues to learn into `flowio/fuzz/corpus/`; it does not update
this authority.
