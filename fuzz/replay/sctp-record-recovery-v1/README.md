# SCTP record-recovery fuzz replay inputs

This fixed input set compares record-recovery scenario schedules in the
`sctp_parse_notification` fuzz target on identical bytes. It contains 435
inputs: 22 canonical fixtures, 109 seeds, and 304 inputs discovered by libFuzzer.

`INPUTS.sha256` lists every input in `sha256sum` format: a content digest,
two spaces, and a logical path, sorted bytewise by path. These path labels
include a `flowio/` prefix; they are not paths to resolve against the package
root. The manifest's SHA-256 is:

```text
174979b477f38c64c138ef7e1f16dbae30483bdd89fc18f39f7c78c661d409f6
```

Each logical path locates its input beneath `inputs/`, including the
`flowio/` prefix. The fixed manifest can therefore be checked from tracked
files alone, without the ignored fuzz corpus or file timestamps. A valid
replay has the manifest hash above and exactly the listed regular files and
their parent directories, with matching digests. Missing, extra, renamed,
linked, or modified inputs invalidate the set.

To replay these inputs, pass a separate disposable directory as libFuzzer's
first, writable corpus argument, followed by the three `sctp_parse_notification`
leaf directories under `inputs/` as read-only corpus arguments. The crate's
ordinary writable corpus is `fuzz/corpus/`; replay leaves this fixed set intact.
