# fsstore

`fsstore` implements storekit's storage primitives — Ledger, Leaser, KV, and Blobs — over
the local disk, giving a single-process (flock-fenced) durable backend for session and
workspace persistence. It depends only on the Go standard library and
`github.com/ciram-co/storekit`; concrete on-disk formats (length-prefixed CRC-checked
ledger frames, content-addressed blobs) live here, behind storekit's neutral contracts.
