# Post-Hard-Switch Remoting Baseline

Captured on 2026-07-12 after macro batch 2 on Apple M1 Max / Darwin 25.5.0 with
`rustc 1.97.0`. The reproducible command was:

```text
cargo run --release -p remoting-benchmark --bin measure
```

Default workload (`10,000` messages, `128` bytes, one bulk stripe):

| Measurement | Observed |
|---|---:|
| Bounded bulk-tell admissions | 661,989/s |
| Elapsed | 15.106 ms |
| Allocations | 190,010 (19.00/message) |
| Deallocations | 190,005 |
| Process FDs before/after | 10 / 10 |
| Association physical lanes | 3 (control, interactive, one bulk) |

The preserved Direct Link steady-state case measured 5.816-6.190 us/message, or roughly
161,550-171,945 messages/s, with four pooled connections. The new result is admission into bounded
Association lanes while drain tasks consume frames; it is not claimed as socket-delivery latency and
is compared only to the legacy pooled-admission role. The initial accepted budgets are:

- at least 150,000 admissions/s for the 128-byte release workload on the capture host;
- no more than 25 allocator calls/message until a legacy allocation baseline exists;
- no FD growth across the workload and no more than `2 + configured bulk stripes` data/control FDs
  per Association in this in-memory topology;
- no more than a 20% regression from this post-switch baseline without an explained benchmark update.

The legacy benchmark did not record allocation or observed-FD numbers, so this document does not
invent a before/after percentage for those dimensions. The same release run records the complete
runtime/reducer comparison matrix:

| Category | Operations/s |
|---|---:|
| Local actor tell admission | 3,665,634 |
| Concrete remote ref admission | 661,989 |
| Stable shard route | 12,902,527 |
| Unknown shard buffer/lookup install | 4,002,334 |
| Allocation evaluation | 3,473,076 |
| Rebalance planning | 1,524,264 |
| Complete handoff reduction | 2,778,743 |
| Reliable-control reconnect replay | 7,187,347 |

These are single-process microbenchmarks of the named operation, not end-to-end latency claims. Real
TCP/TLS adapter latency remains acceptance-test evidence rather than being conflated with queue
admission or pure reducer cost.
