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

## Optimized Outbound Encoding Baseline

Captured on 2026-07-19 on the same Apple M1 Max host with Darwin 26.5.2 and `rustc 1.97.1`, after
removing owned wire-target construction and encoding outbound messages directly into one exactly
sized protobuf frame. The workload remains `10,000` messages, `128` payload bytes, and one bulk
stripe.

The allocator-instrumented release command above produced:

| Measurement | Observed |
|---|---:|
| Bounded bulk-tell admissions | 1,713,857/s |
| Elapsed | 5.835 ms |
| Allocations | 10,027 (1.003/message) |
| Deallocations | 10,020 |
| Process FDs before/after | 10 / 10 |
| Association physical lanes | 3 (control, interactive, one bulk) |

The timing-only Criterion command was:

```text
cargo bench -p remoting-benchmark --bench remoting_benchmark
```

It measured `4.8673-5.0444 ms` per 10,000-message batch, with a `4.9227 ms` point estimate, or
approximately 2,031,406 admissions/s. Criterion is the timing baseline; the `measure` binary remains
the allocation and FD baseline because its counting allocator adds overhead.

Against the original post-hard-switch `measure` capture, the like-for-like instrumented run reduces
elapsed time by 61.4%, increases admission throughput by 2.59x, and reduces allocator calls per
message by 94.7%. The remaining approximately one allocation per message is the final contiguous
protobuf frame buffer. Both captures measure bounded queue admission, not socket-delivery latency.

### Atomic Association Fast Path

A follow-up capture on 2026-07-19 moved active Association state, attached-lane state, and lane-wake
coordination to atomic state while making the negotiated peer protocol catalogue immutable and
lock-free to read. The same Criterion workload measured `4.2733-4.4234 ms`, with a `4.3383 ms`
point estimate, or approximately 2,305,050 admissions/s. Criterion reported a 12.6% improvement over
the direct-encoding baseline above.

The corresponding allocator-instrumented run retained the single-allocation shape at 10,021
allocations (`1.002/message`) with FDs stable at `10 / 10`. Its single-run timing was 5.973 ms, or
1,674,294 admissions/s; as above, Criterion is the timing baseline and the counting-allocator binary
is used for allocation and FD evidence.

### Prepared Exact-Actor Route

The optimized API can bind a stable exact ActorRef and Association before a hot send loop. Route
preparation performs protocol validation, exact-target and optional sender encoding, and bulk-stripe
selection once. Preparation is intentionally outside the timed and allocation-instrumented window;
the one-shot convenience path remains a separate Criterion case.

On the same 2026-07-19 host, Criterion measured:

| Path | 10,000-message batch | Admissions/s |
|---|---:|---:|
| Prepared exact route | 2.4596-2.5402 ms (2.5042 ms point estimate) | ~3,993,291 |
| One-shot convenience API | 4.2961-4.3964 ms (4.3452 ms point estimate) | ~2,301,390 |

The prepared path reduces elapsed admission time by 42.4% relative to the one-shot path in the same
run. The allocator-instrumented prepared run recorded 10,039 allocations (`1.004/message`), stable
FDs at `10 / 10`, and 4,766,728 admissions/s in its single timing sample. The allocation remains the
final contiguous protobuf frame; cached route construction adds no per-message allocation.

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
