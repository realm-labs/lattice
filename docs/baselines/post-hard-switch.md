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

### Vectored Transport Frame Write

A further 2026-07-19 capture kept the protobuf payload as the one owned allocation, constructed the
12-byte transport header on the Writer stack, and wrote header plus payload with vectored I/O. This
avoids allocating and copying a second contiguous transport frame. The reader now also allocates its
bounded frame buffer once instead of reading through an intermediate `Vec` and copying it.

The allocator-instrumented release run measured 10,020 allocations (`1.002/message`) for 10,000
prepared admissions, stable FDs at `10 / 10`, and 4,511,955 admissions/s in its single timing sample.
The framing-specific counters were:

| Write path | Allocations for 10,000 frames | Deallocations |
|---|---:|---:|
| Stack header + vectored Writer | 0 | 0 |
| Contiguous coalescing codec | 10,000 | 10,000 |

Criterion measured the complete admission paths and the isolated framing work as follows:

| Path | Time |
|---|---:|
| Prepared exact route, 10,000 admissions | 2.0632-2.1514 ms (2.1094 ms point estimate) |
| One-shot convenience API, 10,000 admissions | 3.1028-3.1134 ms (3.1088 ms point estimate) |
| Vectored Writer into a sink | 10.204-10.240 ns (10.219 ns point estimate) |
| Contiguous coalescing codec | 34.929-35.037 ns (34.981 ns point estimate) |

The sink comparison isolates header construction, Writer dispatch, allocation, and copying; it does
not include a socket syscall, TLS record construction, scheduling, or delivery latency. TCP and TLS
round-trip tests remain the transport-correctness evidence; a deterministic bounded writer test
covers partial vectored writes across the header/payload boundary.

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

## Actor Completion, TCP Round-Trip, and Persistence Baseline

Captured on 2026-07-20 on Apple M1 Max / Darwin 26.5.2 with `rustc 1.97.1`. These workloads add
completion boundaries that the Association-admission baseline above deliberately does not include.

The timing-only command was:

```text
cargo bench -p remoting-benchmark --bench remoting_benchmark
```

For a 128-byte payload, Criterion measured:

| Workload | Batch time | Throughput |
|---|---:|---:|
| Local bounded-mailbox Actor completion, 10,000 tells | 5.7837-5.9951 ms | 1.6680-1.7290M completed/s |
| Loopback TCP Endpoint to remote Actor and reply, 1,000 sequential asks | 91.492-96.004 ms | 10.416-10.930K round trips/s |

The Actor Criterion case waits for every handler acknowledgement but disables its detailed observer.
The TCP case includes Association lane scheduling, socket I/O, wire decoding, remote Actor mailbox and
handler execution, reply encoding, and client correlation completion. It uses plain loopback TCP; TLS
remains a separate future workload.

The allocator-instrumented `measure` command additionally observed the following distribution for one
10,000-message local completion workload and 1,000 sequential remote asks:

| Measurement | Observed |
|---|---:|
| Local completion throughput | 1,331,381/s |
| Local completion latency p50 / p95 / p99 | 613 / 1,336 / 1,486 us |
| Local Actor queue time p50 / p95 / p99 | 613 / 1,335 / 1,485 us |
| Mailbox-full retries / peak depth | 15,939 / 1,024 |
| Remote round-trip throughput | 9,080/s |
| Remote round-trip latency p50 / p95 / p99 | 101 / 181 / 207 us |

The observer intentionally adds per-message metric collection overhead, so its throughput is not used
as the timing regression baseline. Its percentiles describe the saturated bounded-mailbox workload.

### Local Actor raw completion and Noop observer fast path

A follow-up profile on 2026-07-20 separated the prior completion workload into two Criterion cases:

- `raw_bounded_mailbox` uses one batch-tail barrier and performs no benchmark-side per-message timing
  or completion-channel send;
- `per_message_latency` retains the original per-message timestamp and completion notification.

Before changing the runtime, the raw case measured 3.9481-4.0732 ms per 10,000 messages, or
2.4551-2.5329M completed/s. The per-message case measured 5.6789-6.2160 ms, or
1.6087-1.7609M completed/s. This established that the old completion number included substantial
measurement-harness cost and was not the Actor runtime's raw limit.

The profile also showed that the default Noop Actor observer still read the processing start and end
clocks for every message. After adding a disabled-observer fast path, while preserving full timing for
custom observers, the same run measured:

| Workload | Batch time | Throughput | Point-estimate change |
|---|---:|---:|---:|
| Raw bounded-mailbox completion | 2.8826-2.9788 ms | 3.3570-3.4691M/s | +35.6% |
| Per-message latency completion | 4.7928-5.2140 ms | 1.9179-2.0865M/s | +21.7% |

The allocator-instrumented raw workload measured 1.623M/s and 39,151 allocations for 10,000
messages while encountering 9,096 full-mailbox retries. This is not the timing baseline: the counting
allocator and reconstructing rejected `try_tell` messages both add overhead. The post-change profile
now points primarily to payload reference-count updates, bounded-channel semaphore work, envelope and
handler-future allocation, and the required enqueue timestamp. Removing those costs would require a
larger mailbox/API or dispatch representation change rather than another observer fast-path tweak.

### Native Actor handler futures

A second 2026-07-20 follow-up replaced the public `Actor`, `Handler`, and `Responder`
`async-trait` expansion with native return-position `impl Future + Send`. User implementations keep
the same `async fn` bodies and only remove the `#[async_trait]` attribute. The internal object-safe
Actor envelope remains boxed.

After this change, Criterion measured:

| Workload | Batch time | Throughput | Point-estimate change |
|---|---:|---:|---:|
| Raw bounded-mailbox completion | 2.6215-2.7297 ms | 3.6634-3.8145M/s | +6.9% |
| Per-message latency completion | 4.2910-4.3833 ms | 2.2814-2.3305M/s | +15.1% |

The allocator-instrumented run recorded 21,857 allocations for 10,000 successful messages and 1,803
full-mailbox retries. Subtracting retry envelope allocations leaves approximately 20,054 allocations,
versus approximately 30,055 in the preceding capture after applying the same normalization. This
confirms one eliminated handler-future allocation per successful message and reduces the steady
successful path from approximately three allocations to two. Single-run counting-allocator
throughput remains diagnostic rather than a timing baseline.

The MongoDB persistence framework baseline was captured with:

```text
cargo bench -p lattice-store-mongodb --bench persistence
```

The store is an in-memory acknowledger. The measurements therefore include diff scanning, BSON update
construction, request and per-document outcome allocation, completion validation, cursor/version update,
and baseline advancement, but exclude MongoDB server and network variance.

| Dirty documents | Prepare → flush → complete | Shutdown drain |
|---:|---:|---:|
| 1 | 4.265-4.285 us (233-234K docs/s) | 4.347-4.370 us (229-230K docs/s) |
| 100 | 417.31-418.25 us (239-240K docs/s) | 423.79-446.48 us (224-236K docs/s) |
| 1,000 | 5.046-5.213 ms (192-198K docs/s) | 4.979-5.368 ms (186-201K docs/s) |

This baseline uses one scalar and one two-entry Map mutation per document. Real MongoDB latency, larger
field shapes, partial failures, and budgeted multi-pass drain remain separate benchmark dimensions.
