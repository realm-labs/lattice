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
The queue-time row is a historical capture: the Actor observer no longer records a per-message enqueue
timestamp, so current runs report handler processing time and mailbox depth but not queue duration.

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
allocator and reconstructing rejected `try_tell` messages both add overhead. At this point the profile
pointed primarily to payload reference-count updates, bounded-channel semaphore work, envelope and
handler-future allocation, and the enqueue timestamp. Later sections record the dispatch
representation change that removed the timestamp and common tell handler-future allocation.

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

### Ownership-preserving tell rejection

A third 2026-07-20 follow-up made `ActorTellError<M>` retain the original business message on full,
closed, and lifecycle-rejected admission. `try_tell` keeps Tokio's direct `try_send` hot path and
recovers `M` from the rejected type-erased envelope; `tell` waits for a mailbox permit and rechecks
lifecycle fencing before constructing the envelope. Saturation benchmarks now retry the returned
message rather than cloning `Bytes` and its completion sender on every full-mailbox result.

After rebasing the typed state-machine behavior change, Criterion measured:

| Workload | Batch time | Throughput |
|---|---:|---:|
| Raw bounded-mailbox completion | 2.4607-2.5818 ms | 3.8733-4.0638M/s |
| Per-message latency completion | 4.2107-4.3219 ms | 2.3138-2.3749M/s |

The allocator-instrumented raw run recorded 24,010 allocations for 10,000 successful messages and
3,945 full-mailbox retries. The normalized successful path was approximately two allocations per
message, while each rejected `try_tell` also allocated and then recovered one type-erased envelope.

### Timestamp-free pooled tell dispatch

A fourth 2026-07-20 follow-up removed the enqueue timestamp from Actor envelopes and
`MessageMetadata`. Consequently `ActorObserver::message_started` no longer reports queue duration and
`request_completed` no longer reports total time; processing duration, mailbox depth, outcomes, and
request completion categories remain available.

The SmallBox handler-future experiment was withdrawn. Handler futures use the direct boxed
representation again, while type-erased tell and request envelopes now use an internal fixed-size
allocation pool with transparent allocator fallback for large or unusually aligned messages.
`try_tell` obtains a Tokio mailbox permit before constructing the envelope, so a full or closed
mailbox returns the original typed message without an allocation, downcast, or recovery path.

In the allocation-instrumented raw workload, 10,000 successful tells with 5,601 full-mailbox retries
performed 10,051 allocations. The rejected attempts therefore no longer contribute an envelope
allocation; the remaining approximately one allocation per successful tell is the boxed handler
future.

A completion-throughput comparison used two warmup rounds and five measured rounds of 10 million
tells with a 128-byte `Bytes` payload and a 1,024-entry mailbox:

| Implementation | Median throughput | Median time/message |
|---|---:|---:|
| Pre-change exact baseline | 3.6677M/s | 272.65 ns |
| Withdrawn SmallBox experiment | 4.0378M/s | 247.66 ns |
| Pooled envelope, representative run | 4.8799M/s | 204.92 ns |

Three independent pooled-envelope captures produced medians from 4.7517M/s to 4.9834M/s. The
representative median is 20.9% above the withdrawn SmallBox result and 33.1% above the original exact
baseline.

An isolated same-session remoting A/B kept timestamp removal and direct boxed handler futures in both
binaries, changing only Box envelope admission versus pooled envelope admission. The execution order
was Box, pool, pool, Box; each capture used three warmup rounds and seven measured rounds of 100,000
loopback TCP tells. The two Box medians were 378,523/s and 379,839/s, while the pooled medians were
387,841/s and 397,736/s. Averaging each pair of medians gives 379,181/s versus 392,789/s, a 3.6%
improvement. Separate 10-million-message runs under a 10-second CPU sampler measured 347,934/s versus
358,238/s, a 3.0% improvement despite sampling overhead. Pool allocation, pop, push, and recycle paths
did not appear as material CPU hotspots; protobuf/frame encoding, `BytesMut` writes, allocator work,
and socket `writev` remained dominant.

### Thread-local pooled handler futures

A fifth 2026-07-21 follow-up replaced the remaining boxed handler future with a pinned erased future
stored in a reusable size-class block. Future blocks use a separate thread-local cache rather than
the process-wide envelope pool: handler futures normally start and finish on runtime workers, so this
avoids adding an MPMC atomic queue to every dispatch. A future that resumes on another worker leaves
its block in that worker's cache when it completes. Large and unusually aligned futures retain an
allocator fallback, and moving the erased wrapper never moves its pinned concrete future.

The allocation-instrumented workload recorded 53 allocations and 35 deallocations for 10,000
successful tells plus 5,088 full-mailbox retries. The prior pooled-envelope version recorded 10,051
allocations for the same number of successful messages, confirming that the remaining per-message
handler-future allocation is gone. The small fixed remainder is runtime and benchmark setup plus
thread-local pool growth.

An isolated boxed-versus-thread-local-pooled A/B used three independent process captures per variant,
with two warmup rounds and five measured rounds of five million tells. Machine scheduling made
absolute throughput lower than the earlier baseline, so only the interleaved same-session comparison
is used: boxed-future medians averaged 3.2799M/s and pooled-future medians averaged 3.3783M/s, a 3.0%
improvement (304.89 ns versus 296.01 ns per message).

A separate 1/2/4/8/16-producer comparison found no producer-count-dependent regression from the
future pool. Both variants decline similarly as producers contend on the single bounded actor
mailbox; at 16 producers their medians were 1.0915M/s boxed and 1.0963M/s pooled. This identifies the
mailbox admission path, rather than future pooling, as the multi-producer scaling limit.

Request dispatch also showed no regression. In the alternating window-1 run, after excluding one
123K/s boxed startup outlier, the steady boxed and pooled captures were 159.2K/s and 160.9K/s. With a
64-request window, the two-capture means were 542.5K/s boxed and 568.7K/s pooled, a 4.8% improvement.

### Actor-specialized bounded mailbox

A sixth 2026-07-21 follow-up replaced Tokio's general-purpose bounded MPSC channel in local actor
mailboxes. The actor-specialized channel uses one atomic state word for the closed flag and available
permits, a fixed-capacity MPSC ring for admitted commands, and a single-consumer `AtomicWaker`. The
non-blocking path no longer enters an asynchronous semaphore. Capacity waiters are registered only
after the mailbox is observed full, using stack-based event listeners so `tell().await` does not add
a per-message waiter allocation. The receiver consults the listener set only on a full-to-available
capacity transition. This retains strict bounded admission and the existing permit-first behavior,
so a rejected tell still returns its original typed message before an erased envelope is allocated.

Two stable same-session five-million-message comparisons against the immediately preceding
pooled-future binary measured 5.137M/s versus 5.730M/s (+11.5%) and 5.109M/s versus 5.585M/s (+9.3%).
Median local-tell cost fell from 194.68--195.72 ns to 174.52--179.05 ns. Earlier captures experienced
larger machine-load drift, so they are not used for the final relative result.

The isolated 64-window local-ask workload was unchanged at 781.5K/s versus 781.0K/s. Network
workloads also remained transport-bound after removing the capacity waiter's heap allocation:
remote tell measured 419.2K/s versus 421.5K/s (+0.5%), and a mixed-workload remote-ask capture
measured 42.62K/s versus 42.81K/s (+0.4%).

With 16 concurrent producers, successful throughput rose from 1.116M/s to 1.186M/s (+6.2%). The
median full-mailbox retry count per two-million-message round fell from 0.84 million to 0.19 million,
although retry counts remained scheduler-sensitive and one custom-mailbox round reached 1.01
million. The custom mailbox therefore reduces contention CPU more consistently than it raises the
single actor's terminal processing rate.

### Bounded normal-message turns

A seventh 2026-07-21 follow-up lets an Actor consume an immediately available batch from its normal
lane before returning to the outer mailbox selection. The default turn budget is 64 messages and is
configurable through `MailboxConfig::with_turn_budget`. The system lane is reconsidered at every turn
boundary. This bounds the number of queued normal messages that can precede a waiting system message,
but deliberately does not claim a wall-clock bound for slow or suspending handlers.

An interleaved before/after comparison used two warmup rounds and five measured rounds of five
million tells. The two pre-batch medians were 3.477M/s and 3.303M/s; the two 64-message-turn medians
were 4.933M/s and 4.895M/s. Averaging each pair gives 3.390M/s versus 4.914M/s, a 45.0% improvement
under the same machine-load interval. Absolute throughput was lower than earlier captures, so this
section uses only the interleaved relative result.

The separate producer-task workload, which uses a final barrier message, measured 7.617M/s versus
8.951M/s with one producer (+17.5%). At 16 producers it measured 1.417M/s versus 1.425M/s (+0.6%);
the contended admission path therefore did not regress, but batching cannot remove multi-producer
queue contention. In a second interleaved mixed-workload capture, 64-window local ask improved by
14.5%, remote tell by 2.2%, and 64-window remote ask by 2.8%. The smaller remote deltas confirm that
network encoding and transport remain dominant.

The original cross-framework harness runs the Lattice producer inside its main Tokio future, while
Akka's producer runs outside the Actor dispatcher. Moving only the Lattice producer to a separate
Tokio task, while retaining the exact `TellMessage`, last-message completion marker, 500,000-message
round, and 128-byte payload, measured 8.675M/s (115.27 ns/message). The preserved Akka 2.6.21 result
is 8.850M/s (112.99 ns/message), leaving a 2.0% gap in the comparable producer/consumer topology.
Lattice retains its bounded 1,024-entry mailbox and full-mailbox retry behavior in that result.

A diagnostic high-budget profile reduced mailbox-related active samples from 30.7% to 23.7%.
Dynamic envelope dispatch and handler polling accounted for approximately 29.5%, while `Bytes`
clone/drop reference-count traffic accounted for approximately 28.5%. This profile used a 512-message
experimental turn to expose the asymptote; 64 remains the production default because higher values
offered diminishing throughput returns while delaying system-lane reconsideration.

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
