# NOTES

## Assumptions
- No tokens → job waits in that tenant’s queue (not rejected). No override → default rate limit.
- `maxWaitingJobs` caps waiting jobs only (`waitingJobCount`). Finished history is a separate cap (default 10_000); older ids return empty from `status()`.
- Idle tenants (empty queue, full bucket) are forgotten — including after a rejected submit leaves a queue empty. A tenant still refilling is kept, so forgetting never grants an extra burst.
- Any `Clock` must call its `addAdvanceListener` listeners when it jumps, or waiting jobs only wake on real time. Real-time clocks can ignore it.
- Status names match the brief (`QUEUED`, `RUNNING`, `SUCCEEDED`, `CANCELLED`); a throwing task → `FAILED`. In-memory, one process. No test uses timed pauses; real-clock tests wait with bounded spins. String helpers are package-private test hooks.

## Production would add 
- Persist jobs and multi-node leases so restarts / shared workers are safe.
- Cross-node rate limits, retries/timeouts, dead-letter + replay.
- Per-tenant caps, idempotent submit, priority, metrics and alerts.

## Trade-off
- One global lock (not per-tenant): keeps queue size, tokens, and cancel-vs-claim correct and simple. Per-tenant locks may scale better under many concurrent tenants, but a global waiting bound is harder — start with one lock; split only if it is hot.

## AI disclosure 
- Used AI for  wording, test ideas, and review fixes. I own the lock/dispatch design, reviewed every file, and verified the brief examples and constraints myself.
