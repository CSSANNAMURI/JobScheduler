# Multi-Tenant Job Scheduler

In-process Java job scheduler: fixed N workers, per-tenant rate limits, bounded waiting
queue, bounded finished history, and idle-tenant cleanup. Standard library only.

## Run tests

Requires JDK 17+.

```bash
sh run-tests.sh
```

Windows (PowerShell):

```powershell
powershell -ExecutionPolicy Bypass -File .\run-tests.ps1
```

## API

Brief-shaped types (`TenantId`, `Job`, `JobId`, `JobStatus`), plus string helpers for tests:

```java
FakeClock clock = new FakeClock();
JobScheduler s = new JobScheduler(
        4,                                      // workers
        20,                                     // maxWaitingJobs (waiting only)
        new RateLimit(10, 10.0),                // default burst / per-second
        Map.of("A", new RateLimit(10, 10.0)),   // optional per-tenant overrides
        clock);
// Finished history defaults to JobScheduler.DEFAULT_MAX_FINISHED_JOBS_KEPT (10_000).

JobId id = s.submit(new TenantId("A"), () -> { /* work */ });
s.status(id);                 // Optional<JobStatus>
s.cancel(id);                 // true if this call cancelled a QUEUED job
s.shutdown();
```

Rate limiting sits behind `RateLimiter` / `RateLimiterFactory` (default: token bucket).
`RateLimit.burst` and `RateLimit.perSecond` are public.

## Design (short)

- **Per-tenant FIFO** queues; **global** waiting bound via `waitingJobCount ≤ maxWaitingJobs`.
- **Bounded finished history** (default 10_000, independent of waiting bound); older ids return empty from `status()`.
- **Idle tenants** (empty queue + full rate limiter) are dropped so memory stays bounded.
- **One lock**; claim = QUEUED→RUNNING + token + worker slot, atomic with cancel.
- **User code runs outside the lock.**
- **Clock listeners:** any clock that jumps must call `addAdvanceListener` listeners (FakeClock does on `advance()`). No timed pauses in tests.

See [NOTES.md](NOTES.md) for assumptions, production follow-ons, trade-off, and AI disclosure.
Short design rationale: [docs/design-notes.md](docs/design-notes.md).
