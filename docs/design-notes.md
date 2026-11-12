# Design notes

## Why one lock?
Queue size, tenant queues, tokens, and job state must stay in sync.  
If we use many locks, two threads can both think “there is space” and both add a job — then we go over the limit.  
One lock avoids that. We hold it only for short bookkeeping.  
If later the lock is slow under heavy load, we can try one lock per tenant.

## Why wait when rate limit is hit?
Rate limit means “go slower”, not “reject the job”.  
So the job waits in that tenant’s queue.  
Workers then look at other tenants too, so one slow tenant does not block everyone.

## Cancel vs worker pickup
Both cancel and the worker try to take the job out of `QUEUED`.  
Whoever gets the lock first wins:
- **Worker wins:** job becomes `RUNNING`; cancel returns too late / false.
- **Cancel wins:** job becomes `CANCELLED` and is removed from the queue; it never runs.

A cancelled job does not use a rate-limit token. Token is spent only when the job is claimed to run.

## Why claim inside dispatch?
When we claim a job, we do three things together under the lock:
1. move it to `RUNNING`
2. spend one token
3. reserve one worker slot

So with 2 workers and queue size 3, we always see at most 2 running and 3 waiting — even before a worker thread wakes up. Tests stay clear and stable.

## If a job throws
User code can throw any error. We catch it, mark the job `FAILED`, and free the worker slot.  
Other jobs keep running; we do not lose a worker.

## Idle tenants (memory)
When a tenant’s waiting queue is empty and its rate limiter is full again, we drop that tenant’s queue and limiter.  
A full limiter matches a brand-new one, so forgetting never grants an extra burst.  
A tenant that still has a drained bucket is kept until it refills.  
A rejected submit that empties an existing tenant’s queue also marks that tenant idle.

## Finished history (memory)
Waiting bound and finished-history bound are separate. History defaults to 10_000 so a small
`maxWaitingJobs` does not forget recent statuses. Override via the full constructor when needed.

## Clock and wake (tests)
Rate limits need time. Any clock that jumps forward must notify `addAdvanceListener` listeners.  
`FakeClock.advance()` does that; the scheduler’s listener runs `wake()` so dispatch re-checks tokens.  
Real-time clocks can ignore the listener. No test uses timed pauses — FakeClock advances, or real-clock tests use bounded spins.
