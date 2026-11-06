# Node readiness evidence

`/health` tracks service duty-cycle activity, including idle cycles. `/ready`
requires service application evidence and a live, active consensus module with
its election closed. A role transition alone cannot establish readiness.

`ConsensusReadiness` reads the already-open `ConsensusModule.Context`. In Aeron
1.53.0, ordinary background work follows log application, but `idle()` can also
reenter background work before a callback returns. Engines must suppress consensus
observation during such reentry: `Cluster.logPosition()` is set before the callback
and is not completion evidence while that callback is still running. Both engines
guard their application callbacks and publish observations only after return.
Both local consensus commit and received leader commit positions use that same
cluster log byte space. The counter registration ID, leadership term, election
count and remote image source fence observations. A changed fence, role,
regressed position or unavailable source withdraws proof.

Sampling is limited to once per 10 ms; the remaining duty cycles only update
liveness and observe the role. Sampling reads local memory, the existing mapped
consensus heartbeat and at most 32 fragments of an untethered consensus
subscription. This subscription is registered during service startup and closed
on termination. Poll performs no registration, counter scan, blocking query,
file mapping or wait. Defaults are:

- service liveness age below 30 s;
- readiness observation, consensus heartbeat and leader commit message age below 5 s;
- application lag at most 64 KiB;
- an applied, previously observed committed checkpoint less than 1 s old.

A checkpoint remains the target until applied. This permits bounded lag under
continuous load without requiring equality with a constantly moving commit
counter. In an idle cluster, observing the same applied checkpoint with a fresh
consensus heartbeat renews the proof. Missing information never becomes ready.
`needsReseed` is terminal for the current process; it cannot be cleared by
subsequent ticks or role observations.

The local commit counter does not expose the follower's highest leader-notified
commit. A follower therefore also observes Aeron's existing `CommitPosition`
messages on its own configured consensus endpoint and stream. It requires a
known remote member, the current closed-election term, a fresh image source,
and an applied checkpoint bounded by the larger local/leader commit. Old terms,
regressed positions, conflicting leaders in one term and missing subscriptions
cannot establish readiness. A full fragment batch is treated as observer backlog
until the available queue is drained. The observer does not publish messages or
change membership, snapshots, retention or the application wire schema.

This shares Aeron's configured consensus transport trust boundary; it adds no
independent peer authentication. A leader uses its local quorum commit evidence.
A rollout must still compare independently observed member checkpoints and stop
when the recovering member has not applied the observed leader checkpoint.

The HTTP body includes role, recovery reason, source, term, election count,
applied/commit/checkpoint positions and observation ages. Status and body are
computed from the same published observation. On followers, `commit` is the
larger local/observed-leader bound; `consensusAgeMs` conservatively includes the
age of that leader message. An instantaneous remote position remains unknowable
between messages; the declared freshness and lag bounds apply.

Source contracts:
[service agent](https://github.com/aeron-io/aeron/blob/1.53.0/aeron-cluster/src/main/java/io/aeron/cluster/service/ClusteredServiceAgent.java),
[consensus agent](https://github.com/aeron-io/aeron/blob/1.53.0/aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModuleAgent.java).
