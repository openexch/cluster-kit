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
The consensus commit counter bounds that same cluster log in bytes. The counter
registration ID, leadership term and election count fence observations. A
changed fence, role, regressed position or unavailable source withdraws proof.

Sampling is limited to once per 10 ms; the remaining duty cycles only update
liveness and observe the role. Sampling reads local memory and the existing
mapped consensus heartbeat. It performs no counter scan, network/IPC query,
file mapping or wait. Defaults are:

- service liveness age below 30 s;
- readiness observation and consensus heartbeat age below 5 s;
- application lag at most 64 KiB;
- an applied, previously observed committed checkpoint less than 1 s old.

A checkpoint remains the target until applied. This permits bounded lag under
continuous load without requiring equality with a constantly moving commit
counter. In an idle cluster, observing the same applied checkpoint with a fresh
consensus heartbeat renews the proof. Missing information never becomes ready.
`needsReseed` is terminal for the current process; it cannot be cleared by
subsequent ticks or role observations.

The local commit counter is **not the leader's latest remote commit position**.
It proves service catch-up to local consensus. This API alone does not establish
cross-member replication distance or authorise the next rolling restart. A
rollout must also fence the current leader/term, compare independently observed
member checkpoints and stop when those observations are unavailable or stale.

The HTTP body includes role, recovery reason, source, term, election count,
applied/commit/checkpoint positions and observation ages. Status and body are
computed from the same published observation.

Source contracts:
[service agent](https://github.com/aeron-io/aeron/blob/1.53.0/aeron-cluster/src/main/java/io/aeron/cluster/service/ClusteredServiceAgent.java),
[consensus agent](https://github.com/aeron-io/aeron/blob/1.53.0/aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModuleAgent.java).
