# cluster-kit

Shared Aeron Cluster machinery for the two Open Exchange engines: the matching
engine ([openexch/match](https://github.com/openexch/match)) and the order and
ledger core ([openexch/core](https://github.com/openexch/core)). Java 21,
one jar, no engine code.

Status: nightly prerelease (v1.0.0-nightly.20261105). Not for production.

User documentation: https://openexchange.dev/docs

## Where it fits in v1

Both engines are replicated state machines on Aeron Cluster. Their state
machines differ (an order book, a ledger), but the machinery around them is the
same: authenticate sessions, take and verify snapshots, reclaim the log, report
health and readiness, stop the process cleanly. That machinery lives here once,
so a fix or feature added for one engine reaches the other as the same code.

The boundary is the state machine. Nothing in this repo knows what an order or
a balance is. Where the machinery needs an engine fact (its wire schema, its
auth policy, its image format), the engine passes it in.

## Packages

| Package | What it holds |
|---|---|
| `com.openexchange.cluster` | Snapshots and retention: `SnapshotCadence` (leader-side snapshot trigger, by log bytes with a timer floor), `SnapshotEnvelope` (magic, version, length, CRC32C around a snapshot payload), `SnapshotImageReader`, `SnapshotLogPruner`, `ArchiveHousekeeping`, `RetentionWatermark`, `ReaderRegistry`, `ClusterMemberPositions`, `StagingArchive`, `BundleCapture`. Node process: `EngineLifecycle`, `QuorumWatchdog`, `NodeEndpoint` (`/health`, `/ready`, `/metrics`), `NodeReadiness`, `ConsensusReadiness`, `ConsensusCommitObserver`. Wiring: `ClusterPorts`, `TimerPort`, `TermTimerId`. Driver and transport checks: `EmbeddedDriverDir` (refuses a live driver's directory, replaces a stale one), `SocketBufferProbe` (the kernel's grant of a socket buffer against the configured size). |
| `com.openexchange.cluster.auth` | Session authentication and authorisation shared by both engines: credentials, principals, key table, `ClusterAuthenticator`, `ClassAuthorisationService`. See [docs/auth.md](docs/auth.md). |
| `com.openexchange.cluster.log` | `ClusterLogReader` (reads a committed cluster log without a cluster), `ReplayContainer`, `ShadowEngine` (the apply interface an engine implements), `SkipWindow`. |
| `com.openexchange.cluster.replica` | The replica process: a non-member that follows the cluster through `ClusterBackup`, applies the log into its own engine instance and writes shadow images, with a trust check after every restore. See [docs/replica.md](docs/replica.md). |
| `com.openexchange.cluster.shadow` | Shadow images: a verified engine image at a log position written by a non-leader producer, plus the catalog, loader, floor and retention that use it. The follower producer's kit side: `ShadowProducerSettings`, `ProducerDesignation`, `ProducerWindowGuard`, `CrashedWriteSweep`, `WitnessAlarm`. See [docs/shadow.md](docs/shadow.md). |
| `com.openexchange.cluster.testkit` | JUnit-free determinism harnesses the engines run in their own tests: `RunTwiceAssert`, `SnapshotSpliceRunner`, `ShadowEngineConformance`, `ShadowRestoreRunner`, `LogSpliceRunner` (restore at every SNAPSHOT action of a recorded log, equal image at every later action), `FakeTimerService`, recorded-log fixtures. |

Readiness semantics are in [docs/readiness.md](docs/readiness.md); the replica's own `/ready` is in [docs/replica.md](docs/replica.md#readiness).

## Build and test

Requires JDK 21 and Maven.

```
mvn -B clean verify                          # compile and test against Aeron 1.53.0
mvn -B clean test -Daeron.version=1.53.1     # the same suite against Aeron 1.53.1
```

CI (`.github/workflows/build.yml`) runs both. Aeron 1.53.0 with Agrona 2.6.0 is
the compile floor set in `pom.xml`; the jar that is verified and published is
always the 1.53.0 build.

Test settings (system properties, defaults in brackets):

| Setting | Effect |
|---|---|
| `kit.pullrig.archive.threading` [`DEDICATED`] | archive threading mode of the pull test nodes; DEDICATED is the member's mode, `SHARED` hides the window between REPLICATE_END and a copy's recorded stop |
| `kit.test.pull.deadline.slack.ms` [`1500`] | how far past its deadline a startup pull may return in the deadline rows |
| `kit.test.pull.statistical` [`false`] | runs the statistical startup-pull control (`kit.test.pull.statistical.pulls` [`80`] pulls of 640 KB, none refused for an ACTIVE copy); not a CI row |

## Consuming it

```xml
<dependency>
  <groupId>com.openexchange</groupId>
  <artifactId>cluster-kit</artifactId>
  <version><!-- the version in pom.xml --></version>
</dependency>
```

- Published to GitHub Packages (`https://maven.pkg.github.com/openexch/cluster-kit`)
  from `main` only, and only when the version is not already published.
  Published versions are immutable. Reading from GitHub Packages needs a token
  with `read:packages`.
- Any change under `src/main` must bump `<version>` in `pom.xml`; a pull
  request that does not is rejected by CI.
- Aeron and Agrona are `provided` scope. Each engine brings and shades its own
  copy; this jar never adds a second one.
- Both engines must pin the same kit version.

## Configuration pointers

The kit reads only a few settings itself. Each is an environment variable or a
system property:

| Setting | Read by | Default |
|---|---|---|
| `CLUSTER_PORT_STRIDE` / `-Dcluster.port.stride` | `ClusterPorts` | `0` (every member on the same ports, one member per host). Set `100` when several members share a host. |
| `ENGINE_SHUTDOWN_TIMEOUT_MS` / `-Dengine.shutdown.timeout.ms` | `EngineLifecycle` | `4000` |
| `ENGINE_FATAL_EXIT_GRACE_MS` / `-Dengine.fatal.exit.grace.ms` | `EngineLifecycle` | `5000` |
| `TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS` / `-Dtransport.external.driver.timeout.ms` | `EngineLifecycle` | `30000` |
| `ENGINE_QUORUM_WAIT_MS` / `-Dengine.quorum.wait.ms` | `EngineLifecycle`, `QuorumWatchdog` | `120000` |
| `ENGINE_REJOIN_WAIT_MS` / `-Dengine.rejoin.wait.ms` | `EngineLifecycle`, `RejoinWatchdog` | `60000`. After this process has known a leader, a stretch without a leader (election state INIT, CANVASS, NOMINATE, CANDIDATE_BALLOT or FOLLOWER_BALLOT) that lasts this long **while a quorum of members is reachable** logs one CRITICAL line and exits 1 for a supervised restart. Size it above the election timeouts. It must exceed `ENGINE_REJOIN_PEER_WINDOW_MS`, or the boot is refused. |
| `ENGINE_REJOIN_PEER_WINDOW_MS` / `-Dengine.rejoin.peer.window.ms` | `EngineLifecycle`, `RejoinWatchdog`, `PeerReachability` | `10000` (1000..600000). How long a gap in the evidence of a reachable quorum is bridged before the rejoin watchdog's clock is cleared. Set it no shorter than the media driver's publication connection timeout. See [Rejoin and peer reachability](#rejoin-and-peer-reachability). |
| `ENGINE_LEADER_ELECT_WAIT_MS` / `-Dengine.leader.elect.wait.ms` | `EngineLifecycle`, `RejoinWatchdog` | `300000` (10000..3600000). How long a leader elect may stay in election state LEADER_LOG_REPLICATION or LEADER_READY, counted only **while a quorum of members is reachable**, before it logs one CRITICAL line and exits 1 for a supervised restart. Aeron leaves those states only once a quorum of followers reports its position, without a timeout. Size it above the time followers need to replicate the longest log a new leader may send them. It must exceed `ENGINE_REJOIN_PEER_WINDOW_MS`, or the boot is refused. |
| `ENGINE_REJOIN_PROBE_TERM_LENGTH` / `-Dengine.rejoin.probe.term.length` | `EngineLifecycle`, `PeerReachability` | `65536` (bytes, a power of two in 65536..1073741824). Term length of the rejoin watchdog's peer probe publications, which carry no data and open on a sparse term of their own instead of the consensus channel's. Each probe costs three terms on this member and as much again for the image on each peer, for as long as the member runs. A value outside the range or not a power of two refuses the boot. |
| `ENGINE_CATCHUP_REFUSALS` / `-Dengine.catchup.refusals` | `EngineLifecycle`, `CatchupWatchdog` | `3` (1..1000). Failed catch-up attempts in a row after which a member reads the leader's log start; a member below it exits 1. See [Log retention](#log-retention-and-a-member-that-was-away). |
| `ENGINE_CATCHUP_REFUSAL_WAIT_MS` / `-Dengine.catchup.refusal.wait.ms` | `EngineLifecycle`, `CatchupWatchdog` | `30000` (1000..3600000). The time bound next to the count: catch-up attempts failing for this long trigger the same read. |
| `RETENTION_FLOOR` / `-Dretention.floor` | `SnapshotLogPruner` | `apply`. `report` computes and logs the retention floor without applying it. |
| `RETENTION_MEMBER_WAIT_MS` / `-Dretention.member.wait.ms` | `SnapshotLogPruner`, `RetentionWatermark` | `1800000` (1000..604800000). How long the retention floor waits for a member that does not move before the purge proceeds without it. |
| `RETENTION_MEMBER_WAIT_BYTES` / `-Dretention.member.wait.bytes` | `SnapshotLogPruner`, `RetentionWatermark` | `2147483648` (1048576..1099511627776). How far behind the newest snapshot a member may be before the floor stops waiting for it. |
| `RETENTION_PROBE_TIMEOUT_MS` / `-Dretention.probe.timeout.ms` | `SnapshotLogPruner`, `CatchupWatchdog`, `LogExtent` | `5000` (100..60000). How long one read of the leader's log start from the leader's archive may take. |

A value outside its range refuses the start: `EngineLifecycle.install` parses
every row above before any thread starts.

One more setting is named by the kit and passed in by the host, because the kit
reads no environment for the replica: `TRANSPORT_DRIVER_CLIENT_LIVENESS_MS`
(default `5000`, 1000..600000) and `TRANSPORT_DRIVER_PUBLICATION_UNBLOCK_MS`
(default `10000`, 2000..1200000, above the liveness) are the embedded replica
driver's client liveness and publication unblock timeouts
(`ReplicaConfig.Builder.driverClientLivenessMs`, `driverPublicationUnblockMs`;
`ReplicaConfig.driverClientTimeoutsRefusal` is the range check). They are the
same names the members read for their own embedded driver. With an external
driver the aeronmd's `aeron.client.liveness.timeout` and
`aeron.publication.unblock.timeout` apply instead.

### Log retention and a member that was away

Every member purges its own log after a snapshot (`SnapshotLogPruner`). A
member that is down while the others snapshot and purge comes back at its old
position and asks the leader for the log from there, so no member may purge
past what a member still needs. The rule is the same whoever leads:

- The **leader** holds its purge at the lowest position of any member. It asks
  its own consensus module every few seconds and remembers what it hears, so a
  member that goes silent is held at the position the leader last saw.
- A **follower** never purges past the leader's log start position, which it
  reads from the leader's archive (the archive endpoint of the cluster member
  string; the response comes back to a free port on the follower's own archive
  host). That position is in the archive catalog on disk, not in one node's
  memory.
- A **new leader** therefore starts with a log that reaches at least as far
  back as the old leader's. A member it has not heard from in its term counts
  at the new leader's own log start: nothing more is purged until that member
  reports or the bound passes.

The floor does not hold the log for ever. A member that has held it for
`RETENTION_MEMBER_WAIT_MS`, or that is `RETENTION_MEMBER_WAIT_BYTES` behind the
newest snapshot, is written off: the leader logs
`[RETENTION] CRITICAL: member N STRANDED` and purges past it. The wait runs
from the last time the member's position moved, or the last time it answered
while level with the leader: on a quiet log a member that is alive and has
everything is not waiting, and its wait starts when it stops answering. A
node that cannot work out its floor (its consensus module does not answer, a
follower cannot reach the leader's archive) holds its whole log for the same
wait, then logs a CRITICAL line and purges to its own snapshot again; while
the floor is unknown the pruner's `lastError()` says so. The time half of
the bound runs on the local clock and restarts when leadership moves; the byte
half needs no clock.

A member that returns below the leader's log start cannot catch up: the leader
refuses every catch-up replay and the member times out and tries again. After
`ENGINE_CATCHUP_REFUSALS` failed attempts in a row (or
`ENGINE_CATCHUP_REFUSAL_WAIT_MS` of failing) the member's `CatchupWatchdog`
reads its own log position and the leader's log start. A member with no log
recording at all (its directories were wiped) counts at its newest snapshot's
position, or 0. Below the log start it logs one line and exits 1:

```
CRITICAL catch-up watchdog: 3 catch-up attempt(s) in a row failed over 2006 ms: this member's log position 16256 is below the log start position 393216 of the leader (member 2). ...
```

A supervisor that restarts it gets the same exit again: the member is loud
until it is reseeded. When the positions say the log is there, or the leader's
archive cannot be read, the member does not exit; it logs what it found and
checks again after the same number of further failures.

Both the pruner and the watchdogs open their own Aeron clients on the member's
driver directory, so they behave the same with an embedded driver and an
external one. The pruner takes that directory from the live Aeron client of
the archive context it is given, not from the context's directory name.

### Rejoin and peer reachability

The rejoin watchdog restarts a member whose election is stuck although its
peers are up. A restart cannot repair a quorum loss, and the survivor of one
may be the restore source of a recovering peer, so the watchdog trips only
with evidence that a quorum of members is reachable.

The evidence is outbound: the member holds one idle publication of its own to
every peer's consensus endpoint, and a peer counts as reachable while its
driver answers that publication with status messages. The member counts
itself, so in a three member cluster one answering peer makes a quorum. The
wait (`ENGINE_REJOIN_WAIT_MS`) is counted over the time the quorum has been
reachable: peers that return after a long outage get a full wait to finish the
election. A gap in the evidence shorter than `ENGINE_REJOIN_PEER_WINDOW_MS`
does not restart the clock, and time after the last positive observation is
never counted. A peer that just died still reads reachable until the media
driver's publication connection timeout passes (Aeron's default is 5 s): keep
the window at or above that timeout. `ENGINE_REJOIN_WAIT_MS` must exceed the
window, and a pair that does not refuses the boot.
While no quorum is reachable the member stays up, not ready, and says so once
per election when the wait passes.

A leader elect has the same gate with its own, longer wait
(`ENGINE_LEADER_ELECT_WAIT_MS`). Aeron 1.53 leaves LEADER_LOG_REPLICATION and
LEADER_READY only once a quorum of followers reports its append position, with
no timeout: a member whose voter vanished after the ballot, or whose followers
cannot hear it, stays leader elect for ever. The watchdog times each continuous
stretch in one of those two states within one election, over the time a quorum
is reachable, and restarts the member when it reaches the wait. Leaving the
state (LEADER_REPLAY, LEADER_INIT, CLOSED, a new election, canvass) restarts
the clock; a fall back to canvass is timed against `ENGINE_REJOIN_WAIT_MS`
from there. A leader elect stays in LEADER_LOG_REPLICATION while its followers
replicate the log it sends them, which is why this wait is longer than the
rejoin wait. A leader elect that has lost its quorum
stays up, as above. `ENGINE_LEADER_ELECT_WAIT_MS` must exceed the window.

Other settings are read by the engines and validated by the kit: the auth key
table and client credential (names per engine, see [docs/auth.md](docs/auth.md)),
the snapshot cadence thresholds (`SnapshotCadence`, defaults 1 GiB of log or
5 minutes), and the shadow settings `SHADOW_TERM_LENGTH`, `SHADOW_SCAN_PERIOD_MS`,
`SHADOW_DEEP_READ_PACE_MS`, `SHADOW_RETAIN_N`, `SHADOW_FLOOR_MARGIN`,
`SHADOW_ANCHOR_RULE` and the `SHADOW_PULL_*` puller settings (ranges in
[docs/shadow.md](docs/shadow.md)). The engine READMEs list the names they read.

## License

Apache License 2.0.
