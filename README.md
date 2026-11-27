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
| `com.openexchange.cluster` | Snapshots and retention: `SnapshotCadence` (leader-side snapshot trigger, by log bytes with a timer floor), `SnapshotEnvelope` (magic, version, length, CRC32C around a snapshot payload), `SnapshotLayouts` (the snapshot layouts one build reads, the current and the previous one, and the layout it writes, picked by the recorded behaviour version), `WireCeiling` (the ingress wire gate: a frame above the recorded ceiling is dropped from its header before the jar range is consulted; the ceiling only rises; a jar below it stops), `SnapshotImageReader`, `SnapshotLogPruner`, `ArchiveHousekeeping`, `RetentionWatermark`, `ReaderRegistry`, `ClusterMemberPositions`, `StagingArchive`, `BundleCapture`. Node process: `EngineLifecycle`, `QuorumWatchdog`, `NodeEndpoint` (`/health`, `/ready`, `/metrics`), `NodeReadiness`, `ConsensusReadiness`, `ConsensusCommitObserver`, `StallMetrics` (Aeron's duty-cycle and snapshot-duration counters as `/metrics` series), `LogTimestamps` (one time format at the start of every log line). Wiring: `ClusterPorts` (UDP offsets 1..5 and the HTTP endpoint at offset 6), `ConsensusChannel` (the consensus term), `TimerPort`, `TermTimerId`. Driver and transport checks: `EmbeddedDriverDir` (refuses a live driver's directory, replaces a stale one), `SocketBufferProbe` (the kernel's grant of a socket buffer against the configured size). |
| `com.openexchange.cluster.auth` | Session authentication and authorisation shared by both engines: credentials, principals, key table, `ClusterAuthenticator`, `ClassAuthorisationService`. See [docs/auth.md](docs/auth.md). |
| `com.openexchange.cluster.journal` | Journal retention: `JournalRetention` (the one purge owner for a member's journal recordings: a record goes only when every registered reader has applied it), `JournalRetentionAgent` (its thread inside a member), `JournalReaders` and `JournalReaderTool` (the reader cursor files and the command that writes them), `JournalProfile` (what an engine tells the kit about one journal), `RecordingTail` (the last message of a recording, read without a replay that starts inside a frame). See "Journal retention" below. |
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
| `TRANSPORT_CONSENSUS_TERM_LENGTH` / `-Dtransport.consensus.term.length` | `ConsensusChannel` (both engines, the replica) | `1m`. The consensus channel's term; a power of two in [64k, 1g], anything else refuses the boot. Without it the channel took the driver's term (16m): 192 MB of `/dev/shm` per member of a three-member cluster. |
| `ENGINE_SHUTDOWN_TIMEOUT_MS` / `-Dengine.shutdown.timeout.ms` | `EngineLifecycle` | `4000` |
| `ENGINE_FATAL_EXIT_GRACE_MS` / `-Dengine.fatal.exit.grace.ms` | `EngineLifecycle` | `5000` |
| `TRANSPORT_EXTERNAL_DRIVER_TIMEOUT_MS` / `-Dtransport.external.driver.timeout.ms` | `EngineLifecycle` | `30000` |
| `ENGINE_QUORUM_WAIT_MS` / `-Dengine.quorum.wait.ms` | `EngineLifecycle`, `QuorumWatchdog` | `120000` |
| `ENGINE_REJOIN_WAIT_MS` / `-Dengine.rejoin.wait.ms` | `EngineLifecycle`, `RejoinWatchdog` | `60000`. After this process has known a leader, a stretch without a leader (election state INIT, CANVASS, NOMINATE, CANDIDATE_BALLOT or FOLLOWER_BALLOT) that lasts this long **while a quorum of members is reachable** logs one CRITICAL line and exits 1 for a supervised restart. A peer counts as reachable only once its consensus module has left startup recovery, so a peer still loading its snapshot (a restore that may take minutes) never starts this wait. Size it above the election timeouts. It must exceed `ENGINE_REJOIN_PEER_WINDOW_MS`, or the boot is refused. |
| `ENGINE_REJOIN_PEER_WINDOW_MS` / `-Dengine.rejoin.peer.window.ms` | `EngineLifecycle`, `RejoinWatchdog`, `PeerReachability` | `10000` (1000..600000). How long a gap in the evidence of a reachable quorum is bridged before the rejoin watchdog's clock is cleared. Set it no shorter than the media driver's publication connection timeout; with an external driver, no shorter than its client liveness timeout plus that connection timeout. See [Rejoin and peer reachability](#rejoin-and-peer-reachability). |
| `ENGINE_LEADER_ELECT_WAIT_MS` / `-Dengine.leader.elect.wait.ms` | `EngineLifecycle`, `RejoinWatchdog` | `300000` (10000..3600000). How long a leader elect may stay in election state LEADER_LOG_REPLICATION or LEADER_READY, counted only **while a quorum of members is reachable**, before it logs one CRITICAL line and exits 1 for a supervised restart. Aeron leaves those states only once a quorum of followers reports its position, without a timeout. Size it above the time followers need to replicate the longest log a new leader may send them. It must exceed `ENGINE_REJOIN_PEER_WINDOW_MS`, or the boot is refused. |
| `ENGINE_REJOIN_PROBE_TERM_LENGTH` / `-Dengine.rejoin.probe.term.length` | `EngineLifecycle`, `PeerReachability` | `65536` (bytes, a power of two in 65536..1073741824). Term length of the rejoin watchdog's peer probe publications, which carry no data and open on a sparse term of their own instead of the consensus channel's. Each probe costs three terms on this member and as much again for the image on each peer, for as long as the member runs. A value outside the range or not a power of two refuses the boot. |
| `ENGINE_REJOIN_PROBE_STREAM_ID` / `-Dengine.rejoin.probe.stream.id` | `EngineLifecycle`, `PeerReachability` | `4306` (1..2147483647). Stream id of the participation subscription each member opens on its own consensus endpoint once its consensus module has left startup recovery, and of the peer probes aimed at it. Every member of a cluster must use the same value. A value outside the range refuses the boot; a value equal to the consensus stream id (`aeron.cluster.consensus.stream.id`) refuses to arm the watchdogs. |
| `ENGINE_CATCHUP_REFUSALS` / `-Dengine.catchup.refusals` | `EngineLifecycle`, `CatchupWatchdog` | `3` (1..1000). Failed catch-up attempts in a row after which a member reads the leader's log start; a member below it exits 1. See [Log retention](#log-retention-and-a-member-that-was-away). |
| `ENGINE_CATCHUP_REFUSAL_WAIT_MS` / `-Dengine.catchup.refusal.wait.ms` | `EngineLifecycle`, `CatchupWatchdog` | `30000` (1000..3600000). The time bound next to the count: catch-up attempts failing for this long trigger the same read. |
| `RETENTION_FLOOR` / `-Dretention.floor` | `SnapshotLogPruner` | `apply`. `report` computes and logs the retention floor without applying it. |
| `RETENTION_MEMBER_WAIT_MS` / `-Dretention.member.wait.ms` | `SnapshotLogPruner`, `RetentionWatermark` | `1800000` (1000..604800000). How long the retention floor waits for a member that does not move before the purge proceeds without it. |
| `RETENTION_MEMBER_WAIT_BYTES` / `-Dretention.member.wait.bytes` | `SnapshotLogPruner`, `RetentionWatermark` | `2147483648` (1048576..1099511627776). How far behind the newest snapshot a member may be before the floor stops waiting for it. |
| `RETENTION_PROBE_TIMEOUT_MS` / `-Dretention.probe.timeout.ms` | `SnapshotLogPruner`, `CatchupWatchdog`, `LogExtent` | `5000` (100..60000). How long one read of the leader's log start from the leader's archive may take. |
| `RETENTION_RECHECK_MAX_MS` / `-Dretention.recheck.max.ms` | `SnapshotLogPruner` | `60000` (1000..3600000). After a purge pass the retention floor stopped, the floor is read again without a new snapshot: one poll interval later, then doubling up to this bound; a floor that rose starts a purge pass at once (a follower follows its leader's log start). |
| `RETENTION_OFFBOX` / `-Dretention.offbox` | `SnapshotLogPruner`, `OffBoxWatermark`, `ArchiveHousekeeping` | `off`. `required`: the purge never passes the member's off-box watermark, and a watermark that is missing, invalid, foreign or above the commit position holds the whole log. `report`: the same evaluation, logged and exported, not applied. `off` with a watermark file present refuses the start. |
| `RETENTION_OFFBOX_ALARM_BYTES` / `-Dretention.offbox.alarm.bytes` | `OffBoxWatermark` | `2147483648` (1048576..1099511627776). Log bytes held for the off-box copy above which every purge pass prints a CRITICAL line. An alarm, not a limit. |
| `RETENTION_OFFBOX_STALE_MS` / `-Dretention.offbox.stale.ms` | `OffBoxWatermark` | `3600000` (1000..604800000). Age of the watermark file above which every purge pass prints a CRITICAL line. It does not change the floor. |
| `JOURNAL_RETENTION_<JOURNAL>` / `-Djournal.retention.<journal>` | `JournalRetention`, `JournalRetentionAgent` | `off`. One setting per journal of the engine (the core's are `JOURNAL_RETENTION_MONEY` and `JOURNAL_RETENTION_ORDER`, the matching engine's is `JOURNAL_RETENTION_SETTLEMENT`). `off`: nothing of the journal is ever purged. `report`: evaluated, logged and exported, nothing purged. `readers`: the member purges its journal recordings behind the registered readers. |
| `JOURNAL_RETENTION_REQUIRED_READERS_<JOURNAL>` / `-Djournal.retention.required.readers.<journal>` | `JournalRetention` | empty. Reader names, separated by commas, that must have a valid file; one that has none holds the journal. |
| `JOURNAL_RETENTION_POLL_MS` / `-Djournal.retention.poll.ms` | `JournalRetentionAgent` | `60000` (1000..3600000). The interval between two passes. |
| `JOURNAL_RETENTION_PASS_TIMEOUT_MS` / `-Djournal.retention.pass.timeout.ms` | `JournalRetentionAgent` | `10000` (1000..600000). The budget of one pass over one journal (listing, one-record reads, purges). |
| `JOURNAL_RETENTION_ALARM_BYTES` / `-Djournal.retention.alarm.bytes` | `JournalRetention` | `2147483648` (1048576..1099511627776). Bytes of one journal a member holds above which every pass prints a CRITICAL line. An alarm, not a limit. |
| `JOURNAL_RETENTION_STALE_MS` / `-Djournal.retention.stale.ms` | `JournalRetention` | `3600000` (1000..604800000). Age of the oldest reader file above which every pass prints a CRITICAL line. It does not change the bound. |

A value outside its range refuses the start: `EngineLifecycle.install` parses
every row above before any thread starts. The `JOURNAL_RETENTION_*` rows are the
exception: the engine parses them at boot with the names of its journals
(`JournalRetention.Settings.fromEnv`), and a bad value refuses the start there.

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

### Log retention and a copy off the cluster's hosts

A deployment that keeps a copy of the cluster log off every host of the cluster
(a bundle store, a replica host) can make that copy bound the purge:
**retention never deletes what is not yet durably off-box.** The fact "the log
is durably off-box below position X" is about storage outside the cluster, so it
is not carried in the replicated log. It lives in one small file per member,
`<clusterDir>/retention/offbox-watermark`, written by a command run on that
member's host:

```
java -cp <engine jar> com.openexchange.cluster.OffBoxWatermarkTool
  set     --cluster-dir <dir> --engine core|match --cluster-id <n> --genesis-id <g>
          --log-position-bytes <p> --source <name> [--allow-lower]
  release --cluster-dir <dir> --genesis-id <g> --up-to-log-position-bytes <p> --reason <text>
  show    --cluster-dir <dir>
  clear   --cluster-dir <dir> --genesis-id <g>
```

Every value is a named flag with its unit in the name; a positional argument,
an unknown, missing or repeated flag exits 64. A refused input exits 65 and
leaves the file as it was: another engine, cluster id or genesis id than the
file's (`clear` first), a lower position without `--allow-lower`, a wrong
genesis id on `release` or `clear`. An I/O failure exits 1. The archiver runs
`set` on each member host after each verified upload and repeats the last
value from time to time; a member that missed an update holds more log and
nothing else.

The file is text, one `key=value` per line, LF line ends, in this order:
`format=offbox-watermark/1`, `engine`, `cluster_id`, `genesis_id`,
`log_position_bytes`, `released_up_to_bytes` (-1: none), `release_reason`,
`seq` (+1 on every write), `written_at_epoch_ms`, `source`, `sha256` (over
every byte above its line). It is replaced by temp file, fsync, atomic rename
and fsync of the directory. Anything else in the file makes it invalid.

With `RETENTION_OFFBOX=required` the pruner reads the file on every wake and
purges to the lowest of the node's own floor, the retention floor and the
off-box floor. Nothing is remembered between two reads:

| File and identity | State | Off-box floor |
|---|---|---|
| no file | `ABSENT` | 0 (hold everything) |
| unknown key, missing key, wrong order, number out of range, CR, digest mismatch | `INVALID` | 0 |
| the member has no recorded genesis id yet | `UNBOUND` | 0 |
| `engine`, `cluster_id` or `genesis_id` differs from the member's | `FOREIGN` | 0 |
| `log_position_bytes` above the member's commit position, or the commit position cannot be read | `AHEAD` | 0 |
| valid | `VALID` | `max(log_position_bytes, released_up_to_bytes)` |

Under `off` the states are `OFF` (no file) and `IGNORED` (a file appeared
after the start, CRITICAL on every pass); neither bounds the purge.

Unlike the retention floor this bound has no wait and no give-up. A dead
archiver holds the log, the disk grows, and the alarm lines fire
(`RETENTION_OFFBOX_ALARM_BYTES`, `RETENTION_OFFBOX_STALE_MS`). The only way
past is an operator's decision, per member: `release` raises the floor to the
released position, and while it is above the durable position every pass
prints `[RETENTION] CRITICAL: off-box hold released by operator up to <P>
(durable <X>): ...; reason: <text>`. The next `set` at or above the released
position clears the release.

What a member cannot see: a watermark at or below its commit position that
the store does not in fact hold. The member purges to it. The position must
come only from a verified upload; `source` and `seq` are in every pass line
for audit, and a store that lost data lowers the position with
`--allow-lower`.

Each pass prints `[RETENTION] off-box: floor <f> state <S> position <X> seq
<n> age <ms> source <s>`, and under `required` any state other than `VALID`
is the pruner's `lastError()`. `/ready` is not failed by a hold: a member that
keeps its log is healthy. `OffBoxWatermark.Gauge` lists the gauges the
engines export under their own prefix: `retention_offbox_mode` (0 off, 1
report, 2 required), `retention_offbox_state` (0 VALID, 1 ABSENT, 2 INVALID,
3 UNBOUND, 4 FOREIGN, 5 AHEAD, 6 IGNORED, 7 OFF),
`retention_offbox_position_bytes`, `retention_offbox_held_bytes`,
`retention_offbox_age_ms`, `retention_offbox_released_bytes`.

A follower never purges past the leader's log start, so a leader that holds
also holds its followers. Each member obeys its own setting: set the same
mode on every member, and turn `required` on only once every member runs a
build that knows it (write the watermark on every member first, then restart
with `RETENTION_OFFBOX=required`). To turn it off: `clear` on the member,
then restart with `off`.

`ArchiveHousekeeping` (the command line purge) obeys the same file under the
same setting. It is not the member, so it is told which member this is:
`--offbox-engine=core|match --offbox-cluster-id=N --offbox-genesis-id=G`.
Without them, under `required`, it purges nothing.

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

### Journal retention

An engine's journals (the core's money journal and order journal, the matching
engine's settlement journal) are recordings in an archive of the member (the
agent is given that archive's client context). `journal.JournalRetention` is the only
class that purges one, and `journal.JournalRetentionAgent` runs it inside the
member on its own thread (`journal-retention`) with its own archive session.
A record is deleted from a member only when every registered reader has
durably applied it. Outside processes only deliver the readers' cursors.

A reader is registered by a file on the member,
`<clusterDir>/retention/journal-readers/<journal>/<reader>`, written by a
command run on that member's host (the engine jars carry it):

```
java -cp <engine jar> com.openexchange.cluster.journal.JournalReaderTool
  set     --cluster-dir <dir> --engine core|match --cluster-id <n> --genesis-id <g> --journal <j>
          --reader <r> --applied-journal-seq <s> --source <name> [--allow-lower]
  release --cluster-dir <dir> --genesis-id <g> --journal <j> --reader <r> --up-to-journal-seq <s> --reason <text>
  remove  --cluster-dir <dir> --genesis-id <g> --journal <j> --reader <r> --reason <text>
  show    --cluster-dir <dir>
```

Exit codes: 0 done, 64 usage (a positional value, an unknown, missing or
repeated flag), 65 refused input, 1 I/O failure. Registering a reader is its
first `set` (0 is legal) and is done before the reader first starts; a reader
with no file is not protected. `--applied-journal-seq` says "this reader has
durably applied every record of this genesis up to and including this
`journalSeq`". A lower value needs `--allow-lower` and prints a CRITICAL line.
Journal and reader names are 1 to 64 of `A-Z a-z 0-9 . _ -`.

The file is text, one `key=value` per line with LF line ends, in this order:
`format=journal-reader/1`, `engine`, `cluster_id`, `genesis_id`, `journal`,
`reader`, `applied_journal_seq`, `released_up_to_journal_seq`,
`release_reason`, `seq`, `written_at_epoch_ms`, `source`, `sha256` (over every
byte above its own line). It is replaced through a temp file, an fsync and an
atomic rename.

Every pass works from the files, the archive and the member's identity as they
are at that moment; nothing is remembered between passes. The states
(`JournalRetention.State`, the value of the state gauge):

| state | when | effect |
|---|---|---|
| `VALID` (0) | otherwise | purge up to `safeSeq`, the lowest `max(applied, released)` over the readers |
| `OFF` (1) | the journal's mode is `off` | nothing purged; reader files are ignored |
| `NO_JOURNAL` (2) | no recording of this genesis with bytes | nothing to do, no alarm |
| `UNBOUND` (3) | the member has no recorded genesis id yet | hold |
| `NO_READERS` (4) | no reader file | hold, CRITICAL on every pass |
| `INVALID` (5) | a reader file is invalid | hold, CRITICAL |
| `FOREIGN` (6) | a reader file names another engine, cluster, genesis, journal or reader | hold, CRITICAL |
| `TAIL_UNKNOWN` (7) | the member's own journal tail cannot be read | hold |
| `AHEAD` (8) | a reader's bound is above the member's own journal tail | hold; it clears when the member catches up |
| `MISSING` (9) | a required reader has no file | hold, CRITICAL |
| `OFFBOX_HOLD` (10) | `RETENTION_OFFBOX=required` and the off-box watermark is not VALID | hold |

A reader that stops moving holds the journal; nothing gives up for it. The
disk grows and two alarms are printed (`JOURNAL_RETENTION_ALARM_BYTES`,
`JOURNAL_RETENTION_STALE_MS`). The way past it is an operator's `release`
(said on every pass until a `set` reaches it) or `remove`, per member.

A purge removes whole segments. The first record at a segment base is read
through a bounded replay, and the base qualifies when that record is a whole
record of the journal's schema, its sequence is at most `safeSeq + 1`, its
epoch is the member's genesis (where the journal's records carry one), and,
under `RETENTION_OFFBOX=required`, its log position is at or below the
off-box floor. An active recording keeps two whole segments below its head. A
stopped recording whose last sequence is at or below `safeSeq` is deleted
whole, unless it is the recording that carries the member's journal tail; any
other stopped recording keeps its last segment. A recording whose records
carry another epoch is never touched. A recording whose sequence does not
rise with its positions is not purged and is reported as an ANOMALY. So is a
recording whose last record is this genesis' and whose first record carries
another genesis' epoch (one process run that wrote two epochs): it is neither
cut nor deleted. Epoch 0 at a recording's start reads as "written before the
member had a genesis" and is no other genesis'; an engine that never writes
before its genesis hands a strict profile (`JournalProfile.strictStartEpoch()`),
for which epoch 0 at the start is an ANOMALY too. A purge
the archive refuses (a reader's replay is below the target) is counted and
tried again on the next pass.

An engine hands the agent one `JournalProfile` per journal (stream, schema
id, the offsets of `journalSeq`, epoch and log position). The gauges
(`JournalRetentionAgent.renderMetrics(prefix)`, each labelled `journal`):
`<prefix>_journal_retention_mode`, `_state`, `_safe_seq`, `_tail_seq`,
`_retained_bytes`, `_recordings`, `_recordings_foreign`,
`_recordings_anomaly`, `_oldest_reader_age_ms`, `_purged_bytes_total`,
`_purge_failures_total`, `_last_pass_age_ms`, and per reader
`_reader_applied_seq{journal,reader}` and `_reader_released_seq{journal,reader}`.
`_retained_bytes` is exported under `off` too. `_recordings_anomaly` counts
the recordings that cannot be cut as far as their readers allow: a sequence
that does not rise, or an unreadable record at the start or at a segment base.
`lastError()` is the text for `/health` when a journal in mode `readers` is
held, when a pass fails or when the thread died; a hold never fails `/ready`.

The thread never throws into the member and never ends silently. Each
journal's pass is on its own: an exception or an error out of one is counted
(`_pass_failures_total{journal}`), said on a CRITICAL line, and the other
journal is still passed. `<prefix>_journal_retention_agent_state` is the
thread: 0 not running, 1 running, 2 the last pass failed, 3 dead until the
member restarts, 4 the engine could not start it
(`JournalRetentionAgent.renderNotStarted`). Alert rules have to fire on an
absent series as well as on a bad value.

The last record of each recording (the member's journal tail) is read by
`journal.RecordingTail`. Every replay it asks for starts where a frame starts:
the recording's start, a term base, or the end of the last message it read
from the same recording on an earlier pass. An archive raises a refused replay
start into the node's error handler, so a reader that probes for the last
frame fills the error log of a healthy member; this one asks for none. The
first look at a recording replays at most its last term, a later look only
what was recorded since, and nothing when the end has not moved. The record it
answers is the last whole message of the recording's last term, whatever it
remembers. A caller that reads a tail once (a restore) makes a `RecordingTail`
for the read; one that reads it again and again keeps it.

What a tail read costs: a first look replays from the base of the recording's
last term (or its start) to its end, so up to one journal term per recording,
read from disk and copied through an IPC replay publication the archive creates
with the recording's term length (three terms mapped sparse; pages are touched
as they are written, about the replayed bytes). The agent pays that on its
first pass after the member started, on the first pass after a failed pass
(the agent then opens a new archive session with new ports), after a stop and
a start of the agent, and for a recording whose end moved into a later term
since the pass before (more than the rest of a term was recorded between two
passes; at the default 60 s poll that is a journal writing more than a term a
minute). Every other pass replays the bytes recorded since, and nothing for a
recording that did not grow. Two series show it per journal:
`<prefix>_journal_retention_tail_read_bytes{journal}` (bytes the last
completed pass asked the archive to replay for tail reads) with
`_tail_read_bytes_total{journal}`, and
`<prefix>_journal_retention_last_pass_duration_ms{journal}`. The agent's ports
belong to its thread: no other thread reaches one, and a thread started after
a stop begins without any.

The negative-control seams (`JournalRetention.Seams`) are public statics for
the engines' tests; `JournalRetentionSeamsTest` holds them off by default and
unwritten by any main class.

### Duty-cycle and snapshot-duration series

`StallMetrics` exports the counters Aeron keeps per member for its slowest
work cycle and its slowest snapshot, so "does a snapshot stall this member,
and for how long" is read from `/metrics`. An engine opens one with its metric
prefix, its media driver's directory, its cluster id and service id
(`StallMetrics.open`), starts it and appends `render` to its metrics body. It
reads the driver's counters file through a read-only mapping of its own, so it
opens no Aeron client and is not hurt by one that closes; both driver modes.

| Source (Aeron counter type id) | Maximum `M` | Over-threshold count `C` |
|---|---|---|
| consensus module cycle (216, 217) | `<prefix>_consensus_module_max_cycle_time` | `<prefix>_consensus_module_cycle_time_exceeded` |
| clustered service cycle (218, 219) | `<prefix>_service_max_cycle_time` | `<prefix>_service_cycle_time_exceeded` |
| snapshot of every service, timed by the consensus module (234, 235) | `<prefix>_snapshot_total_max_duration` | `<prefix>_snapshot_total_duration_exceeded` |
| snapshot of this service, timed by its container (236, 237) | `<prefix>_snapshot_service_max_duration` | `<prefix>_snapshot_service_duration_exceeded` |
| driver conductor, sender, receiver cycle (system counters 26..31) | `<prefix>_driver_{conductor,sender,receiver}_max_cycle_time` | `<prefix>_driver_{conductor,sender,receiver}_cycle_time_exceeded` |
| archive agent cycle (103, 104), labels `archive_id` and `agent` (`conductor`; with dedicated archive threads also `recorder` and `replayer`) | `<prefix>_archive_max_cycle_time` | `<prefix>_archive_cycle_time_exceeded` |

Each maximum is three gauges: `M_seconds` (Aeron's counter: the maximum since
the component started), `M_last_rise_age_seconds` (seconds since it last rose,
-1 when it has not risen since the member began to sample it) and
`M_recent_seconds` (`M_seconds` when it rose within the last 300 s, else 0).
Each count is `C_total` (Aeron's counter), `C_last_age_seconds` and `C_recent`
(its rise between the newest sample and the one 300 samples before it: the
last 300 one-second intervals). Aeron keeps one maximum: a slow cycle below the
standing maximum does not move `M`, and shows in `C` when it was over the
component's threshold. A source with no Aeron counter on the member has no
series. `<prefix>_stall_sampler_age_seconds` is the time since the counters
were last read (-1 before the first read).

The counters are read once a second on the object's own daemon thread
(`stall-metrics-sampler`), never on a cluster service, consensus or driver
thread: one volatile read per source (14, and two per archive agent) and no
allocation.

### Log line times

`LogTimestamps.install()`, called first in a process's `main`, wraps
`System.out` and `System.err` so every line starts with its time in ISO-8601
UTC with milliseconds: `2026-11-27T11:44:31.654Z [SNAPSHOT] ...`. A line that
already starts with a time in that format (a logger that stamps its own lines
with `LogTimestamps.now()`), a continuation line (it starts with a tab, a
space, `Caused by: ` or `Suppressed: `) and an empty line are left as they
are. The head line of a printed stack trace is a line like any other and gets
its time. The text after the time is not changed. A line is collected and
handed to the stream below in one write (time, text, newline) when its newline
arrives, so two threads, or out and err over one file, never interleave inside
a line; a line without a newline is written on `flush()`; a line longer than
8192 bytes is written in pieces of that size, the time once before the first.

`EngineLifecycle.install(name, log, info)` takes a second sink for the lines
that are information (a watchdog was armed, and with which bounds), so an
engine whose `log` writes at its error level prints no error line on a healthy
start.

### Rejoin and peer reachability

The rejoin watchdog restarts a member whose election is stuck although its
peers are up. A restart cannot repair a quorum loss, and the survivor of one
may be the restore source of a recovering peer, so the watchdog trips only
with evidence that a quorum of members is reachable.

The evidence is outbound: the member holds one idle publication of its own to
every peer's consensus endpoint, on the participation stream
(`ENGINE_REJOIN_PROBE_STREAM_ID`), and a peer counts as reachable while its
driver answers that publication with status messages. The member counts
itself, so in a three member cluster one answering peer makes a quorum.

A peer answers only once its consensus module has left startup recovery: each
member opens one participation subscription on its own consensus endpoint when
its module state leaves INIT, and keeps it until the member stops. Nothing
reads it. Aeron's consensus module subscribes to the consensus stream as soon
as it is constructed, but it cannot vote until its clustered services have
loaded their snapshots, which can take minutes. A peer in that restore
therefore reads unreachable, and the survivor of a quorum loss stays up while
the peer restores, possibly from the survivor itself. A running peer reads
reachable, including one in the stuck election the watchdog exists for. A peer
on an older kit has no participation subscription and reads unreachable, so
with mixed versions the watchdog does not trip. The
wait (`ENGINE_REJOIN_WAIT_MS`) is counted over the time the quorum has been
reachable: peers that return after a long outage get a full wait to finish the
election. A gap in the evidence shorter than `ENGINE_REJOIN_PEER_WINDOW_MS`
does not restart the clock, and time after the last positive observation is
never counted. A peer that just died still reads reachable until the media
driver's publication connection timeout passes (Aeron's default is 5 s): keep
the window at or above that timeout. With an external driver the driver
outlives a halted member process, and keeps that process's subscriptions,
the participation subscription included, until its client liveness timeout
(`aeron.client.liveness.timeout`, Aeron's default is 10 s) passes; only then
do the status messages stop and the connection timeout start. There a peer
that just died reads reachable for up to the sum of the two (15 s with
Aeron's defaults), and the window has to be at or above that sum.
`ENGINE_REJOIN_WAIT_MS` must exceed the window, and a pair that does not
refuses the boot.
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
