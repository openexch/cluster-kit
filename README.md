# cluster-kit

Shared Aeron Cluster machinery for the two Open Exchange engines: the matching
engine ([openexch/match](https://github.com/openexch/match)) and the order and
ledger core ([openexch/assets](https://github.com/openexch/assets)). Java 21,
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
| `com.openexchange.cluster` | Snapshots and retention: `SnapshotCadence` (leader-side snapshot trigger, by log bytes with a timer floor), `SnapshotEnvelope` (magic, version, length, CRC32C around a snapshot payload), `SnapshotImageReader`, `SnapshotLogPruner`, `ArchiveHousekeeping`, `RetentionWatermark`, `ReaderRegistry`, `ClusterMemberPositions`, `StagingArchive`, `BundleCapture`. Node process: `EngineLifecycle`, `QuorumWatchdog`, `NodeEndpoint` (`/health`, `/ready`, `/metrics`), `NodeReadiness`, `ConsensusReadiness`, `ConsensusCommitObserver`. Wiring: `ClusterPorts`, `TimerPort`, `TermTimerId`. |
| `com.openexchange.cluster.auth` | Session authentication and authorisation shared by both engines: credentials, principals, key table, `ClusterAuthenticator`, `ClassAuthorisationService`. See [docs/auth.md](docs/auth.md). |
| `com.openexchange.cluster.log` | `ClusterLogReader` (reads a committed cluster log without a cluster), `ReplayContainer`, `ShadowEngine` (the apply interface an engine implements), `SkipWindow`. |
| `com.openexchange.cluster.replica` | The replica process: a non-member that follows the cluster through `ClusterBackup`, applies the log into its own engine instance and writes shadow images, with a trust check after every restore. See [docs/replica.md](docs/replica.md). |
| `com.openexchange.cluster.shadow` | Shadow images: a verified engine image at a log position written by a non-leader producer, plus the catalog, loader, floor and retention that use it. See [docs/shadow.md](docs/shadow.md). |
| `com.openexchange.cluster.testkit` | JUnit-free determinism harnesses the engines run in their own tests: `RunTwiceAssert`, `SnapshotSpliceRunner`, `ShadowEngineConformance`, `ShadowRestoreRunner`, `FakeTimerService`, recorded-log fixtures. |

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

Other settings are read by the engines and validated by the kit: the auth key
table and client credential (names per engine, see [docs/auth.md](docs/auth.md)),
the snapshot cadence thresholds (`SnapshotCadence`, defaults 1 GiB of log or
5 minutes), and the shadow settings `SHADOW_TERM_LENGTH`, `SHADOW_SCAN_PERIOD_MS`,
`SHADOW_DEEP_READ_PACE_MS`, `SHADOW_RETAIN_N`, `SHADOW_FLOOR_MARGIN` (ranges in
[docs/shadow.md](docs/shadow.md)). The engine READMEs list the names they read.

## License

Apache License 2.0.
