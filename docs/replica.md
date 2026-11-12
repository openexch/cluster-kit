# Replica process (`com.openexchange.cluster.replica`)

A replica is a fourth process next to a three-member cluster. It is not a
member: it never votes, holds no client sessions and sends the members nothing
but backup queries. It follows the cluster through Aeron `ClusterBackup`,
applies the committed log into its own engine instance and writes
[shadow images](shadow.md) from that state. The members pay nothing for those
images.

The engine supplies the state machine (`ReplicaEngine`), parses its own
settings into a `ReplicaConfig` and starts `ReplicaProcess`. The kit never reads
an environment variable; the engine README lists the setting names (in core:
`java -jar core-cluster.jar replica`, settings `REPLICA_*`).

## Threads

| Thread | Job |
|---|---|
| `ClusterBackup` agent | Queries a member every query interval, retrieves the member's newest snapshot set, records the live log L into the replica's own archive. |
| `shadow-reader-R<slot>` | Bootstrap once, then follow L, apply it to the engine, raise shadow triggers. |
| shadow writer | Serializes, self-checks and records shadow images. Reads the fence latch before every write. |
| `shadow-housekeeping-R<slot>` | Shadow retention (the only shadow purger), purge of L below the floor, trim of retrieved member sets, purge of orphan recordings. |
| node endpoint | `/health`, `/ready`, `/metrics`. |

Media driver: embedded (dev, demo, tests) or an external `aeronmd`
(production). In embedded mode, when `AERON_DIR` already exists, a driver
heartbeat at most `REPLICA_DRIVER_DIR_ACTIVE_MS` old (10 000, 2000..600 000)
means a live driver owns the directory: the replica refuses to start
(ActiveDriverException) and never takes over an `aeronmd`. A crashed driver's
directory counts as stale only after this long, so an embedded replica
restarted sooner after a kill -9 does not start until then. The check is
the kit's `EmbeddedDriverDir`, shared with core members' embedded drivers
(`CORE_DRIVER_DIR_ACTIVE_MS`); the replica keeps Aeron's
`ActiveDriverException` text, a member's refusal reads `AERON_DIR <dir> has a
live media driver (<others>; heartbeat <age> ms old, bound <bound> ms): stop it
or point this member at its own directory`. Close order: endpoint, follower (reader, then writer, so an
in-flight write aborts), housekeeping, clients, backup, archive, driver, then
(a decided re-bootstrap only) `cluster/` moved aside. Exit code 1 on any fault,
3 on a decided re-bootstrap (the unit restarts it: `RestartForceExitStatus=3`),
143 on a signal, never 0 on a fault.

## Bootstrap

The reader waits for L, scans the local shadows once, and takes the first path
that matches:

| Local state | Action |
|---|---|
| Shadows of two or more geneses | Refuse, exit 1. |
| A usable shadow at or above the start of L | Restore from it. The replica starts UNVERIFIED (below). |
| No shadow, L starts at 0 | Replay from genesis. Verified from the start. |
| No shadow (or every one below the start of L), L starts above 0, `SHADOW_PULL_SOURCES` set | Pull the newest ALIGNED with `P_s >= L.start` from the sources, verify it locally, restore from it (`restore_path` PULLED_SHADOW, code 3). Candidates of two or more geneses across the sources refuse, exit 1. |
| No shadow, L starts above 0, nothing pulled | Refuse with a runbook: copy a VERIFIED shadow of this genesis from a member into `BASE_DIR/archive`, or wipe `BASE_DIR` while a member still holds the log from 0. The text ends with ` pull: disabled` or ` pull: tried <n> sources, <c> candidates, refused <r>, last: <reason>`. |
| Shadows exist, all refused or below the start of L | Refuse as above, with usable and refused counts. |

One restore or genesis per process. A failure after the restore halts.

## Re-bootstrap

When the members pruned their logs past this replica's L, the supplier refuses
every live log replay below its start (`Live log replay failed ...
errorCode=INVALID_POSITION`, counted
`shadow_replica_backup_replay_below_start_total`). After
`SHADOW_REBOOTSTRAP_REPLAY_FAILURES` (3, 0..1000; 0 counts and never acts) such
refusals in a row, with L's live recording not alive, and the loop guard
passed, the follower looks for a VERIFIED shadow of this genesis above
`L.stop`, of a kind `SHADOW_PULL_KINDS` accepts. It checks its own archive
first; only when none is held does it pull an ALIGNED with `P_s > L.stop` from
the sources and verify it. A shadow at or below `L.stop` never qualifies. A
MAX_REPLAYS refusal or live log progress resets the run.

The follower logs `CRITICAL replica re-bootstrap: members pruned past
L.stop=<s> (<n> replay refusals); pulled ALIGNED at <P> from <source>; backup
state will move to <dir>; restarting` (or `local VERIFIED ALIGNED at <P>
already held`), counts `shadow_replica_rebootstrap_total`, and reads 503
`rebootstrap: restarting`. The process close moves `BASE_DIR/cluster` to
`BASE_DIR/cluster.rebootstrap-<utc>` after the backup and the archive closed,
keeps the newest `SHADOW_REBOOTSTRAP_KEEP` (2, 1..100) moved dirs, and exits 3.
The restart records a new L from the supplier's start and restores from the
shadow. The old recordings are orphans that housekeeping purges.

A process that stopped between the decision and the move (kill -9, or a media
driver loss, which exits 3 without the close) keeps the shadow and `cluster/`.
The next start restores from that shadow, sees the same refusals and decides
again from the local shadow, without a pull. The rule reads only the archive
and `rebootstrap.count`; the backup's mark files are recovered by Aeron's
liveness timeout. A replica without `SHADOW_PULL_SOURCES` re-bootstraps when
the operator copied such a shadow into `BASE_DIR/archive`.

Loop guard: `BASE_DIR/rebootstrap.count` holds one epoch ms per decision. More
than `SHADOW_REBOOTSTRAP_MAX_CONSECUTIVE` (3, 1..100) inside
`SHADOW_REBOOTSTRAP_WINDOW_MS` (3 600 000, 60 000..604 800 000) refuses, exit 1.
Nothing deletes `cluster/` or edits the backup RecordingLog. The replica slot is
stable for the life of `BASE_DIR`: one slot per replica.

Refusals start with `FATAL replica: re-bootstrap refused: ` followed by the
reason and a runbook: `re-bootstrap loop: <n> in <w> ms; members prune faster
than this replica catches up`, or `re-bootstrap needs SHADOW_PULL_SOURCES (...
L.stop=<s>, no local VERIFIED shadow above it)`. The text `no local shadow and
the cluster log starts at` belongs to bootstrap only.

When no qualifying shadow is held and no source lists one above `L.stop`, the
replica does not exit. `/ready` reads 503 `re-bootstrap not taken: no source
lists a shadow of kind ALIGNED above L.stop=<s>`, or `<c> candidates of kind
ALIGNED above L.stop=<s>, none pulled and VERIFIED`. The line is logged at most
once per `SHADOW_REBOOTSTRAP_NOT_TAKEN_LOG_MS` (60 000, 1000..86 400 000), and
`shadow_replica_rebootstrap_not_taken_total` counts every attempt. While this
lasts, every attempt lists each source once per `SHADOW_PULL_PERIOD_MS` for
`SHADOW_PULL_STARTUP_WAIT_MS`, and the next attempt follows as soon as the next
`SHADOW_REBOOTSTRAP_REPLAY_FAILURES` refusals arrived. A single replica stays
here until a second producer writes an ALIGNED above its `L.stop`; until then,
copy a VERIFIED shadow into `BASE_DIR/archive` or wipe `BASE_DIR`.

A stop (SIGTERM or close) during a bootstrap or re-bootstrap pull ends the pull
before its next pass instead of waiting for `SHADOW_PULL_STARTUP_WAIT_MS`: an
in-flight copy is stopped and its partial recording purged by the puller's own
thread, the stop is neither a refusal nor a "re-bootstrap not taken", and the
process exits 143 within the usual close time.

If the media driver is lost after a re-bootstrap was decided, the process still
exits 3, because the restart was already decided. The components are not
closed, so `cluster/` is not moved and the next start decides again.

A genesis start compares every member snapshot named after its ADOPT against
the adopted genesis. The housekeeping purge gate holds from the ADOPT until the
first compare passes (`genesis check armed at ADOPT: genesisId <g>`).

The pulled shadow a re-bootstrap restores from may be any producer's ALIGNED:
with members in shadow mode the designated follower's `F<id>` copy is pulled
from that member's (or a peer's) archive and restored at the second start
(LOCAL_SHADOW). Crashed writes of the replica's own producer from before this
process are purged by the kit's `CrashedWriteSweep` through retention.

## Trust after a restore

A replica that did not replay from genesis cannot know that its restored state
belongs to the history it now follows. Until it proves it, it is UNVERIFIED:

- `/ready` is 503 with a detail starting `unverified continuation`.
- No STANDBY or LOCAL shadow is written.

Two checks decide:

1. **Cluster identity.** The member snapshot named by the newest backup
   response must carry the genesis of the local shadows. Only a complete copy
   counts (BEGIN and END markers present); a copy refused by its envelope is
   not read again. The check repeats for every newly named snapshot. A mismatch
   fences with `FATAL replica: cluster genesis <g2> != local shadows <g1>; wipe BASE_DIR`.
2. **Byte alignment.** At every ALIGNED trigger the serialized image is held
   and compared with the member snapshot the backup retrieved at the same log
   position. Equal: the replica becomes (or stays) VERIFIED and the held shadow
   is written. Different: see "Live oracle" below. The compare runs at every ALIGNED, so a
   verified replica that later diverges is caught at its next ALIGNED.

**Stall bound.** `REPLICA_VERIFY_WAIT_MS` (default 180 000) bounds a stall, not
the elapsed time. The clock runs only while the replica makes no progress
toward verification; it resets when the applied position advances toward the
target snapshot, when a retrieval passes its previous byte maximum, or when a
compare completes. On expiry: `FATAL replica: unverified continuation at <applied> (history mismatch or a stalled backup); wipe BASE_DIR`.
Boot refuses a value below 2 x query interval + cooldown.

**What to expect.** After a normal restart the replica stays 503 until the next
member snapshot is retrieved and compared: about one member snapshot cadence
(measured 300 to 313 s with the default 5 minute cadence). This is by design;
an unverified replica never writes a shadow a member could restore from.

## Shadow triggers

| Trigger | Kind | When busy |
|---|---|---|
| Cluster snapshot action (`SNAPSHOT`, 0) | ALIGNED | The reader blocks until the writer is idle or `REPLICA_ALIGNED_WAIT_MS`; the deadline fences. |
| Standby toggle (`SNAPSHOT`, 1) | STANDBY | Skipped, counted. |
| No action frame for the fallback interval of cluster time | LOCAL | Skipped. An idle log never fires. |

A trigger only records what is due; the image is serialized after the poll that
raised it returned normally, so nothing is written after a failed apply.

## Fences

Any throwable out of an apply, a self-check failure, a writer failure, a change
of L or an identity mismatch latches the fence and aborts the in-flight write.
After a fence nothing is written, from any thread. Every fence line names the
position and the reason. A fence has one of three families:

| Family | Cause | Then |
|---|---|---|
| Mismatch | the member copy at P opened, holds this replica's genesis, and its bytes differ | hold (see "Live oracle"); exit only on stop (143), driver loss (1) or `REPLICA_MISMATCH_HOLD_MS` (1) |
| History | a member copy of another genesis (at the compare or at the genesis check), or a member snapshot at a position this replica applied past without a SNAPSHOT action there | own shadows above the last EQUAL position or the restore point, whichever is higher, are purged and the ones at or below it kept (`poisoned shadows purged: [...] (above <P>, the last EQUAL position or the restore point; kept at or below <P>; <n> refused)`), awaited for at most `REPLICA_FENCE_PURGE_WAIT_MS`, then exit 1; runbook: wipe `BASE_DIR` |
| Fault | everything else | exit 1 |

## Live oracle

At every ALIGNED the held image is compared with the member service snapshot
the backup retrieved at the same position, in this order:

1. The copy's envelope must open (length and CRC32C). A copy that does not open
   is UNREADABLE: torn or incomplete, never a divergence. It is counted
   (`shadow_oracle_unreadable_total`), logged once per recording, and the held
   image keeps waiting. An UNVERIFIED replica whose stall bound runs out on such a
   copy names it: `...; wipe BASE_DIR; the last member copy at <P> (rec <id>) was unreadable: <reason>`.
2. A stub (members in `SNAPSHOT_MODE=shadow`) is compared by identity; a full
   copy byte for byte.
3. When the bytes differ, the copy's genesis decides: genesis 0 (taken before
   the ADOPT) is ignored; another genesis is a history fence; the same genesis is
   a byte mismatch.

A byte mismatch is never adjudicated by the replica. It latches
(`shadow_oracle_mismatch_total`, `shadow_oracle_mismatch_position`), stops
applying and writing, reads 503 with the readiness field `recovery=fenced:oracle mismatch at <P>: <detail>`, and
keeps `/metrics`, the housekeeping thread and the evidence:

- `REPLICA_MISMATCH_DIR/<P>/` (default `BASE_DIR/mismatch`): `replica.image`,
  `member.image`, `report.txt` (positions, term, both geneses, lengths, first
  differing byte, both SHA-256 digests, the member copy's recording id). Plain
  files, never recordings. A dump I/O failure sets
  `shadow_oracle_dump_incomplete 1` and never changes the hold.
- The retrieved set at P is pinned (never trimmed; `shadow_retrieved_sets` may
  read K+1) and the log purge of L is frozen, so an offline replay from the
  previous VERIFIED shadow to P stays possible.

Runbook: read `/metrics`, copy the dump, adjudicate offline (replay the captured
log with `LogSpliceRunner`), then stop and wipe or keep. A restart re-latches at
the same P (the report gains a `re-latched at` line); it is harmless and useless.

Freshness: STANDBY and LOCAL are written only while the newest member snapshot
the backup named or retrieved is at or below the last EQUAL position; otherwise
they are skipped (`shadow_oracle_stale_named_total`) until the next EQUAL at or
above it. ALIGNED needs nothing but its own compare.

### Rollout gate for `SNAPSHOT_MODE=shadow` on members

Switch a member only when, over the agreed window, read twice from the same
replica `/metrics` and subtracted:

- `shadow_oracle_mismatch_total` delta == 0
- `shadow_oracle_other_genesis_total` delta == 0
- `shadow_oracle_match_total` delta == `shadow_trigger_total{kind="ALIGNED"}` delta
- `shadow_oracle_unreadable_total` and `shadow_align_unverifiable_total` deltas explained
- `shadow_replica_verified == 1`

| Setting | Default | Range |
|---|---|---|
| `REPLICA_MISMATCH_HOLD_MS` | 0 (hold until stopped) | 0..604800000 |
| `REPLICA_MISMATCH_DIR` | `BASE_DIR/mismatch` | a writable path, checked at boot |
| `REPLICA_MISMATCH_DUMP_MAX_MB` | 2048 (per image file; 0 writes the report only) | 0..16384 |
| `REPLICA_FENCE_PURGE_WAIT_MS` | 30000 | 1000..600000 |
| `REPLICA_MEMBER_CADENCE_MS` | 0 (not checked) | 0..86400000; when set, `REPLICA_READY_SHADOW_AGE_MS` must cover it plus `REPLICA_VERIFY_WAIT_MS` |

## Readiness

`/ready` is 200 only when all of these hold; the detail names the first that
fails:

1. Bootstrap done, not refused, not fenced, VERIFIED.
2. The reader ticked within `REPLICA_READY_STALE_MS`.
3. No disk-full shadow write outstanding.
4. A live recording L exists and its position counter is alive.
5. Recorded minus applied is at most `REPLICA_READY_MAX_LAG_BYTES`.
6. The newest shadow is younger than `REPLICA_READY_SHADOW_AGE_MS` (0 disables).
7. Wall clock minus the cluster time of the last applied frame is at most
   `REPLICA_READY_TIME_LAG_MS` (0 disables). The engine leader keeps a liveness
   timer, so a healthy log ticks at least once a second; a still log means a
   frozen or partitioned supplier.

The backup agent's own state is never an input. `/health` covers only the
reader and housekeeping threads (`REPLICA_LIVENESS_STALE_MS`).

## Metrics

Prefixes `shadow_replica_*`, `shadow_align_*`, `shadow_oracle_*`,
`shadow_trigger_*`, `shadow_retrieved_*` and `replica_attach_*`. The ones to
alert on: `shadow_replica_fenced`, `shadow_replica_verified`,
`shadow_oracle_mismatch_total`, `shadow_oracle_other_genesis_total`,
`shadow_oracle_mismatch_latched`, `shadow_oracle_dump_incomplete`,
`shadow_align_verify_timeouts_total`, `shadow_replica_internal_lag_bytes`,
`shadow_replica_time_lag_ms`, `shadow_replica_backup_auth_rejected_total`.
`shadow_oracle_match_total` equals `shadow_align_equal_total` by definition.
Pull and re-bootstrap: `shadow_replica_backup_replay_below_start_total`,
`shadow_replica_rebootstrap_total`, `shadow_replica_rebootstrap_not_taken_total`,
and the puller's `shadow_pull_*` (`shadow_pull_total`,
`shadow_pull_bytes_total`, `shadow_pull_retries_total`,
`shadow_pull_refused_total{reason}`, `shadow_pull_source_unreachable_total`,
`shadow_pull_aborted_leader_total`, `shadow_pull_skipped_leader_total`).
Crashed writes: `shadow_replica_crashed_writes_purged_total` and the shared
`shadow_crashed_writes_purged_total` (same count).