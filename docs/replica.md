# Replica process (`com.openexchange.cluster.replica`)

A replica is a fourth process next to a three-member cluster. It is not a
member: it never votes, holds no client sessions and sends the members nothing
but backup queries. It follows the cluster through Aeron `ClusterBackup`,
applies the committed log into its own engine instance and writes
[shadow images](shadow.md) from that state. The members pay nothing for those
images.

The engine supplies the state machine (`ReplicaEngine`), parses its own
settings into a `ReplicaConfig` and starts `ReplicaProcess`. The kit never reads
an environment variable; the engine README lists the setting names (in assets:
`java -jar assets.jar replica`, settings `REPLICA_*`).

## Threads

| Thread | Job |
|---|---|
| `ClusterBackup` agent | Queries a member every query interval, retrieves the member's newest snapshot set, records the live log L into the replica's own archive. |
| `shadow-reader-R<slot>` | Bootstrap once, then follow L, apply it to the engine, raise shadow triggers. |
| shadow writer | Serializes, self-checks and records shadow images. Reads the fence latch before every write. |
| `shadow-housekeeping-R<slot>` | Shadow retention (the only shadow purger), purge of L below the floor, trim of retrieved member sets, purge of orphan recordings. |
| node endpoint | `/health`, `/ready`, `/metrics`. |

Media driver: embedded (dev, demo, tests) or an external `aeronmd`
(production). Close order: endpoint, follower (reader, then writer, so an
in-flight write aborts), housekeeping, clients, backup, archive, driver. Exit
code 1 on any fault, 143 on a signal, never 0 on a fault.

## Bootstrap

The reader waits for L, scans the local shadows once, and takes the first path
that matches:

| Local state | Action |
|---|---|
| Shadows of two or more geneses | Refuse, exit 1. |
| A usable shadow at or above the start of L | Restore from it. The replica starts UNVERIFIED (below). |
| No shadow, L starts at 0 | Replay from genesis. Verified from the start. |
| No shadow, L starts above 0 | Refuse with a runbook: copy a VERIFIED shadow of this genesis from a member into `BASE_DIR/archive`, or wipe `BASE_DIR` while a member still holds the log from 0. |
| Shadows exist, all refused or below the start of L | Refuse as above, with usable and refused counts. |

One restore or genesis per process. A failure after the restore halts.

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
   is written. Different: `FATAL replica: state at <P> differs from member snapshot (history mismatch or divergence); wipe BASE_DIR`.
   The compare runs at every ALIGNED, so a verified replica that later diverges
   also fences.

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
of L or an identity mismatch latches the fence, aborts the in-flight write and
exits 1 through the host's fatal hook. After a fence nothing is written, from
any thread. Every fence line starts `FATAL replica:` and names the position and
the reason. Recovery is the action the line names, usually wiping `BASE_DIR`.

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

Prefixes `shadow_replica_*`, `shadow_align_*`, `shadow_trigger_*`,
`shadow_retrieved_*` and `replica_attach_*`. The ones to alert on:
`shadow_replica_fenced`, `shadow_replica_verified`,
`shadow_align_verify_timeouts_total`, `shadow_replica_internal_lag_bytes`,
`shadow_replica_time_lag_ms`, `shadow_replica_backup_auth_rejected_total`.
