# Shadow images (`com.openexchange.cluster.shadow`)

A shadow is an engine image at a log position `P_s`, written as one archive
recording by a producer that is not the leader (the replica, a designated
follower). It is either an exact copy of the replicated state at `P_s` or it is
refused loudly. The contract is `RCS/S3/contract.md` in the notebook; this page
is the host's summary.

## Format

One `ExclusivePublication` on `aeron:ipc?term-length=<T>|alias=<alias>`, stream
`ShadowAlias.STREAM_ID` (4301), recorded locally by the host's archive:

```
message 0     ShadowHeader, 128 B (one 160 B frame), CRC32C over [0, 124), payloadDigest = SHA-256[0..8)
message 1..n  the engine's enveloped image, chunks of publication.maxMessageLength() (T / 8)
message n+1   ShadowTrailer, 64 B (one 96 B frame), offered only after every chunk is recorded
```

Alias: `oxs.v1.<engine>.c<clusterId>.g<genesisId>.p<P_s>.t<term>.r<R|F><producerId>.s<seq>.k<kind>`.
The alias is a locator; the header is the authority and the alias must equal it.

`T` (`SHADOW_TERM_LENGTH`) must be in `[64k, 16m]`; 4m is the recommended
value. Every replay of a shadow, on every host, uses the recording's term length.

**Payload digest (RCS S3 amendment Q0).** The header and trailer carry `payloadDigest`: the first 8 bytes of
SHA-256 over the payload bytes as stored, the engine envelope included. Content agreement, the deep read and the
loader compare this digest. It is never a CRC32C. An engine image that ends with the CRC32C of what precedes it has
the same CRC32C for every image of one length (the CRC residue), so a CRC cannot tell two self-consistent states
apart. CRC32C guards only the header and trailer bytes.

## States

A scan classifies each recording on the shadow stream and engine prefix:
ACTIVE, TOO_SHORT, NO_TRAILER, HEADER_INVALID, TRAILER_MISMATCH,
ALIAS_MISMATCH, ALIAS_UNPARSEABLE, SCAN_ERROR, FOREIGN, CONFLICT, COMPLETE,
VERIFIED, CORRUPT.

- COMPLETE is structural only (header window, trailer window, T3/T4, alias).
- VERIFIED means the full CRC read passed in this process for this immutable
  recording. CORRUPT is final for the recording.
- Copies at one position (`engine, cluster, genesis, P_s`) must agree on
  content. If they disagree, every copy is CONFLICT and excluded. There is no
  winner; raise CRITICAL.

## Host obligations

1. **Only VERIFIED moves a floor or authorises a purge.** Use
   `Scan.verifiedAtOrAbove` and `Scan.verifiedNewest` for `ShadowFloor`,
   retention and BundleCapture. `equalAt`, `newestAtOrAbove` and `newestBelow`
   are for restore attempts only, and the loader re-verifies.
2. **Startup deep pass.** Run `ShadowCatalog.verifyAllComplete()` before the
   pruner's first poll. Until it finishes, and on any scan with
   `errors > 0`, every floor query answers UNKNOWN (`-1`). Do not raise the
   floor on UNKNOWN.
3. **Restore order.** Follow these steps, in this order. All of them are
   mandatory.
   1. `ShadowImageLoader.load` (CRC).
   2. `ShadowIdentity.check` under the host's policy. STRICT is the default.
      BOOTSTRAP_ADOPT is for replica bootstrap only.
   3. `ReplayContainer.restore`.
   4. `ShadowIdentity.matches` against the RESTORED engine. A failure discards
      the container and the engine, and counts
      `shadow_identity_crosscheck_failures_total`.
4. **Identity.** Members build `TermBaseLookup` from their `RecordingLog` term
   entries (read side). The replica builds it from the term events its reader
   saw.
5. **Producers.** Call `ShadowIdentity.requireProducible(genesisId)` before
   arming any trigger. A recorded nonce `<= 0` stops the producer.
6. **Writer startup.** Seed `initialProducerSeq = max(seq) + 1` and
   `initialLastWrittenPosition = max(P_s)` from a startup scan of this
   producer's own local copies.
7. **Self-check.** Supply a real `ShadowSelfCheck`: a scratch engine restores
   the buffer, re-serializes it and compares. `UNCHECKED_TEST_ONLY` is refused
   unless `-Doxs.shadow.unchecked=true`, which hosts never set.
8. **Threads.** Each `ShadowCatalog`, `ShadowWriter` and `ShadowImageLoader`
   instance has one owner thread. `ShadowVerdicts` is the only shared object
   and is thread-safe. Nothing in the package runs on a cluster service thread.
   The service thread only serializes and hands the buffer to
   `ShadowWriterThread`.
9. **Pull sources are an allowlist.** A PITR clone carries the same genesisId,
   and the kit cannot tell it apart. Never replicate an ACTIVE source.
10. **Drivers.** Every class takes `Aeron` and `AeronArchive` clients from its
    host and never launches a driver. The package scan enforces this rule.

## Writer edge cases (review NOTE-7, NOTE-8)

- `requestAbort()` aborts only the write in flight; with none in flight it does nothing.
- A RECORD_TIMEOUT before the trailer leaves `producerSeq` and `lastWrittenPosition` unchanged, so a retry at the
  same `P_s` reuses the seq. The position is the same, so the copies group together.
- An `ArchiveException` from `stopRecording` after a fully recorded trailer goes through the abort cleanup and PURGES
  the complete recording. That loses a write but never produces a wrong one, and a retry is possible.
- `ShadowWriterThread` hands every `Throwable` to the host's error handler and counts it in
  `shadow_writer_thread_errors_total`. A listener exception never ends the thread. An `Error` ends it: `offer`
  returns false and `isIdle()` is true.

## /dev/shm (measured on Aeron 1.53, RCS S3)

Every replay creates one IPC publication log of `3 x T + 4 KiB`. The log is
sparse: allocated pages are the pages the replay touched. A trailer or header
window touches about 8 KiB. A full read touches at most its recording. These
logs stay on disk after the replay ends: about 1.8 s on the build box, for 8
window replays at `T = 4m`. The shadow replay channel's `linger=0` does not
shorten that for IPC; the bare channel measured the same. A scan that finds `k` new
shadows therefore leaves `2k` such logs behind for a few seconds. Budget the
allocated bytes, not one log.
