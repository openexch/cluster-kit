# cluster-kit

Engine-agnostic Aeron cluster infrastructure, shared by the matching engine
(`openexch/match`) and the assets engine (`openexch/assets`).

## Why this exists

Both engines are replicated state machines on Aeron Cluster. Their *state
machines* differ — one holds an order book, the other holds balances — but the
machinery around them is identical: take a snapshot, ship it somewhere durable,
reclaim the log behind it.

That machinery used to be copied between the two repos. The copies drifted, and
the failure mode is not theoretical: `--watermark` was added to one engine's
housekeeping and very nearly not the other's. A retention watermark that only
half applies is worse than none, because it reads as protection.

## What belongs here

| in | out |
|---|---|
| `StagingArchive` — a quiescent copy of a live archive, built by Aeron replication | `ClusterConfig` — ports, services, engine wiring |
| `BundleCapture` — snapshot + the log behind it, as a durable bundle | `AeronCluster` — how an engine boots |
| `ArchiveHousekeeping` — watermark-bounded log reclamation | anything that knows what the state machine *means* |

The boundary is the state machine. Cross it and this becomes a library that
knows about order books and balances, which would defeat the point.

The one thing the machinery cannot know by itself is which wire schema an engine
speaks, so `BundleCapture.EngineSchema` is a parameter and each engine keeps a
thin entry point in its own repo supplying its generated constants.

## Consuming it

Aeron and Agrona are `provided`: the engines bring their own and shade them, so
this artifact must never pull a second copy into their uber jars.
