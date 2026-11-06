# Session authority (K6 v1), `com.openexchange.cluster.auth`

One copy of the authenticator for both engines (RC2 S6). The bytes are the RC1 S3 bytes; the
assets engine's decisions are pinned by `src/test/resources/auth-parity-v1.txt`, captured from the
RC1 classes at assets 462b98d.

## Classes

| Code | Class | Code | Class |
|---|---|---|---|
| 1 | GATEWAY | 5 | TOOL |
| 2 | FORWARDER | 6 | TREASURY |
| 3 | BRIDGE | 7 | REPLICA |
| 4 | ADMIN | 0 | UNSET (no authority) |

Codes equal the money-schema `SessionClass` values. A code above 7 is UNSET.

## Credential (what a client presents, 38 bytes)

`[u8 version=1][u8 class][i32 keyId LE][32 B secret]`. Text form `keyId:CLASS:secretHex`, CLASS by
name (uppercased with `Locale.ROOT`) or by decimal code. A challenge is answered with empty bytes;
the authenticator never challenges.

## Principal (what the authenticator mints; rides SessionOpen)

`[u8 1][u8 class][u8 flags][u8 reserved][i32 identity LE][u16 count LE][i64 userId LE x count]`,
at most 510 ids. Flags 1 = any user (count 0); flags 0 = 1..510 ascending unique ids. Decode ignores
the reserved byte and flag bits 1-7 and maps anything malformed to `NONE`.
`ClusterPrincipal.classCodeOf(bytes)` reads the class in place with no allocation.

## Key table

Entries separated by `;`, each `keyId:CLASS:secretHex:identity:users=...`:

- blank identity means identity = keyId;
- the users field is mandatory (decision A16): `users=*` is the only any-user form, `users=a|b|...`
  lists 1..510 unique ids;
- a missing users field, an empty `users=`, a blank id, `*` mixed with ids, a class outside the
  engine policy, an id list under a policy without user scope, a duplicate keyId or a secret that is
  not 32 bytes fails the whole load at boot, naming the entry position and keyId, never the secret.

Minimal entry: `7:ADMIN:<64 hex digits>::users=*`. An absent or empty table rejects every session.

## Backup-style requests share one session id (Aeron 1.53)

Aeron builds every BackupQuery, HeartbeatRequest and StandbySnapshot session with id `-1`
(`SessionManager.onBackupAction`) and hands the authenticator only that id when the session connects; the
`SessionProxy` carries no credential, so a decision cannot be bound to its request. `ClusterAuthenticator`
therefore fails closed for `-1` (RC2 S6 review DEFECT-1):

- every `-1` request is remembered, accepted or not, for a window of the consensus module's session timeout plus
  1 s (`backupWindowMsFor`), which outlives Aeron's own pending session; handing out a principal does not shorten it;
- a `-1` session is authenticated only when exactly one `-1` request is in the window, that request presented a
  valid credential and its principal was not handed out yet; anything else rejects, and the backup retries;
- a forger can delay the backup by one window; it can never receive a principal minted for another request.

Operational consequence: at most one backup-style session per window succeeds on a member, so the ME backup's
query interval (`BACKUP_INTERVAL_SEC`, 60 s) must exceed the session timeout plus 1 s. Upstream issue to raise with
Aeron: backup-style sessions without a unique id. RCS S5/S11 keep this rule and the REPLICA BackupQuery row.

## Engine policies

| | assets | match |
|---|---|---|
| Key table | `ASSETS_AUTH_KEYS` / `-Dassets.auth.keys` | `ME_AUTH_KEYS` / `-Dmatch.auth.keys` |
| Credential | `AE_AUTH_CREDENTIAL` / `-Dassets.auth.credential` | `ME_AUTH_CREDENTIAL` / `-Dmatch.auth.credential` |
| Classes | 1..7 | GATEWAY, FORWARDER, ADMIN, REPLICA |
| User scope | `users=*` or an id list | `users=*` only |
| Authorisation (consensus module) | Aeron default until RCS S5 | `ClassAuthorisationService`: REPLICA may BackupQuery, nothing else |

## Other kit helpers added in S6

- `SnapshotImageReader.readFully`: polls a snapshot image to end of stream, drains what came with it,
  throws when the image closes first.
- `TermTimerId.of(base, term)`: `base + max(0, term) * 1_000_000`, the per-leadership-term timer id.
