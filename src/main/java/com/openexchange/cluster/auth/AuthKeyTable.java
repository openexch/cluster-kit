// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.auth;

import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import java.util.TreeMap;

/**
 * Node-local key table of the K6 v1 authenticator: {@code keyId -> (class, secret, principal)}. Node-local
 * configuration is legitimate here because the table is not replicated state: only the principal it mints
 * enters the log (the SessionOpen event). Every member must carry the same table; a mismatch shows as a
 * reconnect rejection after a leader change, never as divergence.
 *
 * <p>Spec: entries separated by {@code ;} (blank entries skipped, each trimmed), each
 * {@code keyId:CLASS:secretHex:identity:users=...} split on {@code :} with Java semantics (trailing empty parts
 * dropped), 3 to 5 parts, each trimmed. identity is the 4th part, or keyId when that part is missing or blank.
 * The users field is mandatory (decision A16, strict mode): {@code users=*} is the only any-user form,
 * {@code users=a|b|...} lists 1..510 unique decimal ids. A missing users field, an empty {@code users=}, a
 * blank id, {@code *} mixed with ids, a non-numeric id, a class outside the policy, an id list under a policy
 * without user scope, a duplicate keyId, a secret that is not 32 bytes or a malformed entry fails the whole
 * load, naming the entry position and the keyId (when it parsed), never the secret. The minimal entry is
 * {@code keyId:CLASS:secretHex::users=*}.</p>
 *
 * <p>An absent or empty table authenticates nobody.</p>
 */
public final class AuthKeyTable {

    /** One row: the class the key may claim, its secret and the principal it mints. */
    public record Key(int keyId, ClusterSessionClass sessionClass, byte[] secret, ClusterPrincipal principal) {
    }

    private final AuthPolicy policy;
    private final Map<Integer, Key> keys = new TreeMap<>();

    private AuthKeyTable(final AuthPolicy policy) {
        this.policy = policy;
    }

    public static AuthKeyTable empty(final AuthPolicy policy) {
        return new AuthKeyTable(policy);
    }

    /** From the policy's key-table property, else its env variable; empty when neither is set. */
    public static AuthKeyTable fromEnvironment(final AuthPolicy policy) {
        return fromSpec(AuthPolicy.read(policy.keysProperty(), policy.keysEnv()), policy);
    }

    public static AuthKeyTable fromSpec(final String spec, final AuthPolicy policy) {
        final AuthKeyTable t = new AuthKeyTable(policy);
        if (spec == null || spec.isBlank()) {
            return t;
        }
        final String[] entries = spec.split(";");
        for (int e = 0; e < entries.length; e++) {
            final String entry = entries[e].trim();
            if (entry.isEmpty()) {
                continue;
            }
            t.parseEntry(entry, e + 1);
        }
        return t;
    }

    private IllegalArgumentException refuse(final int position, final Integer keyId, final String why) {
        return new IllegalArgumentException(policy.keysEnv() + " entry " + position
                + (keyId == null ? "" : " (keyId " + keyId + ")") + ": " + why);
    }

    private void parseEntry(final String entry, final int position) {
        final String[] p = entry.split(":");
        if (p.length < 3 || p.length > 5) {
            throw refuse(position, null, "malformed entry with " + p.length
                    + " part(s); expected keyId:CLASS:secretHex:identity:users=* (identity may be blank)");
        }
        final int keyId;
        try {
            keyId = Integer.parseInt(p[0].trim());
        } catch (final NumberFormatException e) {
            throw refuse(position, null, "keyId is not a decimal int");
        }
        final ClusterSessionClass cls;
        try {
            cls = ClusterSessionClass.parse(p[1]);
        } catch (final IllegalArgumentException e) {
            throw refuse(position, keyId, e.getMessage());
        }
        if (!policy.allows(cls)) {
            throw refuse(position, keyId, "class " + cls + " is not allowed by the " + policy.engine()
                    + " policy (allowed " + policy.allowedClasses() + ")");
        }
        final byte[] secret;
        try {
            secret = HexFormat.of().parseHex(p[2].trim());
        } catch (final IllegalArgumentException e) {
            throw refuse(position, keyId, "secret is not hex");
        }
        if (secret.length != ClusterCredentials.SECRET_LENGTH) {
            throw refuse(position, keyId, "secret must be " + ClusterCredentials.SECRET_LENGTH + " bytes ("
                    + 2 * ClusterCredentials.SECRET_LENGTH + " hex digits), got " + secret.length);
        }
        final int identity;
        if (p.length >= 4 && !p[3].isBlank()) {
            try {
                identity = Integer.parseInt(p[3].trim());
            } catch (final NumberFormatException e) {
                throw refuse(position, keyId, "identity is not a decimal int");
            }
        } else {
            identity = keyId;
        }
        if (p.length < 5) {
            throw refuse(position, keyId, "no users field; any-user scope needs an explicit users=*");
        }
        final String u = p[4].trim();
        if (!u.startsWith("users=")) {
            throw refuse(position, keyId, "fifth field must be users=* or users=a|b|c");
        }
        final String list = u.substring("users=".length()).trim();
        if (list.isEmpty()) {
            throw refuse(position, keyId, "empty users=; any-user scope needs an explicit users=*");
        }
        final ClusterPrincipal principal;
        if (list.equals("*")) {
            principal = ClusterPrincipal.anyUser(cls, identity);
        } else {
            final String[] ids = list.split("\\|", -1);
            if (ids.length > ClusterPrincipal.MAX_USERS) {
                throw refuse(position, keyId, "users= lists " + ids.length + " ids; at most "
                        + ClusterPrincipal.MAX_USERS);
            }
            final long[] users = new long[ids.length];
            for (int i = 0; i < ids.length; i++) {
                final String id = ids[i].trim();
                if (id.isEmpty()) {
                    throw refuse(position, keyId, "users= has a blank id at position " + (i + 1));
                }
                if (id.equals("*")) {
                    throw refuse(position, keyId, "users=* cannot be mixed with ids");
                }
                try {
                    users[i] = Long.parseLong(id);
                } catch (final NumberFormatException e) {
                    throw refuse(position, keyId, "users= id at position " + (i + 1) + " is not a decimal long");
                }
            }
            if (!policy.userScopeAllowed()) {
                throw refuse(position, keyId, "the " + policy.engine()
                        + " policy has no user scope; use users=* (an id list is refused)");
            }
            try {
                principal = ClusterPrincipal.forUsers(cls, identity, users);
            } catch (final IllegalArgumentException e) {
                throw refuse(position, keyId, e.getMessage());
            }
        }
        if (keys.containsKey(keyId)) {
            throw refuse(position, keyId, "duplicate keyId");
        }
        keys.put(keyId, new Key(keyId, cls, secret, principal));
    }

    public AuthPolicy policy() {
        return policy;
    }

    public int size() {
        return keys.size();
    }

    /** The minted principal of a key, or null; for tests and diagnostics. */
    public ClusterPrincipal principalOf(final int keyId) {
        final Key k = keys.get(keyId);
        return k == null ? null : k.principal;
    }

    /**
     * Authenticate presented credentials: known key, claimed class equals the key's class, secret equal in
     * constant time. Returns the principal to mint, or null to reject.
     */
    public ClusterPrincipal authenticate(final ClusterCredentials presented) {
        if (presented == null) {
            return null;
        }
        final Key key = keys.get(presented.keyId());
        if (key == null) {
            return null;
        }
        if (key.sessionClass != presented.sessionClass()) {
            return null;
        }
        if (!MessageDigest.isEqual(key.secret, presented.secret())) {
            return null;
        }
        return key.principal;
    }
}
