// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.auth;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * What one engine allows of the shared K6 v1 authority: where its key table and its clients' credential
 * are read from, which session classes a key may carry, and whether a key may be scoped to explicit users.
 * Immutable. The key table is node-local input; only the principal it mints is replicated.
 */
public final class AuthPolicy {

    private final String engine;
    private final String keysProperty;
    private final String keysEnv;
    private final String credentialProperty;
    private final String credentialEnv;
    private final Set<ClusterSessionClass> allowedClasses;
    private final boolean userScopeAllowed;

    private AuthPolicy(final String engine, final String keysProperty, final String keysEnv,
                       final String credentialProperty, final String credentialEnv,
                       final Set<ClusterSessionClass> allowedClasses, final boolean userScopeAllowed) {
        this.engine = engine;
        this.keysProperty = keysProperty;
        this.keysEnv = keysEnv;
        this.credentialProperty = credentialProperty;
        this.credentialEnv = credentialEnv;
        this.allowedClasses = allowedClasses;
        this.userScopeAllowed = userScopeAllowed;
    }

    /**
     * @param engine             a short name for messages, e.g. "assets" or "match"
     * @param keysProperty       system property holding the key table (wins over the env variable)
     * @param keysEnv            environment variable holding the key table
     * @param credentialProperty system property holding a client credential (wins over the env variable)
     * @param credentialEnv      environment variable holding a client credential
     * @param userScopeAllowed   false: every key must be {@code users=*}; an explicit id list fails the load
     * @param allowedClasses     the classes a key may carry; never UNSET
     */
    public static AuthPolicy of(final String engine, final String keysProperty, final String keysEnv,
                                final String credentialProperty, final String credentialEnv,
                                final boolean userScopeAllowed, final ClusterSessionClass... allowedClasses) {
        if (allowedClasses.length == 0) {
            throw new IllegalArgumentException("a policy needs at least one allowed class");
        }
        final EnumSet<ClusterSessionClass> set = EnumSet.noneOf(ClusterSessionClass.class);
        for (final ClusterSessionClass c : allowedClasses) {
            if (c == null || c == ClusterSessionClass.UNSET) {
                throw new IllegalArgumentException("UNSET cannot be an allowed class");
            }
            set.add(c);
        }
        return new AuthPolicy(engine, keysProperty, keysEnv, credentialProperty, credentialEnv,
                Collections.unmodifiableSet(set), userScopeAllowed);
    }

    public String engine() {
        return engine;
    }

    public String keysProperty() {
        return keysProperty;
    }

    public String keysEnv() {
        return keysEnv;
    }

    public String credentialProperty() {
        return credentialProperty;
    }

    public String credentialEnv() {
        return credentialEnv;
    }

    public Set<ClusterSessionClass> allowedClasses() {
        return allowedClasses;
    }

    public boolean allows(final ClusterSessionClass c) {
        return allowedClasses.contains(c);
    }

    public boolean userScopeAllowed() {
        return userScopeAllowed;
    }

    /** The system property when set and not blank, else the environment variable; null when neither is set. */
    static String read(final String property, final String env) {
        String spec = System.getProperty(property);
        if (spec == null || spec.isBlank()) {
            spec = System.getenv(env);
        }
        return spec == null || spec.isBlank() ? null : spec;
    }

    @Override
    public String toString() {
        return "AuthPolicy{" + engine + " keys=" + keysEnv + "/-D" + keysProperty + " credential=" + credentialEnv
                + "/-D" + credentialProperty + " classes=" + allowedClasses + " userScope=" + userScopeAllowed + "}";
    }
}
