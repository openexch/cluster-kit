// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.auth;

import io.aeron.security.CredentialsSupplier;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HexFormat;

/**
 * What a client presents on connect (K6 v1, static shared secret):
 * {@code [u8 version=1][u8 class][i32 keyId LE][32 bytes secret]} = 38 bytes. The class is the client's
 * ASSERTION; the key table decides what the key is allowed to be, and the authenticator rejects a mismatch.
 * v1 answers a challenge with empty bytes (the authenticator never challenges). These are the RC1 S3 bytes,
 * byte-equal to the C++ client and the fixture's {@code credentials.py}.
 *
 * <p>Text form {@code keyId:CLASS:secretHex}, CLASS by name (uppercased with {@code Locale.ROOT}) or decimal
 * code, secret 64 hex digits.</p>
 */
public final class ClusterCredentials {

    public static final int VERSION = 1;
    public static final int SECRET_LENGTH = 32;
    public static final int ENCODED_LENGTH = 1 + 1 + 4 + SECRET_LENGTH;

    private final ClusterSessionClass sessionClass;
    private final int keyId;
    private final byte[] secret;

    public ClusterCredentials(final ClusterSessionClass sessionClass, final int keyId, final byte[] secret) {
        if (sessionClass == null || sessionClass == ClusterSessionClass.UNSET) {
            throw new IllegalArgumentException("credentials need a real session class, got " + sessionClass);
        }
        if (secret == null || secret.length != SECRET_LENGTH) {
            throw new IllegalArgumentException("secret must be exactly " + SECRET_LENGTH + " bytes");
        }
        this.sessionClass = sessionClass;
        this.keyId = keyId;
        this.secret = secret.clone();
    }

    /** Parse {@code keyId:CLASS:secretHex} (exactly three {@code :}-separated parts, each trimmed). */
    public static ClusterCredentials fromSpec(final String spec) {
        final String[] parts = spec.trim().split(":");
        if (parts.length != 3) {
            throw new IllegalArgumentException("credential spec must be keyId:class:secretHex");
        }
        return new ClusterCredentials(ClusterSessionClass.parse(parts[1]), Integer.parseInt(parts[0].trim()),
                HexFormat.of().parseHex(parts[2].trim()));
    }

    /** From the policy's credential property, else its env variable; null when neither is set. */
    public static ClusterCredentials fromEnvironment(final AuthPolicy policy) {
        final String spec = AuthPolicy.read(policy.credentialProperty(), policy.credentialEnv());
        return spec == null ? null : fromSpec(spec);
    }

    /**
     * A client's start-up rule: the credential must be configured and must claim one of {@code expected}.
     * Throws {@link IllegalStateException} naming the env and property otherwise, never the secret.
     */
    public static ClusterCredentials requireFromEnvironment(final AuthPolicy policy,
                                                            final ClusterSessionClass... expected) {
        return requireFromEnvironment(policy.credentialProperty(), policy.credentialEnv(), policy.engine(), expected);
    }

    /**
     * The same start-up rule for a client process that does not carry the engine's {@link AuthPolicy} (a
     * standalone gateway or tool): the credential is read from {@code property}, else {@code env}.
     */
    public static ClusterCredentials requireFromEnvironment(final String property, final String env,
                                                            final String engine,
                                                            final ClusterSessionClass... expected) {
        final ClusterCredentials c;
        try {
            final String spec = AuthPolicy.read(property, env);
            c = spec == null ? null : fromSpec(spec);
        } catch (final IllegalArgumentException e) {
            throw new IllegalStateException(env + " (-D" + property + ") is malformed: " + e.getMessage());
        }
        if (c == null) {
            throw new IllegalStateException(env + " (-D" + property
                    + ") is not set; this client cannot authenticate to the " + engine + " cluster");
        }
        if (expected.length > 0) {
            for (final ClusterSessionClass e : expected) {
                if (e == c.sessionClass) {
                    return c;
                }
            }
            throw new IllegalStateException(env + " claims class " + c.sessionClass
                    + " (keyId " + c.keyId + "); this client must present " + java.util.Arrays.toString(expected));
        }
        return c;
    }

    public byte[] encode() {
        final ByteBuffer b = ByteBuffer.allocate(ENCODED_LENGTH).order(ByteOrder.LITTLE_ENDIAN);
        b.put((byte) VERSION).put((byte) sessionClass.code()).putInt(keyId).put(secret);
        return b.array();
    }

    /** Null when the bytes are not a v1 credential frame (length, version, class 1..7). */
    public static ClusterCredentials decode(final byte[] bytes) {
        if (bytes == null || bytes.length != ENCODED_LENGTH) {
            return null;
        }
        final ByteBuffer b = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        if ((b.get() & 0xFF) != VERSION) {
            return null;
        }
        final ClusterSessionClass c = ClusterSessionClass.of(b.get() & 0xFF);
        if (c == ClusterSessionClass.UNSET) {
            return null;
        }
        final int keyId = b.getInt();
        final byte[] secret = new byte[SECRET_LENGTH];
        b.get(secret);
        return new ClusterCredentials(c, keyId, secret);
    }

    /** The Aeron client-side supplier: presents these bytes, answers a challenge with nothing (v1). */
    public CredentialsSupplier supplier() {
        final byte[] encoded = encode();
        return new CredentialsSupplier() {
            public byte[] encodedCredentials() {
                return encoded.clone();
            }

            public byte[] onChallenge(final byte[] encodedChallenge) {
                return new byte[0];
            }
        };
    }

    public ClusterSessionClass sessionClass() {
        return sessionClass;
    }

    public int classCode() {
        return sessionClass.code();
    }

    public int keyId() {
        return keyId;
    }

    /** Copy. */
    public byte[] secret() {
        return secret.clone();
    }

    @Override
    public String toString() {
        return "ClusterCredentials{" + sessionClass + " keyId=" + keyId + "}"; // never the secret
    }
}
