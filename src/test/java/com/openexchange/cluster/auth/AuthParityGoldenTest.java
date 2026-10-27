// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.auth;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.StringJoiner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * RC2 S6 9.1 (W2): the kit reproduces every RC1 S3 credential byte, principal byte and key-table decision
 * captured from assets 462b98d into {@code auth-parity-v1.txt}. A row marked DELTA differs by design (the
 * tr_TR uppercasing fix, contract 12.9, and the A16 users field, 12.12): there the kit must give the recorded
 * S6 expectation and that expectation must differ from the RC1 capture. Key tables use the AE policy, the one
 * RC1 had. ERR/LOAD_ERR compare by kind only.
 */
public class AuthParityGoldenTest {

    private static final HexFormat HEX = HexFormat.of();

    private static String unesc(final String s) {
        return "<empty>".equals(s) ? "" : s.replace("\\t", "\t");
    }

    private static String kind(final String result) {
        if (result.startsWith("LOAD_ERR")) {
            return "LOAD_ERR";
        }
        if (result.startsWith("ERR")) {
            return "ERR";
        }
        final int space = result.indexOf(' ');
        return space < 0 ? result : result.substring(0, space); // drop a DELTA note in parentheses
    }

    private static String withLocale(final String locale, final java.util.function.Supplier<String> s) {
        final Locale prior = Locale.getDefault();
        Locale.setDefault("tr_TR".equals(locale) ? Locale.forLanguageTag("tr-TR") : Locale.ROOT);
        try {
            return s.get();
        } finally {
            Locale.setDefault(prior);
        }
    }

    static String cred(final String spec) {
        try {
            return HEX.formatHex(ClusterCredentials.fromSpec(spec).encode());
        } catch (RuntimeException e) {
            return "ERR:" + e.getClass().getSimpleName();
        }
    }

    static String credDecode(final String in) {
        final byte[] b = "<null>".equals(in) ? null : HEX.parseHex(unesc(in));
        final ClusterCredentials c = ClusterCredentials.decode(b);
        return c == null ? "NULL" : c.classCode() + ":" + c.keyId() + ":" + HEX.formatHex(c.secret());
    }

    static String describe(final ClusterPrincipal p) {
        if (p == ClusterPrincipal.NONE) {
            return "NONE";
        }
        final StringJoiner users = new StringJoiner("|");
        for (long u : p.userIds()) {
            users.add(Long.toString(u));
        }
        return "class=" + p.classCode() + ",flags=" + p.flags() + ",identity=" + p.identity()
                + ",any=" + p.anyUser() + ",users=" + users;
    }

    static String principal(final String in) {
        final String[] f = in.split(":", -1);
        final ClusterSessionClass c = ClusterSessionClass.of(Integer.parseInt(f[1]));
        final int identity = Integer.parseInt(f[2]);
        try {
            if ("any".equals(f[0])) {
                return HEX.formatHex(ClusterPrincipal.anyUser(c, identity).encode());
            }
            final long[] users;
            if ("511ids".equals(f[3])) {
                users = new long[511];
            } else if (f[3].isEmpty()) {
                users = new long[0];
            } else {
                final String[] ids = f[3].split("\\|");
                users = new long[ids.length];
                for (int i = 0; i < ids.length; i++) {
                    users[i] = Long.parseLong(ids[i]);
                }
            }
            final byte[] bytes = ClusterPrincipal.forUsers(c, identity, users).encode();
            return f[3].equals("1|1") || f[3].isEmpty() || f[3].equals("511ids") ? "OK" : HEX.formatHex(bytes);
        } catch (RuntimeException e) {
            return "ERR:" + e.getClass().getSimpleName();
        }
    }

    static String pdecode(final String in) {
        final byte[] b = "<null>".equals(in) ? null : HEX.parseHex(unesc(in));
        final ClusterPrincipal p = ClusterPrincipal.decode(b);
        assertEquals("classCodeOf agrees with decode for " + in, p.classCode(), ClusterPrincipal.classCodeOf(b));
        return describe(p);
    }

    static String table(final String tableSpec, final String credSpec) {
        final AuthKeyTable t;
        try {
            t = AuthKeyTable.fromSpec(unesc(tableSpec), AuthTestPolicies.AE);
        } catch (RuntimeException e) {
            return "LOAD_ERR:" + e.getClass().getSimpleName();
        }
        final ClusterPrincipal p = t.authenticate(ClusterCredentials.fromSpec(unesc(credSpec)));
        return p == null ? "REJECT" : HEX.formatHex(p.encode());
    }

    private static List<String[]> rows() throws Exception {
        final List<String[]> rows = new ArrayList<>();
        try (InputStream in = AuthParityGoldenTest.class.getResourceAsStream("/auth-parity-v1.txt")) {
            if (in == null) {
                fail("auth-parity-v1.txt is missing from the test resources");
            }
            final BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String line;
            while ((line = r.readLine()) != null) {
                if (!line.isEmpty() && !line.startsWith("#")) {
                    rows.add(line.split("\t", -1));
                }
            }
        }
        return rows;
    }

    @Test
    public void kitDecisionsEqualTheRc1CaptureExceptTheDeltaRows() throws Exception {
        int checked = 0;
        int deltas = 0;
        final List<String> failures = new ArrayList<>();
        for (String[] row : rows()) {
            final String kind = row[0];
            final String locale = row[1];
            final int resultAt = "table".equals(kind) ? 4 : 3;
            final String rc1 = row[resultAt];
            final boolean delta = row.length > resultAt + 1 && "DELTA".equals(row[resultAt + 1]);
            final String expected = delta ? row[resultAt + 2] : rc1;
            final String kit = withLocale(locale, () -> {
                switch (kind) {
                    case "cred": return cred(unesc(row[2]));
                    case "creddecode": return credDecode(row[2]);
                    case "principal": return principal(row[2]);
                    case "pdecode": return pdecode(row[2]);
                    case "table": return table(row[2], row[3]);
                    default: throw new AssertionError("unknown row kind " + kind);
                }
            });
            if (!kind(expected).equals(kind(kit))) {
                failures.add(String.join("\t", row).substring(0, Math.min(160, String.join("\t", row).length()))
                        + "  => kit " + kit.substring(0, Math.min(60, kit.length())));
            }
            if (delta) {
                deltas++;
                assertNotEquals("a DELTA row must differ from RC1: " + String.join(" ", row), kind(rc1), kind(expected));
            }
            checked++;
        }
        assertTrue("parity mismatches:\n" + String.join("\n", failures), failures.isEmpty());
        assertEquals("the capture's DELTA rows (3 tr_TR credentials, 1 tr_TR table, 7 A16 tables)", 11, deltas);
        assertTrue("rows checked: " + checked, checked >= 120);
    }
}
