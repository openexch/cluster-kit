// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster;

import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.cluster.codecs.AppendPositionEncoder;
import io.aeron.cluster.codecs.CanvassPositionEncoder;
import io.aeron.cluster.codecs.CatchupPositionEncoder;
import io.aeron.cluster.codecs.CommitPositionEncoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.NewLeadershipTermEncoder;
import io.aeron.cluster.codecs.RequestVoteEncoder;
import io.aeron.cluster.codecs.StopCatchupEncoder;
import io.aeron.cluster.codecs.TerminationAckEncoder;
import io.aeron.cluster.codecs.TerminationPositionEncoder;
import io.aeron.cluster.codecs.VoteEncoder;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;

import java.util.Locale;
import java.util.function.Function;

/**
 * The consensus channel's term length: {@code TRANSPORT_CONSENSUS_TERM_LENGTH}, default {@value #DEFAULT_TERM_LENGTH}.
 *
 * <p>Aeron's consensus channel default is a bare {@code aeron:udp}, so without this setting every consensus
 * publication takes the media driver's publication term (16m on both engines' drivers and on a stock aeronmd). Each
 * member holds a publication to every peer and an image from every peer, each three terms long: on a three-member
 * cluster that is 4 x 48 MB = 192 MB of {@code /dev/shm} per member for messages that are all under 300 bytes.</p>
 *
 * <p>What a member sends on it is fixed size except for the catch-up endpoint: the largest is
 * {@link #LARGEST_MEMBER_MESSAGE_BYTES} (a {@code CatchupPosition} naming a 253-character host name), against a
 * maximum message of term / 8 (8 KB at the 64k floor, 128 KB at the default). So the floor is Aeron's own minimum
 * term and nothing above it is too small for consensus.</p>
 *
 * <p>Both engines and the replica read the same setting through this class, so a member and the replica can never
 * parse it differently. An unparsable value, a value that is not a power of two, or one outside Aeron's
 * [64k, 1g] is refused at boot; nothing falls back to a default.</p>
 */
public record ConsensusChannel(String termLengthText, long termLength, boolean derived) {

    /** The environment name. */
    public static final String SETTING = "TRANSPORT_CONSENSUS_TERM_LENGTH";
    /** The system property read when the environment names nothing. */
    public static final String PROPERTY = "transport.consensus.term.length";
    /** The default term. */
    public static final String DEFAULT_TERM_LENGTH = "1m";
    /** A host name is at most 253 characters, plus {@code :} and a five-digit port. */
    static final int MAX_ENDPOINT_LENGTH = 253 + 1 + 5;

    /**
     * The largest message a member publishes on the consensus channel, header included: the fixed blocks of every
     * member-to-member message, and the catch-up position with the longest endpoint. Messages that clients send to a
     * member (backup query, heartbeat request) travel on the client's own publication and term, not this one.
     */
    public static final int LARGEST_MEMBER_MESSAGE_BYTES = MessageHeaderEncoder.ENCODED_LENGTH + Math.max(
            Math.max(Math.max(CanvassPositionEncoder.BLOCK_LENGTH, RequestVoteEncoder.BLOCK_LENGTH),
                    Math.max(VoteEncoder.BLOCK_LENGTH, NewLeadershipTermEncoder.BLOCK_LENGTH)),
            Math.max(
                    Math.max(Math.max(AppendPositionEncoder.BLOCK_LENGTH, CommitPositionEncoder.BLOCK_LENGTH),
                            Math.max(StopCatchupEncoder.BLOCK_LENGTH, TerminationPositionEncoder.BLOCK_LENGTH)),
                    Math.max(TerminationAckEncoder.BLOCK_LENGTH, CatchupPositionEncoder.BLOCK_LENGTH
                            + CatchupPositionEncoder.catchupEndpointHeaderLength() + MAX_ENDPOINT_LENGTH)));

    /**
     * @param env a setting's raw text by its environment name, null when unset
     * @throws IllegalArgumentException with the refusal text
     */
    public static ConsensusChannel parse(final Function<String, String> env) {
        final String raw = env.apply(SETTING);
        if (raw == null || raw.isBlank()) {
            return new ConsensusChannel(DEFAULT_TERM_LENGTH, 1024 * 1024, true);
        }
        final String text = raw.trim();
        final long length;
        try {
            length = org.agrona.SystemUtil.parseSize(SETTING, text);
        } catch (final RuntimeException e) {
            throw new IllegalArgumentException(SETTING + " '" + text
                    + "' is not a size (expected <n>, <n>k, <n>m or <n>g)");
        }
        if (length < LogBufferDescriptor.TERM_MIN_LENGTH || length > LogBufferDescriptor.TERM_MAX_LENGTH) {
            throw new IllegalArgumentException(SETTING + " " + text + " outside [64k, 1g]");
        }
        if (Long.bitCount(length) != 1) {
            throw new IllegalArgumentException(SETTING + " " + text + " is not a power of two");
        }
        if (FrameDescriptor.computeMaxMessageLength((int) length) < LARGEST_MEMBER_MESSAGE_BYTES) {
            throw new IllegalArgumentException(SETTING + " " + text + " cannot carry a " + LARGEST_MEMBER_MESSAGE_BYTES
                    + " B consensus message");
        }
        return new ConsensusChannel(text, length, false);
    }

    /** This process's setting: the environment first, then {@value #PROPERTY}, else the default. */
    public static ConsensusChannel fromEnvironment() {
        return parse(name -> {
            final String v = System.getenv(name);
            if (v != null && !v.isBlank()) {
                return v;
            }
            return System.getProperty(name.toLowerCase(Locale.ROOT).replace('_', '.'));
        });
    }

    /** The consensus channel a member hands to {@code ConsensusModule.Context#consensusChannel}. */
    public String uri() {
        return "aeron:udp?" + CommonContext.TERM_LENGTH_PARAM_NAME + "=" + termLengthText;
    }

    /** {@code channel} with this term length set (any term it named is replaced). */
    public String applyTo(final String channel) {
        final ChannelUri uri = ChannelUri.parse(channel);
        uri.put(CommonContext.TERM_LENGTH_PARAM_NAME, termLengthText);
        return uri.toString();
    }

    /** As a boot line names it: {@code consensus term 1m default} or {@code consensus term 4m set}. */
    public String line() {
        return "consensus term " + termLengthText + (derived ? " default" : " set");
    }
}
