// SPDX-License-Identifier: Apache-2.0
package com.openexchange.cluster.log;

/**
 * The seven cluster-schema templates that occur as committed log frames (Aeron 1.53 schema 111 version 17):
 * 1 SessionMessageHeader, 20 TimerEvent, 21 SessionOpenEvent, 22 SessionCloseEvent, 23 ClusterActionRequest,
 * 24 NewLeadershipTermEvent, 25 MembershipChangeEvent. {@link #MEMBERSHIP_CHANGE} is counted, never delivered.
 */
public enum FrameKind {
    SESSION_MESSAGE,
    TIMER_EVENT,
    SESSION_OPEN,
    SESSION_CLOSE,
    CLUSTER_ACTION,
    NEW_LEADERSHIP_TERM,
    MEMBERSHIP_CHANGE
}
