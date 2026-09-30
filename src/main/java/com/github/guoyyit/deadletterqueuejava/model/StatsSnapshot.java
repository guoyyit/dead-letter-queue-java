package com.github.guoyyit.deadletterqueuejava.model;

import java.util.Map;

/**
 * 统计快照，全部由当前记录派生，重启后天然一致。
 */
public record StatsSnapshot(
        long pendingRetry,
        long inFlight,
        long dead,
        long replaySuccess,
        long replayFailure,
        Map<String, QueueStats> queues
) {
    public record QueueStats(
            long pending,
            long pendingRetry,
            long inFlight,
            long dead
    ) {
    }
}
