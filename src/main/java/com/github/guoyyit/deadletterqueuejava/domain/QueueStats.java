package com.github.guoyyit.deadletterqueuejava.domain;

public record QueueStats(
        long totalEnqueued,
        long pendingRetry,
        long inFlight,
        long succeeded,
        long deadLetter,
        long replaying,
        long replaySucceeded,
        long replayFailed
) {
}
