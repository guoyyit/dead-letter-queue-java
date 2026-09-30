package com.github.guoyyit.deadletterqueuejava.domain;

public record Message(
        String id,
        String queue,
        String payload,
        MessageStatus status,
        int attempts,
        long createdAt,
        long visibleAt,
        long leasedUntil,
        String lastError,
        RetryPolicy retryPolicy
) {
}
