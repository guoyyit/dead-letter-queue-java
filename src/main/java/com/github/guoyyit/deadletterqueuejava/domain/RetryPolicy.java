package com.github.guoyyit.deadletterqueuejava.domain;

public record RetryPolicy(
        int maxRetries,
        long initialBackoffMs,
        long maxBackoffMs,
        BackoffType backoffType
) {
}
