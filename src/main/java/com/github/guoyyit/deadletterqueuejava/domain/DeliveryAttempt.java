package com.github.guoyyit.deadletterqueuejava.domain;

public record DeliveryAttempt(
        long id,
        String messageId,
        int attemptNo,
        String result,
        String errorReason,
        long startedAt,
        long finishedAt
) {
}
