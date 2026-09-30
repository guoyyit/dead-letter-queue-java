package com.github.guoyyit.deadletterqueuejava.domain;

import java.util.List;

public record DeadLetterRecord(
        long id,
        String messageId,
        String queue,
        String payload,
        String lastReason,
        long firstDiedAt,
        long lastDiedAt,
        int deathCount,
        int replayCount,
        DeadLetterStatus status,
        List<DeliveryAttempt> attempts
) {
}
