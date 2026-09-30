package com.github.guoyyit.deadletterqueuejava.domain;

public record DeadLetterReplay(
        long id,
        long deadLetterId,
        String messageId,
        ReplayOutcome outcome,
        long startedAt,
        long finishedAt,
        String errorReason
) {
}
