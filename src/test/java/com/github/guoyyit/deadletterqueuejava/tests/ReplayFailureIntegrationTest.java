package com.github.guoyyit.deadletterqueuejava.tests;

import com.github.guoyyit.deadletterqueuejava.domain.BackoffType;
import com.github.guoyyit.deadletterqueuejava.domain.DeadLetterRecord;
import com.github.guoyyit.deadletterqueuejava.domain.DeadLetterStatus;
import com.github.guoyyit.deadletterqueuejava.domain.Message;
import com.github.guoyyit.deadletterqueuejava.domain.QueueStats;
import com.github.guoyyit.deadletterqueuejava.domain.RetryPolicy;
import com.github.guoyyit.deadletterqueuejava.service.DeadLetterService;
import com.github.guoyyit.deadletterqueuejava.service.MessageService;
import com.github.guoyyit.deadletterqueuejava.service.StatsService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class ReplayFailureIntegrationTest {

    private static final String DB_DIR = "./target/test-data/replay-fail-" + UUID.randomUUID();

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:file:" + DB_DIR + "/db;MODE=PostgreSQL;LOCK_TIMEOUT=10000");
        registry.add("dlq.data-dir", () -> DB_DIR);
        registry.add("dlq.retry.max-retries", () -> 0);
        registry.add("dlq.retry.initial-backoff-ms", () -> 1L);
        registry.add("dlq.retry.max-backoff-ms", () -> 1L);
        registry.add("dlq.retry.backoff-type", () -> "FIXED");
        registry.add("dlq.dispatch.lease-ttl-ms", () -> 300000L);
        registry.add("dlq.retention.sweep-interval-ms", () -> 3600000L);
    }

    @Autowired
    private MessageService messageService;

    @Autowired
    private DeadLetterService deadLetterService;

    @Autowired
    private StatsService statsService;

    @Test
    void replayedMessageThatFailsReturnsToDeadLetterWithHistory() throws Exception {
        String queue = "replay-fail-q";
        RetryPolicy policy = new RetryPolicy(0, 1L, 1L, BackoffType.FIXED);
        Message message = messageService.enqueue(queue, "payload", policy);
        Message leased = messageService.fetch(queue, 2000L);
        assertEquals(message.id(), leased.id());
        messageService.negativeAcknowledge(message.id(), "original cause");

        long dlId = deadLetterService.list(queue, null, null, null, null, 0, 10)
                .items().get(0).id();

        deadLetterService.replayOne(dlId);
        assertEquals(DeadLetterStatus.REPLAYING, deadLetterService.get(dlId).status());

        assertThrows(com.github.guoyyit.deadletterqueuejava.exception.ConflictException.class,
                () -> deadLetterService.replayOne(dlId),
                "an in-flight replay cannot be replayed again");

        assertEquals(com.github.guoyyit.deadletterqueuejava.domain.MessageStatus.IN_FLIGHT,
                messageService.getMessage(message.id()).status());
        messageService.negativeAcknowledge(message.id(), "replayed failure cause");

        DeadLetterRecord backInDlq = deadLetterService.get(dlId);
        assertEquals(DeadLetterStatus.ACTIVE, backInDlq.status());
        assertEquals(2, backInDlq.deathCount(), "repeat death must accumulate");
        assertEquals(0, backInDlq.replayCount(), "count increments only on finished replays");
        assertEquals("replayed failure cause", backInDlq.lastReason());
        assertEquals("payload", backInDlq.payload());
        assertEquals(2, backInDlq.attempts().size());
        assertTrue(backInDlq.attempts().stream()
                .anyMatch(a -> "replayed failure cause".equals(a.errorReason())));
        assertTrue(backInDlq.attempts().stream()
                .anyMatch(a -> "original cause".equals(a.errorReason())));

        QueueStats stats = statsService.stats(queue);
        assertEquals(1L, stats.deadLetter());
        assertEquals(0L, stats.replaying());
        assertEquals(0L, stats.replaySucceeded());
        assertEquals(1L, stats.replayFailed());
    }
}
