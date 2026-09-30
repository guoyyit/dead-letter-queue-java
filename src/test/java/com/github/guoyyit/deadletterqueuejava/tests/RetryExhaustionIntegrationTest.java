package com.github.guoyyit.deadletterqueuejava.tests;

import com.github.guoyyit.deadletterqueuejava.domain.BackoffType;
import com.github.guoyyit.deadletterqueuejava.domain.DeadLetterRecord;
import com.github.guoyyit.deadletterqueuejava.domain.DeliveryAttempt;
import com.github.guoyyit.deadletterqueuejava.domain.Message;
import com.github.guoyyit.deadletterqueuejava.domain.MessageStatus;
import com.github.guoyyit.deadletterqueuejava.domain.PagedResult;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class RetryExhaustionIntegrationTest {

    private static final String DB_DIR = "./target/test-data/retry-ex-" + UUID.randomUUID();

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:file:" + DB_DIR + "/db;MODE=PostgreSQL;LOCK_TIMEOUT=10000");
        registry.add("dlq.data-dir", () -> DB_DIR);
        registry.add("dlq.retry.max-retries", () -> 2);
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
    void retriesUntilExhaustedThenMovesAtomicallyToDeadLetter() throws Exception {
        RetryPolicy policy = new RetryPolicy(2, 1L, 1L, BackoffType.FIXED);
        Message enqueued = messageService.enqueue("orders", "payload-1", policy);
        String id = enqueued.id();

        for (int attempt = 1; attempt <= 2; attempt++) {
            Message leased = messageService.fetch("orders", 2000L);
            assertNotNull(leased, "message must be redelivered for retry " + attempt);
            assertEquals(id, leased.id());
            assertEquals(attempt - 1, leased.attempts());
            messageService.negativeAcknowledge(id, "transient failure " + attempt);
            assertEquals(MessageStatus.PENDING, messageService.getMessage(id).status());
            assertEquals(attempt, messageService.getMessage(id).attempts());
        }

        Message lastLease = messageService.fetch("orders", 2000L);
        assertNotNull(lastLease);
        assertEquals(2, lastLease.attempts());
        messageService.negativeAcknowledge(id, "permanent failure");

        Message dead = messageService.getMessage(id);
        assertEquals(MessageStatus.DEAD, dead.status());
        assertEquals(3, dead.attempts());
        assertEquals("permanent failure", dead.lastError());

        PagedResult<DeadLetterRecord> page =
                deadLetterService.list("orders", null, null, null, null, 0, 10);
        assertEquals(1, page.items().size());
        DeadLetterRecord record = page.items().get(0);
        assertEquals(id, record.messageId());
        assertEquals("payload-1", record.payload());
        assertEquals("permanent failure", record.lastReason());
        assertEquals(1, record.deathCount());
        assertEquals(3, record.attempts().size());
        assertTrue(record.attempts().stream().allMatch(a -> "FAILED".equals(a.result())));

        assertNull(messageService.fetch("orders", 100L),
                "dead message must never be dispatched again");

        QueueStats stats = statsService.stats("orders");
        assertEquals(1L, stats.totalEnqueued());
        assertEquals(0L, stats.pendingRetry());
        assertEquals(1L, stats.deadLetter());
        assertEquals(0L, stats.succeeded());
    }
}
