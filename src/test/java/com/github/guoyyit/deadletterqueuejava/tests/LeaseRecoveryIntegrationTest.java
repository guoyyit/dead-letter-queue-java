package com.github.guoyyit.deadletterqueuejava.tests;

import com.github.guoyyit.deadletterqueuejava.domain.BackoffType;
import com.github.guoyyit.deadletterqueuejava.domain.Message;
import com.github.guoyyit.deadletterqueuejava.domain.MessageStatus;
import com.github.guoyyit.deadletterqueuejava.domain.QueueStats;
import com.github.guoyyit.deadletterqueuejava.domain.RetryPolicy;
import com.github.guoyyit.deadletterqueuejava.service.MessageService;
import com.github.guoyyit.deadletterqueuejava.service.LeaseRecoveryService;
import com.github.guoyyit.deadletterqueuejava.service.StatsService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class LeaseRecoveryIntegrationTest {

    private static final String DB_DIR = "./target/test-data/lease-" + UUID.randomUUID();

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:file:" + DB_DIR + "/db;MODE=PostgreSQL;LOCK_TIMEOUT=10000");
        registry.add("dlq.data-dir", () -> DB_DIR);
        registry.add("dlq.retry.max-retries", () -> 5);
        registry.add("dlq.retry.initial-backoff-ms", () -> 1L);
        registry.add("dlq.retry.max-backoff-ms", () -> 1L);
        registry.add("dlq.retry.backoff-type", () -> "FIXED");
        registry.add("dlq.dispatch.lease-ttl-ms", () -> 30L);
        registry.add("dlq.dispatch.lease-recovery-initial-delay-ms", () -> 3600000L);
        registry.add("dlq.retention.sweep-interval-ms", () -> 3600000L);
    }

    @Autowired
    private MessageService messageService;

    @Autowired
    private LeaseRecoveryService leaseRecoveryService;

    @Autowired
    private StatsService statsService;

    @Test
    void expiredLeaseIsRecoveredAndRedeliveredWithoutLoss() throws Exception {
        String queue = "lease-q";
        RetryPolicy policy = new RetryPolicy(5, 1L, 1L, BackoffType.FIXED);
        Message message = messageService.enqueue(queue, "lease-payload", policy);

        Message leased = messageService.fetch(queue, 2000L);
        assertEquals(message.id(), leased.id());
        assertEquals(MessageStatus.IN_FLIGHT, messageService.getMessage(message.id()).status());

        Thread.sleep(60L);
        int recovered = leaseRecoveryService.recoverExpiredLeases();
        assertEquals(1, recovered);

        Message redelivered = messageService.fetch(queue, 2000L);
        assertNotNull(redelivered, "message with expired lease must be redelivered");
        assertEquals(message.id(), redelivered.id());
        assertEquals(0, redelivered.attempts(),
                "the crashed attempt produced no committed outcome, so no attempt is counted");

        messageService.acknowledge(message.id());
        assertEquals(1, messageService.getMessage(message.id()).attempts());
        QueueStats stats = statsService.stats(queue);
        assertEquals(1L, stats.succeeded());
        assertEquals(0L, stats.pendingRetry());
        assertEquals(0L, stats.inFlight());
    }
}
