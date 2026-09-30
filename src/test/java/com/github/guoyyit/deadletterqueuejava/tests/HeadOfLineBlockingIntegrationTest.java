package com.github.guoyyit.deadletterqueuejava.tests;

import com.github.guoyyit.deadletterqueuejava.domain.BackoffType;
import com.github.guoyyit.deadletterqueuejava.domain.Message;
import com.github.guoyyit.deadletterqueuejava.domain.QueueStats;
import com.github.guoyyit.deadletterqueuejava.domain.RetryPolicy;
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

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class HeadOfLineBlockingIntegrationTest {

    private static final String DB_DIR = "./target/test-data/hol-" + UUID.randomUUID();

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:file:" + DB_DIR + "/db;MODE=PostgreSQL;LOCK_TIMEOUT=10000");
        registry.add("dlq.data-dir", () -> DB_DIR);
        registry.add("dlq.retry.initial-backoff-ms", () -> 600000L);
        registry.add("dlq.retry.max-backoff-ms", () -> 600000L);
        registry.add("dlq.retry.backoff-type", () -> "FIXED");
        registry.add("dlq.dispatch.lease-ttl-ms", () -> 300000L);
        registry.add("dlq.retention.sweep-interval-ms", () -> 3600000L);
    }

    @Autowired
    private MessageService messageService;

    @Autowired
    private StatsService statsService;

    @Test
    void messageInBackoffDoesNotBlockOtherMessages() throws Exception {
        String queue = "hol-q";
        RetryPolicy backoff = new RetryPolicy(5, 600000L, 600000L, BackoffType.FIXED);

        Message bad = messageService.enqueue(queue, "bad", backoff);
        Message leasedBad = messageService.fetch(queue, 2000L);
        assertEquals(bad.id(), leasedBad.id());
        messageService.negativeAcknowledge(bad.id(), "bad payload");

        for (int i = 0; i < 3; i++) {
            Message good = messageService.enqueue(queue, "good-" + i, backoff);
            Message leasedGood = messageService.fetch(queue, 2000L);
            assertNotNull(leasedGood, "healthy messages must be deliverable while bad one backs off");
            assertEquals(good.id(), leasedGood.id());
            messageService.acknowledge(good.id());
        }

        assertNull(messageService.fetch(queue, 150L),
                "bad message must remain hidden inside its backoff window");

        QueueStats stats = statsService.stats(queue);
        assertEquals(1L, stats.pendingRetry());
        assertEquals(3L, stats.succeeded());
        assertEquals(0L, stats.inFlight());
        assertEquals(0L, stats.deadLetter());
    }
}
