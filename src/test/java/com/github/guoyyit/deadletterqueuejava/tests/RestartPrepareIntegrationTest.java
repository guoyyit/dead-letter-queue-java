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
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class RestartPrepareIntegrationTest {

    static final String QUEUE = "restart-q";
    static final String DB_DIR = "./target/test-data/restart-" + System.getProperty("dlq.test.run.id");

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:file:" + DB_DIR + "/db;MODE=PostgreSQL;DB_CLOSE_DELAY=-1");
        registry.add("dlq.data-dir", () -> DB_DIR);
        registry.add("dlq.retry.max-retries", () -> 3);
        registry.add("dlq.retry.initial-backoff-ms", () -> 600000L);
        registry.add("dlq.retry.max-backoff-ms", () -> 600000L);
        registry.add("dlq.retry.backoff-type", () -> "FIXED");
        registry.add("dlq.dispatch.lease-ttl-ms", () -> 300000L);
        registry.add("dlq.retention.sweep-interval-ms", () -> 3600000L);
    }
    static String waitingId;
    static String deadId;
    static String succeededId;

    @Autowired
    private MessageService messageService;

    @Autowired
    private StatsService statsService;

    @Test
    void prepareDurableState() throws Exception {
        RetryPolicy longBackoff = new RetryPolicy(3, 600000L, 600000L, BackoffType.FIXED);
        Message waiting = messageService.enqueue(QUEUE, "waiting", longBackoff);
        waitingId = waiting.id();
        Message leasedWaiting = messageService.fetch(QUEUE, 2000L);
        assertEquals(waitingId, leasedWaiting.id());
        messageService.negativeAcknowledge(waitingId, "waiting for backoff");

        RetryPolicy noRetry = new RetryPolicy(0, 1L, 1L, BackoffType.FIXED);
        Message dead = messageService.enqueue(QUEUE, "dead-msg", noRetry);
        deadId = dead.id();
        Message leasedDead = messageService.fetch(QUEUE, 2000L);
        assertEquals(deadId, leasedDead.id());
        messageService.negativeAcknowledge(deadId, "immediately dead");

        Message ok = messageService.enqueue(QUEUE, "ok", noRetry);
        succeededId = ok.id();
        Message leasedOk = messageService.fetch(QUEUE, 2000L);
        assertEquals(succeededId, leasedOk.id());
        messageService.acknowledge(succeededId);

        QueueStats stats = statsService.stats(QUEUE);
        assertEquals(1L, stats.pendingRetry());
        assertEquals(1L, stats.deadLetter());
        assertEquals(1L, stats.succeeded());
    }
}
