package com.github.guoyyit.deadletterqueuejava.tests;

import com.github.guoyyit.deadletterqueuejava.domain.BackoffType;
import com.github.guoyyit.deadletterqueuejava.domain.DeadLetterRecord;
import com.github.guoyyit.deadletterqueuejava.domain.DeadLetterStatus;
import com.github.guoyyit.deadletterqueuejava.domain.Message;
import com.github.guoyyit.deadletterqueuejava.domain.MessageStatus;
import com.github.guoyyit.deadletterqueuejava.domain.QueueStats;
import com.github.guoyyit.deadletterqueuejava.domain.RetryPolicy;
import com.github.guoyyit.deadletterqueuejava.exception.ConflictException;
import com.github.guoyyit.deadletterqueuejava.service.DeadLetterService;
import com.github.guoyyit.deadletterqueuejava.service.MessageService;
import com.github.guoyyit.deadletterqueuejava.service.StatsService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class ConcurrentReplayClearIntegrationTest {

    private static final String DB_DIR =
            "./target/test-data/conc-" + UUID.randomUUID();

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

    @Autowired
    private DataSource dataSource;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @AfterEach
    void cleanTables() throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement st = connection.createStatement()) {
            st.execute("DELETE FROM dead_letter_replays");
            st.execute("DELETE FROM dead_letters");
            st.execute("DELETE FROM delivery_attempts");
            st.execute("DELETE FROM messages");
        }
    }

    @Test
    void sameDeadLetterCannotBeReplayedTwiceConcurrently() throws Exception {
        String queue = "conc-parallel-q";
        long dlId = seedDeadLetter(queue, "m1");

        int threads = 8;
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger success = new AtomicInteger();
        AtomicInteger conflict = new AtomicInteger();
        AtomicInteger other = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int i = 0; i < threads; i++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        deadLetterService.replayOne(dlId);
                        success.incrementAndGet();
                    } catch (ConflictException ex) {
                        conflict.incrementAndGet();
                    } catch (Exception ex) {
                        other.incrementAndGet();
                        ex.printStackTrace();
                    }
                });
            }
            start.countDown();
        } finally {
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        }

        assertEquals(0, other.get(), "no unexpected errors under concurrent replay");
        assertEquals(1, success.get(), "exactly one concurrent replay must win");
        assertEquals(threads - 1, conflict.get());

        DeadLetterRecord replaying = deadLetterService.get(dlId);
        assertEquals(DeadLetterStatus.REPLAYING, replaying.status());

        assertThrows(ConflictException.class, () -> deadLetterService.clearOne(dlId),
                "a record being replayed must not be clearable");

        QueueStats stats = statsService.stats(queue);
        assertEquals(1L, stats.replaying());
        assertEquals(0L, stats.deadLetter());
    }

    @Test
    void clearWhileReplayInFlightKeepsTheMessageReachable() throws Exception {
        String queue = "conc-clear-q";
        long dlId = seedDeadLetter(queue, "m2");
        long replayId = deadLetterService.replayOne(dlId);
        assertTrue(replayId > 0);

        assertThrows(ConflictException.class, () -> deadLetterService.clearOne(dlId));

        String messageId = deadLetterService.get(dlId).messageId();
        Message message = messageService.getMessage(messageId);
        assertEquals(MessageStatus.IN_FLIGHT, message.status());
        messageService.acknowledge(messageId);

        assertEquals(DeadLetterStatus.RESOLVED, deadLetterService.get(dlId).status());
        QueueStats stats = statsService.stats(queue);
        assertEquals(0L, stats.replayFailed());
        assertEquals(1L, stats.replaySucceeded());
        assertEquals(0L, stats.deadLetter());
    }

    private long seedDeadLetter(String queue, String payload) throws Exception {
        RetryPolicy policy = new RetryPolicy(0, 1L, 1L, BackoffType.FIXED);
        Message message = new TransactionTemplate(transactionManager)
                .execute(status -> messageService.enqueue(queue, payload, policy));
        Message leased = messageService.fetch(queue, 2000L);
        assertEquals(message.id(), leased.id());
        messageService.negativeAcknowledge(message.id(), "boom");
        return deadLetterService.list(queue, null, null, null, null, 0, 10)
                .items().stream()
                .filter(r -> r.messageId().equals(message.id()))
                .findFirst().orElseThrow().id();
    }
}
