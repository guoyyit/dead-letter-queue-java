package com.github.guoyyit.deadletterqueuejava.tests;

import com.github.guoyyit.deadletterqueuejava.config.DlqProperties;
import com.github.guoyyit.deadletterqueuejava.model.BackoffType;
import com.github.guoyyit.deadletterqueuejava.model.DeadLetterRecord;
import com.github.guoyyit.deadletterqueuejava.model.DeadLetterStatus;
import com.github.guoyyit.deadletterqueuejava.model.DeliveredMessage;
import com.github.guoyyit.deadletterqueuejava.model.MessageStatus;
import com.github.guoyyit.deadletterqueuejava.model.QueueRecord;
import com.github.guoyyit.deadletterqueuejava.model.RetryStrategy;
import com.github.guoyyit.deadletterqueuejava.service.DeadLetterService;
import com.github.guoyyit.deadletterqueuejava.service.DeadLetterServiceImpl;
import com.github.guoyyit.deadletterqueuejava.service.DefaultRetryPlanner;
import com.github.guoyyit.deadletterqueuejava.service.QueueService;
import com.github.guoyyit.deadletterqueuejava.service.QueueServiceImpl;
import com.github.guoyyit.deadletterqueuejava.service.RetryPlanner;
import com.github.guoyyit.deadletterqueuejava.service.StatsService;
import com.github.guoyyit.deadletterqueuejava.service.StatsServiceImpl;
import com.github.guoyyit.deadletterqueuejava.store.FileStateStore;
import com.github.guoyyit.deadletterqueuejava.store.StateStore;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 重试行为验收测试。
 */
class RetryBehaviorTest {

    private static final String QUEUE = "orders";

    private record Harness(StateStore store, QueueService queues, DeadLetterService deadLetters,
                           StatsService stats, DlqProperties properties) {
    }

    private static RetryStrategy strategy(int maxRetries, long initialMillis,
                                          long maxIntervalMillis, BackoffType type) {
        return new RetryStrategy(maxRetries, initialMillis, maxIntervalMillis, type);
    }

    private static Harness harness(Path dir, long visibilityTimeoutMillis) {
        return harness(dir, visibilityTimeoutMillis, true);
    }

    private static Harness harness(Path dir, long visibilityTimeoutMillis, boolean clean) {
        if (clean) {
            cleanDirectory(dir);
        }
        DlqProperties properties = new DlqProperties();
        properties.setStoreDir(dir.toString());
        properties.setVisibilityTimeoutMillis(visibilityTimeoutMillis);
        properties.setDefaultMaxRetries(1);
        properties.setDefaultInitialIntervalMillis(1L);
        properties.setDefaultMaxIntervalMillis(5_000L);
        FileStateStore store = new FileStateStore(properties);
        store.start();
        RetryPlanner planner = new DefaultRetryPlanner(properties);
        QueueService queues = new QueueServiceImpl(store, planner, properties);
        DeadLetterService deadLetters =
                new DeadLetterServiceImpl(store, properties, planner.resolve(null));
        StatsService stats = new StatsServiceImpl(store);
        return new Harness(store, queues, deadLetters, stats, properties);
    }

    private static void cleanDirectory(Path dir) {
        if (!java.nio.file.Files.exists(dir)) {
            return;
        }
        try (java.util.stream.Stream<Path> walk = java.nio.file.Files.walk(dir)) {
            walk.sorted(java.util.Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            java.nio.file.Files.deleteIfExists(path);
                        } catch (java.io.IOException e) {
                            throw new java.io.UncheckedIOException(e);
                        }
                    });
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static Harness restart(Harness previous, Path dir) {
        return harness(dir, previous.properties().getVisibilityTimeoutMillis(), false);
    }

    private static DeliveredMessage poll(Harness h) {
        return h.queues().poll(QUEUE);
    }

    private static DeadLetterRecord pollAndNack(Harness h, String reason) {
        DeliveredMessage message = poll(h);
        assertNotNull(message);
        return h.queues().nack(message.id(), reason);
    }

    private static DeadLetterRecord pollNackAndAwait(Harness h, String reason) {
        DeadLetterRecord deadLetter = pollAndNack(h, reason);
        try {
            Thread.sleep(15L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return deadLetter;
    }

    private static DeliveredMessage awaitPoll(Harness h) {
        DeliveredMessage message = null;
        long deadline = System.currentTimeMillis() + 2_000L;
        while (message == null && System.currentTimeMillis() < deadline) {
            message = poll(h);
            if (message == null) {
                try {
                    Thread.sleep(5L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        assertNotNull(message);
        return message;
    }

    @Test
    void firstFailureEntersBackoffAndThenRedelivered() throws Exception {
        Path dir = Path.of("target/test-dlq-first-backoff");
        Harness h = harness(dir, 60_000L);
        QueueRecord record = h.queues().publish(QUEUE, "payload-1",
                strategy(3, 40L, 5_000L, BackoffType.FIXED));

        DeliveredMessage first = poll(h);
        assertNotNull(first);
        assertEquals(1, first.delivered());
        assertNull(h.queues().nack(first.id(), "boom"));

        assertEquals(1, h.stats().snapshot().pendingRetry());
        assertEquals(0, h.stats().snapshot().dead());
        assertNull(poll(h), "退避未到期前不得再次投递");

        Thread.sleep(90L);
        DeliveredMessage second = poll(h);
        assertNotNull(second, "退避到期后应重新投递");
        assertEquals(record.getId(), second.id());
        assertEquals(2, second.delivered());
    }

    @Test
    void retriesExhaustedMovesToDeadLetterWithFullHistory() {
        Harness h = harness(Path.of("target/test-dlq-exhausted"), 60_000L);
        h.queues().publish(QUEUE, "payload-exhaust",
                strategy(2, 5L, 5_000L, BackoffType.FIXED));

        assertNull(pollNackAndAwait(h, "fail-1"));
        assertNull(pollNackAndAwait(h, "fail-2"));
        DeadLetterRecord deadLetter = pollAndNack(h, "fail-3");

        assertNotNull(deadLetter);
        assertEquals(QUEUE, deadLetter.getOriginQueue());
        assertEquals("payload-exhaust", deadLetter.getOriginalPayload());
        assertEquals("fail-3", deadLetter.getLastFailureReason());
        assertEquals(DeadLetterStatus.DEAD, deadLetter.getStatus());
        assertEquals(3, deadLetter.getRetryHistory().size());
        assertEquals(1, deadLetter.getRetryHistory().get(0).delivery());
        assertTrue(deadLetter.getRetryHistory().get(0).backoffMillis() > 0);
        assertEquals(3, deadLetter.getRetryHistory().get(2).delivery());
        assertEquals(-1L, deadLetter.getRetryHistory().get(2).backoffMillis());
        assertEquals(1, h.stats().snapshot().dead());
        assertEquals(0, h.stats().snapshot().pendingRetry());
    }

    @Test
    void zeroRetriesMovesToDeadOnFirstFailure() {
        Harness h = harness(Path.of("target/test-dlq-zero"), 60_000L);
        h.queues().publish(QUEUE, "payload-zero",
                strategy(0, 1_000L, 5_000L, BackoffType.FIXED));

        DeadLetterRecord deadLetter = pollAndNack(h, "once");
        assertNotNull(deadLetter);
        assertEquals(1, deadLetter.getRetryHistory().size());
        assertEquals(1, h.stats().snapshot().dead());
        assertEquals(0, h.stats().snapshot().pendingRetry());
        assertNull(poll(h));
    }

    @Test
    void nonRetryableReasonSkipsRetries() {
        Harness h = harness(Path.of("target/test-dlq-nonretryable"), 60_000L);
        h.queues().publish(QUEUE, "payload-fatal",
                strategy(3, 1_000L, 5_000L, BackoffType.FIXED));

        DeadLetterRecord deadLetter = pollAndNack(h, "NON_RETRYABLE:poison");
        assertNotNull(deadLetter);
        assertEquals("NON_RETRYABLE:poison", deadLetter.getLastFailureReason());
        assertEquals(1, deadLetter.getRetryHistory().size());
        assertEquals(-1L, deadLetter.getRetryHistory().get(0).backoffMillis());
        assertEquals(1, h.stats().snapshot().dead());
        assertNull(poll(h));
    }

    @Test
    void backoffRulesMatchTypeAndAreCapped() {
        DlqProperties properties = new DlqProperties();
        properties.setStoreDir("target/test-dlq-planner");
        DefaultRetryPlanner planner = new DefaultRetryPlanner(properties);
        RetryStrategy fixed = strategy(5, 100L, 1_000L, BackoffType.FIXED);
        RetryStrategy linear = strategy(5, 100L, 1_000L, BackoffType.LINEAR);
        RetryStrategy exponential = strategy(5, 100L, 1_000L, BackoffType.EXPONENTIAL);

        assertEquals(100L, planner.backoffMillis(fixed, 1));
        assertEquals(100L, planner.backoffMillis(fixed, 4));
        assertEquals(100L, planner.backoffMillis(linear, 1));
        assertEquals(300L, planner.backoffMillis(linear, 3));
        assertEquals(100L, planner.backoffMillis(exponential, 1));
        assertEquals(200L, planner.backoffMillis(exponential, 2));
        assertEquals(400L, planner.backoffMillis(exponential, 3));
        assertEquals(800L, planner.backoffMillis(exponential, 4));
        assertEquals(1_000L, planner.backoffMillis(exponential, 5), "退避受间隔上限约束");
        assertTrue(planner.isNonRetryable("NON_RETRYABLE:bad"));
        assertFalse(planner.isNonRetryable("ordinary failure"));
    }

    @Test
    void restartDuringBackoffKeepsAbsoluteDeadline() throws Exception {
        Path dir = Path.of("target/test-dlq-restart-backoff");
        Harness before = harness(dir, 60_000L);
        before.queues().publish(QUEUE, "payload-restart",
                strategy(3, 800L, 5_000L, BackoffType.FIXED));
        DeliveredMessage first = poll(before);
        before.queues().nack(first.id(), "boom");
        long readyAtBefore = before.store().read(state ->
                state.getMessages().get(0).getNextReadyAt());

        Harness after = restart(before, dir);
        long readyAtAfter = after.store().read(state ->
                state.getMessages().get(0).getNextReadyAt());
        assertEquals(readyAtBefore, readyAtAfter, "重启不得重新计时");
        assertEquals(MessageStatus.RETRYING, after.store().read(state ->
                state.getMessages().get(0).getStatus()));
        assertNull(poll(after), "剩余退避未到期前不得提前投递");

        Thread.sleep(820L);
        DeliveredMessage second = poll(after);
        assertNotNull(second, "到期后应恢复投递");
        assertEquals(2, second.delivered(), "重试次数连续累计，不得清零");
    }

    @Test
    void inFlightMessageRecoveredAfterRestartAndRetriesContinue() throws Exception {
        Path dir = Path.of("target/test-dlq-restart-lease");
        Harness before = harness(dir, 80L);
        before.queues().publish(QUEUE, "payload-lease",
                strategy(3, 1L, 5_000L, BackoffType.FIXED));
        DeliveredMessage first = poll(before);
        assertEquals(1, first.delivered());

        Harness after = restart(before, dir);
        Thread.sleep(120L);
        assertEquals(1, after.queues().recoverExpiredLeases());
        DeliveredMessage second = poll(after);
        assertNotNull(second);
        assertEquals(first.id(), second.id());
        assertEquals(2, second.delivered(), "租约恢复后投递次数连续累计");

        assertNull(after.queues().nack(second.id(), "fail-2"));
        Thread.sleep(10L);
        DeliveredMessage third = poll(after);
        assertNotNull(third);
        assertEquals(3, third.delivered());
        assertNull(after.queues().nack(third.id(), "fail-3"));
        Thread.sleep(10L);
        DeliveredMessage fourth = poll(after);
        assertNotNull(fourth);
        assertEquals(4, fourth.delivered());
        DeadLetterRecord deadLetter = after.queues().nack(fourth.id(), "final");
        assertNotNull(deadLetter);
        assertEquals(3, deadLetter.getRetryHistory().size());
        assertEquals(4, deadLetter.getRetryHistory().get(2).delivery());
    }

    @Test
    void duplicateNackRejectedAndOnlyAppliedOnce() {
        Harness h = harness(Path.of("target/test-dlq-dup-nack"), 60_000L);
        h.queues().publish(QUEUE, "payload-dup",
                strategy(3, 1_000L, 5_000L, BackoffType.FIXED));
        DeliveredMessage first = poll(h);
        assertNull(h.queues().nack(first.id(), "first"));

        assertThrows(IllegalStateException.class,
                () -> h.queues().nack(first.id(), "duplicate"),
                "重复失败回执必须被明确拒绝");
        int attempts = h.store().read(state -> state.getMessages().get(0).getAttempts().size());
        assertEquals(1, attempts, "不得重复累计重试次数");
        assertEquals(MessageStatus.RETRYING, h.store().read(state ->
                state.getMessages().get(0).getStatus()));
    }

    @Test
    void duplicateNackAfterExhaustedCreatesOnlyOneDeadRecord() {
        Harness h = harness(Path.of("target/test-dlq-dup-dead"), 60_000L);
        h.queues().publish(QUEUE, "payload-dup-dead",
                strategy(0, 1_000L, 5_000L, BackoffType.FIXED));
        DeliveredMessage first = poll(h);
        DeadLetterRecord deadLetter = h.queues().nack(first.id(), "dead");
        assertNotNull(deadLetter);

        assertThrows(IllegalStateException.class,
                () -> h.queues().nack(first.id(), "again"));
        long deadCount = h.store().read(state -> (long) state.getDeadLetters().size());
        assertEquals(1, deadCount, "不得产生重复死信记录");
    }

    @Test
    void nackAndLeaseRecoveryAtMostOneAdvances() throws Exception {
        Harness nackWins = harness(Path.of("target/test-dlq-race-nack"), 60_000L);
        nackWins.queues().publish(QUEUE, "payload-a",
                strategy(3, 10_000L, 60_000L, BackoffType.FIXED));
        DeliveredMessage a = poll(nackWins);
        assertNull(nackWins.queues().nack(a.id(), "fail"));
        assertEquals(0, nackWins.queues().recoverExpiredLeases(),
                "失败回执已推进时，租约恢复不得再推进");

        Harness recoveryWins = harness(Path.of("target/test-dlq-race-recover"), 80L);
        recoveryWins.queues().publish(QUEUE, "payload-b",
                strategy(3, 10_000L, 60_000L, BackoffType.FIXED));
        DeliveredMessage b = poll(recoveryWins);
        Thread.sleep(120L);
        assertEquals(1, recoveryWins.queues().recoverExpiredLeases());
        assertThrows(IllegalStateException.class,
                () -> recoveryWins.queues().nack(b.id(), "stale"),
                "租约恢复已推进时，失败回执必须被拒绝");
    }

    @Test
    void longBackoffDoesNotBlockOtherMessagesInSameQueue() {
        Harness h = harness(Path.of("target/test-dlq-long-backoff"), 60_000L);
        h.queues().publish(QUEUE, "slow", strategy(3, 60_000L, 60_000L, BackoffType.FIXED));
        h.queues().publish(QUEUE, "fast", strategy(3, 1_000L, 5_000L, BackoffType.FIXED));

        DeliveredMessage slow = poll(h);
        assertNotNull(slow);
        assertEquals("slow", slow.payload());
        assertNull(h.queues().nack(slow.id(), "slow-fail"));

        DeliveredMessage fast = poll(h);
        assertNotNull(fast, "长退避消息不得阻塞同队列其他消息");
        assertEquals("fast", fast.payload());
        h.queues().ack(fast.id());
        assertNull(poll(h));
        assertEquals(1, h.stats().snapshot().pendingRetry());
    }

    @Test
    void batchMessagesRedeliveredInReadyOrderWithoutLossOrDuplication() throws Exception {
        Harness h = harness(Path.of("target/test-dlq-batch"), 60_000L);
        int total = 200;
        for (int i = 0; i < total; i++) {
            long backoff = 400L + i * 5L;
            h.queues().publish(QUEUE, "batch-" + i,
                    strategy(1, backoff, 100_000L, BackoffType.FIXED));
        }

        for (int i = 0; i < total; i++) {
            DeliveredMessage message = poll(h);
            assertNotNull(message);
            assertNull(h.queues().nack(message.id(), "first-fail"));
        }
        assertEquals(total, h.stats().snapshot().pendingRetry());
        assertEquals(0, h.stats().snapshot().dead());
        assertNull(poll(h), "全部退避等待中，不得提前投递");

        Set<String> redelivered = new HashSet<>();
        long lastReadyAt = Long.MIN_VALUE;
        for (int i = 0; i < total; i++) {
            DeliveredMessage message = null;
            long deadline = System.currentTimeMillis() + 5_000L;
            while (message == null && System.currentTimeMillis() < deadline) {
                message = poll(h);
                if (message == null) {
                    Thread.sleep(5L);
                }
            }
            assertNotNull(message, "退避到期后应依次重新投递");
            assertTrue(redelivered.add(message.id()), "不得重复投递");
            assertEquals(2, message.delivered());
            long readyAt = message.leaseExpiresAt();
            assertTrue(readyAt >= lastReadyAt, "按到期时间依次投递");
            lastReadyAt = readyAt;
            h.queues().ack(message.id());
        }
        assertEquals(total, redelivered.size(), "消息不得丢失");
        assertEquals(0, h.stats().snapshot().pendingRetry());
        assertEquals(0, h.stats().snapshot().dead());
    }

    @Test
    void replayFollowsRetryStrategyAndKeepsCountersConsistent() {
        Harness h = harness(Path.of("target/test-dlq-replay"), 60_000L);
        h.queues().publish(QUEUE, "payload-replay",
                strategy(0, 1_000L, 5_000L, BackoffType.FIXED));
        DeadLetterRecord deadLetter = pollAndNack(h, "origin-fail");
        assertNotNull(deadLetter);

        assertEquals(1, h.deadLetters().replay(java.util.List.of(deadLetter.getId())));
        DeliveredMessage replayed = poll(h);
        assertNotNull(replayed);
        assertEquals(deadLetter.getId(), replayed.originDeadLetterId());
        assertNull(h.queues().nack(replayed.id(), "replay-fail-1"),
                "死信重放消息首次失败同样按策略退避");

        DeliveredMessage redelivered = awaitPoll(h);
        DeadLetterRecord again = h.queues().nack(redelivered.id(), "replay-fail-2");
        assertNotNull(again);
        assertEquals(deadLetter.getId(), again.getId());
        assertEquals(DeadLetterStatus.DEAD, again.getStatus());
        assertEquals(1, again.getReplayCount());
        assertEquals(1, again.getReplayHistory().size());
        assertFalse(again.getReplayHistory().get(0).success());
        assertEquals(1, h.stats().snapshot().dead());
        assertEquals(1, h.stats().snapshot().replayFailure());

        assertEquals(1, h.deadLetters().replay(java.util.List.of(deadLetter.getId())));
        DeliveredMessage success = awaitPoll(h);
        assertNotNull(success);
        h.queues().ack(success.id());
        assertEquals(DeadLetterStatus.RESOLVED,
                h.deadLetters().get(deadLetter.getId()).getStatus());
        assertEquals(0, h.stats().snapshot().dead(), "RESOLVED 不计入死信数量");
        assertEquals(1, h.stats().snapshot().replaySuccess());
    }

    @Test
    void statsStayConsistentAfterRestart() {
        Path dir = Path.of("target/test-dlq-restart-stats");
        Harness before = harness(dir, 60_000L);
        before.queues().publish(QUEUE, "waiting",
                strategy(3, 60_000L, 60_000L, BackoffType.FIXED));
        before.queues().publish(QUEUE, "dying",
                strategy(0, 1_000L, 5_000L, BackoffType.FIXED));
        assertNull(before.queues().nack(poll(before).id(), "wait"));
        assertNotNull(before.queues().nack(poll(before).id(), "dead"));

        Harness after = restart(before, dir);
        var stats = after.stats().snapshot();
        assertEquals(1, stats.pendingRetry());
        assertEquals(1, stats.dead());
        assertEquals(0, stats.inFlight());
    }
}
