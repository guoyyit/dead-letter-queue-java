package com.github.guoyyit.deadletterqueuejava.tests;

import com.github.guoyyit.deadletterqueuejava.config.DlqProperties;
import com.github.guoyyit.deadletterqueuejava.model.BackoffType;
import com.github.guoyyit.deadletterqueuejava.model.DeliveredMessage;
import com.github.guoyyit.deadletterqueuejava.model.MessageStatus;
import com.github.guoyyit.deadletterqueuejava.model.QueueRecord;
import com.github.guoyyit.deadletterqueuejava.model.RetryStrategy;
import com.github.guoyyit.deadletterqueuejava.service.DefaultRetryPlanner;
import com.github.guoyyit.deadletterqueuejava.service.QueueService;
import com.github.guoyyit.deadletterqueuejava.service.QueueServiceImpl;
import com.github.guoyyit.deadletterqueuejava.service.RetryPlanner;
import com.github.guoyyit.deadletterqueuejava.store.FileStateStore;
import com.github.guoyyit.deadletterqueuejava.store.StateStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 重复失败回执的幂等拒绝，以及失败回执与租约恢复的竞争语义。
 */
class DuplicateNackAndRaceTest {

    @TempDir
    Path storeDir;

    private DlqProperties properties;
    private StateStore store;
    private QueueService queueService;

    @BeforeEach
    void setUp() {
        properties = new DlqProperties();
        properties.setStoreDir(storeDir.toString());
        properties.setVisibilityTimeoutMillis(30_000L);
        store = new FileStateStore(properties);
        store.start();
        RetryPlanner planner = new DefaultRetryPlanner(properties);
        queueService = new QueueServiceImpl(store, planner, properties);
    }

    @Test
    void duplicateNackIsRejectedAndDoesNotChangeState() {
        queueService.publish("orders", "payload",
                new RetryStrategy(3, 60_000L, 60_000L, BackoffType.FIXED));
        DeliveredMessage delivered = queueService.poll("orders");
        assertNotNull(delivered);

        assertNull(queueService.nack(delivered.id(), "boom"), "第一次失败回执应安排重试");

        assertThrows(IllegalStateException.class,
                () -> queueService.nack(delivered.id(), "boom"),
                "重复失败回执必须被明确拒绝");

        QueueRecord record = onlyMessage();
        assertEquals(MessageStatus.RETRYING, record.getStatus(), "状态不得被重复回执改变");
        assertEquals(1, record.getDelivered(), "不得重复累计投递计数");
        assertEquals(1, record.getAttempts().size(), "不得重复累计重试历史");
        int deadCount = store.read(state -> state.getDeadLetters().size());
        assertEquals(0, deadCount, "不得产生死信记录");
    }

    @Test
    void duplicateNackAfterDeadLetterIsRejectedWithoutDuplicateRecord() {
        queueService.publish("orders", "payload",
                new RetryStrategy(0, 1L, 1L, BackoffType.FIXED));
        DeliveredMessage delivered = queueService.poll("orders");
        assertNotNull(delivered);

        assertNotNull(queueService.nack(delivered.id(), "boom"), "零次重试应直接转入死信区");

        assertThrows(NoSuchElementException.class,
                () -> queueService.nack(delivered.id(), "boom"),
                "消息已结束后再次回执必须被明确拒绝");
        int deadCount = store.read(state -> state.getDeadLetters().size());
        assertEquals(1, deadCount, "不得产生重复的死信记录");
    }

    @Test
    void leaseRecoveryWinningMakesNackRejected() throws Exception {
        properties.setVisibilityTimeoutMillis(50L);
        queueService.publish("orders", "payload",
                new RetryStrategy(3, 60_000L, 60_000L, BackoffType.FIXED));
        DeliveredMessage delivered = queueService.poll("orders");
        assertNotNull(delivered);

        Thread.sleep(80L);
        assertEquals(1, queueService.recoverExpiredLeases(), "租约恢复应先推进消息");

        assertThrows(IllegalStateException.class,
                () -> queueService.nack(delivered.id(), "boom"),
                "租约恢复已推进后，失败回执必须被明确拒绝");
        QueueRecord record = onlyMessage();
        assertEquals(MessageStatus.RETRYING, record.getStatus());
        assertEquals(1, record.getDelivered());
        assertEquals(0, record.getAttempts().size(), "租约恢复不计为一次失败");
    }

    @Test
    void nackWinningMakesLeaseRecoverySkipMessage() {
        queueService.publish("orders", "payload",
                new RetryStrategy(3, 60_000L, 60_000L, BackoffType.FIXED));
        DeliveredMessage delivered = queueService.poll("orders");
        assertNotNull(delivered);

        assertNull(queueService.nack(delivered.id(), "boom"), "失败回执应先推进消息");

        assertEquals(0, queueService.recoverExpiredLeases(),
                "已回执的消息不得再被租约恢复处理");
        QueueRecord record = onlyMessage();
        assertEquals(MessageStatus.RETRYING, record.getStatus());
        assertEquals(1, record.getAttempts().size());
    }

    @Test
    void concurrentNackAndLeaseRecoveryAdvanceMessageAtMostOnce() throws Exception {
        properties.setVisibilityTimeoutMillis(50L);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            for (int round = 0; round < 30; round++) {
                String queue = "race-" + round;
                queueService.publish(queue, "payload",
                        new RetryStrategy(3, 60_000L, 60_000L, BackoffType.FIXED));
                DeliveredMessage delivered = queueService.poll(queue);
                assertNotNull(delivered);
                Thread.sleep(80L);

                CountDownLatch ready = new CountDownLatch(2);
                CountDownLatch start = new CountDownLatch(1);
                Future<Boolean> nackSucceeded = executor.submit(() -> {
                    ready.countDown();
                    await(start);
                    try {
                        queueService.nack(delivered.id(), "boom");
                        return true;
                    } catch (IllegalStateException rejected) {
                        return false;
                    }
                });
                Future<Integer> recovered = executor.submit(() -> {
                    ready.countDown();
                    await(start);
                    return queueService.recoverExpiredLeases();
                });
                assertTrue(ready.await(5, TimeUnit.SECONDS));
                start.countDown();

                boolean nackWon = nackSucceeded.get(5, TimeUnit.SECONDS);
                int recoveredCount = recovered.get(5, TimeUnit.SECONDS);
                assertTrue(nackWon != (recoveredCount == 1),
                        "同一消息至多被一方推进，另一方必须被明确拒绝");

                QueueRecord record = store.read(state -> state.getMessages().stream()
                        .filter(r -> queue.equals(r.getQueue()))
                        .findFirst()
                        .orElseThrow());
                assertEquals(MessageStatus.RETRYING, record.getStatus());
                assertEquals(1, record.getDelivered(), "投递计数不得跳跃");
                assertEquals(nackWon ? 1 : 0, record.getAttempts().size(),
                        "只有失败回执生效时才记录失败历史");
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private QueueRecord onlyMessage() {
        return store.read(state -> {
            assertEquals(1, state.getMessages().size());
            return state.getMessages().get(0);
        });
    }
}
