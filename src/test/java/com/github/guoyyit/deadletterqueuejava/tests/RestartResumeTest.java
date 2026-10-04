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
import com.github.guoyyit.deadletterqueuejava.service.StatsServiceImpl;
import com.github.guoyyit.deadletterqueuejava.store.FileStateStore;
import com.github.guoyyit.deadletterqueuejava.store.StateStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 服务重启后的续跑语义：退避剩余时长保留、租约到期恢复、重试计数连续。
 */
class RestartResumeTest {

    @TempDir
    Path storeDir;

    private DlqProperties properties;
    private StateStore store;
    private QueueService queueService;
    private StatsServiceImpl statsService;

    @BeforeEach
    void setUp() {
        properties = new DlqProperties();
        properties.setStoreDir(storeDir.toString());
        properties.setVisibilityTimeoutMillis(150L);
        restart();
    }

    private void restart() {
        store = new FileStateStore(properties);
        store.start();
        RetryPlanner planner = new DefaultRetryPlanner(properties);
        queueService = new QueueServiceImpl(store, planner, properties);
        statsService = new StatsServiceImpl(store);
    }

    @Test
    void backoffWaitingMessageKeepsRemainingWaitAfterRestart() throws Exception {
        RetryStrategy strategy = new RetryStrategy(3, 400L, 5_000L, BackoffType.FIXED);
        queueService.publish("orders", "waiting", strategy);
        DeliveredMessage delivered = queueService.poll("orders");
        assertNotNull(delivered);
        queueService.nack(delivered.id(), "boom");
        long nextReadyAt = store.read(state -> state.getMessages().get(0).getNextReadyAt());

        restart();

        QueueRecord restored = store.read(state -> state.getMessages().get(0));
        assertEquals(MessageStatus.RETRYING, restored.getStatus());
        assertEquals(nextReadyAt, restored.getNextReadyAt(), "重启后不得重新计时");
        assertEquals(1, restored.getDelivered(), "重试计数不得清零或跳跃");
        assertEquals(1, restored.getAttempts().size());

        assertNull(queueService.poll("orders"), "重启后不得提前投递");
        assertEquals(1, statsService.snapshot().pendingRetry());
        assertEquals(0, statsService.snapshot().dead());

        long remaining = nextReadyAt - System.currentTimeMillis();
        if (remaining > 0) {
            Thread.sleep(remaining + 50L);
        }
        DeliveredMessage redelivered = queueService.poll("orders");
        assertNotNull(redelivered, "剩余等待结束后应正常投递");
        assertEquals(2, redelivered.delivered());
    }

    @Test
    void inFlightMessageRecoveredAfterLeaseExpiryAcrossRestart() throws Exception {
        RetryStrategy strategy = new RetryStrategy(3, 100L, 5_000L, BackoffType.FIXED);
        queueService.publish("orders", "in-flight", strategy);
        DeliveredMessage delivered = queueService.poll("orders");
        assertNotNull(delivered);
        assertEquals(1, delivered.delivered());

        restart();

        Thread.sleep(200L);
        assertEquals(1, queueService.recoverExpiredLeases(), "租约到期后应恢复投递");

        DeliveredMessage redelivered = queueService.poll("orders");
        assertNotNull(redelivered);
        assertEquals(delivered.id(), redelivered.id());
        assertEquals(2, redelivered.delivered(), "重试次数应连续累计");

        queueService.nack(redelivered.id(), "boom");
        QueueRecord record = store.read(state -> state.getMessages().get(0));
        assertEquals(MessageStatus.RETRYING, record.getStatus());
        assertEquals(2, record.getDelivered(), "失败回执后计数继续累计，不得清零");
        assertEquals(1, record.getAttempts().size());
    }

    @Test
    void statsConsistentAfterRestart() {
        RetryStrategy retrying = new RetryStrategy(3, 60_000L, 60_000L, BackoffType.FIXED);
        queueService.publish("orders", "backing-off", retrying);
        queueService.publish("orders", "dying", new RetryStrategy(0, 1L, 1L, BackoffType.FIXED));

        DeliveredMessage first = queueService.poll("orders");
        assertNotNull(first);
        queueService.nack(first.id(), "boom");
        DeliveredMessage second = queueService.poll("orders");
        assertNotNull(second);
        queueService.nack(second.id(), "boom");

        restart();

        var snapshot = statsService.snapshot();
        assertEquals(1, snapshot.pendingRetry(), "待重试统计应包含退避等待中的消息");
        assertEquals(1, snapshot.dead(), "死信统计只包含已转入死信区的消息");
        int deadRecords = store.read(state -> state.getDeadLetters().size());
        assertEquals(1, deadRecords);
    }
}
