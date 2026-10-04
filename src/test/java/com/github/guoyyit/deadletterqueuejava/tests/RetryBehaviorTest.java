package com.github.guoyyit.deadletterqueuejava.tests;

import com.github.guoyyit.deadletterqueuejava.config.DlqProperties;
import com.github.guoyyit.deadletterqueuejava.model.BackoffType;
import com.github.guoyyit.deadletterqueuejava.model.DeadLetterRecord;
import com.github.guoyyit.deadletterqueuejava.model.DeliveredMessage;
import com.github.guoyyit.deadletterqueuejava.model.MessageStatus;
import com.github.guoyyit.deadletterqueuejava.model.QueueRecord;
import com.github.guoyyit.deadletterqueuejava.model.RetryAttempt;
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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 普通消息首次失败后的重试行为：退避等待、耗尽转区、边界语义与退避计算。
 */
class RetryBehaviorTest {

    @TempDir
    Path storeDir;

    private DlqProperties properties;
    private StateStore store;
    private RetryPlanner planner;
    private QueueService queueService;
    private StatsServiceImpl statsService;

    @BeforeEach
    void setUp() {
        properties = new DlqProperties();
        properties.setStoreDir(storeDir.toString());
        properties.setVisibilityTimeoutMillis(30_000L);
        store = new FileStateStore(properties);
        store.start();
        planner = new DefaultRetryPlanner(properties);
        queueService = new QueueServiceImpl(store, planner, properties);
        statsService = new StatsServiceImpl(store);
    }

    @Test
    void firstFailureSchedulesRetryWithBackoffForNewMessage() throws Exception {
        RetryStrategy strategy = new RetryStrategy(3, 200L, 5_000L, BackoffType.FIXED);
        QueueRecord record = queueService.publish("orders", "payload-1", strategy);
        DeliveredMessage delivered = queueService.poll("orders");
        assertNotNull(delivered);
        assertEquals(1, delivered.delivered());

        long before = System.currentTimeMillis();
        DeadLetterRecord deadLetter = queueService.nack(delivered.id(), "boom");
        long after = System.currentTimeMillis();

        assertNull(deadLetter, "首次失败应安排重试而不是直接进死信区");
        QueueRecord state = onlyMessage();
        assertEquals(MessageStatus.RETRYING, state.getStatus());
        assertEquals(1, state.getAttempts().size());
        RetryAttempt attempt = state.getAttempts().get(0);
        assertEquals(1, attempt.delivery());
        assertEquals("boom", attempt.reason());
        assertEquals(200L, attempt.backoffMillis());
        assertTrue(state.getNextReadyAt() >= before + 200L
                        && state.getNextReadyAt() <= after + 200L,
                "退避到期时间应为失败时刻加退避时长");

        assertNull(queueService.poll("orders"), "退避等待期间不得提前投递");
        assertEquals(1, statsService.snapshot().pendingRetry());
        assertEquals(0, statsService.snapshot().dead());

        Thread.sleep(260L);
        DeliveredMessage redelivered = queueService.poll("orders");
        assertNotNull(redelivered, "退避到期后应重新投递");
        assertEquals(record.getId(), redelivered.id());
        assertEquals(2, redelivered.delivered());
    }

    @Test
    void retriesExhaustedMovesToDeadLetterWithFullHistory() throws Exception {
        RetryStrategy strategy = new RetryStrategy(3, 1L, 5_000L, BackoffType.FIXED);
        queueService.publish("orders", "keep-me", strategy);

        DeadLetterRecord deadLetter = null;
        for (int i = 1; i <= 4; i++) {
            Thread.sleep(5L);
            DeliveredMessage delivered = queueService.poll("orders");
            assertNotNull(delivered, "第 " + i + " 次投递应可用");
            assertEquals(i, delivered.delivered());
            deadLetter = queueService.nack(delivered.id(), "fail-" + i);
            if (i <= 3) {
                assertNull(deadLetter, "第 " + i + " 次失败仍应重试");
            }
        }

        assertNotNull(deadLetter, "重试耗尽后应转入死信区");
        assertEquals("keep-me", deadLetter.getOriginalPayload());
        assertEquals("orders", deadLetter.getOriginQueue());
        assertEquals("fail-4", deadLetter.getLastFailureReason());
        List<RetryAttempt> history = deadLetter.getRetryHistory();
        assertEquals(4, history.size(), "死信记录应保留完整重试历史");
        for (int i = 0; i < 4; i++) {
            assertEquals(i + 1, history.get(i).delivery());
            assertEquals("fail-" + (i + 1), history.get(i).reason());
        }
        assertEquals(-1L, history.get(3).backoffMillis(), "最后一次失败不再有退避");
        assertEquals(0, statsService.snapshot().pendingRetry());
        assertEquals(1, statsService.snapshot().dead());
    }

    @Test
    void zeroMaxRetriesGoesStraightToDeadLetter() {
        RetryStrategy strategy = new RetryStrategy(0, 100L, 1_000L, BackoffType.FIXED);
        queueService.publish("orders", "no-retry", strategy);
        DeliveredMessage delivered = queueService.poll("orders");
        assertNotNull(delivered);

        DeadLetterRecord deadLetter = queueService.nack(delivered.id(), "boom");
        assertNotNull(deadLetter, "最大重试次数为零时首次失败应直接转入死信区");
        assertEquals("no-retry", deadLetter.getOriginalPayload());
        assertEquals(1, deadLetter.getRetryHistory().size());
        assertEquals(1, statsService.snapshot().dead());
    }

    @Test
    void nonRetryableReasonSkipsRetry() {
        RetryStrategy strategy = new RetryStrategy(5, 100L, 1_000L, BackoffType.FIXED);
        queueService.publish("orders", "bad-request", strategy);
        DeliveredMessage delivered = queueService.poll("orders");
        assertNotNull(delivered);

        DeadLetterRecord deadLetter = queueService.nack(delivered.id(), "NON_RETRYABLE:invalid");
        assertNotNull(deadLetter, "不可重试原因应跳过重试直接转入死信区");
        assertEquals("NON_RETRYABLE:invalid", deadLetter.getLastFailureReason());
        assertEquals(1, deadLetter.getRetryHistory().size());
        assertEquals(1, statsService.snapshot().dead());
    }

    @Test
    void backoffComputationHonorsTypeAndCap() {
        RetryStrategy fixed = new RetryStrategy(5, 100L, 10_000L, BackoffType.FIXED);
        assertEquals(100L, planner.backoffMillis(fixed, 1));
        assertEquals(100L, planner.backoffMillis(fixed, 2));
        assertEquals(100L, planner.backoffMillis(fixed, 3));

        RetryStrategy linear = new RetryStrategy(5, 100L, 10_000L, BackoffType.LINEAR);
        assertEquals(100L, planner.backoffMillis(linear, 1));
        assertEquals(200L, planner.backoffMillis(linear, 2));
        assertEquals(300L, planner.backoffMillis(linear, 3));

        RetryStrategy exponential = new RetryStrategy(5, 100L, 10_000L, BackoffType.EXPONENTIAL);
        assertEquals(100L, planner.backoffMillis(exponential, 1));
        assertEquals(200L, planner.backoffMillis(exponential, 2));
        assertEquals(400L, planner.backoffMillis(exponential, 3));
        assertEquals(800L, planner.backoffMillis(exponential, 4));

        RetryStrategy capped = new RetryStrategy(5, 100L, 250L, BackoffType.EXPONENTIAL);
        assertEquals(250L, planner.backoffMillis(capped, 3), "退避时长应受间隔上限约束");
        RetryStrategy cappedLinear = new RetryStrategy(5, 100L, 250L, BackoffType.LINEAR);
        assertEquals(250L, planner.backoffMillis(cappedLinear, 4));
    }

    @Test
    void longBackoffDoesNotBlockOtherMessagesInSameQueue() {
        RetryStrategy longBackoff = new RetryStrategy(3, 60_000L, 60_000L, BackoffType.FIXED);
        queueService.publish("orders", "stuck", longBackoff);
        queueService.publish("orders", "normal", longBackoff);

        DeliveredMessage first = queueService.poll("orders");
        assertNotNull(first);
        queueService.nack(first.id(), "boom");

        DeliveredMessage second = queueService.poll("orders");
        assertNotNull(second, "长退避消息不得阻塞同队列其他消息");
        assertEquals("normal", second.payload());
        queueService.ack(second.id());

        assertNull(queueService.poll("orders"), "退避中的消息不应被提前投递");
        assertEquals(1, statsService.snapshot().pendingRetry());
    }

    private QueueRecord onlyMessage() {
        return store.read(state -> {
            assertEquals(1, state.getMessages().size());
            return state.getMessages().get(0);
        });
    }
}
