package com.github.guoyyit.deadletterqueuejava.tests;

import com.github.guoyyit.deadletterqueuejava.config.DlqProperties;
import com.github.guoyyit.deadletterqueuejava.model.BackoffType;
import com.github.guoyyit.deadletterqueuejava.model.DeadLetterRecord;
import com.github.guoyyit.deadletterqueuejava.model.DeadLetterStatus;
import com.github.guoyyit.deadletterqueuejava.model.DeliveredMessage;
import com.github.guoyyit.deadletterqueuejava.model.QueueRecord;
import com.github.guoyyit.deadletterqueuejava.model.RetryStrategy;
import com.github.guoyyit.deadletterqueuejava.model.StatsSnapshot;
import com.github.guoyyit.deadletterqueuejava.service.DeadLetterService;
import com.github.guoyyit.deadletterqueuejava.service.DeadLetterServiceImpl;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 批量消息的首败退避与按到期重新投递，以及统计与记录的一致性。
 */
class BatchRetryAndStatsTest {

    private static final int BATCH_SIZE = 300;

    @TempDir
    Path storeDir;

    private StateStore store;
    private QueueService queueService;
    private DeadLetterService deadLetterService;
    private StatsServiceImpl statsService;

    @BeforeEach
    void setUp() {
        DlqProperties properties = new DlqProperties();
        properties.setStoreDir(storeDir.toString());
        properties.setVisibilityTimeoutMillis(30_000L);
        store = new FileStateStore(properties);
        store.start();
        RetryPlanner planner = new DefaultRetryPlanner(properties);
        queueService = new QueueServiceImpl(store, planner, properties);
        deadLetterService = new DeadLetterServiceImpl(store, properties, planner.resolve(null));
        statsService = new StatsServiceImpl(store);
    }

    @Test
    void batchMessagesBackoffAndRedeliverInDueOrderWithoutLoss() throws Exception {
        RetryStrategy strategy = new RetryStrategy(2, 2_000L, 5_000L, BackoffType.FIXED);
        Map<String, String> published = new HashMap<>();
        for (int i = 0; i < BATCH_SIZE; i++) {
            QueueRecord record = queueService.publish("bulk", "payload-" + i, strategy);
            published.put(record.getId(), record.getPayload());
        }

        Set<String> firstDelivery = new HashSet<>();
        for (int i = 0; i < BATCH_SIZE; i++) {
            DeliveredMessage delivered = queueService.poll("bulk");
            assertNotNull(delivered, "首次投递应覆盖全部消息");
            assertEquals(1, delivered.delivered());
            assertNull(queueService.nack(delivered.id(), "boom"), "首次失败应安排重试");
            firstDelivery.add(delivered.id());
        }
        assertEquals(published.keySet(), firstDelivery, "首次投递不得丢失或重复");

        StatsSnapshot waiting = statsService.snapshot();
        assertEquals(BATCH_SIZE, waiting.pendingRetry(), "待重试统计应包含全部退避中的消息");
        assertEquals(0, waiting.dead());
        assertEquals(0, waiting.inFlight());
        assertNull(queueService.poll("bulk"), "退避期间不得提前投递");

        Thread.sleep(2_100L);

        Set<String> secondDelivery = new HashSet<>();
        for (int i = 0; i < BATCH_SIZE; i++) {
            DeliveredMessage delivered = queueService.poll("bulk");
            assertNotNull(delivered, "退避到期后应依次重新投递");
            assertEquals(2, delivered.delivered(), "重试计数应连续累计");
            secondDelivery.add(delivered.id());
            queueService.ack(delivered.id());
        }
        assertEquals(published.keySet(), secondDelivery, "重新投递不得丢失或重复");
        assertNull(queueService.poll("bulk"));

        StatsSnapshot drained = statsService.snapshot();
        assertEquals(0, drained.pendingRetry());
        assertEquals(0, drained.dead());
        assertEquals(0, drained.inFlight());
    }

    @Test
    void replaySuccessAndFailureCountsMatchRecords() {
        RetryStrategy noRetry = new RetryStrategy(0, 1L, 1L, BackoffType.FIXED);
        queueService.publish("orders", "will-succeed", noRetry);
        queueService.publish("orders", "will-fail-again", noRetry);

        DeliveredMessage first = queueService.poll("orders");
        assertNotNull(first);
        DeadLetterRecord firstDead = queueService.nack(first.id(), "boom");
        DeliveredMessage second = queueService.poll("orders");
        assertNotNull(second);
        DeadLetterRecord secondDead = queueService.nack(second.id(), "boom");
        assertNotNull(firstDead);
        assertNotNull(secondDead);
        assertEquals(2, statsService.snapshot().dead());

        assertEquals(1, deadLetterService.replay(List.of(firstDead.getId())));
        DeliveredMessage replayedOk = queueService.poll("orders");
        assertNotNull(replayedOk);
        queueService.ack(replayedOk.id());

        StatsSnapshot afterSuccess = statsService.snapshot();
        assertEquals(1, afterSuccess.replaySuccess(), "重放成功计数应与记录一致");
        assertEquals(0, afterSuccess.replayFailure());
        assertEquals(1, afterSuccess.dead());
        assertEquals(DeadLetterStatus.RESOLVED,
                deadLetterService.get(firstDead.getId()).getStatus());

        assertEquals(1, deadLetterService.replay(List.of(secondDead.getId())));
        DeliveredMessage replayedFail = queueService.poll("orders");
        assertNotNull(replayedFail);
        DeadLetterRecord deadAgain = queueService.nack(replayedFail.id(), "NON_RETRYABLE:still-bad");
        assertNotNull(deadAgain, "重放再次失败应回到死信区");

        StatsSnapshot afterFailure = statsService.snapshot();
        assertEquals(1, afterFailure.replaySuccess());
        assertEquals(1, afterFailure.replayFailure(), "重放再次失败计数应与记录一致");
        assertEquals(1, afterFailure.dead());
        assertEquals(DeadLetterStatus.DEAD, deadAgain.getStatus());
        assertEquals("NON_RETRYABLE:still-bad", deadAgain.getLastFailureReason());
        assertEquals(2, deadAgain.getRetryHistory().size(),
                "死信记录应累计首次与重放后的完整失败历史");
    }
}
