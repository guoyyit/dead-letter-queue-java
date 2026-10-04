package com.github.guoyyit.deadletterqueuejava.service;

import com.github.guoyyit.deadletterqueuejava.config.DlqProperties;
import com.github.guoyyit.deadletterqueuejava.model.DeadLetterRecord;
import com.github.guoyyit.deadletterqueuejava.model.DeadLetterStatus;
import com.github.guoyyit.deadletterqueuejava.model.DeliveredMessage;
import com.github.guoyyit.deadletterqueuejava.model.MessageStatus;
import com.github.guoyyit.deadletterqueuejava.model.PersistedState;
import com.github.guoyyit.deadletterqueuejava.model.QueueRecord;
import com.github.guoyyit.deadletterqueuejava.model.ReplayEvent;
import com.github.guoyyit.deadletterqueuejava.model.RetryAttempt;
import com.github.guoyyit.deadletterqueuejava.model.RetryStrategy;
import com.github.guoyyit.deadletterqueuejava.store.StateStore;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * 队列服务实现：所有状态变更在存储锁内完成并立即落盘。
 */
public class QueueServiceImpl implements QueueService {

    private final StateStore store;
    private final RetryPlanner planner;
    private final DlqProperties properties;

    public QueueServiceImpl(StateStore store, RetryPlanner planner, DlqProperties properties) {
        this.store = store;
        this.planner = planner;
        this.properties = properties;
    }

    @Override
    public QueueRecord publish(String queue, String payload, RetryStrategy strategy) {
        if (queue == null || queue.isBlank()) {
            throw new IllegalArgumentException("queue 不能为空");
        }
        if (payload == null) {
            throw new IllegalArgumentException("payload 不能为空");
        }
        RetryStrategy resolved = planner.resolve(strategy);
        long now = System.currentTimeMillis();
        return store.mutate(state -> {
            QueueRecord record = new QueueRecord();
            record.setId(UUID.randomUUID().toString());
            record.setQueue(queue);
            record.setPayload(payload);
            record.setStatus(MessageStatus.PENDING);
            record.setDelivered(0);
            record.setNextReadyAt(now);
            record.setLeaseExpiresAt(0L);
            record.setCreatedAt(now);
            record.setUpdatedAt(now);
            record.setStrategy(resolved);
            record.setAttempts(new ArrayList<>());
            state.getMessages().add(record);
            return record;
        });
    }

    @Override
    public DeliveredMessage poll(String queue) {
        if (queue == null || queue.isBlank()) {
            throw new IllegalArgumentException("queue 不能为空");
        }
        long now = System.currentTimeMillis();
        return store.mutate(state -> {
            QueueRecord candidate = null;
            for (QueueRecord record : state.getMessages()) {
                if (!queue.equals(record.getQueue())
                        || record.getStatus() == MessageStatus.IN_FLIGHT
                        || record.getNextReadyAt() > now) {
                    continue;
                }
                if (candidate == null || record.getNextReadyAt() < candidate.getNextReadyAt()) {
                    candidate = record;
                }
            }
            if (candidate == null) {
                return null;
            }
            candidate.setStatus(MessageStatus.IN_FLIGHT);
            candidate.setDelivered(candidate.getDelivered() + 1);
            candidate.setLeaseExpiresAt(now + properties.getVisibilityTimeoutMillis());
            candidate.setUpdatedAt(now);
            return DeliveredMessage.of(candidate);
        });
    }

    @Override
    public void ack(String messageId) {
        requireMessageId(messageId);
        store.mutate(state -> {
            QueueRecord record = findMessage(state, messageId);
            if (record == null) {
                throw new NoSuchElementException("消息不存在或已结束: " + messageId);
            }
            if (record.getStatus() != MessageStatus.IN_FLIGHT) {
                throw new IllegalStateException("消息当前不在消费中，无法确认: " + messageId);
            }
            state.getMessages().remove(record);
            if (record.getOriginDeadLetterId() != null) {
                DeadLetterRecord deadLetter = findDeadLetter(state, record.getOriginDeadLetterId());
                if (deadLetter != null) {
                    deadLetter.setStatus(DeadLetterStatus.RESOLVED);
                    deadLetter.getReplayHistory().add(
                            new ReplayEvent(System.currentTimeMillis(), true, messageId, null));
                }
            }
            return null;
        });
    }

    @Override
    public DeadLetterRecord nack(String messageId, String reason) {
        requireMessageId(messageId);
        String failureReason = reason == null || reason.isBlank() ? "unknown" : reason;
        return store.mutate(state -> {
            long now = System.currentTimeMillis();
            QueueRecord record = findMessage(state, messageId);
            if (record == null) {
                throw new NoSuchElementException("消息不存在或已结束: " + messageId);
            }
            if (record.getStatus() != MessageStatus.IN_FLIGHT) {
                throw new IllegalStateException("消息当前不在消费中，无法回执失败: " + messageId);
            }
            DeadLetterRecord replayOrigin = record.getOriginDeadLetterId() == null
                    ? null
                    : findDeadLetter(state, record.getOriginDeadLetterId());
            int delivery = record.getDelivered();
            boolean retryExhausted = delivery - 1 >= record.getStrategy().maxRetries();
            boolean nonRetryable = planner.isNonRetryable(failureReason);
            if (!retryExhausted && !nonRetryable) {
                backoffRetry(record, failureReason, delivery, now);
                return null;
            }
            return moveToDeadLetter(state, record, replayOrigin, failureReason, delivery, now);
        });
    }

    @Override
    public int recoverExpiredLeases() {
        long now = System.currentTimeMillis();
        return store.mutate(state -> {
            int recovered = 0;
            for (QueueRecord record : state.getMessages()) {
                if (record.getStatus() == MessageStatus.IN_FLIGHT
                        && record.getLeaseExpiresAt() <= now) {
                    DeadLetterRecord replayOrigin = record.getOriginDeadLetterId() == null
                            ? null
                            : findDeadLetter(state, record.getOriginDeadLetterId());
                    record.setStatus(MessageStatus.RETRYING);
                    record.setNextReadyAt(now);
                    record.setLeaseExpiresAt(0L);
                    record.setUpdatedAt(now);
                    if (replayOrigin != null) {
                        replayOrigin.setStatus(DeadLetterStatus.REPLAYING);
                    }
                    recovered++;
                }
            }
            return recovered;
        });
    }

    @Override
    public int settleFinishedReplays() {
        long now = System.currentTimeMillis();
        return store.mutate(state -> {
            int settled = 0;
            for (QueueRecord record : new ArrayList<>(state.getMessages())) {
                String originId = record.getOriginDeadLetterId();
                if (originId == null
                        || record.getStatus() != MessageStatus.IN_FLIGHT
                        || record.getLeaseExpiresAt() > now) {
                    continue;
                }
                DeadLetterRecord origin = findDeadLetter(state, originId);
                if (origin == null || origin.getStatus() != DeadLetterStatus.REPLAYING) {
                    continue;
                }
                record.setStatus(MessageStatus.RETRYING);
                record.setNextReadyAt(now);
                record.setLeaseExpiresAt(0L);
                record.setUpdatedAt(now);
                settled++;
            }
            return settled;
        });
    }

    private DeadLetterRecord moveToDeadLetter(PersistedState state, QueueRecord record,
                                              DeadLetterRecord replayOrigin, String failureReason,
                                              int delivery, long now) {
        record.getAttempts().add(new RetryAttempt(delivery, now, failureReason, -1L));
        state.getMessages().remove(record);
        if (replayOrigin == null) {
            DeadLetterRecord deadLetter = new DeadLetterRecord();
            deadLetter.setId(UUID.randomUUID().toString());
            deadLetter.setOriginMessageId(record.getId());
            deadLetter.setOriginQueue(record.getQueue());
            deadLetter.setOriginalPayload(record.getPayload());
            deadLetter.setRetryHistory(new ArrayList<>(record.getAttempts()));
            deadLetter.setFirstDeadAt(now);
            deadLetter.setLastDeadAt(now);
            deadLetter.setStatus(DeadLetterStatus.DEAD);
            deadLetter.setLastFailureReason(failureReason);
            state.getDeadLetters().add(deadLetter);
            return deadLetter;
        }
        replayOrigin.getRetryHistory().addAll(record.getAttempts());
        replayOrigin.setLastFailureReason(failureReason);
        replayOrigin.setLastDeadAt(now);
        replayOrigin.setStatus(DeadLetterStatus.DEAD);
        replayOrigin.getReplayHistory().add(
                new ReplayEvent(now, false, record.getId(), failureReason));
        return replayOrigin;
    }

    private void backoffRetry(QueueRecord record, String failureReason, int delivery, long now) {
        long backoff = planner.backoffMillis(record.getStrategy(), delivery);
        record.getAttempts().add(new RetryAttempt(delivery, now, failureReason, backoff));
        record.setStatus(MessageStatus.RETRYING);
        record.setNextReadyAt(now + backoff);
        record.setLeaseExpiresAt(0L);
        record.setUpdatedAt(now);
    }

    private QueueRecord findMessage(PersistedState state, String id) {
        for (QueueRecord record : state.getMessages()) {
            if (record.getId().equals(id)) {
                return record;
            }
        }
        return null;
    }

    private DeadLetterRecord findDeadLetter(PersistedState state, String id) {
        for (DeadLetterRecord record : state.getDeadLetters()) {
            if (record.getId().equals(id)) {
                return record;
            }
        }
        return null;
    }

    private void requireMessageId(String messageId) {
        if (messageId == null || messageId.isBlank()) {
            throw new IllegalArgumentException("messageId 不能为空");
        }
    }
}
