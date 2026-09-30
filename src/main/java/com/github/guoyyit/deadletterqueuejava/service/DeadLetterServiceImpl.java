package com.github.guoyyit.deadletterqueuejava.service;

import com.github.guoyyit.deadletterqueuejava.config.DlqProperties;
import com.github.guoyyit.deadletterqueuejava.model.DeadLetterRecord;
import com.github.guoyyit.deadletterqueuejava.model.DeadLetterStatus;
import com.github.guoyyit.deadletterqueuejava.model.MessageStatus;
import com.github.guoyyit.deadletterqueuejava.model.PersistedState;
import com.github.guoyyit.deadletterqueuejava.model.QueueRecord;
import com.github.guoyyit.deadletterqueuejava.model.RetryStrategy;
import com.github.guoyyit.deadletterqueuejava.store.StateStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

/**
 * 死信区服务实现：重放标记与入队在同一次原子变更内完成。
 */
public class DeadLetterServiceImpl implements DeadLetterService {

    private final StateStore store;
    private final DlqProperties properties;
    private final RetryStrategy defaultStrategy;

    public DeadLetterServiceImpl(StateStore store, DlqProperties properties,
                                 RetryStrategy defaultStrategy) {
        this.store = store;
        this.properties = properties;
        this.defaultStrategy = defaultStrategy;
    }

    @Override
    public List<DeadLetterRecord> list(String queue, DeadLetterStatus status, int limit) {
        int boundedLimit = limit <= 0 ? 100 : Math.min(limit, 1000);
        return store.read(state -> {
            List<DeadLetterRecord> result = new ArrayList<>();
            for (DeadLetterRecord record : state.getDeadLetters()) {
                if (queue != null && !queue.isBlank() && !queue.equals(record.getOriginQueue())) {
                    continue;
                }
                if (status != null && record.getStatus() != status) {
                    continue;
                }
                result.add(record);
                if (result.size() >= boundedLimit) {
                    break;
                }
            }
            result.sort(Comparator.comparingLong(DeadLetterRecord::getLastDeadAt).reversed());
            return result;
        });
    }

    @Override
    public DeadLetterRecord get(String id) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id 不能为空");
        }
        return store.read(state -> {
            for (DeadLetterRecord record : state.getDeadLetters()) {
                if (record.getId().equals(id)) {
                    return record;
                }
            }
            return null;
        });
    }

    @Override
    public int replay(Collection<String> ids) {
        if (ids == null || ids.isEmpty()) {
            throw new IllegalArgumentException("ids 不能为空");
        }
        long now = System.currentTimeMillis();
        return store.mutate(state -> {
            int affected = 0;
            for (String id : ids) {
                DeadLetterRecord deadLetter = find(state, id);
                if (deadLetter == null || deadLetter.getStatus() != DeadLetterStatus.DEAD) {
                    continue;
                }
                QueueRecord record = new QueueRecord();
                record.setId(UUID.randomUUID().toString());
                record.setQueue(deadLetter.getOriginQueue());
                record.setPayload(deadLetter.getOriginalPayload());
                record.setStatus(MessageStatus.PENDING);
                record.setDelivered(0);
                record.setNextReadyAt(now);
                record.setLeaseExpiresAt(0L);
                record.setCreatedAt(now);
                record.setUpdatedAt(now);
                record.setAttempts(new ArrayList<>());
                record.setStrategy(defaultStrategy);
                record.setOriginDeadLetterId(deadLetter.getId());
                state.getMessages().add(record);
                deadLetter.setStatus(DeadLetterStatus.REPLAYING);
                deadLetter.setReplayCount(deadLetter.getReplayCount() + 1);
                affected++;
            }
            return affected;
        });
    }

    @Override
    public int clear(Collection<String> ids) {
        if (ids == null || ids.isEmpty()) {
            throw new IllegalArgumentException("ids 不能为空");
        }
        return store.mutate(state -> {
            int affected = 0;
            for (String id : ids) {
                DeadLetterRecord deadLetter = find(state, id);
                if (deadLetter != null && deadLetter.getStatus() == DeadLetterStatus.DEAD) {
                    state.getDeadLetters().remove(deadLetter);
                    affected++;
                }
            }
            return affected;
        });
    }

    @Override
    public int enforceRetention() {
        return store.mutate(state -> {
            long now = System.currentTimeMillis();
            List<DeadLetterRecord> deadRecords = new ArrayList<>();
            for (DeadLetterRecord record : state.getDeadLetters()) {
                if (record.getStatus() == DeadLetterStatus.DEAD) {
                    deadRecords.add(record);
                }
            }
            List<DeadLetterRecord> removable = new ArrayList<>();
            long retention = properties.getRetentionMillis();
            if (retention > 0) {
                for (DeadLetterRecord record : deadRecords) {
                    if (now - record.getLastDeadAt() >= retention) {
                        removable.add(record);
                    }
                }
            }
            int max = properties.getMaxDeadRecords();
            if (max > 0) {
                List<DeadLetterRecord> sorted = new ArrayList<>(deadRecords);
                sorted.sort(Comparator.comparingLong(DeadLetterRecord::getLastDeadAt));
                int excess = sorted.size() - max;
                for (int i = 0; i < excess; i++) {
                    DeadLetterRecord record = sorted.get(i);
                    if (!removable.contains(record)) {
                        removable.add(record);
                    }
                }
            }
            state.getDeadLetters().removeAll(removable);
            return removable.size();
        });
    }

    private DeadLetterRecord find(PersistedState state, String id) {
        for (DeadLetterRecord record : state.getDeadLetters()) {
            if (record.getId().equals(id)) {
                return record;
            }
        }
        return null;
    }
}
