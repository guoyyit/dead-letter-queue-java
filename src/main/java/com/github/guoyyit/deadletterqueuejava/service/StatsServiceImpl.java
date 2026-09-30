package com.github.guoyyit.deadletterqueuejava.service;

import com.github.guoyyit.deadletterqueuejava.model.DeadLetterRecord;
import com.github.guoyyit.deadletterqueuejava.model.DeadLetterStatus;
import com.github.guoyyit.deadletterqueuejava.model.MessageStatus;
import com.github.guoyyit.deadletterqueuejava.model.QueueRecord;
import com.github.guoyyit.deadletterqueuejava.model.StatsSnapshot;
import com.github.guoyyit.deadletterqueuejava.store.StateStore;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * 统计服务实现：从当前持久化记录实时派生，重启后自动一致。
 */
public class StatsServiceImpl implements StatsService {

    private final StateStore store;

    public StatsServiceImpl(StateStore store) {
        this.store = store;
    }

    @Override
    public StatsSnapshot snapshot() {
        return store.read(state -> {
            long pendingRetry = 0;
            long inFlight = 0;
            Map<String, QueueCounters> perQueue = new TreeMap<>();
            for (QueueRecord message : state.getMessages()) {
                QueueCounters counters =
                        perQueue.computeIfAbsent(message.getQueue(), key -> new QueueCounters());
                if (message.getStatus() == MessageStatus.IN_FLIGHT) {
                    inFlight++;
                    counters.inFlight++;
                } else {
                    pendingRetry++;
                    counters.pendingRetry++;
                }
            }
            long dead = 0;
            long replaySuccess = 0;
            long replayFailure = 0;
            for (DeadLetterRecord deadLetter : state.getDeadLetters()) {
                if (deadLetter.getStatus() == DeadLetterStatus.DEAD) {
                    dead++;
                    perQueue.computeIfAbsent(deadLetter.getOriginQueue(),
                            key -> new QueueCounters()).dead++;
                }
                for (var event : deadLetter.getReplayHistory()) {
                    if (event.success()) {
                        replaySuccess++;
                    } else {
                        replayFailure++;
                    }
                }
            }
            Map<String, StatsSnapshot.QueueStats> queues = new LinkedHashMap<>();
            for (Map.Entry<String, QueueCounters> entry : perQueue.entrySet()) {
                QueueCounters counters = entry.getValue();
                queues.put(entry.getKey(), new StatsSnapshot.QueueStats(
                        counters.pendingRetry,
                        counters.pendingRetry,
                        counters.inFlight,
                        counters.dead));
            }
            return new StatsSnapshot(pendingRetry, inFlight, dead,
                    replaySuccess, replayFailure, queues);
        });
    }

    private static final class QueueCounters {
        private long pendingRetry;
        private long inFlight;
        private long dead;
    }
}
