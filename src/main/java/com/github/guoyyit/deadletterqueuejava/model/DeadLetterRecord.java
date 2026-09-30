package com.github.guoyyit.deadletterqueuejava.model;

import java.util.ArrayList;
import java.util.List;

/**
 * 死信区记录（可变，配合持久化快照使用）。
 */
public class DeadLetterRecord {

    private String id;
    private String originMessageId;
    private String originQueue;
    private String originalPayload;
    private String lastFailureReason;
    private List<RetryAttempt> retryHistory = new ArrayList<>();
    private long firstDeadAt;
    private long lastDeadAt;
    private DeadLetterStatus status;
    private int replayCount;
    private List<ReplayEvent> replayHistory = new ArrayList<>();

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOriginMessageId() {
        return originMessageId;
    }

    public void setOriginMessageId(String originMessageId) {
        this.originMessageId = originMessageId;
    }

    public String getOriginQueue() {
        return originQueue;
    }

    public void setOriginQueue(String originQueue) {
        this.originQueue = originQueue;
    }

    public String getOriginalPayload() {
        return originalPayload;
    }

    public void setOriginalPayload(String originalPayload) {
        this.originalPayload = originalPayload;
    }

    public String getLastFailureReason() {
        return lastFailureReason;
    }

    public void setLastFailureReason(String lastFailureReason) {
        this.lastFailureReason = lastFailureReason;
    }

    public List<RetryAttempt> getRetryHistory() {
        return retryHistory;
    }

    public void setRetryHistory(List<RetryAttempt> retryHistory) {
        this.retryHistory = retryHistory;
    }

    public long getFirstDeadAt() {
        return firstDeadAt;
    }

    public void setFirstDeadAt(long firstDeadAt) {
        this.firstDeadAt = firstDeadAt;
    }

    public long getLastDeadAt() {
        return lastDeadAt;
    }

    public void setLastDeadAt(long lastDeadAt) {
        this.lastDeadAt = lastDeadAt;
    }

    public DeadLetterStatus getStatus() {
        return status;
    }

    public void setStatus(DeadLetterStatus status) {
        this.status = status;
    }

    public int getReplayCount() {
        return replayCount;
    }

    public void setReplayCount(int replayCount) {
        this.replayCount = replayCount;
    }

    public List<ReplayEvent> getReplayHistory() {
        return replayHistory;
    }

    public void setReplayHistory(List<ReplayEvent> replayHistory) {
        this.replayHistory = replayHistory;
    }
}
