package com.github.guoyyit.deadletterqueuejava.model;

import java.util.ArrayList;
import java.util.List;

/**
 * 队列中的消息记录（可变，配合持久化快照使用）。
 */
public class QueueRecord {

    private String id;
    private String queue;
    private String payload;
    private MessageStatus status;
    private int delivered;
    private long nextReadyAt;
    private long leaseExpiresAt;
    private long createdAt;
    private long updatedAt;
    private RetryStrategy strategy;
    private List<RetryAttempt> attempts = new ArrayList<>();
    private String originDeadLetterId;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public MessageStatus getStatus() {
        return status;
    }

    public void setStatus(MessageStatus status) {
        this.status = status;
    }

    public int getDelivered() {
        return delivered;
    }

    public void setDelivered(int delivered) {
        this.delivered = delivered;
    }

    public long getNextReadyAt() {
        return nextReadyAt;
    }

    public void setNextReadyAt(long nextReadyAt) {
        this.nextReadyAt = nextReadyAt;
    }

    public long getLeaseExpiresAt() {
        return leaseExpiresAt;
    }

    public void setLeaseExpiresAt(long leaseExpiresAt) {
        this.leaseExpiresAt = leaseExpiresAt;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public RetryStrategy getStrategy() {
        return strategy;
    }

    public void setStrategy(RetryStrategy strategy) {
        this.strategy = strategy;
    }

    public List<RetryAttempt> getAttempts() {
        return attempts;
    }

    public void setAttempts(List<RetryAttempt> attempts) {
        this.attempts = attempts;
    }

    public String getOriginDeadLetterId() {
        return originDeadLetterId;
    }

    public void setOriginDeadLetterId(String originDeadLetterId) {
        this.originDeadLetterId = originDeadLetterId;
    }
}
