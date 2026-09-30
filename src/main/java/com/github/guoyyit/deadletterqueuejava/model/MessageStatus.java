package com.github.guoyyit.deadletterqueuejava.model;

/**
 * 队列消息状态。
 */
public enum MessageStatus {
    PENDING,
    RETRYING,
    IN_FLIGHT
}
