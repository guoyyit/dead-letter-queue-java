package com.github.guoyyit.deadletterqueuejava.model;

/**
 * 死信记录状态。
 */
public enum DeadLetterStatus {
    DEAD,
    REPLAYING,
    RESOLVED
}
