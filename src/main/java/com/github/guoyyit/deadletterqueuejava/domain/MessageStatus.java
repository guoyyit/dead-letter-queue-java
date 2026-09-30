package com.github.guoyyit.deadletterqueuejava.domain;

public enum MessageStatus {
    PENDING,
    IN_FLIGHT,
    SUCCEEDED,
    DEAD
}
