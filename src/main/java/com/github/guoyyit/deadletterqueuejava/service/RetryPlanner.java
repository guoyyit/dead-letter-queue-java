package com.github.guoyyit.deadletterqueuejava.service;

import com.github.guoyyit.deadletterqueuejava.model.RetryStrategy;

/**
 * 重试策略解析与退避计算。
 */
public interface RetryPlanner {

    /**
     * 失败原因以此前缀开头时，消息不参与重试，直接进入死信区。
     */
    String NON_RETRYABLE_PREFIX = "NON_RETRYABLE:";

    /**
     * 合并消息自带策略与全局默认策略。
     */
    RetryStrategy resolve(RetryStrategy requested);

    /**
     * 计算第 {@code retryAttempt} 次重试（首次失败后为 1）前的退避时长。
     */
    long backoffMillis(RetryStrategy strategy, int retryAttempt);

    /**
     * 按失败原因判断是否不可重试。
     */
    boolean isNonRetryable(String reason);
}
