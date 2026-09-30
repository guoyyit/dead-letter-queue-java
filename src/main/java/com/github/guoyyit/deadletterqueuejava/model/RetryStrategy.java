package com.github.guoyyit.deadletterqueuejava.model;

/**
 * 消息重试策略。
 *
 * @param maxRetries            首次消费失败后允许的最大重试次数
 * @param initialIntervalMillis 首次退避间隔
 * @param maxIntervalMillis     退避间隔上限
 * @param backoffType           退避方式
 */
public record RetryStrategy(
        int maxRetries,
        long initialIntervalMillis,
        long maxIntervalMillis,
        BackoffType backoffType
) {
    public static RetryStrategy defaults() {
        return null;
    }
}
