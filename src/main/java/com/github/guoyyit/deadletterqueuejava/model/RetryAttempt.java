package com.github.guoyyit.deadletterqueuejava.model;

/**
 * 单次消费失败记录。
 *
 * @param delivery      第几次投递（从 1 开始）
 * @param at            失败时间戳
 * @param reason        失败原因
 * @param backoffMillis 该次失败后的退避时长；转入死信区时为 -1
 */
public record RetryAttempt(
        int delivery,
        long at,
        String reason,
        long backoffMillis
) {
}
