package com.github.guoyyit.deadletterqueuejava.model;

/**
 * 单次重放记录。
 *
 * @param at        重放发起时间
 * @param success   重放后是否消费成功
 * @param messageId 重放产生的新消息 id
 * @param reason    再次失败时的原因；成功时为 null
 */
public record ReplayEvent(
        long at,
        boolean success,
        String messageId,
        String reason
) {
}
