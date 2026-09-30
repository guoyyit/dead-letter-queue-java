package com.github.guoyyit.deadletterqueuejava.model;

/**
 * 一次投递视图：消息内容附带本次可见性租约信息。
 *
 * @param id                 消息 id
 * @param queue              所属队列
 * @param payload            消息原始内容
 * @param delivered          本次是第几次投递
 * @param leaseExpiresAt     租约到期时间戳
 * @param originDeadLetterId 若为重放产生的消息，对应原死信记录 id；否则为 null
 */
public record DeliveredMessage(
        String id,
        String queue,
        String payload,
        int delivered,
        long leaseExpiresAt,
        String originDeadLetterId
) {
    public static DeliveredMessage of(QueueRecord record) {
        return new DeliveredMessage(
                record.getId(),
                record.getQueue(),
                record.getPayload(),
                record.getDelivered(),
                record.getLeaseExpiresAt(),
                record.getOriginDeadLetterId());
    }
}
