package com.github.guoyyit.deadletterqueuejava.service;

import com.github.guoyyit.deadletterqueuejava.model.DeadLetterRecord;
import com.github.guoyyit.deadletterqueuejava.model.DeliveredMessage;
import com.github.guoyyit.deadletterqueuejava.model.QueueRecord;
import com.github.guoyyit.deadletterqueuejava.model.RetryStrategy;

/**
 * 消息入队、投递与回执。
 */
public interface QueueService {

    /**
     * 消息入队。
     */
    QueueRecord publish(String queue, String payload, RetryStrategy strategy);

    /**
     * 非阻塞取出一条可消费消息（含退避到期的重试消息），无可用消息返回 null。
     */
    DeliveredMessage poll(String queue);

    /**
     * 消费成功回执，消息被移除；重放消息成功时联动关闭死信记录。
     */
    void ack(String messageId);

    /**
     * 消费失败回执：安排重试；重试耗尽或不可重试时原子转入死信区并返回死信记录。
     */
    DeadLetterRecord nack(String messageId, String reason);

    /**
     * 回收超过可见性超时未回执的消息（崩溃恢复，不额外计为一次失败）。
     */
    int recoverExpiredLeases();

    /**
     * 推进重放中消息的终态：成功关闭死信记录，失败重回死信区并累计重放次数。
     */
    int settleFinishedReplays();
}
