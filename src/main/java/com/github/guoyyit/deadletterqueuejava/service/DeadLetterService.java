package com.github.guoyyit.deadletterqueuejava.service;

import com.github.guoyyit.deadletterqueuejava.model.DeadLetterRecord;
import com.github.guoyyit.deadletterqueuejava.model.DeadLetterStatus;

import java.util.Collection;
import java.util.List;

/**
 * 死信区查看、重放、清除与淘汰。
 */
public interface DeadLetterService {

    /**
     * 条件查看死信记录。
     */
    List<DeadLetterRecord> list(String queue, DeadLetterStatus status, int limit);

    /**
     * 按 id 查看单条记录，不存在返回 null。
     */
    DeadLetterRecord get(String id);

    /**
     * 将处于死信态的记录原子重放回原队列，返回实际发起重放的条数。
     */
    int replay(Collection<String> ids);

    /**
     * 清除死信态记录（重放中记录受保护），返回实际清除条数。
     */
    int clear(Collection<String> ids);

    /**
     * 按保留时长与总量上限淘汰最旧记录，返回淘汰条数。
     */
    int enforceRetention();
}
