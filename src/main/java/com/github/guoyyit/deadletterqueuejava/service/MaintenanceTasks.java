package com.github.guoyyit.deadletterqueuejava.service;

import com.github.guoyyit.deadletterqueuejava.config.DlqProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * 后台维护：回收超时租约、推进重放终态、执行保留淘汰。
 */
public class MaintenanceTasks {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceTasks.class);

    private final QueueService queueService;
    private final DeadLetterService deadLetterService;
    private final DlqProperties properties;

    public MaintenanceTasks(QueueService queueService,
                            DeadLetterService deadLetterService,
                            DlqProperties properties) {
        this.queueService = queueService;
        this.deadLetterService = deadLetterService;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${dlq.maintenance-interval-millis:1000}")
    public void runMaintenance() {
        try {
            queueService.recoverExpiredLeases();
            queueService.settleFinishedReplays();
            deadLetterService.enforceRetention();
        } catch (RuntimeException e) {
            log.warn("死信队列后台维护任务执行失败: {}", e.getMessage(), e);
        }
    }
}
