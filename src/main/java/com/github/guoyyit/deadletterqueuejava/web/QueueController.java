package com.github.guoyyit.deadletterqueuejava.web;

import com.github.guoyyit.deadletterqueuejava.model.BackoffType;
import com.github.guoyyit.deadletterqueuejava.model.DeadLetterRecord;
import com.github.guoyyit.deadletterqueuejava.model.DeliveredMessage;
import com.github.guoyyit.deadletterqueuejava.model.StatsSnapshot;
import com.github.guoyyit.deadletterqueuejava.service.QueueService;
import com.github.guoyyit.deadletterqueuejava.service.StatsService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 消息生产、投递、回执与统计接口。
 */
@RestController
@RequestMapping("/api/queues")
public class QueueController {

    private final QueueService queueService;
    private final StatsService statsService;

    public QueueController(QueueService queueService, StatsService statsService) {
        this.queueService = queueService;
        this.statsService = statsService;
    }

    @PostMapping("/{queue}/messages")
    public PublishResponse publish(@PathVariable String queue,
                                   @RequestBody PublishRequest request) {
        if (request == null || request.payload() == null) {
            throw new IllegalArgumentException("payload 不能为空");
        }
        var record = queueService.publish(queue, request.payload(), request.strategy());
        return new PublishResponse(record.getId(), queue, record.getStatus().name(),
                record.getCreatedAt());
    }

    @PostMapping("/{queue}/poll")
    public ResponseEntity<DeliveredMessage> poll(@PathVariable String queue) {
        DeliveredMessage message = queueService.poll(queue);
        if (message == null) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(message);
    }

    @PostMapping("/messages/{messageId}/ack")
    public void ack(@PathVariable String messageId) {
        queueService.ack(messageId);
    }

    @PostMapping("/messages/{messageId}/nack")
    public NackResponse nack(@PathVariable String messageId,
                             @RequestBody(required = false) NackRequest request) {
        String reason = request == null ? null : request.reason();
        DeadLetterRecord deadLetter = queueService.nack(messageId, reason);
        if (deadLetter == null) {
            return new NackResponse("RETRY_SCHEDULED", null);
        }
        return new NackResponse("DEAD_LETTER", deadLetter);
    }

    @GetMapping("/stats")
    public StatsSnapshot stats() {
        return statsService.snapshot();
    }

    public record PublishRequest(
            String payload,
            Integer maxRetries,
            Long initialIntervalMillis,
            Long maxIntervalMillis,
            BackoffType backoffType
    ) {
        public com.github.guoyyit.deadletterqueuejava.model.RetryStrategy strategy() {
            if (maxRetries == null && initialIntervalMillis == null
                    && maxIntervalMillis == null && backoffType == null) {
                return null;
            }
            return new com.github.guoyyit.deadletterqueuejava.model.RetryStrategy(
                    maxRetries == null ? 0 : maxRetries,
                    initialIntervalMillis == null ? 0L : initialIntervalMillis,
                    maxIntervalMillis == null ? 0L : maxIntervalMillis,
                    backoffType);
        }
    }

    public record NackRequest(String reason) {
    }

    public record NackResponse(String outcome, DeadLetterRecord deadLetter) {
    }

    public record PublishResponse(String id, String queue, String status, long createdAt) {
    }
}
