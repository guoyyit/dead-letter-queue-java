package com.github.guoyyit.deadletterqueuejava.web;

import com.github.guoyyit.deadletterqueuejava.domain.BackoffType;
import com.github.guoyyit.deadletterqueuejava.domain.DeliveryAttempt;
import com.github.guoyyit.deadletterqueuejava.domain.Message;
import com.github.guoyyit.deadletterqueuejava.domain.RetryPolicy;
import com.github.guoyyit.deadletterqueuejava.exception.BadRequestException;
import com.github.guoyyit.deadletterqueuejava.service.MessageService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/queues")
public class QueueController {

    private final MessageService messageService;

    public QueueController(MessageService messageService) {
        this.messageService = messageService;
    }

    public record RetryPolicySpec(Integer maxRetries, Long initialBackoffMs,
                                  Long maxBackoffMs, String backoffType) {
    }

    public record EnqueueRequest(String payload, RetryPolicySpec retryPolicy) {
    }

    public record NackRequest(String reason) {
    }

    @PostMapping("/{queue}/messages")
    public ResponseEntity<Message> enqueue(@PathVariable String queue,
                                           @RequestBody EnqueueRequest request) {
        if (request == null || request.payload() == null) {
            throw new BadRequestException("request body with 'payload' is required");
        }
        Message message = messageService.enqueue(queue, request.payload(), toPolicy(request.retryPolicy()));
        return ResponseEntity.status(HttpStatus.CREATED).body(message);
    }

    @GetMapping("/{queue}/messages/next")
    public ResponseEntity<Message> fetchNext(@PathVariable String queue,
                                             @RequestParam(defaultValue = "0") long timeoutMs)
            throws InterruptedException {
        Message message = messageService.fetch(queue, timeoutMs);
        return message == null ? ResponseEntity.noContent().build() : ResponseEntity.ok(message);
    }

    @PostMapping("/messages/{messageId}/ack")
    public Map<String, Object> ack(@PathVariable String messageId) {
        messageService.acknowledge(messageId);
        return Map.of("messageId", messageId, "status", "SUCCEEDED");
    }

    @PostMapping("/messages/{messageId}/nack")
    public Map<String, Object> nack(@PathVariable String messageId,
                                    @RequestBody(required = false) NackRequest request) {
        String reason = request == null ? null : request.reason();
        messageService.negativeAcknowledge(messageId, reason);
        return Map.of("messageId", messageId, "status", "ACCEPTED");
    }

    @GetMapping("/messages/{messageId}")
    public Message getMessage(@PathVariable String messageId) {
        return messageService.getMessage(messageId);
    }

    @GetMapping("/messages/{messageId}/attempts")
    public List<DeliveryAttempt> getAttempts(@PathVariable String messageId) {
        return messageService.getHistory(messageId);
    }

    private RetryPolicy toPolicy(RetryPolicySpec spec) {
        if (spec == null) {
            return null;
        }
        BackoffType backoffType = null;
        if (spec.backoffType() != null && !spec.backoffType().isBlank()) {
            try {
                backoffType = BackoffType.valueOf(spec.backoffType().trim().toUpperCase());
            } catch (IllegalArgumentException ex) {
                throw new BadRequestException(
                        "backoffType must be one of FIXED, EXPONENTIAL");
            }
        }
        return new RetryPolicy(
                spec.maxRetries() == null ? -1 : spec.maxRetries(),
                spec.initialBackoffMs() == null ? -1L : spec.initialBackoffMs(),
                spec.maxBackoffMs() == null ? -1L : spec.maxBackoffMs(),
                backoffType);
    }
}
