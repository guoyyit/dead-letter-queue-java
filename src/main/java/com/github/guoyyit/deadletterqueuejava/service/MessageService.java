package com.github.guoyyit.deadletterqueuejava.service;

import com.github.guoyyit.deadletterqueuejava.domain.DeliveryAttempt;
import com.github.guoyyit.deadletterqueuejava.domain.Message;
import com.github.guoyyit.deadletterqueuejava.domain.MessageStatus;
import com.github.guoyyit.deadletterqueuejava.domain.RetryPolicy;
import com.github.guoyyit.deadletterqueuejava.config.DlqProperties;
import com.github.guoyyit.deadletterqueuejava.repository.DeadLetterRepository;
import com.github.guoyyit.deadletterqueuejava.repository.MessageRepository;
import com.github.guoyyit.deadletterqueuejava.exception.BadRequestException;
import com.github.guoyyit.deadletterqueuejava.exception.ConflictException;
import com.github.guoyyit.deadletterqueuejava.exception.NotFoundException;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.List;

@Service
public class MessageService {

    private final MessageRepository messageRepository;
    private final DeadLetterRepository deadLetterRepository;
    private final RetryPolicyResolver retryPolicyResolver;
    private final DlqProperties properties;

    public MessageService(MessageRepository messageRepository,
                          DeadLetterRepository deadLetterRepository,
                          RetryPolicyResolver retryPolicyResolver,
                          DlqProperties properties) {
        this.messageRepository = messageRepository;
        this.deadLetterRepository = deadLetterRepository;
        this.retryPolicyResolver = retryPolicyResolver;
        this.properties = properties;
    }

    public Message enqueue(String queue, String payload, RetryPolicy policy) {
        if (queue == null || queue.isBlank()) {
            throw new BadRequestException("queue must not be blank");
        }
        if (payload == null) {
            throw new BadRequestException("payload must not be null");
        }
        RetryPolicy effectivePolicy = retryPolicyResolver.merge(policy);
        long now = System.currentTimeMillis();
        Message message = new Message(
                UUID.randomUUID().toString(),
                queue.trim(),
                payload,
                MessageStatus.PENDING,
                0,
                now,
                now,
                0L,
                null,
                effectivePolicy);
        messageRepository.insert(message);
        return message;
    }

    @Transactional
    public Message fetch(String queue, long timeoutMs) throws InterruptedException {
        if (queue == null || queue.isBlank()) {
            throw new BadRequestException("queue must not be blank");
        }
        long deadline = System.currentTimeMillis() + Math.max(0L, timeoutMs);
        long leaseTtl = 30000L;
        Message next = leaseNext(queue);
        while (next == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(Math.max(10L, properties.getDispatch().getPollTimeoutMs()));
            next = leaseNext(queue);
        }
        return next;
    }

    @Transactional
    public void acknowledge(String messageId) {
        Message message = messageRepository.lockById(messageId);
        if (message == null) {
            throw new NotFoundException("message not found: " + messageId);
        }
        if (message.status() == MessageStatus.SUCCEEDED) {
            return;
        }
        if (message.status() != MessageStatus.IN_FLIGHT) {
            throw new ConflictException("message is not leased for acknowledgement: " + messageId);
        }
        long now = System.currentTimeMillis();
        int outcomeAttempt = message.attempts() + 1;
        messageRepository.insertAttempt(new DeliveryAttempt(
                0L, messageId, outcomeAttempt, "SUCCESS", null,
                message.visibleAt(), now));
        messageRepository.markSucceeded(messageId, outcomeAttempt, now);
        var openReplay = deadLetterRepository.findOpenReplayByMessageId(messageId);
        if (openReplay != null) {
            deadLetterRepository.closeReplay(
                    openReplay.id(),
                    com.github.guoyyit.deadletterqueuejava.domain.ReplayOutcome.SUCCEEDED,
                    null, now);
            var deadLetter = deadLetterRepository.lockById(openReplay.deadLetterId());
            if (deadLetter != null) {
                deadLetterRepository.markResolved(deadLetter.id());
            }
        }
    }

    @Transactional
    public void negativeAcknowledge(String messageId, String reason) {
        Message message = messageRepository.lockById(messageId);
        if (message == null) {
            throw new NotFoundException("message not found: " + messageId);
        }
        if (message.status() == MessageStatus.DEAD || message.status() == MessageStatus.SUCCEEDED) {
            throw new ConflictException(
                    "message is already terminal (" + message.status() + "): " + messageId);
        }
        if (message.status() != MessageStatus.IN_FLIGHT) {
            throw new ConflictException("message is not leased for failure reporting: " + messageId);
        }
        long now = System.currentTimeMillis();
        int outcomeAttempt = message.attempts() + 1;
        messageRepository.insertAttempt(new DeliveryAttempt(
                0L, messageId, outcomeAttempt, "FAILED", reason,
                message.visibleAt(), now));

        int attempts = outcomeAttempt;
        if (attempts <= message.retryPolicy().maxRetries()) {
            long visibleAt = now + retryPolicyResolver.backoffMs(message.retryPolicy(), attempts);
            messageRepository.markPending(messageId, attempts, visibleAt, reason);
        } else {
            moveToDeadLetter(message, attempts, reason, now);
        }
    }

    public Message getMessage(String messageId) {
        Message message = messageRepository.findById(messageId);
        if (message == null) {
            throw new NotFoundException("message not found: " + messageId);
        }
        return message;
    }

    public List<DeliveryAttempt> getHistory(String messageId) {
        getMessage(messageId);
        return messageRepository.findAttempts(messageId);
    }

    public long leaseTtlMs() {
        return properties.getDispatch().getLeaseTtlMs();
    }

    @Transactional
    public Message leaseNext(String queue) {
        long now = System.currentTimeMillis();
        Message candidate = messageRepository.fetchForDispatch(queue, now);
        if (candidate == null) {
            return null;
        }
        long leaseUntil = now + leaseTtlMs();
        messageRepository.markInFlight(candidate.id(), now, leaseUntil);
        return messageRepository.findById(candidate.id());
    }

    private void moveToDeadLetter(Message message, int attempts, String reason, long now) {
        messageRepository.markDead(message.id(), attempts, reason, now);
        var existing = deadLetterRepository.lockByMessageId(message.id());
        if (existing == null) {
            deadLetterRepository.insertForNewDeath(
                    message.id(), message.queue(), message.payload(), reason, now);
        } else {
            var replay = deadLetterRepository.findOpenReplayByMessageId(message.id());
            if (replay != null) {
                deadLetterRepository.closeReplay(replay.id(),
                        com.github.guoyyit.deadletterqueuejava.domain.ReplayOutcome.FAILED, reason, now);
            }
            deadLetterRepository.recordRepeatDeath(existing.id(), reason, now);
        }
    }
}
