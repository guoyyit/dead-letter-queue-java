package com.github.guoyyit.deadletterqueuejava.service;

import com.github.guoyyit.deadletterqueuejava.domain.QueueStats;
import com.github.guoyyit.deadletterqueuejava.domain.ReplayOutcome;
import com.github.guoyyit.deadletterqueuejava.repository.DeadLetterRepository;
import com.github.guoyyit.deadletterqueuejava.repository.MessageRepository;
import org.springframework.stereotype.Service;

@Service
public class StatsService {

    private final MessageRepository messageRepository;
    private final DeadLetterRepository deadLetterRepository;

    public StatsService(MessageRepository messageRepository,
                        DeadLetterRepository deadLetterRepository) {
        this.messageRepository = messageRepository;
        this.deadLetterRepository = deadLetterRepository;
    }

    public QueueStats stats(String queue) {
        long now = System.currentTimeMillis();
        return new QueueStats(
                messageRepository.countTotalEnqueued(queue),
                messageRepository.countPendingRetry(queue, now),
                messageRepository.countByStatus(queue, "IN_FLIGHT"),
                messageRepository.countByStatus(queue, "SUCCEEDED"),
                deadLetterRepository.countActive(queue),
                deadLetterRepository.countReplaying(queue),
                deadLetterRepository.countReplayOutcome(queue, ReplayOutcome.SUCCEEDED),
                deadLetterRepository.countReplayOutcome(queue, ReplayOutcome.FAILED));
    }
}
