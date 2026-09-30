package com.github.guoyyit.deadletterqueuejava.service;

import com.github.guoyyit.deadletterqueuejava.config.DlqProperties;
import com.github.guoyyit.deadletterqueuejava.domain.DeadLetterRecord;
import com.github.guoyyit.deadletterqueuejava.domain.DeadLetterStatus;
import com.github.guoyyit.deadletterqueuejava.repository.DeadLetterRepository;
import com.github.guoyyit.deadletterqueuejava.repository.MessageRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Service
public class RetentionService {

    private static final int BATCH = 200;

    private final DeadLetterRepository deadLetterRepository;
    private final MessageRepository messageRepository;
    private final DlqProperties properties;

    public RetentionService(DeadLetterRepository deadLetterRepository,
                            MessageRepository messageRepository,
                            DlqProperties properties) {
        this.deadLetterRepository = deadLetterRepository;
        this.messageRepository = messageRepository;
        this.properties = properties;
    }

    @Transactional
    public int sweep() {
        long now = System.currentTimeMillis();
        long ageCutoff = now - properties.getRetention().getMaxAgeMs();
        Set<Long> evictable = new LinkedHashSet<>();
        for (DeadLetterRecord record : deadLetterRepository.findExpiredByAge(ageCutoff, BATCH)) {
            evictable.add(record.id());
        }

        long maxRecords = properties.getRetention().getMaxRecords();
        if (maxRecords > 0) {
            long activeCount = deadLetterRepository.countSearch(
                    null, DeadLetterStatus.ACTIVE, null, null, null);
            long excess = activeCount - maxRecords;
            if (excess > 0) {
                int batchSize = (int) Math.min(excess, BATCH);
                int beforeAgeEvictions = evictable.size();
                for (DeadLetterRecord record :
                        deadLetterRepository.findOldestActive(batchSize)) {
                    if (evictable.size() - beforeAgeEvictions >= excess) {
                        break;
                    }
                    evictable.add(record.id());
                }
            }
        }

        int removed = 0;
        for (Long id : new ArrayList<>(evictable)) {
            DeadLetterRecord record = deadLetterRepository.lockById(id);
            if (record == null || record.status() == DeadLetterStatus.REPLAYING) {
                continue;
            }
            deadLetterRepository.deleteById(id);
            deadLetterRepository.deleteReplaysByMessage(record.messageId());
            deadLetterRepository.deleteAttempts(record.messageId());
            messageRepository.deleteByIdIfDead(record.messageId());
            removed++;
        }
        return removed;
    }
}
