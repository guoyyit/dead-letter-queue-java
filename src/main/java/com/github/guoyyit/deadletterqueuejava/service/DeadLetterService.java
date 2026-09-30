package com.github.guoyyit.deadletterqueuejava.service;

import com.github.guoyyit.deadletterqueuejava.config.DlqProperties;
import com.github.guoyyit.deadletterqueuejava.domain.DeadLetterRecord;
import com.github.guoyyit.deadletterqueuejava.domain.DeadLetterReplay;
import com.github.guoyyit.deadletterqueuejava.domain.DeadLetterStatus;
import com.github.guoyyit.deadletterqueuejava.domain.Message;
import com.github.guoyyit.deadletterqueuejava.domain.PagedResult;
import com.github.guoyyit.deadletterqueuejava.exception.BadRequestException;
import com.github.guoyyit.deadletterqueuejava.exception.ConflictException;
import com.github.guoyyit.deadletterqueuejava.exception.NotFoundException;
import com.github.guoyyit.deadletterqueuejava.repository.DeadLetterRepository;
import com.github.guoyyit.deadletterqueuejava.repository.MessageRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class DeadLetterService {

    private final DeadLetterRepository deadLetterRepository;
    private final MessageRepository messageRepository;
    private final DlqProperties properties;

    public DeadLetterService(DeadLetterRepository deadLetterRepository,
                             MessageRepository messageRepository,
                             DlqProperties properties) {
        this.deadLetterRepository = deadLetterRepository;
        this.messageRepository = messageRepository;
        this.properties = properties;
    }

    public PagedResult<DeadLetterRecord> list(String queue, DeadLetterStatus status,
                                              String reasonContains, Long fromMs, Long toMs,
                                              int page, int size) {
        int safePage = Math.max(0, page);
        int safeSize = Math.min(200, Math.max(1, size));
        List<DeadLetterRecord> records = deadLetterRepository.search(
                queue, status, reasonContains, fromMs, toMs, safePage, safeSize);
        records = records.stream().map(this::withAttempts).toList();
        long total = deadLetterRepository.countSearch(
                queue, status, reasonContains, fromMs, toMs);
        return new PagedResult<>(records, safePage, safeSize, total);
    }

    public DeadLetterRecord get(long id) {
        return withAttempts(requireDeadLetter(id, false));
    }

    @Transactional
    public long replayOne(long id) {
        int claimed = deadLetterRepository.claimForReplay(id);
        if (claimed == 0) {
            if (deadLetterRepository.findById(id) == null) {
                throw new NotFoundException("dead letter not found: " + id);
            }
            throw new ConflictException("dead letter is already being replayed: " + id);
        }
        DeadLetterRecord record = deadLetterRepository.findById(id);
        long now = System.currentTimeMillis();
        DeadLetterReplay replay = deadLetterRepository.openReplay(record.id(), record.messageId(), now);
        Message message = messageRepository.lockById(record.messageId());
        if (message == null) {
            throw new NotFoundException("backing message missing for dead letter: " + id);
        }
        long leaseUntil = now + properties.getDispatch().getLeaseTtlMs();
        messageRepository.requeueForReplay(message.id(), now, leaseUntil);
        return replay == null ? 0L : replay.id();
    }

    @Transactional
    public List<Long> replayBatch(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            throw new BadRequestException("ids must not be empty");
        }
        return ids.stream().filter(id -> {
            try {
                replayOne(id);
                return true;
            } catch (ConflictException ex) {
                return false;
            }
        }).toList();
    }

    @Transactional
    public int clearOne(long id) {
        DeadLetterRecord record = requireDeadLetter(id, true);
        int removed = deadLetterRepository.deleteByIdIfNotReplaying(id);
        if (removed == 0) {
            throw new ConflictException("dead letter is being replayed and cannot be cleared: " + id);
        }
        deleteBackingData(record);
        return removed;
    }

    @Transactional
    public int clearBatch(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            throw new BadRequestException("ids must not be empty");
        }
        int cleared = 0;
        for (Long id : ids) {
            cleared += clearOne(id);
        }
        return cleared;
    }

    @Transactional
    public int clearAll(String queue) {
        int cleared = 0;
        int batch = 200;
        while (true) {
            List<DeadLetterRecord> page = deadLetterRepository.searchAllClearable(queue, batch);
            if (page.isEmpty()) {
                return cleared;
            }
            for (DeadLetterRecord record : page) {
                int removed = deadLetterRepository.deleteByIdIfNotReplaying(record.id());
                if (removed > 0) {
                    deleteBackingData(record);
                    cleared += removed;
                }
            }
            if (page.size() < batch) {
                return cleared;
            }
        }
    }

    private void deleteBackingData(DeadLetterRecord record) {
        deadLetterRepository.deleteReplaysByMessage(record.messageId());
        deadLetterRepository.deleteAttempts(record.messageId());
        messageRepository.deleteByIdIfDead(record.messageId());
    }

    private DeadLetterRecord requireDeadLetter(long id, boolean lock) {
        DeadLetterRecord record = lock ? deadLetterRepository.lockById(id)
                : deadLetterRepository.findById(id);
        if (record == null) {
            throw new NotFoundException("dead letter not found: " + id);
        }
        if (lock && record.status() == DeadLetterStatus.REPLAYING) {
            throw new ConflictException("dead letter is already being replayed: " + id);
        }
        return record;
    }

    private DeadLetterRecord withAttempts(DeadLetterRecord record) {
        return new DeadLetterRecord(
                record.id(),
                record.messageId(),
                record.queue(),
                record.payload(),
                record.lastReason(),
                record.firstDiedAt(),
                record.lastDiedAt(),
                record.deathCount(),
                record.replayCount(),
                record.status(),
                messageRepository.findAttempts(record.messageId()));
    }
}
