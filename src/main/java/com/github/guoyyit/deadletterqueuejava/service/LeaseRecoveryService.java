package com.github.guoyyit.deadletterqueuejava.service;
import com.github.guoyyit.deadletterqueuejava.domain.DeadLetterRecord;
import com.github.guoyyit.deadletterqueuejava.domain.DeadLetterReplay;
import com.github.guoyyit.deadletterqueuejava.domain.Message;
import com.github.guoyyit.deadletterqueuejava.domain.MessageStatus;
import com.github.guoyyit.deadletterqueuejava.domain.ReplayOutcome;
import com.github.guoyyit.deadletterqueuejava.repository.DeadLetterRepository;
import com.github.guoyyit.deadletterqueuejava.repository.MessageRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class LeaseRecoveryService {

    private final MessageRepository messageRepository;
    private final DeadLetterRepository deadLetterRepository;

    public LeaseRecoveryService(MessageRepository messageRepository,
                                DeadLetterRepository deadLetterRepository) {
        this.messageRepository = messageRepository;
        this.deadLetterRepository = deadLetterRepository;
    }

    @Transactional
    public int recoverExpiredLeases() {
        long now = System.currentTimeMillis();
        List<Message> expired = messageRepository.findExpiredLeases(now);
        int recovered = 0;
        for (Message stale : expired) {
            Message locked = messageRepository.lockById(stale.id());
            if (locked == null || locked.status() != MessageStatus.IN_FLIGHT
                    || locked.leasedUntil() == 0 || locked.leasedUntil() >= now) {
                continue;
            }
            messageRepository.resetLease(locked.id(), now, "lease expired before acknowledgement");
            DeadLetterReplay openReplay =
                    deadLetterRepository.findOpenReplayByMessageId(locked.id());
            if (openReplay != null) {
                deadLetterRepository.closeReplay(openReplay.id(), ReplayOutcome.FAILED,
                        "replay interrupted: lease expired", now);
                DeadLetterRecord deadLetter =
                        deadLetterRepository.lockById(openReplay.deadLetterId());
                if (deadLetter != null) {
                    deadLetterRepository.recordRepeatDeath(
                            deadLetter.id(), "replay interrupted: lease expired", now);
                }
            }
            recovered++;
        }
        return recovered;
    }
}
