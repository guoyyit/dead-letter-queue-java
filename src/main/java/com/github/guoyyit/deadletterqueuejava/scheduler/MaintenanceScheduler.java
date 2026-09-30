package com.github.guoyyit.deadletterqueuejava.scheduler;

import com.github.guoyyit.deadletterqueuejava.service.LeaseRecoveryService;
import com.github.guoyyit.deadletterqueuejava.service.RetentionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class MaintenanceScheduler {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceScheduler.class);

    private final LeaseRecoveryService leaseRecoveryService;
    private final RetentionService retentionService;

    public MaintenanceScheduler(LeaseRecoveryService leaseRecoveryService,
                                RetentionService retentionService) {
        this.leaseRecoveryService = leaseRecoveryService;
        this.retentionService = retentionService;
    }

    @Scheduled(fixedDelayString = "${dlq.dispatch.lease-ttl-ms:30000}",
               initialDelayString = "${dlq.dispatch.lease-recovery-initial-delay-ms:30000}")
    public void recoverLeases() {
        int recovered = leaseRecoveryService.recoverExpiredLeases();
        if (recovered > 0 && log.isDebugEnabled()) {
            log.debug("recovered {} expired message leases", recovered);
        }
    }

    @Scheduled(fixedDelayString = "${dlq.retention.sweep-interval-ms:60000}",
               initialDelayString = "${dlq.retention.sweep-interval-ms:60000}")
    public void sweepRetention() {
        int removed = retentionService.sweep();
        if (removed > 0 && log.isDebugEnabled()) {
            log.debug("retention sweep removed {} dead letters", removed);
        }
    }
}
