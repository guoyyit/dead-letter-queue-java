package com.github.guoyyit.deadletterqueuejava.tests;

import com.github.guoyyit.deadletterqueuejava.domain.BackoffType;
import com.github.guoyyit.deadletterqueuejava.domain.DeadLetterRecord;
import com.github.guoyyit.deadletterqueuejava.domain.DeadLetterStatus;
import com.github.guoyyit.deadletterqueuejava.domain.Message;
import com.github.guoyyit.deadletterqueuejava.domain.PagedResult;
import com.github.guoyyit.deadletterqueuejava.domain.RetryPolicy;
import com.github.guoyyit.deadletterqueuejava.service.DeadLetterService;
import com.github.guoyyit.deadletterqueuejava.service.MessageService;
import com.github.guoyyit.deadletterqueuejava.service.RetentionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class RetentionIntegrationTest {

    private static final String DB_DIR = "./target/test-data/retention-" + UUID.randomUUID();

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:file:" + DB_DIR + "/db;MODE=PostgreSQL;LOCK_TIMEOUT=10000");
        registry.add("dlq.data-dir", () -> DB_DIR);
        registry.add("dlq.retry.max-retries", () -> 0);
        registry.add("dlq.retry.initial-backoff-ms", () -> 1L);
        registry.add("dlq.retry.max-backoff-ms", () -> 1L);
        registry.add("dlq.retry.backoff-type", () -> "FIXED");
        registry.add("dlq.retention.max-age-ms", () -> 1000L);
        registry.add("dlq.retention.max-records", () -> 3L);
        registry.add("dlq.retention.sweep-interval-ms", () -> 3600000L);
    }

    @Autowired
    private MessageService messageService;

    @Autowired
    private DeadLetterService deadLetterService;

    @Autowired
    private RetentionService retentionService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void sweepsOldestByAgeAndCountButProtectsReplayingRecords() throws Exception {
        RetryPolicy policy = new RetryPolicy(0, 1L, 1L, BackoffType.FIXED);

        Long oldId = killOne("ret-q", "old", policy);
        Thread.sleep(10);
        jdbcTemplate.update("UPDATE dead_letters SET first_died_at = ? WHERE id = ?",
                System.currentTimeMillis() - 5000L, oldId);

        int removed = retentionService.sweep();
        assertEquals(1, removed, "only the expired old record should be age-evicted");
        assertNull(deadLetterService.list("ret-q", null, null, null, null, 0, 50)
                .items().stream().filter(r -> r.id() == oldId).findFirst().orElse(null));

        Long replayingId = killOne("ret-q", "replaying", policy);
        deadLetterService.replayOne(replayingId);

        Long new1 = killOne("ret-q", "new1", policy);
        Long new2 = killOne("ret-q", "new2", policy);
        Long new3 = killOne("ret-q", "new3", policy);

        assertNotNull(deadLetterService.get(replayingId));

        Long new4 = killOne("ret-q", "new4", policy);
        Long new5 = killOne("ret-q", "new5", policy);
        removed = retentionService.sweep();
        assertEquals(2, removed, "count cap evicts down to the oldest cap, skipping replaying");

        PagedResult<DeadLetterRecord> active =
                deadLetterService.list("ret-q", DeadLetterStatus.ACTIVE, null, null, null, 0, 50);
        assertEquals(3, active.items().size(), "active count never exceeds cap");
        assertEquals(DeadLetterStatus.REPLAYING, deadLetterService.get(replayingId).status());
        assertNull(deadLetterService.list("ret-q", DeadLetterStatus.ACTIVE, null, null, null, 0, 50)
                .items().stream().filter(r -> r.id() == new1).findFirst().orElse(null));

        assertNotNull(messageService.getMessage(deadLetterService.get(new5).messageId()));
    }

    private Long killOne(String queue, String payload, RetryPolicy policy) throws Exception {
        Message message = messageService.enqueue(queue, payload, policy);
        Message leased = messageService.fetch(queue, 2000L);
        assertEquals(message.id(), leased.id());
        messageService.negativeAcknowledge(message.id(), "fail-" + payload);
        return deadLetterService.list(queue, DeadLetterStatus.ACTIVE, null, null, null, 0, 50)
                .items().stream()
                .filter(r -> r.messageId().equals(message.id()))
                .findFirst().orElseThrow().id();
    }
}
