package com.github.guoyyit.deadletterqueuejava.tests;

import com.github.guoyyit.deadletterqueuejava.domain.MessageStatus;
import com.github.guoyyit.deadletterqueuejava.domain.QueueStats;
import com.github.guoyyit.deadletterqueuejava.service.DeadLetterService;
import com.github.guoyyit.deadletterqueuejava.service.MessageService;
import com.github.guoyyit.deadletterqueuejava.service.StatsService;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestMethodOrder(MethodOrderer.MethodName.class)
class RestartVerifyIntegrationTest {

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:file:" + RestartPrepareIntegrationTest.DB_DIR
                        + "/db;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;IFEXISTS=TRUE");
        registry.add("dlq.data-dir", () -> RestartPrepareIntegrationTest.DB_DIR);
        registry.add("dlq.dispatch.lease-ttl-ms", () -> 300000L);
        registry.add("dlq.retention.sweep-interval-ms", () -> 3600000L);
    }

    @Autowired
    private MessageService messageService;

    @Autowired
    private StatsService statsService;

    @Autowired
    private DeadLetterService deadLetterService;

    @Test
    void a_committedRetryProgressAndStatsSurviveRestart() throws Exception {
        assertEquals(MessageStatus.PENDING,
                messageService.getMessage(RestartPrepareIntegrationTest.waitingId).status());
        assertTrue(messageService.getMessage(RestartPrepareIntegrationTest.waitingId).attempts() >= 1);
        assertNull(messageService.fetch(RestartPrepareIntegrationTest.QUEUE, 100L),
                "message still inside backoff window must not be dispatched after restart");

        QueueStats stats = statsService.stats(RestartPrepareIntegrationTest.QUEUE);
        assertEquals(1L, stats.pendingRetry());
        assertEquals(1L, stats.deadLetter());
        assertEquals(1L, stats.succeeded());
        assertEquals(0L, stats.replaySucceeded());

        var deadLetters = deadLetterService
                .list(RestartPrepareIntegrationTest.QUEUE, null, null, null, null, 0, 10)
                .items();
        assertEquals(1, deadLetters.size());
        assertEquals(RestartPrepareIntegrationTest.deadId, deadLetters.get(0).messageId());
        assertEquals("dead-msg", deadLetters.get(0).payload());
        assertEquals("immediately dead", deadLetters.get(0).lastReason());
        assertEquals(1, deadLetters.get(0).attempts().size());
    }

    @Test
    void b_waitingMessageResumesFromCommittedAttemptAfterBackoff() throws Exception {
        var deadLetters = deadLetterService
                .list(RestartPrepareIntegrationTest.QUEUE, null, null, null, null, 0, 10)
                .items();
        long dlId = deadLetters.get(0).id();

        long replayId = deadLetterService.replayOne(dlId);
        assertTrue(replayId > 0);
        messageService.acknowledge(RestartPrepareIntegrationTest.deadId);

        QueueStats stats = statsService.stats(RestartPrepareIntegrationTest.QUEUE);
        assertEquals(1L, stats.replaySucceeded());
        assertEquals(0L, stats.replayFailed());
        assertEquals(0L, stats.deadLetter());
    }
}
