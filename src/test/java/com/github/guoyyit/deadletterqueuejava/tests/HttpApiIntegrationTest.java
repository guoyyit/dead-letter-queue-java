package com.github.guoyyit.deadletterqueuejava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class HttpApiIntegrationTest {

    private static final String DB_DIR = "./target/test-data/http-" + UUID.randomUUID();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
                () -> "jdbc:h2:file:" + DB_DIR + "/db;MODE=PostgreSQL;LOCK_TIMEOUT=10000");
        registry.add("dlq.data-dir", () -> DB_DIR);
        registry.add("dlq.retry.max-retries", () -> 0);
        registry.add("dlq.retry.initial-backoff-ms", () -> 1L);
        registry.add("dlq.retry.max-backoff-ms", () -> 1L);
        registry.add("dlq.retention.sweep-interval-ms", () -> 3600000L);
        registry.add("dlq.dispatch.lease-ttl-ms", () -> 300000L);
    }

    @Autowired
    private MockMvc mockMvc;

    @Test
    void fullLifecycleOverHttp() throws Exception {
        MvcResult enqueued = mockMvc.perform(post("/api/queues/http-q/messages")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"payload":"hello","retryPolicy":{"maxRetries":0,"backoffType":"FIXED"}}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").isNotEmpty())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andReturn();
        JsonNode enqueueBody = MAPPER.readTree(enqueued.getResponse().getContentAsString());
        String messageId = enqueueBody.get("id").asText();

        mockMvc.perform(get("/api/queues/http-q/messages/next?timeoutMs=2000"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(messageId))
                .andExpect(jsonPath("$.status").value("IN_FLIGHT"));

        mockMvc.perform(post("/api/queues/messages/" + messageId + "/nack")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"reason\":\"http failure\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ACCEPTED"));

        mockMvc.perform(get("/api/dead-letters?queue=http-q"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(1))
                .andExpect(jsonPath("$.items[0].payload").value("hello"))
                .andExpect(jsonPath("$.items[0].lastReason").value("http failure"))
                .andExpect(jsonPath("$.items[0].attempts[0].result").value("FAILED"));

        long dlId = MAPPER.readTree(mockMvc.perform(get("/api/dead-letters?queue=http-q"))
                        .andReturn().getResponse().getContentAsString())
                .get("items").get(0).get("id").asLong();

        mockMvc.perform(post("/api/dead-letters/" + dlId + "/replay"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("REPLAYING"));

        mockMvc.perform(delete("/api/dead-letters/" + dlId))
                .andExpect(status().isConflict());

        mockMvc.perform(post("/api/queues/messages/" + messageId + "/ack"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCEEDED"));

        mockMvc.perform(get("/api/stats/http-q"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalEnqueued").value(1))
                .andExpect(jsonPath("$.succeeded").value(1))
                .andExpect(jsonPath("$.deadLetter").value(0))
                .andExpect(jsonPath("$.replaySucceeded").value(1));

        mockMvc.perform(get("/api/queues/messages/missing-id"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("NOT_FOUND"));
    }
}
