package com.github.guoyyit.deadletterqueuejava.web;

import com.github.guoyyit.deadletterqueuejava.domain.DeadLetterRecord;
import com.github.guoyyit.deadletterqueuejava.domain.DeadLetterStatus;
import com.github.guoyyit.deadletterqueuejava.domain.PagedResult;
import com.github.guoyyit.deadletterqueuejava.service.DeadLetterService;
import org.springframework.web.bind.annotation.DeleteMapping;
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
@RequestMapping("/api/dead-letters")
public class DeadLetterController {

    private final DeadLetterService deadLetterService;

    public DeadLetterController(DeadLetterService deadLetterService) {
        this.deadLetterService = deadLetterService;
    }

    public record BatchIdsRequest(List<Long> ids) {
    }

    @GetMapping
    public PagedResult<DeadLetterRecord> list(
            @RequestParam(required = false) String queue,
            @RequestParam(required = false) DeadLetterStatus status,
            @RequestParam(required = false) String reasonContains,
            @RequestParam(required = false) Long fromMs,
            @RequestParam(required = false) Long toMs,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "50") int size) {
        return deadLetterService.list(queue, status, reasonContains, fromMs, toMs, page, size);
    }

    @GetMapping("/{id}")
    public DeadLetterRecord get(@PathVariable long id) {
        return deadLetterService.get(id);
    }

    @PostMapping("/{id}/replay")
    public Map<String, Object> replayOne(@PathVariable long id) {
        long replayId = deadLetterService.replayOne(id);
        return Map.of("deadLetterId", id, "replayId", replayId, "status", "REPLAYING");
    }

    @PostMapping("/replay")
    public Map<String, Object> replayBatch(@RequestBody BatchIdsRequest request) {
        List<Long> replayed = deadLetterService.replayBatch(request == null ? null : request.ids());
        return Map.of("replayed", replayed, "replayedCount", replayed.size());
    }

    @DeleteMapping("/{id}")
    public Map<String, Object> clearOne(@PathVariable long id) {
        deadLetterService.clearOne(id);
        return Map.of("cleared", List.of(id));
    }

    @PostMapping("/clear")
    public Map<String, Object> clearBatch(@RequestBody BatchIdsRequest request) {
        int cleared = deadLetterService.clearBatch(request == null ? null : request.ids());
        return Map.of("clearedCount", cleared);
    }

    @DeleteMapping
    public Map<String, Object> clearAll(@RequestParam(required = false) String queue) {
        int cleared = deadLetterService.clearAll(queue);
        return Map.of("clearedCount", cleared);
    }
}
