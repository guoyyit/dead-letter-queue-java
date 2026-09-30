package com.github.guoyyit.deadletterqueuejava.web;

import com.github.guoyyit.deadletterqueuejava.model.DeadLetterRecord;
import com.github.guoyyit.deadletterqueuejava.model.DeadLetterStatus;
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
import java.util.NoSuchElementException;

/**
 * 死信区查看、重放与清除接口。
 */
@RestController
@RequestMapping("/api/dead-letters")
public class DeadLetterController {

    private final DeadLetterService deadLetterService;

    public DeadLetterController(DeadLetterService deadLetterService) {
        this.deadLetterService = deadLetterService;
    }

    @GetMapping
    public List<DeadLetterRecord> list(@RequestParam(required = false) String queue,
                                       @RequestParam(required = false) DeadLetterStatus status,
                                       @RequestParam(defaultValue = "100") int limit) {
        return deadLetterService.list(queue, status, limit);
    }

    @GetMapping("/{id}")
    public DeadLetterRecord get(@PathVariable String id) {
        DeadLetterRecord record = deadLetterService.get(id);
        if (record == null) {
            throw new NoSuchElementException("死信记录不存在: " + id);
        }
        return record;
    }

    @PostMapping("/replay")
    public OperationResponse replay(@RequestBody IdsRequest request) {
        if (request == null || request.ids() == null || request.ids().isEmpty()) {
            throw new IllegalArgumentException("ids 不能为空");
        }
        return new OperationResponse(deadLetterService.replay(request.ids()));
    }

    @PostMapping("/{id}/replay")
    public OperationResponse replayOne(@PathVariable String id) {
        return new OperationResponse(deadLetterService.replay(List.of(id)));
    }

    @DeleteMapping
    public OperationResponse clear(@RequestBody IdsRequest request) {
        if (request == null || request.ids() == null || request.ids().isEmpty()) {
            throw new IllegalArgumentException("ids 不能为空");
        }
        return new OperationResponse(deadLetterService.clear(request.ids()));
    }

    @DeleteMapping("/{id}")
    public OperationResponse clearOne(@PathVariable String id) {
        return new OperationResponse(deadLetterService.clear(List.of(id)));
    }

    public record IdsRequest(List<String> ids) {
    }

    public record OperationResponse(int affected) {
    }
}
