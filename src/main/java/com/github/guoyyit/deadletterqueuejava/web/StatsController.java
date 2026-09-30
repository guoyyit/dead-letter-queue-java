package com.github.guoyyit.deadletterqueuejava.web;

import com.github.guoyyit.deadletterqueuejava.domain.QueueStats;
import com.github.guoyyit.deadletterqueuejava.service.StatsService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/stats")
public class StatsController {

    private final StatsService statsService;

    public StatsController(StatsService statsService) {
        this.statsService = statsService;
    }

    @GetMapping("/{queue}")
    public QueueStats stats(@PathVariable String queue) {
        return statsService.stats(queue);
    }
}
