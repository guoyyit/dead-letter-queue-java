package com.github.guoyyit.deadletterqueuejava.config;

import com.github.guoyyit.deadletterqueuejava.service.DeadLetterService;
import com.github.guoyyit.deadletterqueuejava.service.DeadLetterServiceImpl;
import com.github.guoyyit.deadletterqueuejava.service.DefaultRetryPlanner;
import com.github.guoyyit.deadletterqueuejava.service.MaintenanceTasks;
import com.github.guoyyit.deadletterqueuejava.service.QueueService;
import com.github.guoyyit.deadletterqueuejava.service.QueueServiceImpl;
import com.github.guoyyit.deadletterqueuejava.service.RetryPlanner;
import com.github.guoyyit.deadletterqueuejava.service.StatsService;
import com.github.guoyyit.deadletterqueuejava.service.StatsServiceImpl;
import com.github.guoyyit.deadletterqueuejava.model.RetryStrategy;
import com.github.guoyyit.deadletterqueuejava.store.FileStateStore;
import com.github.guoyyit.deadletterqueuejava.store.StateStore;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 死信队列组件装配。
 */
@Configuration
@EnableScheduling
@EnableConfigurationProperties(DlqProperties.class)
public class DlqConfiguration {

    @Bean
    public StateStore stateStore(DlqProperties properties) {
        return new FileStateStore(properties);
    }

    @Bean
    public RetryPlanner retryPlanner(DlqProperties properties) {
        return new DefaultRetryPlanner(properties);
    }

    @Bean
    public QueueService queueService(StateStore store, RetryPlanner planner, DlqProperties properties) {
        return new QueueServiceImpl(store, planner, properties);
    }

    @Bean
    public DeadLetterService deadLetterService(StateStore store, RetryPlanner planner,
                                               DlqProperties properties) {
        RetryStrategy defaultStrategy = planner.resolve(null);
        return new DeadLetterServiceImpl(store, properties, defaultStrategy);
    }

    @Bean
    public StatsService statsService(StateStore store) {
        return new StatsServiceImpl(store);
    }

    @Bean
    public MaintenanceTasks maintenanceTasks(QueueService queueService,
                                             DeadLetterService deadLetterService,
                                             DlqProperties properties) {
        return new MaintenanceTasks(queueService, deadLetterService, properties);
    }

    @Bean
    public ApplicationRunner stateStoreBootstrap(StateStore stateStore) {
        return args -> stateStore.start();
    }
}
