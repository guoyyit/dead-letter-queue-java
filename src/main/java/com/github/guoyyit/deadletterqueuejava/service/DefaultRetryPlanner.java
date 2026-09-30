package com.github.guoyyit.deadletterqueuejava.service;

import com.github.guoyyit.deadletterqueuejava.config.DlqProperties;
import com.github.guoyyit.deadletterqueuejava.model.BackoffType;
import com.github.guoyyit.deadletterqueuejava.model.RetryStrategy;

/**
 * 默认重试策略：FIXED / LINEAR / EXPONENTIAL 三种退避，均受间隔上限约束。
 */
public class DefaultRetryPlanner implements RetryPlanner {

    private final DlqProperties properties;

    public DefaultRetryPlanner(DlqProperties properties) {
        this.properties = properties;
    }

    @Override
    public RetryStrategy resolve(RetryStrategy requested) {
        if (requested == null) {
            return new RetryStrategy(
                    properties.getDefaultMaxRetries(),
                    properties.getDefaultInitialIntervalMillis(),
                    properties.getDefaultMaxIntervalMillis(),
                    properties.getDefaultBackoffType());
        }
        BackoffType type = requested.backoffType() == null
                ? properties.getDefaultBackoffType()
                : requested.backoffType();
        int maxRetries = Math.max(0, requested.maxRetries());
        long initial = requested.initialIntervalMillis() > 0
                ? requested.initialIntervalMillis()
                : properties.getDefaultInitialIntervalMillis();
        long maxInterval = requested.maxIntervalMillis() > 0
                ? requested.maxIntervalMillis()
                : properties.getDefaultMaxIntervalMillis();
        return new RetryStrategy(maxRetries, initial, maxInterval, type);
    }

    @Override
    public long backoffMillis(RetryStrategy strategy, int retryAttempt) {
        long initial = Math.max(1L, strategy.initialIntervalMillis());
        long max = Math.max(initial, strategy.maxIntervalMillis());
        long raw;
        switch (strategy.backoffType()) {
            case FIXED -> raw = initial;
            case LINEAR -> raw = initial * (long) Math.max(1, retryAttempt);
            case EXPONENTIAL -> {
                int exponent = Math.max(0, retryAttempt - 1);
                if (exponent >= 63) {
                    raw = Long.MAX_VALUE;
                } else {
                    raw = initial * (1L << exponent);
                    if (raw < 0) {
                        raw = Long.MAX_VALUE;
                    }
                }
            }
            default -> raw = initial;
        }
        return Math.min(max, raw);
    }

    @Override
    public boolean isNonRetryable(String reason) {
        return reason != null && reason.startsWith(NON_RETRYABLE_PREFIX);
    }
}
