package com.github.guoyyit.deadletterqueuejava.service;

import com.github.guoyyit.deadletterqueuejava.config.DlqProperties;
import com.github.guoyyit.deadletterqueuejava.domain.BackoffType;
import com.github.guoyyit.deadletterqueuejava.domain.RetryPolicy;
import org.springframework.stereotype.Component;

@Component
public class RetryPolicyResolver {

    private final DlqProperties properties;

    public RetryPolicyResolver(DlqProperties properties) {
        this.properties = properties;
    }

    public RetryPolicy defaultPolicy() {
        DlqProperties.Retry retry = properties.getRetry();
        return new RetryPolicy(
                retry.getMaxRetries(),
                retry.getInitialBackoffMs(),
                retry.getMaxBackoffMs(),
                retry.getBackoffType() == null ? BackoffType.EXPONENTIAL : retry.getBackoffType());
    }

    public RetryPolicy merge(RetryPolicy requested) {
        if (requested == null) {
            return defaultPolicy();
        }
        RetryPolicy defaults = defaultPolicy();
        int maxRetries = requested.maxRetries() >= 0 ? requested.maxRetries() : defaults.maxRetries();
        long initialBackoffMs = requested.initialBackoffMs() > 0
                ? requested.initialBackoffMs() : defaults.initialBackoffMs();
        long maxBackoffMs = requested.maxBackoffMs() > 0
                ? requested.maxBackoffMs() : defaults.maxBackoffMs();
        if (maxBackoffMs < initialBackoffMs) {
            maxBackoffMs = initialBackoffMs;
        }
        BackoffType backoffType = requested.backoffType() == null
                ? defaults.backoffType() : requested.backoffType();
        return new RetryPolicy(maxRetries, initialBackoffMs, maxBackoffMs, backoffType);
    }

    public long backoffMs(RetryPolicy policy, int attempt) {
        if (attempt <= 0) {
            return 0L;
        }
        long delay;
        if (policy.backoffType() == BackoffType.FIXED) {
            delay = policy.initialBackoffMs();
        } else {
            int shifts = Math.min(attempt - 1, 62);
            double factor = Math.scalb(1.0d, shifts);
            delay = (long) Math.min((double) policy.initialBackoffMs() * factor,
                    (double) policy.maxBackoffMs());
        }
        return Math.min(delay, policy.maxBackoffMs());
    }
}
