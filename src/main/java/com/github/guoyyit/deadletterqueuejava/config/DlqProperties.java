package com.github.guoyyit.deadletterqueuejava.config;

import com.github.guoyyit.deadletterqueuejava.domain.BackoffType;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "dlq")
public class DlqProperties {

    private String dataDir = "./data/dlq";
    private Retry retry = new Retry();
    private Dispatch dispatch = new Dispatch();
    private Retention retention = new Retention();

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public Retry getRetry() {
        return retry;
    }

    public void setRetry(Retry retry) {
        this.retry = retry;
    }

    public Dispatch getDispatch() {
        return dispatch;
    }

    public void setDispatch(Dispatch dispatch) {
        this.dispatch = dispatch;
    }

    public Retention getRetention() {
        return retention;
    }

    public void setRetention(Retention retention) {
        this.retention = retention;
    }

    public static class Retry {
        private int maxRetries = 3;
        private long initialBackoffMs = 1000L;
        private long maxBackoffMs = 60000L;
        private BackoffType backoffType = BackoffType.EXPONENTIAL;

        public int getMaxRetries() {
            return maxRetries;
        }

        public void setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
        }

        public long getInitialBackoffMs() {
            return initialBackoffMs;
        }

        public void setInitialBackoffMs(long initialBackoffMs) {
            this.initialBackoffMs = initialBackoffMs;
        }

        public long getMaxBackoffMs() {
            return maxBackoffMs;
        }

        public void setMaxBackoffMs(long maxBackoffMs) {
            this.maxBackoffMs = maxBackoffMs;
        }

        public BackoffType getBackoffType() {
            return backoffType;
        }

        public void setBackoffType(BackoffType backoffType) {
            this.backoffType = backoffType;
        }
    }

    public static class Dispatch {
        private long leaseTtlMs = 30000L;
        private int pollTimeoutMs = 200;
        private long leaseRecoveryInitialDelayMs = 30000L;

        public long getLeaseTtlMs() {
            return leaseTtlMs;
        }

        public void setLeaseTtlMs(long leaseTtlMs) {
            this.leaseTtlMs = leaseTtlMs;
        }

        public int getPollTimeoutMs() {
            return pollTimeoutMs;
        }

        public void setPollTimeoutMs(int pollTimeoutMs) {
            this.pollTimeoutMs = pollTimeoutMs;
        }

        public long getLeaseRecoveryInitialDelayMs() {
            return leaseRecoveryInitialDelayMs;
        }

        public void setLeaseRecoveryInitialDelayMs(long leaseRecoveryInitialDelayMs) {
            this.leaseRecoveryInitialDelayMs = leaseRecoveryInitialDelayMs;
        }
    }

    public static class Retention {
        private long maxAgeMs = 7L * 24 * 60 * 60 * 1000L;
        private long maxRecords = 10000L;
        private long sweepIntervalMs = 60000L;

        public long getMaxAgeMs() {
            return maxAgeMs;
        }

        public void setMaxAgeMs(long maxAgeMs) {
            this.maxAgeMs = maxAgeMs;
        }

        public long getMaxRecords() {
            return maxRecords;
        }

        public void setMaxRecords(long maxRecords) {
            this.maxRecords = maxRecords;
        }

        public long getSweepIntervalMs() {
            return sweepIntervalMs;
        }

        public void setSweepIntervalMs(long sweepIntervalMs) {
            this.sweepIntervalMs = sweepIntervalMs;
        }
    }
}
