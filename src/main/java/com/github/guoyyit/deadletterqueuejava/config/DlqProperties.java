package com.github.guoyyit.deadletterqueuejava.config;

import com.github.guoyyit.deadletterqueuejava.model.BackoffType;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 死信队列配置项，前缀 {@code dlq}。
 */
@ConfigurationProperties(prefix = "dlq")
public class DlqProperties {

    private String storeDir = "data";
    private long visibilityTimeoutMillis = 30_000L;
    private long maintenanceIntervalMillis = 1_000L;
    private long retentionMillis = 0L;
    private int maxDeadRecords = 0;
    private int defaultMaxRetries = 3;
    private long defaultInitialIntervalMillis = 1_000L;
    private long defaultMaxIntervalMillis = 60_000L;
    private BackoffType defaultBackoffType = BackoffType.EXPONENTIAL;

    public String getStoreDir() {
        return storeDir;
    }

    public void setStoreDir(String storeDir) {
        this.storeDir = storeDir;
    }

    public long getVisibilityTimeoutMillis() {
        return visibilityTimeoutMillis;
    }

    public void setVisibilityTimeoutMillis(long visibilityTimeoutMillis) {
        this.visibilityTimeoutMillis = visibilityTimeoutMillis;
    }

    public long getMaintenanceIntervalMillis() {
        return maintenanceIntervalMillis;
    }

    public void setMaintenanceIntervalMillis(long maintenanceIntervalMillis) {
        this.maintenanceIntervalMillis = maintenanceIntervalMillis;
    }

    public long getRetentionMillis() {
        return retentionMillis;
    }

    public void setRetentionMillis(long retentionMillis) {
        this.retentionMillis = retentionMillis;
    }

    public int getMaxDeadRecords() {
        return maxDeadRecords;
    }

    public void setMaxDeadRecords(int maxDeadRecords) {
        this.maxDeadRecords = maxDeadRecords;
    }

    public int getDefaultMaxRetries() {
        return defaultMaxRetries;
    }

    public void setDefaultMaxRetries(int defaultMaxRetries) {
        this.defaultMaxRetries = defaultMaxRetries;
    }

    public long getDefaultInitialIntervalMillis() {
        return defaultInitialIntervalMillis;
    }

    public void setDefaultInitialIntervalMillis(long defaultInitialIntervalMillis) {
        this.defaultInitialIntervalMillis = defaultInitialIntervalMillis;
    }

    public long getDefaultMaxIntervalMillis() {
        return defaultMaxIntervalMillis;
    }

    public void setDefaultMaxIntervalMillis(long defaultMaxIntervalMillis) {
        this.defaultMaxIntervalMillis = defaultMaxIntervalMillis;
    }

    public BackoffType getDefaultBackoffType() {
        return defaultBackoffType;
    }

    public void setDefaultBackoffType(BackoffType defaultBackoffType) {
        this.defaultBackoffType = defaultBackoffType;
    }
}
