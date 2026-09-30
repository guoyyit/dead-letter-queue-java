package com.github.guoyyit.deadletterqueuejava.store;

import com.github.guoyyit.deadletterqueuejava.model.PersistedState;

import java.util.function.Function;

/**
 * 状态存储：所有变更在同一把锁内完成并立即落盘。
 */
public interface StateStore {

    /**
     * 启动时加载本机状态。
     */
    void start();

    /**
     * 在锁内执行一次状态变更，返回后变更已持久化。
     */
    <T> T mutate(Function<PersistedState, T> action);

    /**
     * 在锁内读取一致快照。
     */
    <T> T read(Function<PersistedState, T> reader);
}
