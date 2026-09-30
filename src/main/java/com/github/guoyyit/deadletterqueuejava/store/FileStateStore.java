package com.github.guoyyit.deadletterqueuejava.store;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.guoyyit.deadletterqueuejava.config.DlqProperties;
import com.github.guoyyit.deadletterqueuejava.model.PersistedState;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

/**
 * 基于本地 JSON 文件（临时文件 + 原子移动）的状态存储。
 */
public class FileStateStore implements StateStore {

    private static final String STATE_FILE_NAME = "dlq-state.json";

    private final DlqProperties properties;
    private final ObjectMapper objectMapper;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Path stateFile;
    private PersistedState state;

    public FileStateStore(DlqProperties properties) {
        this.properties = properties;
        this.objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public void start() {
        Path dir = Path.of(properties.getStoreDir());
        try {
            Files.createDirectories(dir);
            stateFile = dir.resolve(STATE_FILE_NAME);
            if (Files.exists(stateFile) && Files.size(stateFile) > 0) {
                state = objectMapper.readValue(stateFile.toFile(), PersistedState.class);
            } else {
                state = new PersistedState();
                persist();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("加载死信队列状态失败: " + stateFile, e);
        }
    }

    @Override
    public <T> T mutate(Function<PersistedState, T> action) {
        lock.writeLock().lock();
        try {
            T result = action.apply(state);
            persist();
            return result;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public <T> T read(Function<PersistedState, T> reader) {
        lock.readLock().lock();
        try {
            return reader.apply(state);
        } finally {
            lock.readLock().unlock();
        }
    }

    private void persist() {
        try {
            Path tmp = stateFile.resolveSibling(STATE_FILE_NAME + ".tmp");
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), state);
            try {
                Files.move(tmp, stateFile,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicMoveFailed) {
                Files.copy(tmp, stateFile,
                        StandardCopyOption.REPLACE_EXISTING);
                Files.deleteIfExists(tmp);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("持久化死信队列状态失败: " + stateFile, e);
        }
    }
}
