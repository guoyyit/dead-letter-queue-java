package com.github.guoyyit.deadletterqueuejava.model;

import java.util.ArrayList;
import java.util.List;

/**
 * 本机持久化的完整状态快照。
 */
public class PersistedState {

    private int version = 1;
    private List<QueueRecord> messages = new ArrayList<>();
    private List<DeadLetterRecord> deadLetters = new ArrayList<>();

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public List<QueueRecord> getMessages() {
        return messages;
    }

    public void setMessages(List<QueueRecord> messages) {
        this.messages = messages;
    }

    public List<DeadLetterRecord> getDeadLetters() {
        return deadLetters;
    }

    public void setDeadLetters(List<DeadLetterRecord> deadLetters) {
        this.deadLetters = deadLetters;
    }
}
