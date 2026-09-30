package com.github.guoyyit.deadletterqueuejava.repository;

import com.github.guoyyit.deadletterqueuejava.domain.BackoffType;
import com.github.guoyyit.deadletterqueuejava.domain.DeliveryAttempt;
import com.github.guoyyit.deadletterqueuejava.domain.Message;
import com.github.guoyyit.deadletterqueuejava.domain.MessageStatus;
import com.github.guoyyit.deadletterqueuejava.domain.RetryPolicy;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Repository
public class MessageRepository {

    private static final String SELECT_COLUMNS =
            "id, queue, payload, status, attempts, created_at, visible_at, leased_until, last_error, "
                    + "policy_max_retries, policy_initial_backoff, policy_max_backoff, policy_backoff_type";

    private static final RowMapper<Message> MESSAGE_ROW_MAPPER = (rs, rowNum) -> {
        RetryPolicy policy = new RetryPolicy(
                rs.getInt("policy_max_retries"),
                rs.getLong("policy_initial_backoff"),
                rs.getLong("policy_max_backoff"),
                BackoffType.valueOf(rs.getString("policy_backoff_type")));
        return new Message(
                rs.getString("id"),
                rs.getString("queue"),
                rs.getString("payload"),
                MessageStatus.valueOf(rs.getString("status")),
                rs.getInt("attempts"),
                rs.getLong("created_at"),
                rs.getLong("visible_at"),
                rs.getLong("leased_until"),
                rs.getString("last_error"),
                policy);
    };

    private static final RowMapper<DeliveryAttempt> ATTEMPT_ROW_MAPPER = (rs, rowNum) ->
            new DeliveryAttempt(
                    rs.getLong("id"),
                    rs.getString("message_id"),
                    rs.getInt("attempt_no"),
                    rs.getString("result"),
                    rs.getString("error_reason"),
                    rs.getLong("started_at"),
                    rs.getLong("finished_at"));

    private final JdbcTemplate jdbcTemplate;

    public MessageRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(Message message) {
        jdbcTemplate.update("""
                        INSERT INTO messages (id, queue, payload, status, attempts, created_at, visible_at,
                                              leased_until, last_error, policy_max_retries,
                                              policy_initial_backoff, policy_max_backoff, policy_backoff_type)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                message.id(), message.queue(), message.payload(), message.status().name(),
                message.attempts(), message.createdAt(), message.visibleAt(), message.leasedUntil(),
                message.lastError(), message.retryPolicy().maxRetries(),
                message.retryPolicy().initialBackoffMs(), message.retryPolicy().maxBackoffMs(),
                message.retryPolicy().backoffType().name());
    }

    public Message findById(String id) {
        List<Message> results = jdbcTemplate.query(
                "SELECT " + SELECT_COLUMNS + " FROM messages WHERE id = ?", MESSAGE_ROW_MAPPER, id);
        return results.isEmpty() ? null : results.get(0);
    }

    public Message lockById(String id) {
        List<Message> results = jdbcTemplate.query(
                "SELECT " + SELECT_COLUMNS + " FROM messages WHERE id = ? FOR UPDATE",
                MESSAGE_ROW_MAPPER, id);
        return results.isEmpty() ? null : results.get(0);
    }

    public void markInFlight(String id, long now, long leaseUntil) {
        jdbcTemplate.update(
                "UPDATE messages SET status = 'IN_FLIGHT', leased_until = ?, visible_at = ? WHERE id = ?",
                leaseUntil, now, id);
    }

    public void markPending(String id, int attempts, long visibleAt, String lastError) {
        jdbcTemplate.update(
                "UPDATE messages SET status = 'PENDING', attempts = ?, visible_at = ?, "
                        + "leased_until = 0, last_error = ? WHERE id = ?",
                attempts, visibleAt, lastError, id);
    }

    public void resetLease(String id, long visibleAt, String lastError) {
        jdbcTemplate.update(
                "UPDATE messages SET status = 'PENDING', visible_at = ?, "
                        + "leased_until = 0, last_error = COALESCE(?, last_error) WHERE id = ?",
                visibleAt, lastError, id);
    }

    public void markSucceeded(String id, int attempts, long now) {
        jdbcTemplate.update(
                "UPDATE messages SET status = 'SUCCEEDED', attempts = ?, leased_until = 0 WHERE id = ?",
                attempts, id);
    }

    public void markDead(String id, int attempts, String lastError, long now) {
        jdbcTemplate.update(
                "UPDATE messages SET status = 'DEAD', attempts = ?, last_error = ?, "
                        + "leased_until = 0, visible_at = ? WHERE id = ?",
                attempts, lastError, now, id);
    }

    public void requeueForReplay(String id, long now, long leaseUntil) {
        jdbcTemplate.update(
                "UPDATE messages SET status = 'IN_FLIGHT', attempts = 1, "
                        + "visible_at = ?, leased_until = ?, last_error = NULL WHERE id = ?",
                now, leaseUntil, id);
    }

    public int deleteByIdIfDead(String id) {
        return jdbcTemplate.update(
                "DELETE FROM messages WHERE id = ? AND status = 'DEAD'", id);
    }

    @Transactional
    public Message fetchForDispatch(String queue, long now) {
        List<String> candidateIds = jdbcTemplate.queryForList(
                "SELECT id FROM messages WHERE queue = ? AND status = 'PENDING' AND visible_at <= ? "
                        + "ORDER BY visible_at ASC, created_at ASC LIMIT 16 FOR UPDATE",
                String.class, queue, now);
        for (String candidateId : candidateIds) {
            Message locked = lockById(candidateId);
            if (locked != null
                    && locked.status() == MessageStatus.PENDING
                    && locked.visibleAt() <= now) {
                return locked;
            }
        }
        return null;
    }

    public List<Message> findExpiredLeases(long now) {
        return jdbcTemplate.query(
                "SELECT " + SELECT_COLUMNS + " FROM messages "
                        + "WHERE status = 'IN_FLIGHT' AND leased_until > 0 AND leased_until <= ?",
                MESSAGE_ROW_MAPPER, now);
    }

    public long countByStatus(String queue, String status) {
        Long count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM messages WHERE queue = ? AND status = ?",
                Long.class, queue, status);
        return count == null ? 0L : count;
    }

    public long countPendingRetry(String queue, long now) {
        Long waiting = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM messages WHERE queue = ? AND status = 'PENDING' AND visible_at > ?",
                Long.class, queue, now);
        Long ready = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM messages WHERE queue = ? AND status = 'PENDING' AND visible_at <= ?",
                Long.class, queue, now);
        return (waiting == null ? 0L : waiting) + (ready == null ? 0L : ready);
    }

    public long countTotalEnqueued(String queue) {
        Long count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM messages WHERE queue = ?", Long.class, queue);
        return count == null ? 0L : count;
    }

    public void insertAttempt(DeliveryAttempt attempt) {
        jdbcTemplate.update("""
                        INSERT INTO delivery_attempts
                            (message_id, attempt_no, result, error_reason, started_at, finished_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                attempt.messageId(), attempt.attemptNo(), attempt.result(),
                attempt.errorReason(), attempt.startedAt(), attempt.finishedAt());
    }

    public List<DeliveryAttempt> findAttempts(String messageId) {
        return jdbcTemplate.query(
                "SELECT id, message_id, attempt_no, result, error_reason, started_at, finished_at "
                        + "FROM delivery_attempts WHERE message_id = ? ORDER BY attempt_no ASC, id ASC",
                ATTEMPT_ROW_MAPPER, messageId);
    }
}
