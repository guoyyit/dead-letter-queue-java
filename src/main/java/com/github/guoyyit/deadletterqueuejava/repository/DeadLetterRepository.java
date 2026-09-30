package com.github.guoyyit.deadletterqueuejava.repository;

import com.github.guoyyit.deadletterqueuejava.domain.DeadLetterRecord;
import com.github.guoyyit.deadletterqueuejava.domain.DeadLetterReplay;
import com.github.guoyyit.deadletterqueuejava.domain.DeadLetterStatus;
import com.github.guoyyit.deadletterqueuejava.domain.ReplayOutcome;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Repository
public class DeadLetterRepository {

    private static final RowMapper<DeadLetterRecord> DEAD_LETTER_ROW_MAPPER = (rs, rowNum) ->
            new DeadLetterRecord(
                    rs.getLong("id"),
                    rs.getString("message_id"),
                    rs.getString("queue"),
                    rs.getString("payload"),
                    rs.getString("last_reason"),
                    rs.getLong("first_died_at"),
                    rs.getLong("last_died_at"),
                    rs.getInt("death_count"),
                    rs.getInt("replay_count"),
                    DeadLetterStatus.valueOf(rs.getString("status")),
                    List.of());

    private static final RowMapper<DeadLetterReplay> REPLAY_ROW_MAPPER = (rs, rowNum) ->
            new DeadLetterReplay(
                    rs.getLong("id"),
                    rs.getLong("dead_letter_id"),
                    rs.getString("message_id"),
                    ReplayOutcome.valueOf(rs.getString("outcome")),
                    rs.getLong("started_at"),
                    rs.getObject("finished_at") == null ? 0L : rs.getLong("finished_at"),
                    rs.getString("error_reason"));

    private final JdbcTemplate jdbcTemplate;

    public DeadLetterRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public DeadLetterRecord lockByMessageId(String messageId) {
        List<DeadLetterRecord> results = jdbcTemplate.query(
                "SELECT * FROM dead_letters WHERE message_id = ? FOR UPDATE",
                DEAD_LETTER_ROW_MAPPER, messageId);
        return results.isEmpty() ? null : results.get(0);
    }

    public DeadLetterRecord findById(long id) {
        List<DeadLetterRecord> results = jdbcTemplate.query(
                "SELECT * FROM dead_letters WHERE id = ?", DEAD_LETTER_ROW_MAPPER, id);
        return results.isEmpty() ? null : results.get(0);
    }

    public DeadLetterRecord lockById(long id) {
        List<DeadLetterRecord> results = jdbcTemplate.query(
                "SELECT * FROM dead_letters WHERE id = ? FOR UPDATE",
                DEAD_LETTER_ROW_MAPPER, id);
        return results.isEmpty() ? null : results.get(0);
    }

    public DeadLetterRecord insertForNewDeath(String messageId, String queue, String payload,
                                              String reason, long now) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO dead_letters (message_id, queue, payload, last_reason, "
                            + "first_died_at, last_died_at, death_count, replay_count, status) "
                            + "VALUES (?, ?, ?, ?, ?, ?, 1, 0, 'ACTIVE')",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, messageId);
            ps.setString(2, queue);
            ps.setString(3, payload);
            ps.setString(4, reason);
            ps.setLong(5, now);
            ps.setLong(6, now);
            return ps;
        }, keyHolder);
        Number key = keyHolder.getKey();
        return key == null ? null : findById(key.longValue());
    }

    public DeadLetterRecord recordRepeatDeath(long id, String reason, long now) {
        jdbcTemplate.update(
                "UPDATE dead_letters SET last_reason = ?, last_died_at = ?, "
                        + "death_count = death_count + 1, status = 'ACTIVE' WHERE id = ?",
                reason, now, id);
        return findById(id);
    }

    public int claimForReplay(long id) {
        return jdbcTemplate.update(
                "UPDATE dead_letters SET status = 'REPLAYING' WHERE id = ? AND status = 'ACTIVE'",
                id);
    }

    public void markResolved(long id) {
        jdbcTemplate.update(
                "UPDATE dead_letters SET status = 'RESOLVED', replay_count = replay_count + 1 "
                        + "WHERE id = ?", id);
    }

    public void deleteById(long id) {
        jdbcTemplate.update("DELETE FROM dead_letters WHERE id = ?", id);
    }

    public void deleteAttempts(String messageId) {
        jdbcTemplate.update("DELETE FROM delivery_attempts WHERE message_id = ?", messageId);
    }

    public int deleteByIdIfNotReplaying(long id) {
        return jdbcTemplate.update(
                "DELETE FROM dead_letters WHERE id = ? AND status <> 'REPLAYING'", id);
    }

    public List<DeadLetterRecord> search(String queue, DeadLetterStatus status, String reasonContains,
                                         Long fromMs, Long toMs, int page, int size) {
        StringBuilder sql = new StringBuilder("SELECT * FROM dead_letters WHERE 1 = 1");
        List<Object> args = new ArrayList<>();
        appendFilters(sql, args, queue, status, reasonContains, fromMs, toMs);
        sql.append(" ORDER BY first_died_at ASC, id ASC LIMIT ? OFFSET ?");
        args.add(size);
        args.add((long) page * size);
        return jdbcTemplate.query(sql.toString(), DEAD_LETTER_ROW_MAPPER, args.toArray());
    }

    public long countSearch(String queue, DeadLetterStatus status, String reasonContains,
                            Long fromMs, Long toMs) {
        StringBuilder sql = new StringBuilder("SELECT COUNT(*) FROM dead_letters WHERE 1 = 1");
        List<Object> args = new ArrayList<>();
        appendFilters(sql, args, queue, status, reasonContains, fromMs, toMs);
        Long count = jdbcTemplate.queryForObject(sql.toString(), Long.class, args.toArray());
        return count == null ? 0L : count;
    }

    public long countActive(String queue) {
        Long count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM dead_letters WHERE queue = ? AND status = 'ACTIVE'",
                Long.class, queue);
        return count == null ? 0L : count;
    }

    public long countReplaying(String queue) {
        Long count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM dead_letters WHERE queue = ? AND status = 'REPLAYING'",
                Long.class, queue);
        return count == null ? 0L : count;
    }

    public DeadLetterReplay openReplay(long deadLetterId, String messageId, long now) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO dead_letter_replays "
                            + "(dead_letter_id, message_id, outcome, started_at) "
                            + "VALUES (?, ?, 'IN_PROGRESS', ?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setLong(1, deadLetterId);
            ps.setString(2, messageId);
            ps.setLong(3, now);
            return ps;
        }, keyHolder);
        Number key = keyHolder.getKey();
        if (key == null) {
            return null;
        }
        return findReplayById(key.longValue());
    }

    public void closeReplay(long replayId, ReplayOutcome outcome, String reason, long now) {
        jdbcTemplate.update(
                "UPDATE dead_letter_replays SET outcome = ?, finished_at = ?, error_reason = ? WHERE id = ?",
                outcome.name(), now, reason, replayId);
    }

    public DeadLetterReplay findOpenReplayByMessageId(String messageId) {
        List<DeadLetterReplay> results = jdbcTemplate.query(
                "SELECT * FROM dead_letter_replays WHERE message_id = ? AND outcome = 'IN_PROGRESS' "
                        + "ORDER BY id DESC LIMIT 1",
                REPLAY_ROW_MAPPER, messageId);
        return results.isEmpty() ? null : results.get(0);
    }

    public int deleteReplaysByMessage(String messageId) {
        return jdbcTemplate.update(
                "DELETE FROM dead_letter_replays WHERE message_id = ?", messageId);
    }

    public long countReplayOutcome(String queue, ReplayOutcome outcome) {
        Long count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM dead_letter_replays r JOIN dead_letters d "
                        + "ON r.dead_letter_id = d.id WHERE d.queue = ? AND r.outcome = ?",
                Long.class, queue, outcome.name());
        return count == null ? 0L : count;
    }

    public List<DeadLetterRecord> findExpiredByAge(long cutoff, int limit) {
        return jdbcTemplate.query(
                "SELECT * FROM dead_letters WHERE status <> 'REPLAYING' AND first_died_at < ? "
                        + "ORDER BY first_died_at ASC, id ASC LIMIT ?",
                DEAD_LETTER_ROW_MAPPER, cutoff, limit);
    }

    public List<DeadLetterRecord> findOldestActive(int limit) {
        return jdbcTemplate.query(
                "SELECT * FROM dead_letters WHERE status = 'ACTIVE' "
                        + "ORDER BY first_died_at ASC, id ASC LIMIT ?",
                DEAD_LETTER_ROW_MAPPER, limit);
    }

    public DeadLetterReplay findReplayById(long replayId) {
        List<DeadLetterReplay> results = jdbcTemplate.query(
                "SELECT * FROM dead_letter_replays WHERE id = ?", REPLAY_ROW_MAPPER, replayId);
        return results.isEmpty() ? null : results.get(0);
    }

    public List<DeadLetterRecord> searchAllClearable(String queue, int limit) {
        if (queue == null || queue.isBlank()) {
            return jdbcTemplate.query(
                    "SELECT * FROM dead_letters WHERE status <> 'REPLAYING' "
                            + "ORDER BY first_died_at ASC, id ASC LIMIT ?",
                    DEAD_LETTER_ROW_MAPPER, limit);
        }
        return jdbcTemplate.query(
                "SELECT * FROM dead_letters WHERE queue = ? AND status <> 'REPLAYING' "
                        + "ORDER BY first_died_at ASC, id ASC LIMIT ?",
                DEAD_LETTER_ROW_MAPPER, queue, limit);
    }

    private void appendFilters(StringBuilder sql, List<Object> args, String queue,
                               DeadLetterStatus status, String reasonContains,
                               Long fromMs, Long toMs) {
        if (queue != null && !queue.isBlank()) {
            sql.append(" AND queue = ?");
            args.add(queue);
        }
        if (status != null) {
            sql.append(" AND status = ?");
            args.add(status.name());
        }
        if (reasonContains != null && !reasonContains.isBlank()) {
            sql.append(" AND last_reason LIKE ?");
            args.add("%" + reasonContains + "%");
        }
        if (fromMs != null) {
            sql.append(" AND last_died_at >= ?");
            args.add(fromMs);
        }
        if (toMs != null) {
            sql.append(" AND last_died_at <= ?");
            args.add(toMs);
        }
    }
}
