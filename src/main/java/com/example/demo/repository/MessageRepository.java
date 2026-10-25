package com.example.demo.repository;

import com.example.demo.model.EventMessage;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class MessageRepository {

    private final JdbcTemplate jdbc;

    public MessageRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Returns 1 if inserted, 0 if a row with this id already existed. */
    public int insert(EventMessage m, String topic, int partition, long offset) {
        int rows = jdbc.update("""
                INSERT INTO consumed_messages
                    (id, event_type, description, payload, created_by, occurred_at,
                     source_topic, source_partition, source_offset)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (id) DO NOTHING
                """,
            m.id(), m.type(), m.description(), m.payload(), m.createdBy(),
            Timestamp.from(m.createdAt()), topic, partition, offset);
        System.out.println("insert() returned " + rows + " for id " + m.id());
        return rows;
    }

    public Optional<ConsumedRow> find(UUID id) {
        List<ConsumedRow> rows = jdbc.query(
            "SELECT * FROM consumed_messages WHERE id = ?", this::map, id);
        return rows.stream().findFirst();
    }

    public List<ConsumedRow> list(int page, int size) {
        return jdbc.query(
            "SELECT * FROM consumed_messages ORDER BY consumed_at DESC, id DESC LIMIT ? OFFSET ?",
            this::map, size, (long) page * size);
    }

    private ConsumedRow map(ResultSet rs, int rowNum) throws SQLException {
        return new ConsumedRow(
            rs.getObject("id", UUID.class),
            rs.getString("event_type"),
            rs.getString("description"),
            rs.getString("payload"),
            rs.getString("created_by"),
            rs.getTimestamp("occurred_at").toInstant(),
            rs.getTimestamp("consumed_at").toInstant());
    }

    public record ConsumedRow(UUID id, String type, String description, String payload,
                              String createdBy, java.time.Instant occurredAt,
                              java.time.Instant consumedAt) {}
}