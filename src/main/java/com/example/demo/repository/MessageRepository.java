package com.example.demo.repository;
import com.example.demo.model.EventMessage;

import java.sql.ResultSet;

import java.sql.SQLException;


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

        int rows= jdbc.update("""

                INSERT INTO consumed_messages

                    (id, event_type, payload, source_topic, source_partition, source_offset)

                VALUES (?, ?, ?, ?, ?, ?)

                ON CONFLICT (id) DO NOTHING

                """,

            m.id(), m.type(), m.payload(),

             topic, partition, offset);
             System.out.println("Inserted message with id: " + m.id() + " into consumed_messages table.");
        
return rows;


            
      

    }

 

    public Optional<ConsumedRow> find(UUID id) {

        List<ConsumedRow> rows = jdbc.query(

            "SELECT * FROM consumed_messages WHERE id = ?", this::map, id);

        return rows.stream().findFirst();

    }

 

    private ConsumedRow map(ResultSet rs, int rowNum) throws SQLException {

        return new ConsumedRow(

            rs.getObject("id", UUID.class),

            rs.getString("event_type"),

            rs.getString("payload"),

            rs.getTimestamp("occurred_at").toInstant(),

            rs.getTimestamp("consumed_at").toInstant());

    }

 

    public record ConsumedRow(UUID id, String type, String payload,

                              java.time.Instant occurredAt, java.time.Instant consumedAt) {}


    public List<ConsumedRow> list(int page,int size) {


        return jdbc.query("SELECT * FROM consumed_messages ORDER BY consumed_at DESC LIMIT ? OFFSET ?", this::map, size, page * size);

    }
}