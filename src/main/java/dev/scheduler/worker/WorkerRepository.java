package dev.scheduler.worker;

import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class WorkerRepository {

    private final JdbcClient jdbc;

    public WorkerRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void register(UUID id, String hostname) {
        jdbc.sql("INSERT INTO workers (id, hostname) VALUES (:id, :hostname)")
                .param("id", id)
                .param("hostname", hostname)
                .update();
    }

    /** ALIVE -> STOPPED. */
    public boolean markStopped(UUID id) {
        return jdbc.sql("UPDATE workers SET status = 'STOPPED' WHERE id = :id AND status = 'ALIVE'")
                        .param("id", id)
                        .update()
                == 1;
    }
}
