package dev.scheduler.worker;

import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/workers")
@ConditionalOnProperty(prefix = "scheduler.api", name = "enabled", matchIfMissing = true)
public class WorkerController {

    private static final int MAX_WORKERS = 200;

    public enum Status {
        ALIVE,
        STOPPED,
        DEAD
    }

    private final WorkerRepository workers;

    public WorkerController(WorkerRepository workers) {
        this.workers = workers;
    }

    /** The 200 most recently started workers, optionally only those in one status. */
    @GetMapping
    public List<WorkerInfo> list(@RequestParam(required = false) Status status) {
        return workers.list(status == null ? null : status.name(), MAX_WORKERS);
    }
}
