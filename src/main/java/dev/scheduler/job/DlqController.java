package dev.scheduler.job;

import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** The dead-letter queue: jobs whose retry budget ran out or that failed non-retryably. */
@RestController
@RequestMapping("/api/v1/dlq")
@ConditionalOnProperty(prefix = "scheduler.api", name = "enabled", matchIfMissing = true)
public class DlqController {

    private final JobService service;

    public DlqController(JobService service) {
        this.service = service;
    }

    /** Dead jobs, most recently dead first. */
    @GetMapping
    public JobPage list(
            @RequestParam(required = false) String type,
            @RequestParam(defaultValue = "50") int limit,
            @RequestParam(required = false) String cursor) {
        return service.listDead(type, limit, cursor);
    }

    /** Puts a dead job back in the queue with a fresh retry budget. */
    @PostMapping("/{id}/requeue")
    public JobResponse requeue(@PathVariable UUID id) {
        return JobResponse.from(service.requeue(id));
    }
}
