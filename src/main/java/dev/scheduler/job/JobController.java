package dev.scheduler.job;

import jakarta.validation.Valid;
import java.net.URI;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/jobs")
@ConditionalOnProperty(prefix = "scheduler.api", name = "enabled", matchIfMissing = true)
public class JobController {

    private final JobService service;

    public JobController(JobService service) {
        this.service = service;
    }

    /** 201 for a new job; 200 with the original job when an idempotency key is replayed. */
    @PostMapping
    public ResponseEntity<JobResponse> submit(@Valid @RequestBody SubmitJobRequest request) {
        var submission = service.submit(request);
        JobResponse body = JobResponse.from(submission.job());
        return submission.created()
                ? ResponseEntity.created(URI.create("/api/v1/jobs/" + body.id())).body(body)
                : ResponseEntity.ok(body);
    }

    @GetMapping("/{id}")
    public JobResponse get(@PathVariable UUID id) {
        return JobResponse.from(service.get(id));
    }

    @GetMapping
    public JobPage list(
            @RequestParam(required = false) JobStatus status,
            @RequestParam(required = false) String type,
            @RequestParam(defaultValue = "50") int limit,
            @RequestParam(required = false) String cursor) {
        return service.list(status, type, limit, cursor);
    }

    @PostMapping("/{id}/cancel")
    public JobResponse cancel(@PathVariable UUID id) {
        return JobResponse.from(service.cancel(id));
    }
}
