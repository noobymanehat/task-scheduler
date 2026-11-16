package dev.scheduler.schedule;

import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/schedules")
@ConditionalOnProperty(prefix = "scheduler.api", name = "enabled", matchIfMissing = true)
public class ScheduleController {

    private final ScheduleService service;

    public ScheduleController(ScheduleService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<Schedule> create(@Valid @RequestBody CreateScheduleRequest request) {
        Schedule schedule = service.create(request);
        return ResponseEntity.created(URI.create("/api/v1/schedules/" + schedule.id())).body(schedule);
    }

    /** Up to 500 schedules, ordered by name. */
    @GetMapping
    public List<Schedule> list() {
        return service.list();
    }

    @GetMapping("/{id}")
    public Schedule get(@PathVariable UUID id) {
        return service.get(id);
    }

    /** Jobs the schedule already created are kept. */
    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable UUID id) {
        service.delete(id);
    }

    @PostMapping("/{id}/pause")
    public Schedule pause(@PathVariable UUID id) {
        return service.pause(id);
    }

    @PostMapping("/{id}/resume")
    public Schedule resume(@PathVariable UUID id) {
        return service.resume(id);
    }
}
