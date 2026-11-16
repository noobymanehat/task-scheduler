-- Reaper: find ALIVE workers whose heartbeat has expired without scanning the
-- ever-growing history of STOPPED and DEAD worker rows.
CREATE INDEX workers_alive_idx ON workers (last_heartbeat_at) WHERE status = 'ALIVE';
