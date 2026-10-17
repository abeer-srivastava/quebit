-- Transactional outbox marker.
--
-- A payment row doubles as its own outbox entry: "published_at IS NULL" means
-- the payment has been persisted but its processing task has not been handed to
-- the queue yet. Because the marker is written by the same INSERT that creates
-- the payment, there is no window in which a payment exists without a pending
-- publish: the relay always finds it, even across a crash between the insert
-- and the enqueue.
--
-- The relay publishes unpublished rows and sets published_at = now(); the
-- sweeper and the reaper reset it to NULL to request a re-publish.
--
-- A marker column is used instead of a separate outbox table because the
-- message is exactly 1:1 with the payment row — a second table would add a
-- foreign key, a backfill, and a join without changing the guarantee.
ALTER TABLE payments ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ;

-- Rows that predate the relay were already handed to the queue by the old
-- code path. Mark them published so the relay does not republish the whole
-- table; the sweeper resets any that turn out to be genuinely stuck.
UPDATE payments SET published_at = now() WHERE published_at IS NULL;

-- The relay only ever scans unpublished rows, oldest first.
CREATE INDEX IF NOT EXISTS idx_payments_unpublished
    ON payments (created_at)
    WHERE published_at IS NULL;
