-- A payment is "processing" only for as long as a worker holds a lease on it.
-- If the worker crashes between charging the provider and persisting the
-- result, the lease expires and the reaper returns the payment to "pending" so
-- it can be re-enqueued. Provider calls are idempotent per payment ID, so
-- re-charging after an expired lease cannot double-charge.
ALTER TABLE payments ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ;

-- The reaper only ever scans processing rows with an expired lease.
CREATE INDEX IF NOT EXISTS idx_payments_processing_lease
    ON payments (locked_until)
    WHERE status = 'processing';
