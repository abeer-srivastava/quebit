-- Fingerprint of the parameters a payment was created with. When an
-- idempotency key is replayed, the stored hash is compared against the new
-- request; a mismatch means the client reused a key for a different payment
-- and must be rejected rather than silently served the original.
ALTER TABLE payments ADD COLUMN IF NOT EXISTS request_hash TEXT;
