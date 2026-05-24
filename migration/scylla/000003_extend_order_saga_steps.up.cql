ALTER TABLE order_saga_steps ADD attempt int;
ALTER TABLE order_saga_steps ADD max_attempts int;
ALTER TABLE order_saga_steps ADD next_attempt_at timestamp;
ALTER TABLE order_saga_steps ADD locked_until timestamp;
ALTER TABLE order_saga_steps ADD last_error text;
ALTER TABLE order_saga_steps ADD idempotency_key text;
