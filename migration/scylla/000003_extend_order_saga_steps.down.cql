ALTER TABLE order_saga_steps DROP attempt;
ALTER TABLE order_saga_steps DROP max_attempts;
ALTER TABLE order_saga_steps DROP next_attempt_at;
ALTER TABLE order_saga_steps DROP locked_until;
ALTER TABLE order_saga_steps DROP last_error;
ALTER TABLE order_saga_steps DROP idempotency_key;
