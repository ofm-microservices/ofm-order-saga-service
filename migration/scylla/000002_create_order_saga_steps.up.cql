CREATE TABLE IF NOT EXISTS order_saga_steps (
	saga_id TEXT,
	step_key TEXT,
	status TEXT,
	created_at TIMESTAMP,
	updated_at TIMESTAMP,
	PRIMARY KEY ((saga_id), step_key)
);
ALTER TABLE order_saga_steps WITH cdc = {'enabled': true, 'preimage': 'full', 'postimage': true};
