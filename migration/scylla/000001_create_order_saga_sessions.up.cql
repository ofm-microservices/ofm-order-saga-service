CREATE TABLE IF NOT EXISTS order_saga_sessions (
	saga_id TEXT PRIMARY KEY,
	order_id TEXT,
	buyer_id TEXT,
	seller_id TEXT,
	buyer_email TEXT,
	realtime_connection_id TEXT,
	gig_id TEXT,
	gig_title TEXT,
	package_id TEXT,
	package_tier TEXT,
	package_description TEXT,
	package_delivery_days INT,
	price_cents BIGINT,
	currency TEXT,
	status TEXT,
	created_at TIMESTAMP,
	updated_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS order_saga_sessions_order_id_idx
	ON order_saga_sessions (order_id);
