CREATE TABLE IF NOT EXISTS order_saga_sessions (
 saga_id text PRIMARY KEY, order_id text NOT NULL, buyer_id text NOT NULL, seller_id text NOT NULL,
 seller_username text NOT NULL DEFAULT '', buyer_email text NOT NULL, gig_id text NOT NULL, gig_title text NOT NULL,
 picture_file_id text NOT NULL DEFAULT '', package_id text NOT NULL, package_tier text NOT NULL,
 package_description text NOT NULL, package_delivery_days integer NOT NULL, price_cents bigint NOT NULL,
 currency text NOT NULL, status text NOT NULL, created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS order_saga_sessions_order_id_idx ON order_saga_sessions(order_id);
CREATE TABLE IF NOT EXISTS order_saga_steps (
 saga_id text NOT NULL, step_key text NOT NULL, status text NOT NULL, attempt integer NOT NULL DEFAULT 0,
 max_attempts integer NOT NULL DEFAULT 6, next_attempt_at timestamptz, locked_until timestamptz,
 last_error text NOT NULL DEFAULT '', idempotency_key text NOT NULL DEFAULT '', created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
 PRIMARY KEY(saga_id,step_key)
);
CREATE TABLE IF NOT EXISTS processed_events (event_id text PRIMARY KEY,event_type text NOT NULL,source_service text NOT NULL,aggregate_type text NOT NULL,aggregate_id text NOT NULL,aggregate_version bigint NOT NULL,processed_at timestamptz NOT NULL DEFAULT now());
