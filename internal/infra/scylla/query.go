package scylla

const (
	insertSessionQuery = `
		INSERT INTO order_saga_sessions (saga_id, order_id, buyer_id, seller_id, seller_username, buyer_email, realtime_connection_id, gig_id, gig_title, picture_file_id, package_id, package_tier, package_description, package_delivery_days, price_cents, currency, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	getSessionByIDQuery = `
		SELECT saga_id, order_id, buyer_id, seller_id, seller_username, buyer_email, realtime_connection_id, gig_id, gig_title, picture_file_id, package_id, package_tier, package_description, package_delivery_days, price_cents, currency, status, created_at, updated_at
		FROM order_saga_sessions
		WHERE saga_id = ?
		LIMIT 1
	`

	getSessionByOrderIDQuery = `
		SELECT saga_id, order_id, buyer_id, seller_id, seller_username, buyer_email, realtime_connection_id, gig_id, gig_title, picture_file_id, package_id, package_tier, package_description, package_delivery_days, price_cents, currency, status, created_at, updated_at
		FROM order_saga_sessions
		WHERE order_id = ?
		LIMIT 1
	`

	updateSessionStatusQuery = `
		UPDATE order_saga_sessions
		SET status = ?, updated_at = ?
		WHERE saga_id = ?
	`

	insertStepQuery = `
		INSERT INTO order_saga_steps (saga_id, step_key, status, attempt, max_attempts, next_attempt_at, locked_until, last_error, idempotency_key, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	getStepByKeyQuery = `
		SELECT saga_id, step_key, status, attempt, max_attempts, next_attempt_at, locked_until, last_error, idempotency_key, created_at, updated_at
		FROM order_saga_steps
		WHERE saga_id = ? AND step_key = ?
		LIMIT 1
	`

	listStepsBySagaIDQuery = `
		SELECT saga_id, step_key, status, attempt, max_attempts, next_attempt_at, locked_until, last_error, idempotency_key, created_at, updated_at
		FROM order_saga_steps
		WHERE saga_id = ?
	`

	updateStepStatusQuery = `
		UPDATE order_saga_steps
		SET status = ?, updated_at = ?
		WHERE saga_id = ? AND step_key = ?
	`
)
