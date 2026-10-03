DROP TRIGGER IF EXISTS order_saga_sessions_outbox_event ON order_saga_sessions;
DROP TRIGGER IF EXISTS order_saga_steps_outbox_event ON order_saga_steps;
DROP FUNCTION IF EXISTS emit_order_saga_outbox_event();
DROP TABLE IF EXISTS outbox_events;
