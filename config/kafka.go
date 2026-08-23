package config

import "time"

// KafkaConfig defines the Kafka broker and topic settings for order-saga-service.
// The parent Config applies the KAFKA_ environment prefix, so field tags here
// intentionally contain only the suffix after KAFKA_.
type KafkaConfig struct {
	URL                           string        `env:"LEGACY_NATS_URL"`
	User                          string        `env:"LEGACY_NATS_USER"`
	Password                      string        `env:"LEGACY_NATS_PASSWORD"`
	Brokers                       []string      `env:"BROKERS" envSeparator:"," envDefault:"127.0.0.1:9092"`
	GroupID                       string        `env:"ORDER_SAGA_GROUP_ID" envDefault:"order-saga-service"`
	StartTopic                    string        `env:"ORDER_SAGA_START_TOPIC" envDefault:"order.saga.start"`
	OrderResultTopic              string        `env:"ORDER_SAGA_ORDER_RESULT_TOPIC" envDefault:"order.create.result"`
	PaymentIntentTopic            string        `env:"ORDER_SAGA_PAYMENT_INTENT_TOPIC" envDefault:"payment.intent.result"`
	PaymentSucceededTopic         string        `env:"ORDER_SAGA_PAYMENT_SUCCEEDED_TOPIC" envDefault:"payment.order_payment_succeeded"`
	PaymentFailedTopic            string        `env:"ORDER_SAGA_PAYMENT_FAILED_TOPIC" envDefault:"payment.order_payment_failed"`
	ReleaseTopic                  string        `env:"ORDER_SAGA_RELEASE_TOPIC" envDefault:"order.release.request"`
	DeadLetterTopic               string        `env:"ORDER_SAGA_DLQ_TOPIC" envDefault:"order.saga.dead_letter"`
	OrderCreateSubject            string        `env:"ORDER_CREATE_TOPIC" envDefault:"order.create"`
	OrderCreateResultSubject      string        `env:"ORDER_CREATE_RESULT_TOPIC" envDefault:"order.create.result"`
	OrderConfirmSubject           string        `env:"ORDER_CONFIRM_TOPIC" envDefault:"order.confirm"`
	OrderFundedSubject            string        `env:"ORDER_FUNDED_TOPIC" envDefault:"order.funded"`
	OrderFailSubject              string        `env:"ORDER_FAIL_TOPIC" envDefault:"order.fail"`
	OrderSagaStartSubject         string        `env:"ORDER_SAGA_START_TOPIC" envDefault:"order.saga.start"`
	PaymentIntentSubject          string        `env:"PAYMENT_INTENT_TOPIC" envDefault:"payment.intent"`
	PaymentIntentResultSubject    string        `env:"PAYMENT_INTENT_RESULT_TOPIC" envDefault:"payment.intent.result"`
	PaymentWebhookSubject         string        `env:"PAYMENT_WEBHOOK_TOPIC" envDefault:"payment.webhook"`
	PaymentOrderSucceededSubject  string        `env:"PAYMENT_ORDER_SUCCEEDED_TOPIC" envDefault:"payment.order_payment_succeeded"`
	PaymentOrderFailedSubject     string        `env:"PAYMENT_ORDER_FAILED_TOPIC" envDefault:"payment.order_payment_failed"`
	MailSendSubject               string        `env:"MAIL_SEND_TOPIC" envDefault:"mail.send"`
	RealtimeOrderAcceptedSubject  string        `env:"REALTIME_ORDER_ACCEPTED_TOPIC" envDefault:"realtime.order.accepted"`
	RealtimePaymentReadySubject   string        `env:"REALTIME_PAYMENT_READY_TOPIC" envDefault:"realtime.payment.ready"`
	RealtimeOrderConfirmedSubject string        `env:"REALTIME_ORDER_CONFIRMED_TOPIC" envDefault:"realtime.order.confirmed"`
	RealtimeOrderFailedSubject    string        `env:"REALTIME_ORDER_FAILED_TOPIC" envDefault:"realtime.order.failed"`
	OrderDeliveredSubject         string        `env:"ORDER_DELIVERED_TOPIC" envDefault:"order.delivered"`
	OrderRevisionRequestedSubject string        `env:"ORDER_REVISION_REQUESTED_TOPIC" envDefault:"order.revision_requested"`
	OrderDisputedSubject          string        `env:"ORDER_DISPUTED_TOPIC" envDefault:"order.disputed"`
	OrderDisputeResolvedSubject   string        `env:"ORDER_DISPUTE_RESOLVED_TOPIC" envDefault:"order.dispute_resolved"`
	OrderCompletedSubject         string        `env:"ORDER_COMPLETED_TOPIC" envDefault:"order.completed"`
	OrderReleaseFailedSubject     string        `env:"ORDER_RELEASE_FAILED_TOPIC" envDefault:"order.release_failed"`
	OrderReleaseRequestSubject    string        `env:"ORDER_RELEASE_REQUEST_TOPIC" envDefault:"order.release.request"`
	PaymentReleaseSubject         string        `env:"PAYMENT_RELEASE_TOPIC" envDefault:"payment.release_funds"`
	ChatCreateSubject             string        `env:"CHAT_CREATE_TOPIC" envDefault:"chat.create"`
	ChatCloseSubject              string        `env:"CHAT_CLOSE_TOPIC" envDefault:"chat.close"`
	OrderCommandsStream           string        `env:"ORDER_COMMANDS_STREAM" envDefault:"ORDER_COMMANDS"`
	OrderEventsStream             string        `env:"ORDER_EVENTS_STREAM" envDefault:"ORDER_EVENTS"`
	PaymentCommandsStream         string        `env:"PAYMENT_COMMANDS_STREAM" envDefault:"PAYMENT_COMMANDS"`
	PaymentEventsStream           string        `env:"PAYMENT_EVENTS_STREAM" envDefault:"PAYMENT_EVENTS"`
	RealtimeEventsStream          string        `env:"REALTIME_EVENTS_STREAM" envDefault:"REALTIME_EVENTS"`
	ChatLifecycleStream           string        `env:"CHAT_LIFECYCLE_STREAM" envDefault:"CHAT_LIFECYCLE"`
	CommandBatchSize              int           `env:"COMMAND_BATCH_SIZE" envDefault:"32"`
	CommandMaxWait                time.Duration `env:"COMMAND_MAX_WAIT" envDefault:"10ms"`
}
