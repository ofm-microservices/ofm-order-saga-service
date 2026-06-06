package config

import "time"

// NATSConfig defines the NATS subjects used by the order saga service.
type NATSConfig struct {
	URL      string `env:"URL,required"`
	User     string `env:"USER"`
	Password string `env:"PASSWORD"`

	OrderCommandsStream   string `env:"STREAM_ORDER_COMMANDS" envDefault:"ORDER_COMMANDS"`
	OrderEventsStream     string `env:"STREAM_ORDER_EVENTS" envDefault:"ORDER_EVENTS"`
	PaymentCommandsStream string `env:"STREAM_PAYMENT_COMMANDS" envDefault:"PAYMENT_COMMANDS"`
	PaymentEventsStream   string `env:"STREAM_PAYMENT_EVENTS" envDefault:"PAYMENT_EVENTS"`
	RealtimeEventsStream  string `env:"STREAM_REALTIME_EVENTS" envDefault:"REALTIME_EVENTS"`

	OrderCreateSubject       string `env:"SUBJECT_ORDER_CREATE" envDefault:"order.create"`
	OrderCreateResultSubject string `env:"SUBJECT_ORDER_CREATE_RESULT" envDefault:"order.create.result"`
	OrderConfirmSubject      string `env:"SUBJECT_ORDER_CONFIRM" envDefault:"order.confirm"`
	OrderFundedSubject       string `env:"SUBJECT_ORDER_FUNDED" envDefault:"order.funded"`
	OrderFailSubject         string `env:"SUBJECT_ORDER_FAIL" envDefault:"order.fail"`
	OrderSagaStartSubject    string `env:"SUBJECT_ORDER_SAGA_START" envDefault:"order.saga.start"`

	PaymentIntentSubject         string `env:"SUBJECT_PAYMENT_INTENT" envDefault:"payment.intent"`
	PaymentIntentResultSubject   string `env:"SUBJECT_PAYMENT_INTENT_RESULT" envDefault:"payment.intent.result"`
	PaymentWebhookSubject        string `env:"SUBJECT_PAYMENT_WEBHOOK" envDefault:"payment.webhook"`
	PaymentOrderSucceededSubject string `env:"SUBJECT_PAYMENT_ORDER_SUCCEEDED" envDefault:"payment.order_payment_succeeded"`
	PaymentOrderFailedSubject    string `env:"SUBJECT_PAYMENT_ORDER_FAILED" envDefault:"payment.order_payment_failed"`
	MailSendSubject              string `env:"SUBJECT_MAIL_SEND" envDefault:"mail.send"`

	RealtimeOrderAcceptedSubject  string `env:"SUBJECT_REALTIME_ORDER_ACCEPTED" envDefault:"realtime.order.accepted"`
	RealtimePaymentReadySubject   string `env:"SUBJECT_REALTIME_PAYMENT_READY" envDefault:"realtime.payment.ready"`
	RealtimeOrderConfirmedSubject string `env:"SUBJECT_REALTIME_ORDER_CONFIRMED" envDefault:"realtime.order.confirmed"`
	RealtimeOrderFailedSubject    string `env:"SUBJECT_REALTIME_ORDER_FAILED" envDefault:"realtime.order.failed"`
	RealtimeUserDeliverySubject   string `env:"SUBJECT_REALTIME_USER_DELIVERY" envDefault:"realtime.user.delivery"`
	OrderDeliveredSubject         string `env:"SUBJECT_ORDER_DELIVERED" envDefault:"order.delivered"`
	OrderRevisionRequestedSubject string `env:"SUBJECT_ORDER_REVISION_REQUESTED" envDefault:"order.revision_requested"`
	OrderDisputedSubject          string `env:"SUBJECT_ORDER_DISPUTED" envDefault:"order.disputed"`
	OrderCompletedSubject         string `env:"SUBJECT_ORDER_COMPLETED" envDefault:"order.completed"`
	OrderReleaseFailedSubject     string `env:"SUBJECT_ORDER_RELEASE_FAILED" envDefault:"order.release_failed"`
	OrderReleaseRequestSubject    string `env:"SUBJECT_ORDER_RELEASE_REQUEST" envDefault:"order.release.request"`
	PaymentReleaseSubject         string `env:"SUBJECT_PAYMENT_RELEASE" envDefault:"payment.release_funds"`

	CommandBatchSize int           `env:"COMMAND_BATCH_SIZE" envDefault:"32"`
	CommandMaxWait   time.Duration `env:"COMMAND_MAX_WAIT" envDefault:"10ms"`
}
