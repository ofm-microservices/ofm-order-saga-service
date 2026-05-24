package application

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"order-saga-service/config"
	"order-saga-service/internal/domain"
)

var _ = Describe("Service", func() {
	var (
		ctx    context.Context
		logger logging.Logger
		broker *testBroker
		svc    Service
		sess   *testSessions
		steps  *testSteps
		orders *testOrders
		pays   *testPayments
	)

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		logger, err = logging.New("order-saga-service", "test", "debug")
		Expect(err).NotTo(HaveOccurred())
		broker = &testBroker{}
		sess = &testSessions{session: &domain.Session{
			SagaID:               "saga-1",
			OrderID:              "order-1",
			BuyerID:              "buyer-1",
			BuyerEmail:           "buyer@example.com",
			RealtimeConnectionID: "startup-123.01JTEST",
			GigTitle:             "Logo design",
			PackageTier:          "Pro",
			PackageDescription:   "Fast delivery",
			PackageDeliveryDays:  3,
			PriceCents:           2599,
			Currency:             "USD",
			Status:               domain.SessionStatusPendingPayment,
		}}
		steps = &testSteps{}
		orders = &testOrders{lifecycleSnapshot: &OrderLifecycleSnapshot{
			OrderID:               "order-1",
			SagaID:                "saga-1",
			BuyerID:               "buyer-1",
			SellerID:              "seller-1",
			GigID:                 "gig-1",
			GigTitle:              "Logo design",
			PackageID:             "package-1",
			PackageTitle:          "Pro",
			PackageDescription:    "Fast delivery",
			PriceCents:            2599,
			Currency:              "USD",
			PaymentIntentID:       "pi_123",
			PaymentReleaseID:      "release-1",
			RevisionCountSnapshot: 3,
			RevisionCountUsed:     3,
			Status:                domain.SessionStatusDelivered,
		}}
		pays = &testPayments{}

		svc, err = New(
			sess,
			steps,
			&testGigClient{},
			&testAuthClient{},
			orders,
			pays,
			broker,
			config.NATSConfig{
				MailSendSubject:               "mail.send",
				RealtimeOrderConfirmedSubject: "realtime.order.confirmed",
				RealtimeOrderAcceptedSubject:  "realtime.order.accepted",
				OrderFailSubject:              "order.fail",
				OrderReleaseRequestSubject:    "order.release.request",
			},
			logger,
		)
		Expect(err).NotTo(HaveOccurred())
	})

	It("publishes the async release request and marks the order release pending on buyer acceptance", func() {
		sess.session = &domain.Session{Status: domain.SessionStatusDelivered}

		res, err := svc.AcceptDelivery(ctx, AcceptDeliveryCommand{
			OrderID:     "order-1",
			BuyerID:     "buyer-1",
			RequestedAt: "2026-05-21T00:00:00Z",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal(&AcceptDeliveryResult{
			OrderID:     "order-1",
			Status:      domain.SessionStatusReleasePending,
			CurrentStep: string(domain.StepKeyReleaseFunds),
		}))
		Expect(broker.published).To(HaveLen(1))
		Expect(broker.published[0].subject).To(Equal("order.release.request"))
		Expect(sess.session.Status).To(Equal(domain.SessionStatusReleasePending))
	})

	It("skips a second transfer when payment recovery already reports released", func() {
		orders.lifecycleSnapshot.Status = domain.SessionStatusReleasePending
		pays.recovery = &GetReleaseByOrderResult{
			OrderID:          "order-1",
			PaymentReleaseID: "release-1",
			PaymentID:        "pi_123",
			SellerUserID:     "seller-1",
			AmountCents:      2599,
			Currency:         "USD",
			Status:           "released",
			OccurredAt:       "2026-05-21T00:00:00Z",
		}

		err := svc.HandleReleaseFunds(ctx, "order-1")
		Expect(err).NotTo(HaveOccurred())
		Expect(pays.releaseCalls).To(BeEmpty())
		Expect(orders.markCompletedCalls).To(HaveLen(1))
		Expect(orders.markCompletedCalls[0].PaymentReleaseID).To(Equal("release-1"))
		Expect(sess.session.Status).To(Equal(domain.SessionStatusCompleted))
	})

	It("creates the release once when no recovered payout exists", func() {
		orders.lifecycleSnapshot.Status = domain.SessionStatusReleasePending
		pays.recovery = nil

		err := svc.HandleReleaseFunds(ctx, "order-1")
		Expect(err).NotTo(HaveOccurred())
		Expect(pays.releaseCalls).To(HaveLen(1))
		Expect(pays.releaseCalls[0].OrderID).To(Equal("order-1"))
		Expect(orders.markCompletedCalls).To(HaveLen(1))
		Expect(orders.markCompletedCalls[0].PaymentReleaseID).To(Equal("release-1"))
		Expect(broker.published).NotTo(BeEmpty())
		Expect(sess.session.Status).To(Equal(domain.SessionStatusCompleted))
	})

	It("marks payment failed and emits failure notifications when the webhook reports failure", func() {
		orders.lifecycleSnapshot.Status = domain.SessionStatusPendingPayment

		err := svc.HandlePaymentStatus(ctx, PaymentStatusEvent{
			SagaID:          "saga-1",
			OrderID:         "order-1",
			PaymentIntentID: "pi_123",
			Status:          "failed",
			Error:           "card_declined",
			OccurredAt:      "2026-05-21T00:00:00Z",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(orders.markPaymentFailedCalls).To(HaveLen(1))
		Expect(orders.markPaymentFailedCalls[0].OrderID).To(Equal("order-1"))
		Expect(broker.published).To(HaveLen(2))
		Expect([]string{broker.published[0].subject, broker.published[1].subject}).To(ContainElement("order.fail"))
		Expect(sess.session.Status).To(Equal(domain.SessionStatusFailed))
	})

	It("does not republish the receipt email when the receipt step is already complete", func() {
		steps.statusByKey = map[string]string{
			domain.StepKeySendReceipt: domain.StepStatusCompleted,
		}
		orders.lifecycleSnapshot.Status = domain.SessionStatusPendingPayment

		err := svc.HandlePaymentStatus(ctx, PaymentStatusEvent{
			SagaID:          "saga-1",
			OrderID:         "order-1",
			PaymentIntentID: "pi_123",
			Status:          "payment.order_payment_succeeded",
			OccurredAt:      "2026-05-21T00:00:00Z",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(broker.published).To(HaveLen(2))
		Expect([]string{broker.published[0].subject, broker.published[1].subject}).NotTo(ContainElement("mail.send"))
		Expect(sess.session.Status).To(Equal(domain.SessionStatusFunded))
	})

	It("does nothing when the order is already completed", func() {
		orders.lifecycleSnapshot.Status = domain.SessionStatusCompleted

		err := svc.HandleReleaseFunds(ctx, "order-1")
		Expect(err).NotTo(HaveOccurred())
		Expect(pays.releaseCalls).To(BeEmpty())
		Expect(orders.markCompletedCalls).To(BeEmpty())
	})

	It("stops release processing when the payment-service release step fails", func() {
		orders.lifecycleSnapshot.Status = domain.SessionStatusReleasePending
		pays.recovery = nil
		pays.releaseErr = assertAnError()

		err := svc.HandleReleaseFunds(ctx, "order-1")
		Expect(err).To(HaveOccurred())
		Expect(orders.markCompletedCalls).To(BeEmpty())
		Expect(sess.session.Status).To(Equal(domain.SessionStatusPendingPayment))
	})
})

func assertAnError() error {
	return fmt.Errorf("release failed")
}
