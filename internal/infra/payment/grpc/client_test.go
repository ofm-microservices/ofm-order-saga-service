package grpc

import (
	"context"
	"testing"

	app "order-saga-service/internal/application"

	paymentcheckoutv1 "github.com/ofm-microservices/ofm-common/proto/paymentcheckout/v1"
	paymentconnectv1 "github.com/ofm-microservices/ofm-common/proto/paymentconnect/v1"
	"go.uber.org/mock/gomock"
	grpc "google.golang.org/grpc"
)

func TestClientMapsReleaseAndRecoveryRequests(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCheckout := NewMockPaymentCheckoutServiceClient(ctrl)
	mockCheckout.EXPECT().ReleaseFunds(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *paymentcheckoutv1.ReleaseFundsRequest, _ ...grpc.CallOption) (*paymentcheckoutv1.ReleaseFundsResponse, error) {
			if req.GetOrderId() != "order-1" {
				t.Fatalf("order id = %q, want %q", req.GetOrderId(), "order-1")
			}
			if req.GetPaymentId() != "pi_1" {
				t.Fatalf("payment id = %q, want %q", req.GetPaymentId(), "pi_1")
			}
			return &paymentcheckoutv1.ReleaseFundsResponse{
				OrderId:          req.GetOrderId(),
				PaymentReleaseId: "release-1",
				StripeTransferId: "tr_1",
				Status:           "released",
			}, nil
		},
	)
	mockCheckout.EXPECT().GetReleaseByOrderId(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *paymentcheckoutv1.GetReleaseByOrderIdRequest, _ ...grpc.CallOption) (*paymentcheckoutv1.GetReleaseByOrderIdResponse, error) {
			if req.GetOrderId() != "order-1" {
				t.Fatalf("order id = %q, want %q", req.GetOrderId(), "order-1")
			}
			return &paymentcheckoutv1.GetReleaseByOrderIdResponse{
				OrderId:          "order-1",
				PaymentReleaseId: "release-1",
				PaymentIntentId:  "pi_1",
				SellerUserId:     "seller-1",
				AmountCents:      2599,
				Currency:         "usd",
				IdempotencyKey:   "saga-1:payment.release_funds",
				StripeTransferId: "tr_1",
				Status:           "released",
				OccurredAt:       "2026-05-24T00:00:00Z",
			}, nil
		},
	)

	mockConnect := NewMockPaymentOnboardingServiceClient(ctrl)
	mockConnect.EXPECT().GetConnectStatus(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *paymentconnectv1.GetConnectStatusRequest, _ ...grpc.CallOption) (*paymentconnectv1.GetConnectStatusResponse, error) {
			if req.GetUserId() != "seller-1" {
				t.Fatalf("user id = %q, want %q", req.GetUserId(), "seller-1")
			}
			return &paymentconnectv1.GetConnectStatusResponse{
				UserId:          "seller-1",
				StripeAccountId: "acct_1",
				Status:          "connected",
			}, nil
		},
	)

	c := &client{checkout: mockCheckout, connect: mockConnect}
	if _, err := c.ReleaseFunds(context.Background(), app.ReleaseFundsCommand{
		OrderID:        "order-1",
		PaymentID:      "pi_1",
		SellerUserID:   "seller-1",
		AmountCents:    2599,
		Currency:       "usd",
		IdempotencyKey: "saga-1:payment.release_funds",
		RequestedAt:    "2026-05-24T00:00:00Z",
	}); err != nil {
		t.Fatalf("ReleaseFunds: %v", err)
	}
	release, err := c.GetReleaseByOrderID(context.Background(), "order-1")
	if err != nil {
		t.Fatalf("GetReleaseByOrderID: %v", err)
	}
	if release == nil || release.Status != "released" || release.PaymentReleaseID != "release-1" {
		t.Fatalf("release = %#v", release)
	}
	connect, err := c.GetConnectStatus(context.Background(), "seller-1")
	if err != nil {
		t.Fatalf("GetConnectStatus: %v", err)
	}
	if connect == nil || connect.Status != "connected" || connect.StripeAccountID != "acct_1" {
		t.Fatalf("connect = %#v", connect)
	}
}
