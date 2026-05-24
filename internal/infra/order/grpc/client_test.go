package grpc

import (
	"context"
	"testing"

	app "order-saga-service/internal/application"

	orderwritev1 "github.com/ofm-microservices/ofm-common/proto/orderwrite/v1"
	"go.uber.org/mock/gomock"
	grpc "google.golang.org/grpc"
)

func TestClientMapsReleaseCommands(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCl := NewMockOrderWriteServiceClient(ctrl)
	mockCl.EXPECT().MarkReleasePending(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *orderwritev1.MarkReleasePendingRequest, _ ...grpc.CallOption) (*orderwritev1.MarkReleasePendingResponse, error) {
			if req.GetOrderId() != "order-1" {
				t.Fatalf("order id = %q, want %q", req.GetOrderId(), "order-1")
			}
			if req.GetPaymentReleaseId() != "release-1" {
				t.Fatalf("payment release id = %q, want %q", req.GetPaymentReleaseId(), "release-1")
			}
			return &orderwritev1.MarkReleasePendingResponse{}, nil
		},
	)
	mockCl.EXPECT().MarkOrderCompleted(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *orderwritev1.MarkOrderCompletedRequest, _ ...grpc.CallOption) (*orderwritev1.MarkOrderCompletedResponse, error) {
			if req.GetOrderId() != "order-1" {
				t.Fatalf("order id = %q, want %q", req.GetOrderId(), "order-1")
			}
			if req.GetPaymentReleaseId() != "release-1" {
				t.Fatalf("payment release id = %q, want %q", req.GetPaymentReleaseId(), "release-1")
			}
			return &orderwritev1.MarkOrderCompletedResponse{}, nil
		},
	)
	mockCl.EXPECT().MarkReleaseFailed(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *orderwritev1.MarkReleaseFailedRequest, _ ...grpc.CallOption) (*orderwritev1.MarkReleaseFailedResponse, error) {
			if req.GetOrderId() != "order-1" {
				t.Fatalf("order id = %q, want %q", req.GetOrderId(), "order-1")
			}
			return &orderwritev1.MarkReleaseFailedResponse{}, nil
		},
	)

	c := &client{cl: mockCl}
	if _, err := c.MarkReleasePending(context.Background(), app.MarkReleasePendingCommand{
		OrderID:          "order-1",
		PaymentReleaseID: "release-1",
		RequestedAt:      "2026-05-24T00:00:00Z",
	}); err != nil {
		t.Fatalf("MarkReleasePending: %v", err)
	}
	if _, err := c.MarkOrderCompleted(context.Background(), app.MarkOrderCompletedCommand{
		OrderID:          "order-1",
		PaymentReleaseID: "release-1",
		RequestedAt:      "2026-05-24T00:00:00Z",
	}); err != nil {
		t.Fatalf("MarkOrderCompleted: %v", err)
	}
	if _, err := c.MarkReleaseFailed(context.Background(), app.MarkReleaseFailedCommand{
		OrderID:     "order-1",
		Reason:      "boom",
		RequestedAt: "2026-05-24T00:00:00Z",
	}); err != nil {
		t.Fatalf("MarkReleaseFailed: %v", err)
	}
}

func TestClientMapsSnapshotLookups(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCl := NewMockOrderWriteServiceClient(ctrl)
	mockCl.EXPECT().GetOrderLifecycleSnapshot(gomock.Any(), gomock.Any()).Return(&orderwritev1.GetOrderLifecycleSnapshotResponse{
		Order: &orderwritev1.OrderSnapshot{
			OrderId:                    "order-1",
			SagaId:                     "saga-1",
			BuyerUserId:                "buyer-1",
			SellerUserId:               "seller-1",
			GigId:                      "gig-1",
			GigTitleSnapshot:           "Gig title",
			PackageId:                  "package-1",
			PackageTitleSnapshot:       "Basic",
			PackageDescriptionSnapshot: "desc",
			PriceAmountSnapshot:        1000,
			PriceCurrencySnapshot:      "usd",
			Status:                     "delivered",
			PaymentReleaseId:           "release-1",
		},
	}, nil)
	mockCl.EXPECT().GetOrderPaymentSnapshot(gomock.Any(), gomock.Any()).Return(&orderwritev1.GetOrderPaymentSnapshotResponse{
		Order: &orderwritev1.OrderSnapshot{
			OrderId:               "order-1",
			SagaId:                "saga-1",
			BuyerUserId:           "buyer-1",
			SellerUserId:          "seller-1",
			GigTitleSnapshot:      "Gig title",
			PackageTitleSnapshot:  "Basic",
			PriceAmountSnapshot:   1000,
			PriceCurrencySnapshot: "usd",
			Status:                "funded",
		},
	}, nil)

	c := &client{cl: mockCl}
	snap, err := c.GetOrderLifecycleSnapshot(context.Background(), "order-1")
	if err != nil {
		t.Fatalf("GetOrderLifecycleSnapshot: %v", err)
	}
	if snap == nil || snap.PaymentReleaseID != "release-1" || snap.Status != "delivered" {
		t.Fatalf("snapshot = %#v", snap)
	}
	pay, err := c.GetOrderPaymentSnapshot(context.Background(), "order-1")
	if err != nil {
		t.Fatalf("GetOrderPaymentSnapshot: %v", err)
	}
	if pay == nil || pay.Status != "funded" {
		t.Fatalf("payment snapshot = %#v", pay)
	}
}
