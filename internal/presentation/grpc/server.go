package grpc

import (
	"context"
	"fmt"
	"net"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	ordercheckoutv1 "github.com/ofm-microservices/ofm-common/proto/ordercheckout/v1"
	otelgrpc "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	grpcpkg "google.golang.org/grpc"
	"order-saga-service/config"
	app "order-saga-service/internal/application"
)

type server struct {
	ordercheckoutv1.UnimplementedOrderCheckoutServiceServer
	svc      app.Service
	cfg      config.GRPCConfig
	log      logging.Logger
	srv      *grpcpkg.Server
	listener net.Listener
}

func NewServer(svc app.Service, cfg config.GRPCConfig, log logging.Logger) (Server, error) {
	if svc == nil {
		return nil, ErrNilService
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	grpcSrv := grpcpkg.NewServer(grpcpkg.StatsHandler(otelgrpc.NewServerHandler()), grpcpkg.UnaryInterceptor(metrics.UnaryServerInterceptor()))
	s := &server{svc: svc, cfg: cfg, log: log.With(logging.String("module", "grpc-order-checkout-server")), srv: grpcSrv}
	ordercheckoutv1.RegisterOrderCheckoutServiceServer(grpcSrv, s)
	return s, nil
}
func (s *server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = lis
	s.log.Info("starting grpc server", logging.String("addr", addr))
	return s.srv.Serve(lis)
}
func (s *server) Shutdown(context.Context) error {
	if s.srv != nil {
		s.srv.GracefulStop()
	}
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func (s *server) StartOrder(ctx context.Context, req *ordercheckoutv1.StartOrderRequest) (*ordercheckoutv1.StartOrderResponse, error) {
	res, err := s.svc.StartOrder(ctx, app.StartOrderCommand{
		BuyerID:        req.GetBuyerUserId(),
		BuyerEmail:     req.GetBuyerEmail(),
		GigID:          req.GetGigId(),
		PackageID:      req.GetPackageId(),
		IdempotencyKey: req.GetIdempotencyKey(),
		RequestedAt:    req.GetRequestedAt(),
	})
	if err != nil {
		return nil, mapOrderCheckoutError(err)
	}
	resp := &ordercheckoutv1.StartOrderResponse{SagaId: res.SagaID, OrderId: res.OrderID, Status: res.Status, Snapshot: &ordercheckoutv1.OrderSnapshot{GigId: res.Snapshot.GigID, PackageId: res.Snapshot.PackageID, SellerUserId: res.Snapshot.SellerID, GigTitle: res.Snapshot.GigTitle, PackageTitle: res.Snapshot.PackageTitle, PackageDescription: res.Snapshot.PackageDescription, PriceAmount: res.Snapshot.PriceCents, PriceCurrency: res.Snapshot.Currency, DeliveryDays: res.Snapshot.DeliveryDays, RevisionCount: res.Snapshot.RevisionCount}}
	for _, q := range res.Snapshot.Questions {
		resp.Questions = append(resp.Questions, &ordercheckoutv1.OrderQuestion{QuestionId: q.ID, Text: q.Text, Type: "text", Required: true, SortOrder: q.SortOrder})
	}
	return resp, nil
}
func (s *server) SubmitRequirements(ctx context.Context, req *ordercheckoutv1.SubmitRequirementsRequest) (*ordercheckoutv1.SubmitRequirementsResponse, error) {
	answers := make([]app.OrderAnswer, 0, len(req.GetAnswers()))
	for _, ans := range req.GetAnswers() {
		answers = append(answers, app.OrderAnswer{QuestionID: ans.GetQuestionId(), Value: ans.GetValue()})
	}
	res, err := s.svc.SubmitRequirements(ctx, app.SubmitRequirementsCommand{OrderID: req.GetOrderId(), BuyerID: req.GetBuyerUserId(), RequestedAt: req.GetRequestedAt(), Answers: answers})
	if err != nil {
		return nil, mapOrderCheckoutError(err)
	}
	return &ordercheckoutv1.SubmitRequirementsResponse{OrderId: res.OrderID, Status: res.Status, CurrentStep: res.CurrentStep}, nil
}
func (s *server) SubmitMessage(ctx context.Context, req *ordercheckoutv1.SubmitMessageRequest) (*ordercheckoutv1.SubmitMessageResponse, error) {
	res, err := s.svc.SubmitMessage(ctx, app.SubmitMessageCommand{OrderID: req.GetOrderId(), BuyerID: req.GetBuyerUserId(), Message: req.GetMessage(), RequestedAt: req.GetRequestedAt()})
	if err != nil {
		return nil, mapOrderCheckoutError(err)
	}
	return &ordercheckoutv1.SubmitMessageResponse{OrderId: res.OrderID, Status: res.Status, CurrentStep: res.CurrentStep}, nil
}
func (s *server) ConfirmOrder(ctx context.Context, req *ordercheckoutv1.ConfirmOrderRequest) (*ordercheckoutv1.ConfirmOrderResponse, error) {
	res, err := s.svc.ConfirmOrder(ctx, app.ConfirmOrderCommand{BuyerID: req.GetBuyerUserId(), OrderID: req.GetOrderId(), RequestedAt: req.GetRequestedAt()})
	if err != nil {
		return nil, mapOrderCheckoutError(err)
	}
	return &ordercheckoutv1.ConfirmOrderResponse{OrderId: res.OrderID, PaymentId: res.PaymentID, Status: res.Status, CheckoutUrl: res.CheckoutURL}, nil
}

func (s *server) DeliverOrder(ctx context.Context, req *ordercheckoutv1.DeliverOrderRequest) (*ordercheckoutv1.DeliverOrderResponse, error) {
	res, err := s.svc.DeliverOrder(ctx, app.DeliverOrderCommand{
		OrderID:       req.GetOrderId(),
		SellerID:      req.GetSellerUserId(),
		Message:       req.GetDeliveryMessage(),
		AttachmentIDs: req.GetAttachmentIds(),
		RequestedAt:   req.GetRequestedAt(),
	})
	if err != nil {
		return nil, mapOrderCheckoutError(err)
	}
	return &ordercheckoutv1.DeliverOrderResponse{OrderId: res.OrderID, Status: res.Status, CurrentStep: res.CurrentStep}, nil
}

func (s *server) AcceptDelivery(ctx context.Context, req *ordercheckoutv1.AcceptDeliveryRequest) (*ordercheckoutv1.AcceptDeliveryResponse, error) {
	res, err := s.svc.AcceptDelivery(ctx, app.AcceptDeliveryCommand{OrderID: req.GetOrderId(), BuyerID: req.GetBuyerUserId(), RequestedAt: req.GetRequestedAt()})
	if err != nil {
		return nil, mapOrderCheckoutError(err)
	}
	return &ordercheckoutv1.AcceptDeliveryResponse{OrderId: res.OrderID, Status: res.Status, CurrentStep: res.CurrentStep}, nil
}

func (s *server) RequestRevision(ctx context.Context, req *ordercheckoutv1.RequestRevisionRequest) (*ordercheckoutv1.RequestRevisionResponse, error) {
	res, err := s.svc.RequestRevision(ctx, app.RequestRevisionCommand{OrderID: req.GetOrderId(), BuyerID: req.GetBuyerUserId(), Reason: req.GetReason(), RequestedAt: req.GetRequestedAt()})
	if err != nil {
		return nil, mapOrderCheckoutError(err)
	}
	return &ordercheckoutv1.RequestRevisionResponse{OrderId: res.OrderID, Status: res.Status, CurrentStep: res.CurrentStep}, nil
}

func (s *server) OpenDispute(ctx context.Context, req *ordercheckoutv1.OpenDisputeRequest) (*ordercheckoutv1.OpenDisputeResponse, error) {
	res, err := s.svc.OpenDispute(ctx, app.OpenDisputeCommand{OrderID: req.GetOrderId(), ActorID: req.GetBuyerUserId(), Reason: req.GetReason(), RequestedAt: req.GetRequestedAt()})
	if err != nil {
		return nil, mapOrderCheckoutError(err)
	}
	return &ordercheckoutv1.OpenDisputeResponse{OrderId: res.OrderID, Status: res.Status, CurrentStep: res.CurrentStep}, nil
}

func (s *server) ResolveDispute(ctx context.Context, req *ordercheckoutv1.ResolveDisputeRequest) (*ordercheckoutv1.ResolveDisputeResponse, error) {
	res, err := s.svc.ResolveDispute(ctx, app.SettleDisputeCommand{
		OrderID:              req.GetOrderId(),
		AdminUserID:          req.GetAdminUserId(),
		FreelancerPercentage: req.GetFreelancerPercentage(),
		CustomerPercentage:   req.GetCustomerPercentage(),
		Reason:               req.GetReason(),
		RequestedAt:          req.GetRequestedAt(),
	})
	if err != nil {
		return nil, mapOrderCheckoutError(err)
	}
	return &ordercheckoutv1.ResolveDisputeResponse{
		OrderId:          res.OrderID,
		Status:           res.Status,
		CurrentStep:      res.CurrentStep,
		PaymentReleaseId: res.PaymentReleaseID,
		StripeTransferId: res.StripeTransferID,
		StripeRefundId:   res.StripeRefundID,
	}, nil
}
