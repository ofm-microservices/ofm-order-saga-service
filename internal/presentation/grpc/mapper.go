package grpc

import (
	"errors"

	app "order-saga-service/internal/application"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func mapOrderCheckoutError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, app.ErrSelfOrderNotAllowed):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, app.ErrOrderNotOwned):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, app.ErrOrderNotConfirmable),
		errors.Is(err, app.ErrOrderAlreadyPaymentPending),
		errors.Is(err, app.ErrOrderAlreadyFunded),
		errors.Is(err, app.ErrOrderRequirementsIncomplete),
		errors.Is(err, app.ErrOrderMessageIncomplete),
		errors.Is(err, app.ErrOrderNotDeliverable),
		errors.Is(err, app.ErrOrderNotAcceptable),
		errors.Is(err, app.ErrOrderNotRevisionable),
		errors.Is(err, app.ErrOrderNotDisputable),
		errors.Is(err, app.ErrOrderReleaseFailed),
		errors.Is(err, app.ErrConnectOnboardingIncomplete):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return err
	}
}
