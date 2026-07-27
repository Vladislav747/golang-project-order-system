package grpcserver

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Vladislav747/golang-project-order-system/internal/config"
	"github.com/Vladislav747/golang-project-order-system/internal/model"
	order_event_v1 "github.com/Vladislav747/golang-project-order-system/internal/pkg/api/order_event/v1"
	"github.com/Vladislav747/golang-project-order-system/internal/pkg/utils"
)

type OrderEventGrpcServer struct {
	order_event_v1.UnimplementedOrderEventServiceServer
	service  Service
	logger   *zap.Logger
	provider *config.Provider
}

func NewOrderEventServer(service Service, logger *zap.Logger, provider *config.Provider) *OrderEventGrpcServer {
	return &OrderEventGrpcServer{
		service:  service,
		logger:   logger,
		provider: provider,
	}
}

func (s *OrderEventGrpcServer) GetOrderEvents(ctx context.Context, req *order_event_v1.GetOrderEventsRequest) (*order_event_v1.GetOrderEventsResponse, error) {
	cfg := s.provider.Get()

	ctx, cancel := utils.ContextWithTimeout(ctx, cfg.HttpServer.RequestTimeout)
	defer cancel()

	events, err := s.service.GetOrderEvents(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get order events: %v", err)
	}

	out := make([]*order_event_v1.OrderEvent, 0, len(events))
	for _, event := range events {
		out = append(out, toProtoOrderEvent(event))
	}

	return &order_event_v1.GetOrderEventsResponse{
		Events: out,
	}, nil
}

func toProtoOrderEvent(e model.OrderEvent) *order_event_v1.OrderEvent {
	return &order_event_v1.OrderEvent{
		Id:        e.ID.String(),
		OrderId:   e.OrderID.String(),
		EventType: string(e.EventType),
		Source:    toProtoSource(e.Source),
		Payload:   e.Payload,
		CreatedAt: e.CreatedAt.Format(time.RFC3339),
	}
}

func toProtoSource(s model.EventSource) order_event_v1.EventSource {
	switch s {
	case model.SourceHTTPSync:
		return order_event_v1.EventSource_EVENT_SOURCE_HTTP_SYNC
	case model.SourceKafka:
		return order_event_v1.EventSource_EVENT_SOURCE_KAFKA
	default:
		return order_event_v1.EventSource_EVENT_SOURCE_UNSPECIFIED
	}
}
