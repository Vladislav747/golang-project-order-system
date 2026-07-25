package grpcserver

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Vladislav747/golang-project-order-system/internal/config"
	"github.com/Vladislav747/golang-project-order-system/internal/model"
	orderv1 "github.com/Vladislav747/golang-project-order-system/internal/pkg/api/order/v1"
	"github.com/Vladislav747/golang-project-order-system/internal/pkg/utils"
)

type Service interface {
	CreateOrder(ctx context.Context, order model.Order) error
	CreateOrderKafka(ctx context.Context, order model.Order) error
	GetOrders(ctx context.Context) ([]model.Order, error)
	GetOrder(ctx context.Context, id string) (model.Order, error)
	UpdateOrder(ctx context.Context, order model.Order) error
	DeleteOrder(ctx context.Context, id string) error
	DeleteSoftOrder(ctx context.Context, id string) error
	UpdateOrderKafka(ctx context.Context, order model.Order) error
	DeleteOrderKafka(ctx context.Context, id string) error
}

type OrderGrpcServer struct {
	orderv1.UnimplementedOrderServiceServer
	service  Service
	logger   *zap.Logger
	provider *config.Provider
}

func NewOrderServer(service Service, logger *zap.Logger, provider *config.Provider) *OrderGrpcServer {
	return &OrderGrpcServer{
		service:  service,
		logger:   logger,
		provider: provider,
	}
}

func (s *OrderGrpcServer) CreateOrder(
	ctx context.Context,
	req *orderv1.CreateOrderRequest,
) (*orderv1.CreateOrderResponse, error) {
	order, err := fromCreateRequest(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	cfg := s.provider.Get()
	ctx, cancel := utils.ContextWithTimeout(ctx, cfg.HttpServer.RequestTimeout)
	defer cancel()

	async := cfg.ProcessingMode.IsAsync()
	if async {
		err = s.service.CreateOrderKafka(ctx, order)
	} else {
		err = s.service.CreateOrder(ctx, order)
	}
	if err != nil {
		s.logger.Error("failed to create order in grpc server", zap.Error(err))
		return nil, status.Error(codes.Internal, err.Error())
	}

	s.logger.Info("CreateOrder grpc", zap.String("order_id", order.ID.String()))

	return &orderv1.CreateOrderResponse{
		Id:    order.ID.String(),
		Async: async,
	}, nil
}

func (s *OrderGrpcServer) GetOrder(
	ctx context.Context,
	req *orderv1.GetOrderRequest,
) (*orderv1.GetOrderResponse, error) {
	id := req.GetId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	cfg := s.provider.Get()
	ctx, cancel := utils.ContextWithTimeout(ctx, cfg.HttpServer.RequestTimeout)
	defer cancel()

	order, err := s.service.GetOrder(ctx, id)
	if err != nil {
		if errors.Is(err, model.ErrOrderNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		s.logger.Error("failed to get order in grpc server", zap.Error(err))
		return nil, status.Error(codes.Internal, err.Error())
	}

	s.logger.Info("GetOrder grpc", zap.String("order_id", order.ID.String()))

	return &orderv1.GetOrderResponse{Order: toProto(order)}, nil
}

func (s *OrderGrpcServer) GetOrders(
	ctx context.Context,
	_ *orderv1.GetOrdersRequest,
) (*orderv1.GetOrdersResponse, error) {
	cfg := s.provider.Get()
	ctx, cancel := utils.ContextWithTimeout(ctx, cfg.HttpServer.RequestTimeout)
	defer cancel()

	orders, err := s.service.GetOrders(ctx)
	if err != nil {
		s.logger.Error("failed to get orders in grpc server", zap.Error(err))
		return nil, status.Error(codes.Internal, err.Error())
	}

	out := make([]*orderv1.Order, 0, len(orders))
	for _, o := range orders {
		out = append(out, toProto(o))
	}

	s.logger.Info("GetOrders grpc")

	return &orderv1.GetOrdersResponse{Orders: out}, nil
}

func (s *OrderGrpcServer) UpdateOrder(
	ctx context.Context,
	req *orderv1.UpdateOrderRequest,
) (*orderv1.UpdateOrderResponse, error) {
	order, err := fromUpdateRequest(req)
	if err != nil {
		s.logger.Error("id is required")
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	cfg := s.provider.Get()
	ctx, cancel := utils.ContextWithTimeout(ctx, cfg.HttpServer.RequestTimeout)
	defer cancel()

	async := cfg.ProcessingMode.IsAsync()
	if async {
		err = s.service.UpdateOrderKafka(ctx, order)
	} else {
		err = s.service.UpdateOrder(ctx, order)
	}
	if err != nil {
		s.logger.Error("failed to update order in grpc server", zap.Error(err))
		return nil, status.Error(codes.Internal, err.Error())
	}

	s.logger.Info("UpdateOrder grpc", zap.String("order_id", order.ID.String()))

	return &orderv1.UpdateOrderResponse{Async: async}, nil
}

func (s *OrderGrpcServer) DeleteOrder(
	ctx context.Context,
	req *orderv1.DeleteOrderRequest,
) (*orderv1.DeleteOrderResponse, error) {
	id := req.GetId()
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	cfg := s.provider.Get()
	ctx, cancel := utils.ContextWithTimeout(ctx, cfg.HttpServer.RequestTimeout)
	defer cancel()

	async := cfg.ProcessingMode.IsAsync() && !req.GetHard()

	var err error
	switch {
	case req.GetHard():
		err = s.service.DeleteOrder(ctx, id)
		async = false
	case async:
		err = s.service.DeleteOrderKafka(ctx, id)
	default:
		err = s.service.DeleteSoftOrder(ctx, id)
	}
	if err != nil {
		s.logger.Error("failed to delete order in grpc server", zap.Error(err))
		return nil, status.Error(codes.Internal, err.Error())
	}

	s.logger.Info("DeleteOrder grpc", zap.String("order_id", id))

	return &orderv1.DeleteOrderResponse{Async: async}, nil
}

func fromCreateRequest(req *orderv1.CreateOrderRequest) (model.Order, error) {
	var id uuid.UUID
	if req.GetId() == "" {
		id = uuid.New()
	} else {
		parsed, err := uuid.Parse(req.GetId())
		if err != nil {
			return model.Order{}, errors.New("invalid id")
		}
		id = parsed
	}

	customerID, err := uuid.Parse(req.GetCustomerId())
	if err != nil {
		return model.Order{}, errors.New("invalid customer_id")
	}

	return model.Order{
		ID:          id,
		CustomerID:  customerID,
		Status:      req.GetStatus(),
		TotalAmount: req.GetTotalAmount(),
		Currency:    req.GetCurrency(),
		Items:       req.GetItems(),
	}, nil
}

func fromUpdateRequest(req *orderv1.UpdateOrderRequest) (model.Order, error) {
	id, err := uuid.Parse(req.GetId())
	if err != nil {
		return model.Order{}, errors.New("invalid id")
	}

	customerID, err := uuid.Parse(req.GetCustomerId())
	if err != nil {
		return model.Order{}, errors.New("invalid customer_id")
	}

	return model.Order{
		ID:          id,
		CustomerID:  customerID,
		Status:      req.GetStatus(),
		TotalAmount: req.GetTotalAmount(),
		Currency:    req.GetCurrency(),
		Items:       req.GetItems(),
	}, nil
}

func toProto(o model.Order) *orderv1.Order {
	deletedAt := ""
	if o.DeletedAt != nil {
		deletedAt = o.DeletedAt.Format(time.RFC3339)
	}
	return &orderv1.Order{
		Id:          o.ID.String(),
		CustomerId:  o.CustomerID.String(),
		Status:      o.Status,
		TotalAmount: o.TotalAmount,
		Currency:    o.Currency,
		Items:       o.Items,
		CreatedAt:   o.CreatedAt.Format(time.RFC3339),
		UpdatedAt:   o.UpdatedAt.Format(time.RFC3339),
		DeletedAt:   deletedAt,
	}
}
