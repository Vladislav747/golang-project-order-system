package grpcserver

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

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

	GetOrderEvents(ctx context.Context) ([]model.OrderEvent, error)
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

	status, err := statusFromProto(req.GetStatus())
	if err != nil {
		return model.Order{}, err
	}

	return model.Order{
		ID:          id,
		CustomerID:  customerID,
		Status:      status,
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

	status, err := statusFromProto(req.GetStatus())
	if err != nil {
		return model.Order{}, err
	}

	return model.Order{
		ID:          id,
		CustomerID:  customerID,
		Status:      status,
		TotalAmount: req.GetTotalAmount(),
		Currency:    req.GetCurrency(),
		Items:       req.GetItems(),
	}, nil
}

func toProto(o model.Order) *orderv1.Order {
	var deletedAt *timestamppb.Timestamp
	if o.DeletedAt != nil {
		deletedAt = timestamppb.New(*o.DeletedAt)
	}
	return &orderv1.Order{
		Id:          o.ID.String(),
		CustomerId:  o.CustomerID.String(),
		Status:      statusToProto(o.Status),
		TotalAmount: o.TotalAmount,
		Currency:    o.Currency,
		Items:       o.Items,
		CreatedAt:   timestamppb.New(o.CreatedAt),
		UpdatedAt:   timestamppb.New(o.UpdatedAt),
		DeletedAt:   deletedAt,
	}
}

func statusFromProto(s orderv1.StatusType) (string, error) {
	switch s {
	case orderv1.StatusType_UNSPECIFIED:
		return "", errors.New("status is required")
	case orderv1.StatusType_CREATED:
		return "created", nil
	case orderv1.StatusType_PENDING:
		return "pending", nil
	case orderv1.StatusType_COMPLETED:
		return "completed", nil
	case orderv1.StatusType_FAILED:
		return "failed", nil
	case orderv1.StatusType_DELETED:
		return "deleted", nil
	default:
		return "", errors.New("invalid status")
	}
}

func statusToProto(s string) orderv1.StatusType {
	switch s {
	case "created":
		return orderv1.StatusType_CREATED
	case "pending":
		return orderv1.StatusType_PENDING
	case "completed":
		return orderv1.StatusType_COMPLETED
	case "failed":
		return orderv1.StatusType_FAILED
	case "deleted":
		return orderv1.StatusType_DELETED
	default:
		return orderv1.StatusType_UNSPECIFIED
	}
}
