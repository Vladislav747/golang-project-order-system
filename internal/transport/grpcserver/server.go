package grpcserver

import (
	"context"
	"fmt"
	"net"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	orderv1 "github.com/Vladislav747/golang-project-order-system/internal/pkg/api/order/v1"
)

type Server struct {
	grpcServer *grpc.Server
	listener   net.Listener
	logger     *zap.Logger
}

func NewServer(port int, orderGrpcServer *OrderGrpcServer, logger *zap.Logger) (*Server, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	grpcServer := grpc.NewServer()
	orderv1.RegisterOrderServiceServer(grpcServer, orderGrpcServer)
	return &Server{grpcServer: grpcServer, listener: listener, logger: logger}, nil
}

func (s *Server) Serve() error {
	return s.grpcServer.Serve(s.listener)
}

func (s *Server) Shutdown(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		s.logger.Info("gRPC server gracefully stopped")
		s.grpcServer.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		s.logger.Info("gRPC server gracefully stopped")
	case <-ctx.Done():
		s.logger.Warn("gRPC graceful stop timed out, forcing stop")
		s.grpcServer.Stop() // рвёт оставшиеся соединения
		<-done              // дождаться выхода GracefulStop
	}
}
