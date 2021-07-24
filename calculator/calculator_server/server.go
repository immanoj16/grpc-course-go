package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/immanoj16/grpc-go-course/calculator/calculatorpb"
	"google.golang.org/grpc"
)

type server struct{}

func (*server) Sum(ctx context.Context, req *calculatorpb.SumRequest) (*calculatorpb.SumResponse, error) {
	fmt.Printf("Sum function was invoked with %v", req)
	res := &calculatorpb.SumResponse{
		Result: req.FirstNumber + req.SecondNumber,
	}
	return res, nil
}

func main() {
	fmt.Println("Calculator SErver")

	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	calculatorpb.RegisterCalculatorServiceServer(s, &server{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
