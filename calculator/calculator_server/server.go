package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net"

	"github.com/immanoj16/grpc-go-course/calculator/calculatorpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct{}

func (*server) Sum(ctx context.Context, req *calculatorpb.SumRequest) (*calculatorpb.SumResponse, error) {
	fmt.Printf("Sum function was invoked with %v", req)
	res := &calculatorpb.SumResponse{
		Result: req.FirstNumber + req.SecondNumber,
	}
	return res, nil
}

func (*server) PrimeNumberDecomposition(req *calculatorpb.PrimeNumberDecompositionRequest, stream calculatorpb.CalculatorService_PrimeNumberDecompositionServer) error {
	fmt.Printf("PrimeNumberDecomposition function was invoked with %v", req)
	number := req.GetNumber()
	divisor := int32(2)
	for number > 1 {
		if number%divisor == 0 {
			stream.Send(&calculatorpb.PrimeNumberDecompositionResponse{
				PrimeFactor: divisor,
			})
			number /= divisor
		} else {
			divisor++
			fmt.Printf("Divisor has increased to %v", divisor)
		}
	}
	return nil
}

func (*server) ComputeAverage(stream calculatorpb.CalculatorService_ComputeAverageServer) error {
	fmt.Printf("PrimeNumberDecomposition function was invoked client streaming")

	sum := 0
	count := 0
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&calculatorpb.ComputeAverageResponse{
				Average: float64(sum) / float64(count),
			})
		}
		if err != nil {
			log.Fatalf("error while reading client stream: %v", err)
		}
		sum += int(req.GetNumber())
		count++
	}
}

func (*server) FindMaximum(stream calculatorpb.CalculatorService_FindMaximumServer) error {
	fmt.Printf("FindMaximum function was invoked with a bidi streaming request")

	var maximum int32
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			log.Fatalf("error while reading client stream: %v", err)
			return err
		}
		if maximum < req.GetNumber() {
			maximum = req.GetNumber()
		}
		err = stream.Send(&calculatorpb.FindMaximumResponse{
			Maximum: float64(maximum),
		})
		if err != nil {
			log.Fatalf("error while sending data to client %v", err)
			return err
		}
	}
}

func (*server) FindSquareRoot(ctx context.Context, req *calculatorpb.FindSquareRootRequest) (*calculatorpb.FindSquareRootResponse, error) {
	fmt.Printf("FindSquareRoot function with request: %v\n", req)
	number := req.GetNumber()
	if number < 0 {
		return nil, status.Errorf(
			codes.InvalidArgument,
			fmt.Sprintf("Received a negative number %v\n", number),
		)
	}
	return &calculatorpb.FindSquareRootResponse{
		Result: math.Sqrt(float64(number)),
	}, nil
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
