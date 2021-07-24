package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/immanoj16/grpc-go-course/greet/greetpb"
	"google.golang.org/grpc"
)

type server struct{}

func (*server) Greet(ctx context.Context, req *greetpb.GreetRequest) (*greetpb.GreetResponse, error) {
	fmt.Printf("Greet function was invoked with %v", req)
	firstName := req.GetGreeting().GetFirstName()
	lastName := req.GetGreeting().GetLastName()
	res := &greetpb.GreetResponse{
		Result: "Hello " + firstName + " " + lastName,
	}
	return res, nil
}

func main() {
	fmt.Println("Hello, 世界")

	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	greetpb.RegisterGreetServiceServer(s, &server{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
