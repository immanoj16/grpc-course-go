package main

import (
	"context"
	"fmt"
	"log"

	"github.com/immanoj16/grpc-go-course/calculator/calculatorpb"
	"google.golang.org/grpc"
)

func main() {
	fmt.Println("Client....")

	conn, err := grpc.Dial("0.0.0.0:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}
	defer conn.Close()

	c := calculatorpb.NewCalculatorServiceClient(conn)

	doUnary(c)
}

func doUnary(c calculatorpb.CalculatorServiceClient) {
	fmt.Println("Starting to do a unary RPC...")
	req := &calculatorpb.SumRequest{
		FirstNumber:  1,
		SecondNumber: 2,
	}

	res, err := c.Sum(context.Background(), req)
	if err != nil {
		log.Fatalf("error while calling Sum RPC: %v", err)
	}
	log.Printf("Response from Sum: %v", res.Result)
}
