package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

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

	// doUnary(c)
	// doServerStreaming(c)
	// doClientStreaming(c)
	doBiDiStreaming(c)
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

func doServerStreaming(c calculatorpb.CalculatorServiceClient) {
	fmt.Println("Starting to do a PrimeDecomposition Server Streaming RPC...")
	req := &calculatorpb.PrimeNumberDecompositionRequest{
		Number: 12,
	}

	resStream, err := c.PrimeNumberDecomposition(context.Background(), req)
	if err != nil {
		log.Fatalf("error while calling Sum RPC: %v", err)
	}
	for {
		res, err := resStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Error has occurred: %v", err)
		}
		log.Printf("Response from Prime: %v", res.GetPrimeFactor())
	}
}

func doClientStreaming(c calculatorpb.CalculatorServiceClient) {
	fmt.Println("Starting to do a client streaming RPC...")

	stream, err := c.ComputeAverage(context.Background())
	if err != nil {
		log.Fatalf("error while calling opening average stream %v", err)
	}

	numbers := []int32{21, 5, 12, 22, 4, 212}
	for _, number := range numbers {
		req := &calculatorpb.ComputeAverageRequest{
			Number: number,
		}
		fmt.Printf("Sending request: %v\n", req)
		stream.Send(req)
		time.Sleep(100 * time.Millisecond)
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("error while receiving response from ComputeAverage: %v", err)
	}
	fmt.Printf("ComputeAverage Response: %v\n", res.GetAverage())
}

func doBiDiStreaming(c calculatorpb.CalculatorServiceClient) {
	fmt.Println("Starting to do a bidi streaming RPC...")

	// Create a stream by invoking the client
	stream, err := c.FindMaximum(context.Background())
	if err != nil {
		log.Fatalf("error while creating stream: %v", err)
		return
	}

	numbers := []int32{21, 5, 12, 22, 4, 212}

	waitc := make(chan struct{})
	// send a bunch of message to the server (go routine)
	go func() {

		for _, number := range numbers {
			req := &calculatorpb.FindMaximumRequest{
				Number: number,
			}
			fmt.Printf("Sending request: %v\n", req)
			stream.Send(req)
			time.Sleep(100 * time.Millisecond)
		}
		err = stream.CloseSend()
		if err != nil {
			log.Fatalf("Couldn't close request %v", err)
		}
	}()

	// recieve a bunch of messages from the server
	go func() {
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("error while receiving %v", err)
				break
			}
			fmt.Printf("Maximum number is: %v\n", res.Maximum)
		}
		close(waitc)
	}()

	// block until everyting is done
	<-waitc
}
