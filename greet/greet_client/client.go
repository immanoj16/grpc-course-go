package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/immanoj16/grpc-go-course/greet/greetpb"
	"google.golang.org/grpc"
)

func main() {
	fmt.Println("Client....")

	conn, err := grpc.Dial("0.0.0.0:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("could not connect: %v", err)
	}
	defer conn.Close()

	c := greetpb.NewGreetServiceClient(conn)

	// doUnary(c)
	// doServerStreaming(c)
	// doClientStreaming(c)
	doBiDiStreaming(c)
}

func doUnary(c greetpb.GreetServiceClient) {
	fmt.Println("Starting to do a unary RPC...")
	req := &greetpb.GreetRequest{
		Greeting: &greetpb.Greeting{
			FirstName: "Manoj",
			LastName:  "Kumar",
		},
	}

	res, err := c.Greet(context.Background(), req)
	if err != nil {
		log.Fatalf("error while calling Greet RPC: %v", err)
	}
	log.Printf("Response from Greet: %v", res.Result)
}

func doServerStreaming(c greetpb.GreetServiceClient) {
	fmt.Println("Starting to do a server streaming RPC...")
	req := &greetpb.GreetManyTimesRequest{
		Greeting: &greetpb.Greeting{
			FirstName: "Manoj",
			LastName:  "Kumar",
		},
	}

	resStream, err := c.GreetManyTimes(context.Background(), req)
	if err != nil {
		log.Fatalf("error while calling Greet RPC: %v", err)
	}
	for {
		msg, err := resStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("error while reading stream: %v", err)
		}
		log.Printf("Response from GreetManyTimes: %v", msg.Result)
	}
}

func doClientStreaming(c greetpb.GreetServiceClient) {
	fmt.Println("Starting to do a client streaming RPC...")

	requests := []*greetpb.LongGreetRequest{
		{
			Greeting: &greetpb.Greeting{
				FirstName: "Manoj",
				LastName:  "Kumar",
			},
		},
		{
			Greeting: &greetpb.Greeting{
				FirstName: "Kanhu",
				LastName:  "Kumar",
			},
		},
		{
			Greeting: &greetpb.Greeting{
				FirstName: "sldfs",
				LastName:  "Kumar",
			},
		},
		{
			Greeting: &greetpb.Greeting{
				FirstName: "Masdfsdfnoj",
				LastName:  "Kumar",
			},
		},
		{
			Greeting: &greetpb.Greeting{
				FirstName: "sdfsdf",
				LastName:  "Kumar",
			},
		},
	}

	stream, err := c.LongGreet(context.Background())
	if err != nil {
		log.Fatalf("error while calling LongGreet %v", err)
	}

	for _, req := range requests {
		fmt.Printf("Sending request: %v", req)
		stream.Send(req)
		time.Sleep(100 * time.Millisecond)
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("error while receiving response from LongGreet: %v", err)
	}
	fmt.Printf("LongGreet Response: %v\n", res.GetResult())
}

func doBiDiStreaming(c greetpb.GreetServiceClient) {
	fmt.Println("Starting to do a bidi streaming RPC...")

	// Create a stream by invoking the client
	stream, err := c.GreetEveryone(context.Background())
	if err != nil {
		log.Fatalf("error while creating stream: %v", err)
		return
	}

	requests := []*greetpb.GreetEveryoneRequest{
		{
			Greeting: &greetpb.Greeting{
				FirstName: "Manoj",
				LastName:  "Kumar",
			},
		},
		{
			Greeting: &greetpb.Greeting{
				FirstName: "Kanhu",
				LastName:  "Kumar",
			},
		},
		{
			Greeting: &greetpb.Greeting{
				FirstName: "sldfs",
				LastName:  "Kumar",
			},
		},
		{
			Greeting: &greetpb.Greeting{
				FirstName: "Masdfsdfnoj",
				LastName:  "Kumar",
			},
		},
		{
			Greeting: &greetpb.Greeting{
				FirstName: "sdfsdf",
				LastName:  "Kumar",
			},
		},
	}

	waitc := make(chan struct{})
	// send a bunch of message to the server (go routine)
	go func() {
		for _, req := range requests {
			fmt.Printf("Sending request: %v\n", req)
			err := stream.Send(req)
			if err != nil {
				log.Fatalf("Couldn't send request %v", err)
				continue
			}
			time.Sleep(1000 * time.Millisecond)
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
			fmt.Printf("Received %v\n", res.Result)
		}
		close(waitc)
	}()

	// block until everyting is done
	<-waitc
}
