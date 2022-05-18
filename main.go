package main

import (
	"broker/broker"
	"broker/core"
	"broker/proto"
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
)

// var wg sync.WaitGroup
var opts []grpc.ServerOption

type server struct{}

func (*server) Publish(ctx context.Context, req *proto.PubReq) (*proto.PubRes, error) {
	// fmt.Println(req.GetDateTime(), req.GetMessage(), req.GetTitle())
	err := broker.Publish(req.GetFromPub(), req.GetToQueue(), req.GetMessage())
	if err != nil {
		return &proto.PubRes{Title: "Error in Publish"}, err
	}

	return &proto.PubRes{Title: "Success in Publish"}, nil

}

func (*server) UnSubscribe(ctx context.Context, req *proto.UnSubReq) (*proto.UnSubRes, error) {
	err := broker.Unsubscribe(req.GetTitle())

	if err != nil {
		return &proto.UnSubRes{Message: "Error in UnSubscribe"}, err
	}
	return &proto.UnSubRes{Message: "Success in UnSubscribe"}, nil
}

func (*server) Subscribe(ctx context.Context, req *proto.SubReq) (*proto.SubRes, error) {
	msg, err := broker.Subscribe(req.GetTitle(), req.GetAdress())

	if err != nil {
		return &proto.SubRes{Message: msg}, err
	}
	return &proto.SubRes{Message: msg}, nil
}

func (*server) PublishGetResult(ctx context.Context, req *proto.PubGetReq) (*proto.PubGetRes, error) {
	fmt.Println("Recieving response")
	msg, err := broker.GetResult(req.GetFromPub(), req.GetToQueue())

	if err != nil {
		return &proto.PubGetRes{Message: msg, DateTime: time.Now().Unix()}, err
	}
	return &proto.PubGetRes{Message: msg, DateTime: time.Now().Unix()}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":3010")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	serv := grpc.NewServer(opts...)
	proto.RegisterReverseServer(serv, &server{})
	fmt.Println("Starting Message broker...")
	go core.SendingDataToSubs()

	if err := serv.Serve(lis); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}

}
