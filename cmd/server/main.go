package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	fleetv1 "fleettracker/gen/fleet/v1"

	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

// Our server. The embedded struct is required by the generated code.
type server struct {
	fleetv1.UnimplementedFleetServiceServer
	events *kafka.Writer
}

func (s *server) CreateTrip(
	ctx context.Context,
	req *fleetv1.CreateTripRequest,
) (*fleetv1.CreateTripResponse, error) {

	trip := &fleetv1.Trip{
		TripId:         fmt.Sprintf("trip-%d", time.Now().UnixNano()),
		DriverId:       req.GetDriverId(),
		PickupAddress:  req.GetPickupAddress(),
		DropoffAddress: req.GetDropoffAddress(),
	}

	payload, err := protojson.Marshal(trip)
	if err != nil {
		// Our bug. Retrying will not help.
		return nil, status.Errorf(codes.Internal, "encode trip: %v", err)
	}

	// Key by driver id, so one driver's events stay in order.
	err = s.events.WriteMessages(ctx, kafka.Message{
		Key:   []byte(trip.DriverId),
		Value: payload,
	})
	if err != nil {
		// Kafka is down. Retrying might work.
		return nil, status.Errorf(codes.Unavailable, "publish trip.created: %v", err)
	}

	log.Printf("CreateTrip  trip_id=%s  driver_id=%s  published", trip.TripId, trip.DriverId)

	return &fleetv1.CreateTripResponse{Trip: trip}, nil
}

func main() {
	writer := &kafka.Writer{
		Addr:  kafka.TCP("localhost:9092"),
		Topic: "trip.created",
		// Pick the partition from the key.
		Balancer: &kafka.Hash{},
		// Only count the write as done once the broker confirms it.
		RequiredAcks: kafka.RequireAll,
		// Default is 1 second, too slow when sending one at a time.
		BatchTimeout: 10 * time.Millisecond,
	}
	defer writer.Close()

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	s := grpc.NewServer()
	fleetv1.RegisterFleetServiceServer(s, &server{events: writer})

	// Lets tools ask what this server can do, so they do not need the proto file.
	reflection.Register(s)

	log.Println("gRPC server listening on :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
