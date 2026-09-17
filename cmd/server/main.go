package main

import (
	"context"
	"fmt"
	"io"
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
	events *kafka.Writer // trip.created
	pings  *kafka.Writer // location.pings
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


// The driver holds one connection open and pushes pings down it.
// We read until they hang up, then reply once with a summary.
func (s *server) StreamLocation(
	stream grpc.ClientStreamingServer[fleetv1.StreamLocationRequest, fleetv1.StreamLocationResponse],
) error {

	start := time.Now()
	var count int32

	for {
		ping, err := stream.Recv()

		if err == io.EOF {
			// Driver closed the stream cleanly. Send the summary and finish.
			log.Printf("StreamLocation  ended  pings=%d", count)
			return stream.SendAndClose(&fleetv1.StreamLocationResponse{
				PingsReceived: count,
				DurationMs:    time.Since(start).Milliseconds(),
			})
		}
		if err != nil {
			// Driver vanished mid-shift. Not clean, but not our bug.
			log.Printf("StreamLocation  broke after %d pings: %v", count, err)
			return err
		}

		payload, err := protojson.Marshal(ping)
		if err != nil {
			return status.Errorf(codes.Internal, "encode ping: %v", err)
		}

		// Same keying rule. All of one driver's pings land on one partition.
		err = s.pings.WriteMessages(stream.Context(), kafka.Message{
			Key:   []byte(ping.GetDriverId()),
			Value: payload,
		})
		if err != nil {
			return status.Errorf(codes.Unavailable, "publish ping: %v", err)
		}

		count++
	}
}

func (s *server) WatchTrip(
	req *fleetv1.WatchTripRequest,
	stream grpc.ServerStreamingServer[fleetv1.WatchTripResponse],
) error {
	tripID := req.GetTripId()
	log.Printf("WatchTrip started trip_id%s", tripID)

	lat := 28.61
	lng := 77.20

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stream.Context().Done():
			log.Printf("WatchTrip  ended  trip_id=%s", tripID)
			return nil

		case <-ticker.C:
			lat += 0.0005
			lng += 0.0003

			err := stream.Send(&fleetv1.WatchTripResponse{
				TripId:      tripID,
				Latitude:    lat,
				Longitude:   lng,
				TimestampMs: time.Now().UnixMilli(),
			})
			if err != nil {
				log.Printf("WatchTrip  send failed  trip_id=%s: %v", tripID, err)
				return err
			}
		}
	}
}

func main() {
	writer := &kafka.Writer{
		Addr:  kafka.TCP("127.0.0.1:9092"),
		Topic: "trip.created",
		// Pick the partition from the key.
		Balancer: &kafka.Hash{},
		// Only count the write as done once the broker confirms it.
		RequiredAcks: kafka.RequireAll,
		// Default is 1 second, too slow when sending one at a time.
		BatchTimeout: 10 * time.Millisecond,
	}
	defer writer.Close()

	pings := &kafka.Writer{
		Addr:         kafka.TCP("127.0.0.1:9092"),
		Topic:        "location.pings",
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}
	defer pings.Close()

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	s := grpc.NewServer()
	fleetv1.RegisterFleetServiceServer(s, &server{events: writer, pings: pings})

	// Lets tools ask what this server can do, so they do not need the proto file.
	reflection.Register(s)

	log.Println("gRPC server listening on :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
