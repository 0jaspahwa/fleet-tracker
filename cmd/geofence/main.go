package main

import (
	"context"
	"log"
	"math"
	"os"
	"os/signal"

	fleetv1 "fleettracker/gen/fleet/v1"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/encoding/protojson"
)

// One hardcoded zone, sat in the middle of where the simulator scatters drivers.
const (
	zoneLat    = 28.635
	zoneLng    = 77.225
	zoneRadius = 8000 // metres
)

// Metres per degree near Delhi. Flat-earth maths, fine over a few km.
const (
	metresPerLat = 111320.0
	metresPerLng = 97700.0
)

func distanceToZone(lat, lng float64) float64 {
	dLat := (lat - zoneLat) * metresPerLat
	dLng := (lng - zoneLng) * metresPerLng
	return math.Hypot(dLat, dLng)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

		reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"127.0.0.1:9092"},
		Topic:       "location.pings",
		GroupID:     "geofence",
		StartOffset: kafka.FirstOffset,
		ErrorLogger: kafka.LoggerFunc(log.Printf),
	})
	defer reader.Close()

	log.Println("geofence started")

	// create map to store the drivers(inside or outside the zone)
	m := make(map[string]bool)


	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("stopping: %v", err)
			return
		}

		var ping fleetv1.StreamLocationRequest
		if err := protojson.Unmarshal(msg.Value, &ping); err != nil {
			log.Printf("bad message p=%d offset=%d: %v", msg.Partition, msg.Offset, err)
			continue
		}
		driverId := ping.GetDriverId() //get driver id and store it in map
		d := distanceToZone(ping.GetLatitude(), ping.GetLongitude())
		isInside := d <= zoneRadius

		wasInside, ok := m[driverId] //read from map
		m[driverId] = isInside
		
		switch {
		case !ok:
			log.Printf("never seen %s", driverId)
		case isInside && !wasInside:
			log.Printf("Arrived %s",driverId)
		case !isInside && wasInside:
			log.Printf("Departed %s", driverId)
		}
		//log.Printf("%s  %v %v  %v %.0fm from zone", ping.GetDriverId(), wasInside, isInside, ok, d)

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit failed: %v", err)
		}
	}
}