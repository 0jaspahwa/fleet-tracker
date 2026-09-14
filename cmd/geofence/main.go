package main

import (
	"context"
	"log"
	"math"
	"os"
	"os/signal"
	"time"
	"encoding/json"

	fleetv1 "fleettracker/gen/fleet/v1"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/encoding/protojson"
)


type ZoneEvent struct{ //field name must start with capital letter, in GO lowercase name is unexported
	DriverId string `json:"driver_id"`
	EventType string `json:"event"`
	TimestampMs int64 `json:"ts_ms"`
}

type DriverState struct{
	Inside bool `json:"inside"`
}

// One hardcoded zone, sat in the middle of where the simulator scatters drivers.
const (
	zoneLat    = 28.635
	zoneLng    = 77.225
	zoneRadius = 2500 // metres
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

// func to create map again from the topic->geofence.state 
func restoreState() map[string]bool {
	m := make(map[string]bool)

	for p := 0; p<6; p++{
		conn, err := kafka.DialLeader(context.Background(),"tcp", "127.0.0.1:9092", "geofence.state", p) //connects to the broker, leads the partition
		if err != nil {
			log.Printf("dial partition %d: %v", p, err)
			continue
		}

		last, err := conn.ReadLastOffset() //one request to the broker asking where the partition ends
		if err != nil {
			log.Printf("dial partition %d: %v") //dial partition 3: connection refused
			conn.Close()
			continue
		}

		if _, err := conn.Seek(0, kafka.SeekStart); err != nil {
			log.Printf("seek partition %d: %v", p, err)
			conn.Close()
			continue
		}

		batch := conn.ReadBatch(1, 10e6)
		for{
			msg, err := batch.ReadMessage()
			if err != nil {
				break
			}
			var st DriverState
			if err := json.Unmarshal(msg.Value, &st); err != nil {
				continue
			}
			m[string(msg.Key)] = st.Inside

			if msg.Offset >= last-1 {
				break
			}
		}
		
		batch.Close()
		conn.Close()

	}
	return m
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

		reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"127.0.0.1:9092"},
		Topic:       "location.pings",
		GroupID:     "geofence",
		StartOffset: kafka.FirstOffset,
		//Logger:      kafka.LoggerFunc(log.Printf),		
		ErrorLogger: kafka.LoggerFunc(log.Printf),
	})
	defer reader.Close()

	events := &kafka.Writer{
		Addr: kafka.TCP("127.0.0.1:9092"),
		Topic: "zone.events",
		Balancer: &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}
	defer events.Close()

	state := &kafka.Writer{
		Addr: kafka.TCP("127.0.0.1:9092"),
		Topic: "geofence.state",
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}
	defer state.Close()
	
	
	log.Println("geofence started")

	//call the function check watermark to build again the map


	// create map to store the drivers(inside or outside the zone)
		m := restoreState()
		log.Printf("restored %d drivers", len(m))

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

		// variable for storing the each entry of map into new kafka topic: geofence.state
		st := DriverState{
			Inside: isInside,
		}
		payload, err := json.Marshal(st)
		if err != nil{
			log.Printf("marshal state: %v", err)
			continue
		}
		//publish the values
		if err := state.WriteMessages(ctx, kafka.Message{
			Key: []byte(driverId),
			Value: payload,
		}); err != nil {
			log.Printf("publish state: %v", err)
		}

		switch {
		case !ok:
			log.Printf("never seen %s", driverId)
		case isInside && !wasInside:
			log.Printf("Arrived %s",driverId)
			ev := ZoneEvent{
				DriverId: driverId,
				EventType: "arrived",
				TimestampMs: ping.GetTimestampMs(),
			}
			payload, err := json.Marshal(ev)
			if err != nil{
				log.Printf("marshal zone event: %v", err)
				continue
			}
			if err := events.WriteMessages(ctx, kafka.Message{
				Key: []byte(driverId),
				Value: payload,
			}); err != nil {
				log.Printf("publish zone event: %v", err)
			}
			
		case !isInside && wasInside:
			log.Printf("Departed %s", driverId)
			ev := ZoneEvent{
				DriverId: driverId,
				EventType: "departed",
				TimestampMs: ping.GetTimestampMs(),
			}
			payload, err := json.Marshal(ev)
			if err != nil {
				log.Printf("marshal zone event: %v", err)
				continue
			}
			if err := events.WriteMessages(ctx, kafka.Message{
				Key: []byte(driverId),
				Value: payload,
			}); err != nil {
				log.Printf("publish zone event: %v", err)
			}
		}
		//log.Printf("%s  %v %v  %v %.0fm from zone", ping.GetDriverId(), wasInside, isInside, ok, d)

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit failed: %v", err)
		}
	}
}