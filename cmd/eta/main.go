package main

import (
	"context"
	//"encoding/json"
	"log"
	//"math"
	"os"
	"os/signal"
	//"time"

	fleetv1 "fleettracker/gen/fleet/v1"
	"fleettracker/internal/geo"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/encoding/protojson"
)

type Point struct {
	Latitude float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	TimestampMs int64 `json:"ts_ms"`
}
// we need something to hold the history of driver(pings) because if use map -> unordered sequence of the driver
// we need ordered sequence -> using SLICE -> kinda same like a vector

// window size for slicing
const windowSize = 5


func timeBetween(oldMs, newMs int64) int64{
	return newMs - oldMs
}


func main(){
	//ctx -> letting the fucn know when to stop
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	
	//read the driver pings form the topic location pings
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"127.0.0.1:9092"},
		Topic: "location.pings",
		GroupID: "eta",
		StartOffset: kafka.FirstOffset,
		ErrorLogger: kafka.LoggerFunc(log.Printf),
	})
	defer reader.Close()

	// we need to store the driver and its history
	m := make(map[string][]Point)	
	
	
	for{
		//read the message from the ping
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
		driverId := ping.GetDriverId()
		latitude := ping.GetLatitude()
		longitude := ping.GetLongitude()



		pt := Point{
			Latitude:    latitude,
			Longitude:   longitude,
			TimestampMs: ping.GetTimestampMs(),
		}

		//storing in the map
		m[driverId] = append(m[driverId], pt)
		
		//slicing the window to the N size cap
		if len(m[driverId]) > windowSize{
			m[driverId] = m[driverId][1:] //takes the slice from index 1 to the end
		}
		//distance of the points
		pts := m[driverId]
		if len(pts) >= 2{
			oldest := pts[0]
			newest := pts[len(pts)-1]

			// for distance
			d := geo.DistanceBetween(oldest.Latitude, oldest.Longitude, newest.Latitude, newest.Longitude)

			//for time
			t := timeBetween(oldest.TimestampMs, newest.TimestampMs);
			tSeconds := float64(t)/1000

			//speed
			speed := d / tSeconds

			//remaining distance
			rd := geo.DistanceToZone(newest.Latitude, newest.Longitude)

			//seconds to arrive
			sa:= rd/speed
			log.Printf("%s speed %.0f m/s, %.0f m remaining, eta %.0f sec", driverId, speed, rd, sa)
			//log.Printf("%s has %.0f distance %d ms elasped %.0f speed", driverId, d, t, speed)
		}
		
		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit failed: %v", err)
		}
	}
	

}
