package main

import (
	"context"
	"encoding/json"
	"log"
	//"math"
	"os"
	"os/signal"
	"time"
	"sync"

	fleetv1 "fleettracker/gen/fleet/v1"
	"fleettracker/internal/geo"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/encoding/protojson"
)

type ZoneEvent struct { //field name must start with capital letter, in GO lowercase name is unexported
	DriverId    string `json:"driver_id"`
	EventType   string `json:"event"`
	TimestampMs int64  `json:"ts_ms"`
}

type DriverState struct {
	Inside bool `json:"inside"`
}





// func to create map again from the topic->geofence.state
func restoreState(partitions []int) map[string]bool {
	m := make(map[string]bool)

	for _, p := range partitions {
		conn, err := kafka.DialLeader(context.Background(), "tcp", "127.0.0.1:9092", "geofence.state", p) //connects to the broker, leads the partition
		if err != nil {
			log.Printf("read last offset partition %d: %v", p, err)
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
		for {
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

	//instead of kafka.Reader 
	// changed it to consumerGroup which is a low level call then .Reader allows to see the partition
	group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:      "geofence",
		Brokers: []string{"127.0.0.1:9092"},
		Topics:  []string{"location.pings"},
		//Logger:  kafka.LoggerFunc(log.Printf),
		ErrorLogger: kafka.LoggerFunc(log.Printf),
	})
	if err != nil {
		log.Fatalf("create consumer group: %v", err)
	}
	
	defer group.Close()

	events := &kafka.Writer{
		Addr:         kafka.TCP("127.0.0.1:9092"),
		Topic:        "zone.events",
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}
	defer events.Close()

	state := &kafka.Writer{
		Addr:         kafka.TCP("127.0.0.1:9092"),
		Topic:        "geofence.state",
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}
	defer state.Close()

	log.Println("geofence started")

	//call the function check watermark to build again the map

	// create map to store the drivers(inside or outside the zone)
	//m := restoreState()
	//log.Printf("restored %d drivers", len(m))

	

	for {
		gen, err := group.Next(ctx) //group.next is call gets whos in the group and which partitions u own
		if err != nil {
			log.Printf("stopping: %v", err)
			return
		}

		log.Printf("generation %d assigned: %v", gen.ID, gen.Assignments["location.pings"])

		// restores the drivers according to there partitions
		var partitions []int
		//assignment is kafka telling one member of group: u own this partition
		for _, a := range gen.Assignments["location.pings"]{ // each assignment is a struct with .Partition and .Offset
			//to read the messages
			partitions = append(partitions, a.ID)
		}

		m:= restoreState(partitions)
		var mu sync.Mutex
		log.Printf("generation %d restored %d drivers for partitions %v", gen.ID, len(m), partitions)

		for _, a := range gen.Assignments["location.pings"]{
			partition, offset := a.ID, a.Offset

			//start a go routine with lock (each go routine for each partition) 
			gen.Start(func(ctx context.Context){
				reader := kafka.NewReader(kafka.ReaderConfig{
					Brokers: []string{"127.0.0.1:9092"},
					Topic: "location.pings",
					Partition: partition,
				})
				defer reader.Close()
				reader.SetOffset(offset)

				for {
					msg, err := reader.FetchMessage(ctx)
					if err != nil{
						return
					}

					var ping fleetv1.StreamLocationRequest
					if err := protojson.Unmarshal(msg.Value, &ping); err != nil {
						log.Printf("bad message p=%d offset=%d: %v", msg.Partition, msg.Offset, err)
						continue
					}

					driverID := ping.GetDriverId()
					d := geo.DistanceToZone(ping.GetLatitude(),ping.GetLongitude())
					isInside := d <= geo.ZoneRadius
					
					//when multiple go routines write to a map, we have to do this using lock then
					mu.Lock()
					wasInside, ok := m[driverID]
					m[driverID] = isInside
					mu.Unlock()

					if !ok || isInside != wasInside {
						st := DriverState{Inside: isInside}
						payload, err := json.Marshal(st)
						if err != nil {
							log.Printf("marshal state: %v", err)
							continue
						}
						if err := state.WriteMessages(ctx, kafka.Message{
							Key:   []byte(driverID),
							Value: payload,
						}); err != nil {
							log.Printf("publish state: %v", err)
						}
					}

					switch {
					case !ok:
						log.Printf("never seen %s", driverID)
					case isInside && !wasInside:
						log.Printf("Arrived %s", driverID)
						ev := ZoneEvent{
							DriverId:    driverID,
							EventType:   "arrived",
							TimestampMs: ping.GetTimestampMs(),
						}
						payload, err := json.Marshal(ev)
						if err != nil {
							log.Printf("marshal zone event: %v", err)
							continue
						}
						if err := events.WriteMessages(ctx, kafka.Message{
							Key:   []byte(driverID),
							Value: payload,
						}); err != nil {
							log.Printf("publish zone event: %v", err)
						}

					case !isInside && wasInside:
						log.Printf("Departed %s", driverID)
						ev := ZoneEvent{
							DriverId:    driverID,
							EventType:   "departed",
							TimestampMs: ping.GetTimestampMs(),
						}
						payload, err := json.Marshal(ev)
						if err != nil {
							log.Printf("marshal zone event: %v", err)
							continue
						}
						if err := events.WriteMessages(ctx, kafka.Message{
							Key:   []byte(driverID),
							Value: payload,
						}); err != nil {
							log.Printf("publish zone event: %v", err)
						}
					}

					//as go routines have no groupID, they are plain partitions readers no membership no offset tracking, they cant commit
					gen.CommitOffsets(map[string]map[int]int64{  //opic name → partition number → offset.
						"location.pings": {partition: msg.Offset +1},
					})
				}
			}) 

		}	
	}
}
