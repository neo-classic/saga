package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/neo-classic/saga/domain"
	"syreclabs.com/go/faker"
)

func main() {
	ctx := context.Background()
	// create client and ping redis
	var err error
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: "", DB: 0})
	if _, err = client.Ping(ctx).Result(); err != nil {
		log.Fatalf("error creating redis client %s", err)
	}

	// subscribe to the required channels
	pubsub := client.Subscribe(ctx, domain.OrderChannel, domain.ReplyChannel)
	if _, err = pubsub.Receive(ctx); err != nil {
		log.Fatalf("error subscribing %s", err)
	}
	defer func() { _ = pubsub.Close() }()
	ch := pubsub.Channel()

	log.Println("starting the order service")
	for {
		select {
		case msg := <-ch:
			m := domain.Message{}
			err := json.Unmarshal([]byte(msg.Payload), &m)
			if err != nil {
				log.Println(err)
				continue
			}

			switch msg.Channel {
			case domain.OrderChannel:
				log.Printf("recieved message with id %s ", m.ID)

				// random sleep to simulate some work in action
				d := faker.RandomInt(1, 3)
				time.Sleep(time.Duration(d) * time.Second)

				// Happy Flow
				if m.Action == domain.ActionStart {
					m.Action = domain.ActionDone
					if err = client.Publish(ctx, domain.ReplyChannel, m).Err(); err != nil {
						log.Printf("error publishing done-message to %s channel", domain.ReplyChannel)
					}
					log.Printf("done message published to channel :%s", domain.ReplyChannel)
				}

				// Rollback flow
				if m.Action == domain.ActionRollback {
					log.Printf("rolling back transaction with ID :%s", m.ID)
				}

			}
		}
	}

}
