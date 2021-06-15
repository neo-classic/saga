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
	var err error

	// create client and ping redis
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", Password: "", DB: 0})
	if _, err = client.Ping(ctx).Result(); err != nil {
		log.Fatalf("error creating redis client %s", err)
	}

	// subscribe to the required channels
	pubsub := client.Subscribe(ctx, domain.DeliveryChannel, domain.ReplyChannel)
	if _, err = pubsub.Receive(ctx); err != nil {
		log.Fatalf("error subscribing %s", err)
	}
	defer func() { _ = pubsub.Close() }()

	ch := pubsub.Channel()
	log.Println("starting the delivery service")
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
			case domain.DeliveryChannel:
				// random sleep to simulate some work in action
				log.Printf("recieved message with id %s ", m.ID)
				d := faker.RandomInt(1, 3)
				time.Sleep(time.Duration(d) * time.Second)

				// IMPORTANT : To demonstrate a rollback, we send the Action as Error to the orchestrator
				// once orchestrator receives this error message, it asks all the services to rollback
				if m.Action == domain.ActionStart {
					m.Action = domain.ActionError // To simulate an error or a failure in the process
					m.Service = domain.ServiceDelivery
					log.Printf("delivery message is %#v", m)
					if err = client.Publish(ctx, domain.ReplyChannel, m).Err(); err != nil {
						log.Printf("error publishing error-message to %s channel %s", domain.ReplyChannel, err)
					}
					log.Printf("error message published to channel :%s", domain.ReplyChannel)
				}

				if m.Action == domain.ActionRollback {
					log.Printf("rolling back transaction with ID :%s", m.ID)
				}

			}
		}
	}
}
