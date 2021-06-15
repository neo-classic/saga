package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/go-redis/redis"
	"github.com/neo-classic/saga/domain"
	"syreclabs.com/go/faker"
)

// Orchestrator orchestrates the order processing task
type Orchestrator struct {
	c *redis.Client
	r *redis.PubSub
}

func main() {
	ctx := context.Background()
	var err error
	// create client and ping redis
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	if _, err = client.Ping(ctx).Result(); err != nil {
		log.Fatalf("error creating redis client %s", err)
	}

	// initialize and start the orchestrator in the background
	o := &Orchestrator{
		c: client,
		r: client.Subscribe(ctx, domain.PaymentChannel, domain.OrderChannel, domain.DeliveryChannel, domain.RestaurantChannel, domain.ReplyChannel),
	}
	go o.start(ctx)

	// define ServeMux and start the server
	mux := http.NewServeMux()
	mux.HandleFunc("/create", o.create)
	log.Println("starting server")

	log.Fatal(http.ListenAndServe(":8080", mux))
}

// create the order - step 1 in the order processing pipeline
func (o Orchestrator) create(writer http.ResponseWriter, request *http.Request) {
	ctx := context.Background()
	if _, err := fmt.Fprintf(writer, "responding"); err != nil {
		log.Printf("error while writing %s", err.Error())
	}
	m := domain.Message{ID: faker.Bitcoin().Address(), Message: "Something"}
	o.next(ctx, domain.OrderChannel, domain.ServiceOrder, m)
}

// start the goroutine to orchestrate the process
func (o Orchestrator) start(ctx context.Context) {
	var err error
	if _, err = o.r.Receive(ctx); err != nil {
		log.Fatalf("error setting up redis %s \n", err)
	}
	ch := o.r.Channel()
	defer func() { _ = o.r.Close() }()

	log.Println("starting the redis client")
	for {
		select {
		case msg := <-ch:
			m := domain.Message{}
			if err = json.Unmarshal([]byte(msg.Payload), &m); err != nil {
				log.Println(err)
				// continue to skip bad messages
				continue
			}

			// only process the messages on ReplyChannel
			switch msg.Channel {
			case domain.ReplyChannel:
				// if there is any error, just rollback
				if m.Action != domain.ActionDone {
					log.Printf("Rolling back transaction with id %s", m.ID)
					o.rollback(ctx, m)
					continue
				}

				// else start the next stage
				switch m.Service {
				case domain.ServiceOrder:
					o.next(ctx, domain.PaymentChannel, domain.ServicePayment, m)
				case domain.ServicePayment:
					o.next(ctx, domain.RestaurantChannel, domain.ServiceRestaurant, m)
				case domain.ServiceRestaurant:
					o.next(ctx, domain.DeliveryChannel, domain.ServiceDelivery, m)
				case domain.ServiceDelivery:
					log.Println("Food Delivered")
				}
			}
		}
	}
}

// next triggers start operation on the other micro-services
// based on the channel and service
func (o Orchestrator) next(ctx context.Context, channel, service string, message domain.Message) {
	var err error
	message.Action = domain.ActionStart
	message.Service = service
	if err = o.c.Publish(ctx, channel, message).Err(); err != nil {
		log.Printf("error publishing start-message to %s channel", channel)
	}
	log.Printf("start message published to channel :%s", channel)
}

// rollback instructs the other micro-services to rollback the transaction
func (o Orchestrator) rollback(ctx context.Context, m domain.Message) {
	var err error
	message := domain.Message{
		ID:      m.ID, // ID is mandatory
		Action:  domain.ActionRollback,
		Message: "May day !! May day!!",
	}
	if err = o.c.Publish(ctx, domain.OrderChannel, message).Err(); err != nil {
		log.Printf("error publishing rollback message to %s channel", domain.OrderChannel)
	}
	if err = o.c.Publish(ctx, domain.PaymentChannel, message).Err(); err != nil {
		log.Printf("error publishing rollback message to %s channel", domain.PaymentChannel)
	}
	if err = o.c.Publish(ctx, domain.RestaurantChannel, message).Err(); err != nil {
		log.Printf("error publishing rollback message to %s channel", domain.RestaurantChannel)
	}
	if err = o.c.Publish(ctx, domain.DeliveryChannel, message).Err(); err != nil {
		log.Printf("error publishing rollback message to %s channel", domain.DeliveryChannel)
	}
}
