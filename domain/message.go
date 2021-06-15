package domain

import "encoding/json"

const (
	PaymentChannel    string = "PaymentChannel"
	OrderChannel      string = "OrderChannel"
	DeliveryChannel   string = "DeliveryChannel"
	RestaurantChannel string = "RestaurantChannel"
	ReplyChannel      string = "ReplyChannel"

	ServicePayment    string = "Payment"
	ServiceOrder      string = "Order"
	ServiceRestaurant string = "Restaurant"
	ServiceDelivery   string = "Delivery"

	ActionStart    string = "Start"
	ActionDone     string = "DoneMsg"
	ActionError    string = "ErrorMsg"
	ActionRollback string = "RollbackMsg"
)

// Message represents the payload sent over redis pub/sub
type Message struct {
	ID      string `json:"id"`
	Service string `json:"service"`
	Action  string `json:"action"`
	Message string `json:"message"`
}

// MarshalBinary should be implemented to send message to redis
func (m Message) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}
