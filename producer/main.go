package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
)

type Click struct {
	UserID string `json:"user_id"`
	Page   string `json:"page"`
	TS     int64  `json:"ts"`
}

var pages = []string{"/home", "/cart", "/product", "/checkout"}

func NewClick(r *rand.Rand) Click {
	return Click{
		UserID: "user-" + string(rune('A'+r.Intn(10))),
		Page:   pages[r.Intn(len(pages))],
		TS:     time.Now().UnixMilli(),
	}
}

func main() {
	w := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		Topic:                  "clicks",
		AllowAutoTopicCreation: true,
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	log.Println("Producer started, sending clicks...")
	for {
		b, _ := json.Marshal(NewClick(r))
		err := w.WriteMessages(context.Background(), kafka.Message{Value: b})
		if err != nil {
			log.Println("write error:", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}