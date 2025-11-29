package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"time"

	"github.com/segmentio/kafka-go"
)

// Transaction represents the payment event we are simulating
type Transaction struct {
	TransactionID string  `json:"transaction_id"`
	UserID        int     `json:"user_id"`
	Amount        float64 `json:"amount"`
	Currency      string  `json:"currency"`
	Timestamp     string  `json:"timestamp"`
}

const (
	topic         = "transactions"
	brokerAddress = "localhost:9092" // Connects to the "outside" port of Docker
)

func main() {
	// 1. Set up the Kafka writer
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{brokerAddress},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
	defer w.Close()

	fmt.Println("🚀 Starting Transaction Generator...")
	fmt.Printf("   --> Writing to Kafka topic: %s\n", topic)
	fmt.Println("   --> Press Ctrl+C to stop")

	// 2. Handle graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		for {
			select {
			case <-c:
				fmt.Println("\n🛑 Stopping generator...")
				w.Close()
				os.Exit(0)
			default:
				// 3. Generate a fake transaction
				txn := generateTransaction()

				// 4. Serialize to JSON
				msgBytes, err := json.Marshal(txn)
				if err != nil {
					log.Printf("Error marshalling JSON: %v", err)
					continue
				}

				// 5. Write to Kafka
				err = w.WriteMessages(context.Background(),
					kafka.Message{
						Key:   []byte(fmt.Sprintf("%d", txn.UserID)), // Partition by UserID
						Value: msgBytes,
					},
				)
				if err != nil {
					log.Printf("Failed to write message: %v", err)
				} else {
					fmt.Printf("✅ Sent: User %d | Amount $%.2f\n", txn.UserID, txn.Amount)
				}

				// Simulate traffic (10 transactions per second)
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	// Keep main thread alive
	select {}
}

func generateTransaction() Transaction {
	rand.New(rand.NewSource(time.Now().UnixNano()))

	// Simulate mostly normal users (IDs 1-100)
	userID := rand.Intn(100) + 1
	amount := float64(rand.Intn(10000)) / 100.0 // Random amount $0.00 - $100.00

	// INJECT FRAUD: Every 50th transaction is an outlier
	if rand.Intn(50) == 0 {
		amount = amount * 100 // Sudden spike! e.g., $5000.00
		fmt.Print("⚠️  INJECTING FRAUDULENT PATTERN \n")
	}

	return Transaction{
		TransactionID: fmt.Sprintf("txn_%d", time.Now().UnixNano()),
		UserID:        userID,
		Amount:        amount,
		Currency:      "USD",
		Timestamp:     time.Now().Format(time.RFC3339),
	}
}
