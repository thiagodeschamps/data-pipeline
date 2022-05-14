// Example function-based Apache Kafka producer
package main

/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type response1 struct {
	User_id int
	Movie   string
	Score   int
}

func main() {

	// if len(os.Args) != 3 {
	// 	fmt.Fprintf(os.Stderr, "Usage: %s <broker> <topic>\n",
	// 		os.Args[0])
	// 	os.Exit(1)
	// }

	// broker := os.Args[1]
	// topic := os.Args[2]

	movies := make([]string, 0)
	movies = append(movies,
		"The Shawshank Redemption (1994)",
		"The Godfather (1972)",
		"The Dark Knight (2008)",
		"The Lord of the Rings (2003)",
		"Pulp Fiction (1994)",
		"Forrest Gump (1994)",
		"Fight Club (1999)",
		"Inception (2010)",
		"Star Wars (1980)",
		"The Matrix (1999)")

	i := 1
	topic := "messages"
	broker := "host.docker.internal:9094"

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Producer %v\n", p)

	// Optional delivery channel, if not specified the Producer object's
	// .Events channel is used.
	deliveryChan := make(chan kafka.Event)

	for i < 1000 {
		res1D := &response1{
			User_id: i,
			Movie:   fmt.Sprintf(movies[rand.Intn(len(movies))]),
			Score:   rand.Intn(11),
		}
		msg, _ := json.Marshal(res1D)
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic},
			Value:          msg,
			Key:            []byte("Go"),
		}, deliveryChan)

		e := <-deliveryChan
		m := e.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		} else {
			fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
				*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
		}
		i++
	}

	close(deliveryChan)
}
