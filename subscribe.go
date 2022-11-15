package main

import (
	"context"
	"log"

	"github.com/go-redis/redis/v8"
	"github.com/hamba/avro"
)

type Movie struct {
	Title     string   `avro:"title"`
	Year      int32    `avro:"year"`
	Rating    float32  `avro:"rating"`
	Horror    bool     `avro:"horror"`
	Actors    []string `avro:"actors"`
	Directors []string `avro:"directors"`
}

var ctx = context.Background()

var redisClient = redis.NewClient(&redis.Options{
	Addr: "localhost:6379",
})

func main() {

	movieAvroSchema := `{
		"type": "record",
		"name": "Movie",
		"fields": [
			{"name": "title", "type": "string"},
			{"name": "year", "type": "int"},
			{"name": "rating", "type": "float"},
			{"name": "horror", "type": "boolean", "default": false},
			{"name": "actors", "type": {"type": "array", "items": "string"}},
			{"name": "directors", "type": {"type": "array", "items": "string"}}
		]
	}`

	schema, err := avro.Parse(movieAvroSchema)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Subscribing to movies channel")
	subscribeMOvie(schema)
	log.Println("Recevied movie from channel")
}

func subscribeMOvie(schema avro.Schema) {
	sub := redisClient.Subscribe(ctx, "movies")

	// we need to get the message from the channel
	msg, err := sub.ReceiveMessage(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// we need to convert the message to byte array
	avroDataFromRedis := []byte(msg.Payload)

	// we need to unmarshal the avro data
	var movieFromRedis Movie
	err = avro.Unmarshal(schema, avroDataFromRedis, &movieFromRedis)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(movieFromRedis)
}
