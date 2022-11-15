package main

import (
	"log"
	"time"

	"github.com/go-redis/redis"
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

var Schema = `{
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

var redisClient = redis.NewClient(&redis.Options{
	Addr: "localhost:6379",
})

func main() {

	// parsing the avro schema
	schema, err := avro.Parse(Schema)
	if err != nil {
		log.Fatal(err)
	}

	movie := Movie{
		Title:  "The Matrix",
		Year:   1999,
		Rating: 8.7,
		Horror: false,
		Actors: []string{
			"Keanu Reeves",
			"Laurence Fishburne",
			"Carrie-Anne Moss",
		},
		Directors: []string{
			"Lana Wachowski",
			"Lilly Wachowski",
		},
	}

	// Marshal the movie to avro
	avroData, err := avro.Marshal(schema, movie)
	if err != nil {
		log.Fatal(err)
	}

	// sending avrodata to redis stream using redis client and redis stream client
	for i := 0; i < 1000; i++ {
		err := publishMovieEvent(avroData)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func publishMovieEvent(avroData []byte) error {
	err := redisClient.XAdd(&redis.XAddArgs{
		Stream: "movies",
		Values: map[string]interface{}{
			"movie": avroData,
		},
	}).Err()

	log.Println("Movie published to stream")

	// sleep for 5 seconds
	time.Sleep(5 * time.Second)
	return err
}
