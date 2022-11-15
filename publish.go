package main

import (
	"context"
	"io/ioutil"
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

	// parsing the avro schema
	schema, err := avro.Parse(movieAvroSchema)
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

	// writing the avroData to file
	ioutil.WriteFile("movie.avro", avroData, 0644)

	avroDataString := string(avroData)

	log.Println("Publishing avro data to redis")
	err = redisClient.Publish(ctx, "movies", avroDataString).Err()
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Published avro data to redis")
}
