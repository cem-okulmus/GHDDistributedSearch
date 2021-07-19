package cloudfunction

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log"

	"cloud.google.com/go/pubsub"
	"github.com/cem-okulmus/BalancedGo/lib"

	cloudlib "github.com/cem-okulmus/GHDDistributedSearch/lib"
)

// PubSubMessage is the payload of a Pub/Sub event.
// See the documentation for more details:
// https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
type PubSubMessage struct {
	Data []byte `json:"data"`
}

// WorkerDistributedSearch replies to a request
func WorkerDistributedSearch(ctx context.Context, m PubSubMessage) error {
	buffer := bytes.NewBuffer(m.Data)
	dec := gob.NewDecoder(buffer)

	var request cloudlib.Request

	var pred lib.Predicate
	var genExample lib.Generator

	pred = lib.BalancedCheck{}
	genExample = &lib.CombinationIterator{}

	gob.Register(pred)

	pred = lib.ParentCheck{}
	gob.Register(pred)

	gob.Register(genExample)

	err := dec.Decode(&request) // parse the incoming byte slice

	if err != nil {
		fmt.Println("Read input from message,", m.Data)
		fmt.Println("Decode error", err)
		return nil
	}

	// fmt.Println("Allowed edges received, and order: ")

	// for _, e := range request.Edges.Slice() {
	// 	fmt.Println(e.Name, "(", (e.Vertices), ")")
	// }

	// create the generator based on info in the request

	gen := request.Gen

	// let it run to completion, and then send back the Solution struct
	var solution []int

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered from panic: ", r)

			fmt.Print("Current request type: ")
			fmt.Println("Subgraph length: ", request.Subgraph.Edges.Len())
			fmt.Println("gen last returned sep:", request.Gen.GetNext())
		}
	}()

	var sep lib.Edges

	for gen.HasNext() && len(solution) == 0 {
		// j := make([]int, len(gen.Combination))
		// copy(gen.Combination, j)
		j := gen.GetNext()

		sep = lib.GetSubset(request.Edges, j) // check new possible sep

		if request.Predicate.Check(&request.Subgraph, &sep, request.BalFactor) {
			gen.Found() // cache result

			solution = make([]int, len(j))
			copy(solution, j)
			// log.Printf("Worker %d \" won \"", workernum)
			// gen.Confirm()

		}
		gen.Confirm()
	}

	// Sets your Google Cloud Platform project ID.
	projectID := "hgtest-1"

	// Creates a client.

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	topic := client.Topic("answerTopic") // try to create topic

	sol := cloudlib.Solution{
		Valid:     len(solution) > 0,
		Selection: solution,
		Gen:       gen,
		ID:        request.ID,
	}
	fmt.Println("Passing on the ID: ", sol.ID)

	// encode the solution into []byte
	var Encodebuffer bytes.Buffer
	enc := gob.NewEncoder(&Encodebuffer)

	err = enc.Encode(sol)
	if err != nil {
		log.Fatal("Encoding error: ", err)
	}

	result := topic.Publish(ctx, &pubsub.Message{
		Data: Encodebuffer.Bytes(),
	})

	_, err = result.Get(ctx)
	if err != nil {
		return fmt.Errorf("Get: %v", err)
	}

	// fmt.Print("Send the solution", sol)

	// if len(solution) > 0 {
	// 	fmt.Print("Solution vertices: ", sep.Vertices())
	// }

	return nil
}
