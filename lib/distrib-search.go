package lib

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log"

	"cloud.google.com/go/pubsub"
	"github.com/cem-okulmus/BalancedGo/lib"
)

// DistributedSearch implements a search module that distributes the search for separators to
// some number of external nodes, possibly in a distributed or cloud environment
type DistributedSearch struct {
	H               *lib.Graph
	Edges           *lib.Edges
	BalFactor       int
	Result          []int
	Generators      []lib.Generator
	ExhaustedSearch bool
}

// DistSearchGen is needed to use the DistributedSearch module for the search
type DistSearchGen struct{}

// GetSearch produces the corresponding Search interface of the DistributedSearch module
func (dg DistSearchGen) GetSearch(H *lib.Graph, Edges *lib.Edges, BalFactor int, Gens []lib.Generator) lib.Search {
	return &DistributedSearch{
		H:               H,
		Edges:           Edges,
		BalFactor:       BalFactor,
		Result:          []int{},
		Generators:      Gens,
		ExhaustedSearch: false,
	}
}

// A Request sent over pubsub to the workers
type Request struct {
	Subgraph   lib.Graph
	Predicate  lib.Predicate
	Width      int
	Index      int
	NumWorkers int
}

// A Solution is the result sent back by the workers
type Solution struct {
	Valid     bool  // true if a solution found, false if not
	Selection []int // the selection of edges to form the separator, empty if valid is false
}

// TODO
//  * set up a github repo, to ensure cloud function can be properly uploaded
//  * use some golang encoding function to (de)serialise the structs into []byte
//  * write a cloud function to handle incoming requests (look at pubsubtest for this)
//  * write the distributed search below to send requests
//

// FindNext starts the search and stops if some separator which satisfies the predicate
// is found, or if the entire search space has been exhausted
func (d *DistributedSearch) FindNext(pred lib.Predicate) {

}

// SearchEnded returns true if search is completed
func (d *DistributedSearch) SearchEnded() bool {
	return d.ExhaustedSearch
}

// GetResult returns the last found result
func (d *DistributedSearch) GetResult() []int {
	return d.Result
}

// PubSubMessage is the payload of a Pub/Sub event.
// See the documentation for more details:
// https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
type PubSubMessage struct {
	Data []byte `json:"data"`
}

// WorkerDistributedSearch replies to a request
func WorkerDistributedSearch(ctx context.Context, m PubSubMessage) error {
	var buffer bytes.Buffer
	dec := gob.NewDecoder(&buffer)
	buffer.Read(m.Data)
	// name := string(m.Data) // Automatically decoded from base64.
	var request Request
	err := dec.Decode(&request) // parse the incoming byte slice

	if err != nil {
		fmt.Println("Decode error")
		return nil
	}

	// create the generator based on info in the request

	// let it run to completion, and then send back the Solution struct

	// Sets your Google Cloud Platform project ID.
	projectID := "hgtest-1"

	// Creates a client.

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	topic := client.Topic("answerTopic") // try to create topic

	result := topic.Publish(ctx, &pubsub.Message{
		Data: []byte("Request received"),
	})

	_, err = result.Get(ctx)
	if err != nil {
		return fmt.Errorf("Get: %v", err)
	}

	return nil
}
