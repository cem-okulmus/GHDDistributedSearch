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
	Width           int
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
	BalFactor  int
}

// A Solution is the result sent back by the workers
type Solution struct {
	Valid     bool  // true if a solution found, false if not
	Selection []int // the selection of edges to form the separator, empty if valid is false
}

// TODO
//  * set up a github repo, to ensure cloud function can be properly uploaded (done)
//  * use some golang encoding function to (de)serialise the structs into []byte  (done)
//  * write a cloud function to handle incoming requests (look at pubsubtest for this) (done)
//  * write the distributed search below to send requests
//

// FindNext starts the search and stops if some separator which satisfies the predicate
// is found, or if the entire search space has been exhausted
func (d *DistributedSearch) FindNext(pred lib.Predicate) {

	// set up request struct

	req := Request{
		Subgraph:   *d.H,
		Predicate:  pred,
		Width:      d.Width,
		Index:      1,
		NumWorkers: 1,
		BalFactor:  d.BalFactor,
	}

	// Set up connection to the topic
	var buffer bytes.Buffer
	dec := gob.NewDecoder(&buffer)

	var Encodebuffer bytes.Buffer
	enc := gob.NewEncoder(&Encodebuffer)

	ctx := context.Background()

	// Sets your Google Cloud Platform project ID.
	projectID := "hgtest-1"

	fmt.Println("Creating a client.")

	// Creates a client.
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	fmt.Println("Client created.")

	// Sets the id for the new topic.
	topicID := "workerTopic"
	topic := client.Topic(topicID)

	topicID2 := "answerTopic"
	topic2, err := client.CreateTopic(ctx, topicID2)
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}

	fmt.Println("writing to topic")

	// Publish "hello world" on topic1.

	err = enc.Encode(req)
	if err != nil {
		log.Fatal("encode error", err)
	}

	res := topic.Publish(ctx, &pubsub.Message{
		Data: buffer.Bytes(),
	})

	// The publish happens asynchronously.
	// Later, you can get the result from res:

	_, err = res.Get(ctx)
	if err != nil {
		log.Fatal(err)
	}

	var sol Solution

	fmt.Println("Read something from sub")
	sub, err := client.CreateSubscription(context.Background(), "testSub",
		pubsub.SubscriptionConfig{Topic: topic2})
	if err != nil {
		log.Println(err)
	}

	cctx, cancel := context.WithCancel(ctx)
	err = sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {

		err = dec.Decode(&sol)

		if err != nil {
			log.Fatal("decode error 1:", err)
		}

		msg.Ack()

		cancel() // cancel right after receiving the first message

	})

	d.Result = sol.Selection // set up the current result to the found value
	// d.ExhaustedSearch
	// TODO how to ensure the search space is not repeated?

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

	numEdges := request.Subgraph.Len()

	gen := lib.SplitCombin(numEdges, request.Width, request.NumWorkers, false)[request.Index] // this is the dumbest way to get only a single generator out of this

	// let it run to completion, and then send back the Solution struct
	var solution []int

	for gen.HasNext() {
		// j := make([]int, len(gen.Combination))
		// copy(gen.Combination, j)
		j := gen.GetNext()

		sep := lib.GetSubset(request.Subgraph.Edges, j) // check new possible sep
		if request.Predicate.Check(&request.Subgraph, &sep, request.BalFactor) {
			gen.Found() // cache result
			solution = j
			// log.Printf("Worker %d \" won \"", workernum)
			gen.Confirm()

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

	sol := Solution{
		Valid:     true,
		Selection: solution,
	}

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

	return nil
}
