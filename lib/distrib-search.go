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
	H               lib.Graph
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
		H:               *H,
		Edges:           Edges,
		BalFactor:       BalFactor,
		Result:          []int{},
		Generators:      Gens,
		ExhaustedSearch: false,
	}
}

// A Request sent over pubsub to the workers
type Request struct {
	Subgraph  lib.Graph     // the graph to check balancedness against
	Edges     lib.Edges     // edges to form the separator with
	Predicate lib.Predicate //
	Gen       lib.Generator
	BalFactor int
	ID        string
}

// A Solution is the result sent back by the workers
type Solution struct {
	Valid     bool // true if a solution found, false if not
	ID        string
	Selection []int         // the selection of edges to form the separator, empty if valid is false
	Gen       lib.Generator // sending back the generator to keep track of search state
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

	// fmt.Println("Unencoded graph: ")

	// for _, e := range d.Edges.Slice() {
	// 	fmt.Println(e, "(", (e.Vertices), ")")
	// }

	req := Request{
		Subgraph:  d.H,
		Edges:     *d.Edges,
		Predicate: pred,
		Gen:       d.Generators[0],
		BalFactor: d.BalFactor,
		ID:        "random", // should be fine? 🤷‍♀️️
	}

	// Set up connection to the topic

	var Encodebuffer bytes.Buffer
	enc := gob.NewEncoder(&Encodebuffer)

	gob.Register(pred)
	gob.Register(req.Gen)

	err := enc.Encode(req)
	if err != nil {
		log.Fatal("encode error", err)
	}
	// fmt.Println(Encodebuffer.Bytes())

	// var decodedReq Request
	// dec.Decode(&decodedReq)

	// fmt.Println("initial Graph", d.H.Edges.FullString())
	// fmt.Println("decoded Graph", decodedReq.Subgraph.Edges.FullString())

	// if !reflect.DeepEqual(decodedReq, req) {
	// 	log.Panicln("decoding didn't produce equal request, ", req, decodedReq)
	// }

	ctx := context.Background()

	// Sets your Google Cloud Platform project ID.
	projectID := "hgtest-1"

	// fmt.Println("Creating a client.")

	// Creates a client.
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// fmt.Println("Client created.")

	// Sets the id for the new topic.
	topicID := "workerTopic"
	topic := client.Topic(topicID)

	// topicID2 := "answerTopic"
	// topic2 := client.Topic(topicID2)

	sub := client.Subscription("answerTopic-sub")

	// sub, err := client.CreateSubscription(context.Background(), "testSub",
	// 	pubsub.SubscriptionConfig{Topic: topic2})
	// if err != nil {
	// 	log.Println(err)
	// }

	// if err != nil {
	// 	log.Fatalf("Failed to create topic: %v", err)
	// }

	fmt.Println("writing to topic")

	res := topic.Publish(ctx, &pubsub.Message{
		Data: Encodebuffer.Bytes(),
	})

	// The publish happens asynchronously.

	// Later, you can get the result from res:

	_, err = res.Get(ctx)
	if err != nil {
		log.Fatal(err)
	}

	var sol Solution

	fmt.Println("Read something from sub")

	cctx, cancel := context.WithCancel(ctx)
	err = sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {

		buffer := bytes.NewBuffer(msg.Data)
		dec := gob.NewDecoder(buffer)

		gob.Register(req.ID)

		err = dec.Decode(&sol)

		if err != nil {
			fmt.Println("received data, ", msg.Data)
			log.Fatal("decode error 1:", err)
		}

		if sol.ID != req.ID { // only acknowledge and cancel if message ID fits
			fmt.Println("received message id,", sol.ID)
			fmt.Println("Was expecting, ", req.ID)
			return
		}

		msg.Ack()

		cancel() // cancel right after receiving the first message

	})

	// if err = sub.Delete(ctx); err != nil {
	// 	log.Println(err)
	// }

	d.Result = sol.Selection // set up the current result to the found value

	// tmp := lib.GetSubset(*d.Edges, sol.Selection)
	// fmt.Println("result is", tmp)
	// fmt.Println("Vertices: ", tmp.Vertices())

	d.Generators[0] = sol.Gen // update the generator to keeep track of progress

	if len(d.Result) == 0 {
		d.ExhaustedSearch = true
	}
}

// SearchEnded returns true if search is completed
func (d *DistributedSearch) SearchEnded() bool {
	return d.ExhaustedSearch
}

// GetResult returns the last found result
func (d *DistributedSearch) GetResult() []int {
	return d.Result
}
