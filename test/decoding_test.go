package test

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/cem-okulmus/BalancedGo/lib"
	cloudlib "github.com/cem-okulmus/GHDDistributedSearch/lib"
)

var EDGE int

//getRandomEdge will produce a random Edge
func getRandomEdge(size int) lib.Edge {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)

	arity := r.Intn(size) + 1
	var vertices []int
	name := r.Intn(size*10) + EDGE + 1
	EDGE = name

	for i := 0; i < arity; i++ {

		vertices = append(vertices, r.Intn(size*10)+i+1)
	}

	return lib.Edge{Name: name, Vertices: vertices}
}

//getRandomGraph will produce a random Graph
func getRandomGraph(size int) (lib.Graph, map[string]int) {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	card := r.Intn(size) + 1

	var edges []lib.Edge
	var SpEdges []lib.Edges

	for i := 0; i < card; i++ {
		edges = append(edges, getRandomEdge(size))
	}

	outString := lib.Graph{Edges: lib.NewEdges(edges), Special: SpEdges}.ToHyberBenchFormat()
	parsedGraph, pGraph := lib.GetGraph(outString)

	return parsedGraph, pGraph.Encoding
}

func TestDecoding(t *testing.T) {

	graphInitial, _ := getRandomGraph(10)

	gen := lib.CombinationIterator{
		N:           graphInitial.Edges.Len(),
		K:           2,
		Combination: []int{0, 1},
		StepSize:    1,
		Extended:    false,
	}

	req := cloudlib.Request{
		Subgraph:  graphInitial,
		Predicate: lib.BalancedCheck{},
		Gen:       &gen,
		BalFactor: 2,
	}

	var Encodebuffer bytes.Buffer
	enc := gob.NewEncoder(&Encodebuffer)

	gob.Register(req.Predicate)
	gob.Register(req.Gen)

	err := enc.Encode(req)
	if err != nil {
		log.Fatal("encode error", err)
	}

	DecodeBuffer := bytes.NewBuffer(Encodebuffer.Bytes())
	dec := gob.NewDecoder(DecodeBuffer)

	var request cloudlib.Request
	err = dec.Decode(&request) // parse the incoming byte slice

	if err != nil {
		fmt.Println("Decode error", err)
		t.Errorf("Issue with decoding!")
	}

	sol := cloudlib.Solution{
		Valid:     false,
		ID:        0,
		Selection: []int{},
	}

	var Encodebuffer2 bytes.Buffer
	enc2 := gob.NewEncoder(&Encodebuffer2)

	err = enc2.Encode(sol)
	if err != nil {
		log.Fatal("encode error", err)
	}

	DecodeBuffer2 := bytes.NewBuffer(Encodebuffer2.Bytes())
	dec2 := gob.NewDecoder(DecodeBuffer2)

	var solution cloudlib.Solution
	err = dec2.Decode(&solution) // parse the incoming byte slice

	if err != nil {
		fmt.Println("Decode error", err)
		t.Errorf("Issue with decoding!")
	}

}
