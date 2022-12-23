package process

import (
	"context"
	"errors"
	"github.com/obolnetwork/charon/app/log"
	"github.com/obolnetwork/charon/app/z"
	"github.com/stretchr/testify/require"
	"testing"
)

// block is a block of transactions.
type block struct{}

// vertexID is a unique identifier for a vertex. The round and source uniquely identifies a vertex in a DAG.
type vertexID struct {
	round  int // The round for this vertex
	source int // The ID of the process which delivered this vertex
}

// The struct of a vertex in the DAG (Directed Acyclic Graph).
type vertex struct {
	id          vertexID
	block       block
	strongEdges []vertexID // A set of vertices in round − 1 that represent strong edges
	weakEdges   []vertexID // A set of vertices in rounds < round − 1 that represent weak edges
}

func New(index, faulty int) (process, error) {
	// 𝐷𝐴𝐺𝑖 [0] ← predefined hardcoded set of 2𝑓 + 1 “genesis” vertices.
	// ∀𝑗 ≥ 1: 𝐷𝐴𝐺𝑖 [𝑗] ← {}.
	// Note that round 0 is a bootstrap (initializer) round, not an actual round.
	if index < 1 {
		return process{}, errors.New("process indexes should be 1-indexed")
	}

	dag := make([][]vertex, 1)
	for i := 0; i < 2*faulty+1; i++ {
		dag[0] = append(dag[0], vertex{
			id: vertexID{
				source: index,
			},
		})
	}

	return process{
		index:           index,
		faulty:          faulty,
		dag:             dag,
		blocksToPropose: []block{},
	}, nil
}

// NewForT returns a new process for use in testing.
func NewForT(t *testing.T, index, faulty int) process {
	t.Helper()

	p, err := New(index, faulty)
	require.NoError(t, err)

	return p
}

type process struct {
	index           int        // Process's index, p_i (1-indexed)
	round           int        // Current round as registered by this process
	faulty          int        // No of byzantine faulty processes that are allowed
	dag             [][]vertex // An array of sets of vertices
	blocksToPropose []block    // A queue, initially empty, 𝑝𝑖 enqueues valid blocks of transactions from clients
	buffer          []vertex
}

// path checks if there exists a path consisting of strong and weak edges in the DAG. If the strongPath boolean is set to true, only strong
// edges are considered. Note that we use BFS (Breadth-first search) for this.
func (p process) path(from, to vertexID, strongPath bool) bool {
	// return exists a sequence of 𝑘 ∈ N, vertices 𝑣1, 𝑣2, . . . , 𝑣𝑘 s.t. 𝑣1 = 𝑣, 𝑣𝑘 = 𝑢.
	if from == to { // A path always exists between the same vertex (self-loop).
		return true
	}

	// Mark all vertices as unvisited.
	var (
		queue   []vertexID
		visited = make(map[vertexID]bool)
	)

	// Mark starting vertex as visited and enqueue it.
	visited[from] = true
	queue = append(queue, from)

	for len(queue) > 0 {
		// Dequeue a vertex.
		vID := queue[0]
		queue = queue[1:]
		log.Debug(context.Background(), "vertex dequeued", z.Any("vertexid", vID))

		var v vertex
		for _, temp := range p.dag[vID.round] {
			if temp.id == vID {
				v = temp
			}
		}

		// Get all adjacent vertices of the dequeued vertex s.
		// If an adjacent vertex has not been visited, then mark it visited and enqueue it.
		// Start with strong edges.
		for _, temp := range v.strongEdges {
			if !visited[temp] {
				if temp == to {
					return true
				}

				visited[temp] = true
				queue = append(queue, temp)
			}
		}

		if !strongPath {
			// Then weak edges if a strong path is not required.
			for _, temp := range v.weakEdges {
				if !visited[temp] {
					if temp == to {
						return true
					}

					visited[temp] = true
					queue = append(queue, temp)
				}
			}
		}
	}

	return false
}

// Always is meant to be run as a goroutine by the caller process.
func (p process) Always() {
	// Each process 𝑝𝑖 continuously goes through its buffer to check if there is a vertex 𝑣
	// in it that can be added to its 𝐷𝐴𝐺i.
	for true {
		var newBuffer []vertex
		for _, v := range p.buffer {
			if v.id.round > p.round {
				newBuffer = append(newBuffer, v)
				continue
			}

			var absentPre bool
			// Check if all of v's predecessors are present in the DAG.
			for _, pre := range v.strongEdges {
				if !p.present(pre) {
					absentPre = true
					log.Debug(context.Background(), "strong predecessor not present", z.Any("pre", pre), z.Any("current", v.id))
					break
				}
			}
			for _, pre := range v.weakEdges {
				if !p.present(pre) {
					absentPre = true
					log.Debug(context.Background(), "weak predecessor not present", z.Any("pre", pre), z.Any("current", v.id))
					break
				}
			}

			if absentPre { // The vertex cannot be processed so added to the next buffer
				newBuffer = append(newBuffer, v)
			} else { // All predecessors present
				// Add the vertex to DAG.
				p.dag[v.id.round] = append(p.dag[v.id.round], v)
			}
		}

		p.buffer = newBuffer
	}

	if len(p.dag[p.round]) >= 2*p.faulty+1 { // Start a new round
		// If a new wave is complete, signal to Algorithm 3 that a new wave is complete.
		if p.round%4 == 0 {
			p.waveReady()
		}

		p.round = p.round + 1
		v := p.createNewVertex(p.round)
		p.reliableBroadcast(v, p.round)
	}
}

// reliableBroadcast reliably broadcasts the provided vertex to other processes. TODO(xenowits): Complete this method.
func (p process) reliableBroadcast(v vertex, round int) {}

// createNewVertex creates a new vertex to be added to the DAG. TODO(xenowits): Complete this method.
func (p process) createNewVertex(round int) vertex {
	return vertex{}
}

// TODO(xenowits): Complete this method.
func (p process) waveReady() {}

// present returns true if the provided vertex is present in the process's local DAG.
func (p process) present(vID vertexID) bool {
	for r := 0; r <= p.round; r++ {
		for _, v := range p.dag[r] {
			if v.id == vID {
				return true
			}
		}
	}

	return false
}
