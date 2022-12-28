package process

import (
	"context"
	"errors"
	"github.com/obolnetwork/charon/app/log"
	"github.com/obolnetwork/charon/app/z"
	"github.com/stretchr/testify/require"
	"github.com/xenowits/dag-rider/stack"
	"sync"
	"testing"
)

// block is a block of transactions.
type block struct {
	data []byte
}

// vertexID is a unique identifier for a vertex. The round and source uniquely identifies a vertex in a DAG.
type vertexID struct {
	round  int // The round for this vertex
	source int // The ID of the Process which delivered this vertex
}

// The struct of a vertex in the DAG (Directed Acyclic Graph).
type vertex struct {
	id          vertexID
	block       block
	strongEdges []vertexID // A set of vertices in round − 1 that represent strong edges
	weakEdges   []vertexID // A set of vertices in rounds < round − 1 that represent weak edges
}

// New instantiates a new process.
func New(index, faulty int, tp *Transport) (*Process, error) {
	// 𝐷𝐴𝐺𝑖 [0] ← predefined hardcoded set of 2𝑓 + 1 “genesis” vertices.
	// ∀𝑗 ≥ 1: 𝐷𝐴𝐺𝑖 [𝑗] ← {}.
	// Note that round 0 is a bootstrap (initializer) round, not an actual round.
	if index < 1 {
		return &Process{}, errors.New("process indexes should be 1-indexed")
	}

	dag := make([][]vertex, 1)
	for i := 0; i < 2*faulty+1; i++ {
		dag[0] = append(dag[0], vertex{
			id: vertexID{
				source: index,
			},
		})
	}

	return &Process{
		quit:            make(chan struct{}),
		tp:              tp,
		index:           index,
		faulty:          faulty,
		dag:             dag,
		blocksToPropose: []block{},
		buffer:          []vertex{},
	}, nil
}

// NewForT returns a new process for use in testing.
func NewForT(t *testing.T, index, faulty int, tp *Transport) *Process {
	t.Helper()

	p, err := New(index, faulty, tp)
	require.NoError(t, err)

	return p
}

type Process struct {
	mu                sync.Mutex
	quit              chan struct{}
	tp                *Transport
	index             int        // Process's index, p_i (1-indexed)
	round             int        // Current round as registered by this Process
	faulty            int        // No of byzantine faulty processes that are allowed
	dag               [][]vertex // An array of sets of vertices
	blocksToPropose   []block    // A queue, initially empty, 𝑝𝑖 enqueues valid blocks of transactions from clients
	buffer            []vertex   // Buffer contains vertices that are ultimately added to the DAG
	decidedWave       int
	deliveredVertices []vertex
	leadersStack      stack.Stack[vertex] // Stack of leader vertices
}

// path checks if there exists a path consisting of strong and weak edges in the DAG. If the strongPath boolean is set to true, only strong
// edges are considered. Note that we use BFS (Breadth-first search) for this.
func (p Process) path(from, to vertexID, strongPath bool) bool {
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

// Start invokes the goroutines and starts the process. TODO(xenowits): fix concurrency issue in this function.
func (p Process) Start() {
	// Start one goroutine to listen for braodcast messages.
	// 22: upon r_deliver𝑖 (𝑣, round, 𝑝𝑘) do ⊲ The deliver output from the reliable broadcast
	// 23: 𝑣.source ← 𝑝𝑘
	// 24: 𝑣.round ← round
	// 25: if |𝑣.strongEdges| ≥ 2𝑓 + 1 then
	// 26: buffer ← buffer ∪ {𝑣 }
	uponDeliver := func(msg *bcastMsg) {
		msg.v.id = vertexID{
			round:  msg.round,
			source: msg.sender,
		}

		p.mu.Lock()
		if len(msg.v.strongEdges) >= 2*p.faulty+1 {
			p.buffer = append(p.buffer, msg.v)
		}
		p.mu.Unlock()
	}

	// Creating a buffered channel as theoretically, each process broadcasts infinitely many
	// messages with consecutive sequence numbers.
	// A goroutine is started which will accept all broadcast messages.
	ch := make(chan bcastMsg, 10)
	go func() {
		for {
			select {
			case <-p.quit:
				return
			case msg := <-ch:
				uponDeliver(&msg)
			}
		}
	}()

	p.tp.Subscribe(ch)

	// Start another goroutine which checks the process's buffer and adds vertices to DAG.
	// 5: while True do
	// 6: for 𝑣 ∈ buffer: 𝑣.round ≤ 𝑟 do
	// 7: if ∀𝑣′ ∈ 𝑣.strongEdges ∪ 𝑣.weakEdges: 𝑣′ ∈ Ð 𝑘≥1 𝐷𝐴𝐺 [𝑘] then ⊲ We have 𝑣’s predecessors
	// 8: 𝐷𝐴𝐺 [𝑣.round] ← 𝐷𝐴𝐺 [𝑣.round] ∪ {𝑣 }
	// 9: buffer ← buffer \ {𝑣 }
	// 10: if |𝐷𝐴𝐺 [𝑟]| ≥ 2𝑓 + 1 then ⊲ Start a new round
	// 11: if 𝑟 mod 4 = 0 then ⊲ If a new wave is complete
	// 12: wave_ready(𝑟 /4) ⊲ Signal to Algorithm 3 that a new wave is complete
	// 13: 𝑟 ← 𝑟 + 1
	// 14: 𝑣 ← create_new_vertex(𝑟)
	// 15: r_bcast𝑖 (𝑣, 𝑟)
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
			p.waveReady(p.round / 4)
		}

		p.round = p.round + 1
		v := p.createNewVertex(p.round)
		p.reliableBroadcast(v, p.round)
	}
}

// Stop stops the process.
func (p Process) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	close(p.quit)
}

// reliableBroadcast reliably broadcasts the provided vertex to other processes.
func (p *Process) reliableBroadcast(v vertex, round int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	msg := bcastMsg{
		v:      v,
		round:  round,
		sender: p.index,
	}
	p.tp.Broadcast(msg)
}

// createNewVertex creates a new vertex to be added to the DAG.
func (p Process) createNewVertex(round int) vertex {
	// 17: wait until ¬blocksToPropose.empty() ⊲ atomic broadcast blocks are enqueued (Line 32)
	// 18: 𝑣.block ← blocksToPropose.dequeue() ⊲ We assume each process atomically broadcast infinitely many blocks
	// 19: 𝑣.strongEdges ← 𝐷𝐴𝐺 [round − 1]
	// 20: set_weak_edges(𝑣,round)
	// 21: return v

	for len(p.blocksToPropose) == 0 {
		// Wait for some enqueued blocks.
	}

	var resp vertex
	// Dequeue block.
	b := p.blocksToPropose[0]
	p.blocksToPropose = p.blocksToPropose[1:]

	// Add block and strong edges to the new vertex.
	resp.block = b
	for _, v := range p.dag[round-1] {
		resp.strongEdges = append(resp.strongEdges, v.id)
	}

	// Set weak edges.
	p.setWeakEdges(&resp, round)

	return resp
}

// setWeakEdges adds weak edges to orphan vertices.
func (p Process) setWeakEdges(v *vertex, round int) {
	// 29: for 𝑟 = round − 2 down to 1 do
	// 30: for every 𝑢 ∈ 𝐷𝐴𝐺𝑖 [𝑟] s.t. ¬path(𝑣, 𝑢) do
	// 31: 𝑣.weakEdges ← 𝑣.weakEdges ∪ {𝑢}
	for r := round - 2; r >= 1; r-- {
		for _, vtemp := range p.dag[r] {
			if !p.path(v.id, vtemp.id, false) {
				v.weakEdges = append(v.weakEdges, vtemp.id)
			}
		}
	}
}

// waveReady is a signal from the DAG layer that a new wave is completed.
// TODO(xenowits): Fix concurrency issues.
func (p Process) waveReady(wave int) {
	// 35: 𝑣 ← get_wave_vertex_leader(𝑤)
	// 36: if 𝑣 = ⊥ ∨ | {𝑣′ ∈ 𝐷𝐴𝐺𝑖 [round(𝑤,4)]: strong_path(𝑣′, 𝑣)} | < 2𝑓 + 1 then ⊲ No commit
	// 37: return
	// 38: leadersStack.push(𝑣)
	// 39: for wave 𝑤′ from 𝑤 − 1 down to decidedWave + 1 do
	// 40: 𝑣′ ← get_wave_vertex_leader(𝑤′)
	// 41: if 𝑣′ ≠ ⊥ ∧ strong_path(𝑣, 𝑣′) then
	// 42: leadersStack.push(𝑣′)
	// 43: 𝑣 ← 𝑣′
	// 44: decidedWave ← 𝑤
	// 45: order_vertices(leadersStack)
	leader, ok := p.getWaveVertexLeader(wave)
	if !ok {
		return
	}

	var vCount int
	for _, v := range p.dag[waveRound(wave, 4)] {
		if p.path(v.id, leader.id, true) {
			vCount++
		}
	}
	if vCount < 2*p.faulty+1 {
		return
	}

	p.leadersStack.Push(leader)
	for w := wave - 1; w >= p.decidedWave+1; w-- {
		v, ok := p.getWaveVertexLeader(w)
		if !ok || !p.path(leader.id, v.id, true) {
			continue
		}

		p.leadersStack.Push(v)
		leader = v
	}

	p.decidedWave = wave

}

// getWaveVertexLeader returns the leader vertex for the wave if found.
func (p Process) getWaveVertexLeader(w int) (vertex, bool) {
	// 47: 𝑝𝑗 ← choose_leader𝑖 (𝑤)
	// 48: if ∃𝑣 ∈ 𝐷𝐴𝐺 [round(𝑤, 1) ] s.t. 𝑣.𝑠𝑜𝑢𝑟𝑐𝑒 = 𝑝𝑗 then
	// 49: return 𝑣 ⊲ There can only be one such vertex
	// 50: return ⊥
	leader := chooseLeader(w)
	round := waveRound(w, 1)
	for _, v := range p.dag[round] {
		if v.id.source == leader {
			return v, true
		}
	}

	return vertex{}, false
}

// present returns true if the provided vertex is present in the Process's local DAG.
func (p Process) present(vID vertexID) bool {
	for r := 0; r <= p.round; r++ {
		for _, v := range p.dag[r] {
			if v.id == vID {
				return true
			}
		}
	}

	return false
}

// chooseLeader implements a global perfect coin which is unpredictable by the adversary.
// The coin guarantees these properties: agreement, termination, unpredictability and fairness.
// TODO(xenowits): One way to implement a global perfect coin is by using PKI and a threshold signature scheme with a threshold of (𝑓 + 1)-of-𝑛.
// For simplicity of implementation, we're return process 1 as the leader everytime.
func chooseLeader(w int) int {
	return 1
}

// waveRound returns the round given the wave and k.
// For example, 𝑝𝑖’s first wave consists of 𝐷𝐴𝐺𝑖 [1], 𝐷𝐴𝐺𝑖 [2], 𝐷𝐴𝐺𝑖 [3], and 𝐷𝐴𝐺𝑖 [4]. Formally, the 𝑘𝑡ℎ round of wave 𝑤,
// where 𝑘 ∈ [1..4], 𝑤 ∈ N, is defined as round(𝑤, 𝑘) ≜ 4(𝑤 − 1) + 𝑘.
// We also say that a process 𝑝𝑖 completes round 𝑟 once 𝐷𝐴𝐺𝑖 [𝑟] has at least 2𝑓 + 1 vertices, and a
// process completes wave 𝑤 once the process completes round(𝑤, 4). In a nutshell,
// the idea is to interpret the DAG as a wave-bywave protocol and try to commit a randomly chosen single leader vertex in every wave.
func waveRound(w, k int) int {
	return 4*(w-1) + k
}

func (p Process) orderVertices() {
	// 51: procedure order_vertices(leadersStack)
	// 52: while ¬leadersStack.isEmpty() do
	// 53: 𝑣 ← leadersStack.pop()
	// 54: verticesToDeliver ← {𝑣′ ∈ Ð 𝑟 >0 𝐷𝐴𝐺𝑖 [𝑟] | 𝑝𝑎𝑡ℎ(𝑣, 𝑣′) ∧ 𝑣′ ∉ deliveredVertices}
	// 55: for every 𝑣′ ∈ verticesToDeliver in some deterministic order do
	// 56: output a_deliver𝑖 (𝑣′.block, 𝑣′.round, 𝑣′.source)
	// 57: deliveredVertices ← deliveredVertices ∪ {𝑣′}
	for !p.leadersStack.IsEmpty() {
		poppedVertex := p.leadersStack.Pop()
		currRound := p.round

		var verticesToDeliver []vertex
		for r := 1; r <= currRound; r++ {
			for _, temp := range p.dag[r] {
				if !p.path(poppedVertex.id, temp.id, false) { // A path should exist between the vertices
					continue
				}

				for _, deliveredVertex := range p.deliveredVertices {
					if temp.id == deliveredVertex.id { // The vertex should not be already delivered
						continue
					}
				}

				verticesToDeliver = append(verticesToDeliver, temp)
			}
		}

		for _, v := range verticesToDeliver { // The order should be deterministic
			msg := bcastMsg{
				v:      v,
				round:  v.id.round,
				sender: v.id.source,
			}
			p.tp.Broadcast(msg)
			p.deliveredVertices = append(p.deliveredVertices, v)
		}
	}
}
