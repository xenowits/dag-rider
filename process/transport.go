package process

import "sync"

// Transport is a reliable communication layer that acts as a broker among the communicating processes.
type Transport struct {
	mu   sync.Mutex
	subs []chan<- bcastMsg
}

// bcastMsg is the message that is broadcasted by the sending process. This message is meant to be
// delivered to each process.
type bcastMsg struct {
	v      vertex
	round  int
	sender int // Process index of the broadcaster
}

// Broadcast sends the input message to all receivers.
func (t *Transport) Broadcast(msg bcastMsg) {
	for _, c := range t.subs {
		c <- msg
	}
}

// Subscribe registers the channel for broadcasting messages to it.
func (t *Transport) Subscribe(c chan<- bcastMsg) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.subs = append(t.subs, c)
}
