package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	s := &server{
		node:     n,
		messages: make(map[float64]bool, 0),
	}

	n.Handle("broadcast", s.broadcastHandler)
	n.Handle("read", s.readHandler)
	n.Handle("topology", s.topologyHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type server struct {
	node *maelstrom.Node

	messages map[float64]bool
	mu       sync.Mutex
}

func (s *server) broadcastHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	message := body["message"].(float64)
	s.mu.Lock()
	if _, exists := s.messages[message]; exists {
		s.mu.Unlock()
		return nil
	}
	s.messages[message] = true
	s.mu.Unlock()

	s.broadcast(msg.Src, body)

	response := make(map[string]any)
	response["type"] = "broadcast_ok"

	return s.node.Reply(msg, response)
}

func (s *server) readHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	response := make(map[string]any)
	response["type"] = "read_ok"
	result := make([]float64, 0)
	for msg := range s.messages {
		result = append(result, msg)
	}
	response["messages"] = result

	return s.node.Reply(msg, response)
}

func (s *server) topologyHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	response := make(map[string]any)
	response["type"] = "topology_ok"

	return s.node.Reply(msg, response)
}

func (s *server) broadcast(src string, body map[string]any) {
	for _, nodeId := range s.node.NodeIDs() {
		if nodeId == s.node.ID() || nodeId == src {
			continue
		}

		go func() {
			err := s.node.Send(nodeId, body)
			for err != nil {
				time.Sleep(time.Millisecond * 100)
				err = s.node.Send(nodeId, body)
			}
		}()
	}
}
