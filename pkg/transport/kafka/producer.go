package kafka

import (
	"github.com/Shopify/sarama"
	"log"
	"github.com/projectriff/message-transport/pkg/message"
	"time"
	"encoding/json"
)

func NewProducer(brokerAddrs []string) (*producer, error) {
	asyncProducer, err := sarama.NewAsyncProducer(brokerAddrs, nil)
	if err != nil {
		return &producer{}, err
	}

	errors := make(chan error)
	go func(errChan <-chan *sarama.ProducerError) {
		for {
			errors <- <-errChan
		}
	}(asyncProducer.Errors())

	p := producer{
		asyncProducer: asyncProducer,
		errors:        errors,
		stats:         make(chan stat, 100),
	}
	go p.emitMetrics()
	return &p, nil
}

type producer struct {
	asyncProducer sarama.AsyncProducer
	errors        chan error

	stats chan stat
}

type stat struct {
	topic string
}

func (p *producer) Send(topic string, message message.Message) error {
	kafkaMsg, err := toKafka(message)
	if err != nil {
		return err
	}
	kafkaMsg.Topic = topic

	p.asyncProducer.Input() <- kafkaMsg
	p.stats <- stat{topic: topic}

	return nil
}

func (p *producer) Errors() <-chan error {
	return p.errors
}

func (p *producer) Close() error {
	err := p.asyncProducer.Close()
	if err != nil {
		log.Fatalln(err)
	}
	return err
}

func (p *producer) emitMetrics() {
	var start time.Time
	var m map[string]int
	var t *time.Timer

	reset := func() {
		t = time.NewTimer(5000 * time.Millisecond)
		start = time.Now()
		m = make(map[string]int)
	}

	reset()

	for {
		select {
		case s := <-p.stats:
			m[s.topic] += 1
		case <-t.C:
			now := time.Now()
			delta := now.Sub(start)
			data := map[string]interface{} {"elapsed":delta, "stats": m}
			bytes, err := json.Marshal(data)
			if err != nil {
				log.Printf("Error %v", err)
			}
			log.Printf("Stats: %v", string(bytes))
			p.asyncProducer.Input() <- &sarama.ProducerMessage{Value: sarama.ByteEncoder(bytes), Topic: "metrics"}
			reset()
		}
	}
}
