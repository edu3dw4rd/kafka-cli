package main

import (
	"crypto/sha512"
	"fmt"
	"log"
	"strings"

	"github.com/IBM/sarama"
	"github.com/spf13/cobra"
	"github.com/xdg-go/scram"
)

type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *XDGSCRAMClient) Begin(userName, password, authID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

func NewScramClientFunc() func() sarama.SCRAMClient {
	// fcn := SHA512Fcn
	// if mechanismType == sarama.SASLTypeSCRAMSHA256 {
	// 	fcn = SHA256Fcn
	// }
	return func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: sha512.New} }
}

func main() {
	var (
		brokers string
		// group   string
		topic              string
		headers            string
		message            string
		username, password string
	)

	var rootCmd = &cobra.Command{
		Use:   "kafka-cli",
		Short: "Kafka CLI is a tool for sending messages to Kafka with headers",
		Run: func(cmd *cobra.Command, args []string) {
			headers := parseHeaders(headers)
			sendMessage(brokers, topic, message, username, password, headers)
		},
	}

	rootCmd.Flags().StringVarP(&brokers, "brokers", "b", "localhost:9092", "Comma-separated list of Kafka brokers")
	rootCmd.Flags().StringVarP(&topic, "topic", "t", "", "Kafka topic to send the message to")
	rootCmd.Flags().StringVarP(&message, "message", "m", "", "Message to send to Kafka")
	rootCmd.Flags().StringVarP(&headers, "headers", "H", "", "Comma-separated list of headers (e.g., key1=value1,key2=value2)")
	rootCmd.Flags().StringVarP(&username, "username", "u", "", "ACL username")
	rootCmd.Flags().StringVarP(&password, "password", "p", "", "ACL password")
	rootCmd.MarkFlagRequired("topic")
	rootCmd.MarkFlagRequired("message")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		return
	}
}

func parseHeaders(headers string) []sarama.RecordHeader {
	var (
		headerList []sarama.RecordHeader
	)
	headerPairs := strings.Split(headers, ",")

	for _, pair := range headerPairs {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 {
			headerList = append(headerList, sarama.RecordHeader{
				Key:   []byte(kv[0]),
				Value: []byte(kv[1]),
			})
		}
	}

	return headerList
}

func sendMessage(brokers, topic, message, username, password string, headers []sarama.RecordHeader) {
	log.Printf("brokers: %s; topic: %s; message: %s; username: %s; password: %s; headers: %+v\n", strings.Split(brokers, ","), topic, message, username, password, headers)
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Net.SASL.Enable = true
	config.Net.SASL.User = username
	config.Net.SASL.Password = password
	config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
	config.Net.SASL.SCRAMClientGeneratorFunc = NewScramClientFunc()
	// config.Net.SASL.Handshake = true
	// config.Net.TLS.Enable = false
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Version = sarama.V2_1_0_0 // Use appropriate Kafka version

	producer, err := sarama.NewSyncProducer(strings.Split(brokers, ","), config)
	if err != nil {
		log.Fatalf("Failed to start Sarama producer: %v", err)
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic:   topic,
		Value:   sarama.StringEncoder(message),
		Headers: headers,
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)
}
