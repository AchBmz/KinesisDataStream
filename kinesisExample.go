package main

import (
	"encoding/json"
	"github.com/a8m/kinesis-producer"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/google/uuid"
	"log"
	"sync"
	"time"
)

type UserIDRequest struct {
	UserID        string
	CorrelationID string
}

func main() {
	var wg sync.WaitGroup
	const streamName = "TestingDataStream"
	kc := getKinesisClient()
	wg.Add(1)
	go consume(kc, streamName)
	defer wg.Wait()

	request := UserIDRequest{
		UserID:        uuid.New().String(),
		CorrelationID: uuid.New().String(),
	}
	req, _ := json.Marshal(request)

	pr := producer.New(&producer.Config{
		StreamName:   streamName,
		BacklogCount: 2000,
		Client:       kc,
	})
	pr.Start()
	err := pr.Put(req, "1")
	if err != nil {
		log.Printf("Put Record error: %v\n", err)
	}
	pr.Stop()
}

func consume(kc *kinesis.Kinesis, streamName string) {
	streams, err := kc.DescribeStream(&kinesis.DescribeStreamInput{StreamName: &streamName})
	if err != nil {
		log.Println("Describe Stream Error: ", err)
		log.Panic(err)
	}
	log.Println(streams)
	// retrieve iterator
	iteratorOutput, err := kc.GetShardIterator(&kinesis.GetShardIteratorInput{
		ShardId:           aws.String(*streams.StreamDescription.Shards[0].ShardId),
		ShardIteratorType: aws.String("TRIM_HORIZON"),
		StreamName:        &streamName,
	})
	if err != nil {
		log.Println("Get Shard Iterator Error: ", err)
		log.Panic(err)
	}
	shardIterator := iteratorOutput.ShardIterator

	// get data in infinite loop
	for {
		// get records using shard iterator
		records, err := kc.GetRecords(&kinesis.GetRecordsInput{
			ShardIterator: shardIterator,
		})
		// if error, wait 1 seconds and continue the looping process
		if err != nil {
			time.Sleep(1000 * time.Millisecond)
			continue
		}

		// process the data
		var a *string
		if len(records.Records) > 0 {
			log.Printf("records: %v", records.Records)
		} else if records.NextShardIterator == a || shardIterator == records.NextShardIterator {
			log.Printf("GetRecords Error: %v\n", err)
			break
		}
		// get the returned NextShardIterator to use it with the next iteration
		shardIterator = records.NextShardIterator
		time.Sleep(1000 * time.Millisecond)
	}
}

// getKinesisClient creates an aws kinesis client session
func getKinesisClient() *kinesis.Kinesis {
	s := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	kc := kinesis.New(s)
	return kc
}
