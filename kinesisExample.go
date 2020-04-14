package main

import (
	"fmt"
	"github.com/3almadmoon/protobuf/common"
	user "github.com/3almadmoon/protobuf/usersvc"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"log"
	"sync"
	"time"
)

func main() {
	var wg sync.WaitGroup
	const streamName = "TestingDataStream"
	kc := getKinesisClient()
	wg.Add(1)
	go consume(kc, streamName)
	defer wg.Wait()

	request := &user.UserIDRequest{
		UserID:        &common.UserID{Value: uuid.New().String()},
		CorrelationID: uuid.New().String(),
	}

	req, err := proto.Marshal(request)
	if err != nil {
		fmt.Println("Marshal Error: ", err)
		panic(err)
	}

	putOutput, err := kc.PutRecord(&kinesis.PutRecordInput{
		Data:         req,
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String("1"),
	})
	if err != nil {
		fmt.Println("Put Record Error: ", err)
		panic(err)
	}
	fmt.Printf("Put Record Output: %v\n", putOutput)
}

func consume(kc *kinesis.Kinesis, streamName string) {
	streams, err := kc.DescribeStream(&kinesis.DescribeStreamInput{StreamName: &streamName})
	if err != nil {
		fmt.Println("Describe Stream Error: ", err)
		log.Panic(err)
	}
	fmt.Println(streams)
	// retrieve iterator
	iteratorOutput, err := kc.GetShardIterator(&kinesis.GetShardIteratorInput{
		ShardId:           aws.String(*streams.StreamDescription.Shards[0].ShardId),
		ShardIteratorType: aws.String("TRIM_HORIZON"),
		StreamName:        &streamName,
	})
	if err != nil {
		fmt.Println("Get Shard Iterator Error: ", err)
		log.Panic(err)
	}
	shardIterator := iteratorOutput.ShardIterator

	// get data using infinite loop
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
			processRecords(records.Records)
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

// processRecords loops over data returned from GetRecords and displays every record
func processRecords(records []*kinesis.Record) {
	for _, d := range records {
		fmt.Println(records)
		newMessage := &user.UserIDRequest{}
		err := proto.Unmarshal(d.Data, newMessage)
		if err != nil {
			log.Println(err)
			continue
		}
		log.Printf("GetRecords Data: %v\n", newMessage)
	}
	return
}
