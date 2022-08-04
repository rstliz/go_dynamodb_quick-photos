package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func main() {
	f, _ := os.Open("scripts/sample.json")
	bu := bufio.NewReaderSize(f, 1024)

	items := make([]interface{}, 0)

	for {
		line, _, err := bu.ReadLine()
		if err == io.EOF {
			break
		}

		var jsonObj interface{}
		_ = json.Unmarshal(line, &jsonObj)
		items = append(items, jsonObj)
		//fmt.Println(jsonObj)
	}

	f.Close()

	var ctx = context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("ap-northeast-1"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	client := dynamodb.NewFromConfig(cfg)

	for _, item := range items {
		av, err := attributevalue.MarshalMap(item)
		if err != nil {
			fmt.Printf("dynamodb marshal: %s\n", err.Error())
			return
		}

		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String("quick-photos"),
			Item:      av,
		})
		if err != nil {
			fmt.Printf("put item: %s\n", err.Error())
			return
		}
	}

}
