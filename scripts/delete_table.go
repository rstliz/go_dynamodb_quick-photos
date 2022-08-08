package main

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("ap-northeast-1"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	client := dynamodb.NewFromConfig(cfg)

	err = deleteTable(client, "quick-photos")
	if err != nil {
		log.Fatalf("unable to delete table, %v", err)
	}
}

func deleteTable(client *dynamodb.Client, name string) error {
	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(name),
	}

	if _, err := client.DeleteTable(context.TODO(), input); err != nil {
		return err
	}

	return nil
}
