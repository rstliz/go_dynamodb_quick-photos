package main

import (
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type User struct {
	Username           string `dynamodbav:"username"`
	Name               string `dynamodbav:"name"`
	Email              string `dynamodbav:"email"`
	Birthdate          string `dynamodbav:"birthdate"`
	Address            string `dynamodbav:"address"`
	Status             string `dynamodbav:"status"`
	Interests          string `dynamodbav:"interests"`
	PinnedImage        string `dynamodbav:"pinnedImage"`
	RecommendedFriends int64  `dynamodbav:"recommendedFriends"`
	Photos             []Photo
}

type Photo struct {
	Username  string `dynamodbav:"username"`
	Timestamp string `dynamodbav:"timestamp"`
	Location  string `dynamodbav:"location"`
}

func main() {
	var ctx = context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("ap-northeast-1"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	client := dynamodb.NewFromConfig(cfg)

	username := "jacksonjason"

	out, err := client.Query(context.TODO(), &dynamodb.QueryInput{
		TableName:              aws.String("quick-photos"),
		KeyConditionExpression: aws.String("PK = :pk AND SK BETWEEN :metadata AND :photos"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk":       &types.AttributeValueMemberS{Value: "USER#" + username},
			":metadata": &types.AttributeValueMemberS{Value: "#METADATA#" + username},
			":photos":   &types.AttributeValueMemberS{Value: "PHOTO$" + username},
		},
		ScanIndexForward: aws.Bool(true),
	})
	if err != nil {
		panic(err)
	}

	var user User
	attributevalue.UnmarshalMap(out.Items[0], &user)

	for _, item := range out.Items[1:] {
		var photo Photo
		attributevalue.UnmarshalMap(item, &photo)
		user.Photos = append(user.Photos, photo)
	}

	fmt.Println(user)
	for _, photo := range user.Photos {
		fmt.Println(photo)
	}
}
