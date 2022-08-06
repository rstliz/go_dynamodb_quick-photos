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
	Reactions []Reaction
}

type Reaction struct {
	ReactingUser string `dynamodbav:"reactingUser"`
	Photo        string `dynamodbav:"photo"`
	ReactionType string `dynamodbav:"reactionType"`
	Timestamp    string `dynamodbav:"timestamp"`
}

const (
	USER             = "david25"
	TIMESTAMP string = "2019-03-02T09:11:30"
)

func main() {
	var ctx = context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("ap-northeast-1"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	client := dynamodb.NewFromConfig(cfg)

	photo, _ := fetch_photo_and_reactions(client, USER, TIMESTAMP)

	fmt.Println(photo)
	for _, reaction := range photo.Reactions {
		fmt.Println(reaction)
	}
}

func fetch_photo_and_reactions(client *dynamodb.Client, username string, timestamp string) (Photo, error) {

	out, err := client.Query(context.TODO(), &dynamodb.QueryInput{
		TableName:              aws.String("quick-photos"),
		IndexName:              aws.String("InvertedIndex"),
		KeyConditionExpression: aws.String("SK = :sk AND PK BETWEEN :reactions AND :user"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":sk":        &types.AttributeValueMemberS{Value: fmt.Sprintf("PHOTO#%s#%s", username, timestamp)},
			":user":      &types.AttributeValueMemberS{Value: "USER$"},
			":reactions": &types.AttributeValueMemberS{Value: "REACTION#"},
		},
		ScanIndexForward: aws.Bool(true),
	})
	if err != nil {
		panic(err)
	}

	items := out.Items
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}

	var photo Photo
	attributevalue.UnmarshalMap(items[0], &photo)

	for _, item := range out.Items[1:] {
		var reaction Reaction
		attributevalue.UnmarshalMap(item, &reaction)
		photo.Reactions = append(photo.Reactions, reaction)
	}
	return photo, nil

}
