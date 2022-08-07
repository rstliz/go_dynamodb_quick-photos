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

type Friendship struct {
	FollowedUser  string `dynamodbav:"followedUser"`
	FollowingUser string `dynamodbav:"followingUser"`
	Timestamp     string `dynamodbav:"timestamp"`
}

const (
	USERNAME = "haroldwatkins"
)

func main() {
	var ctx = context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("ap-northeast-1"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	client := dynamodb.NewFromConfig(cfg)

	follows, _ := find_and_enrich_following_for_user(client, USERNAME)

	fmt.Println("Users followed by " + USERNAME)
	for _, follow := range follows {
		fmt.Println(follow.Username, follow.Name)
	}

}

func find_and_enrich_following_for_user(client *dynamodb.Client, username string) ([]User, error) {

	out, err := client.Query(context.TODO(), &dynamodb.QueryInput{
		TableName:              aws.String("quick-photos"),
		IndexName:              aws.String("InvertedIndex"),
		KeyConditionExpression: aws.String("SK = :sk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":sk": &types.AttributeValueMemberS{Value: "#FRIEND#" + username},
		},
		ScanIndexForward: aws.Bool(true),
	})
	if err != nil {
		panic(err)
	}

	keys := []map[string]types.AttributeValue{}
	for _, item := range out.Items {
		var friendship Friendship
		attributevalue.UnmarshalMap(item, &friendship)
		keys = append(keys, map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: "USER#" + friendship.FollowedUser},
			"SK": &types.AttributeValueMemberS{Value: "#METADATA#" + friendship.FollowedUser},
		})
	}

	batchOut, err := client.BatchGetItem(context.TODO(), &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			"quick-photos": {
				Keys: keys,
			},
		},
	})

	if err != nil {
		panic(err)
	}

	var friends []User
	for _, item := range batchOut.Responses["quick-photos"] {
		var user User
		attributevalue.Unmarshal(item["username"], &user.Username)
		attributevalue.Unmarshal(item["name"], &user.Name)
		friends = append(friends, user)
	}
	return friends, nil

}
