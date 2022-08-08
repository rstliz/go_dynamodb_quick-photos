package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	FOLLOWED_USER  = "tmartinez"
	FOLLOWING_USER = "john42"
)

func main() {
	var ctx = context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("ap-northeast-1"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	client := dynamodb.NewFromConfig(cfg)

	follow_user(client, FOLLOWED_USER, FOLLOWING_USER)
}

func follow_user(client *dynamodb.Client, followed_user, following_user string) bool {
	user := fmt.Sprintf("USER#%s", followed_user)
	friend := fmt.Sprintf("#FRIEND#%s", following_user)
	user_metadata := fmt.Sprintf("#METADATA#%s", followed_user)
	friend_user := fmt.Sprintf("USER#%s", following_user)
	friend_metadata := fmt.Sprintf("#METADATA#%s", following_user)
	now := time.Now().Format(time.RFC3339)

	input := &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Put: &types.Put{
					TableName: aws.String("quick-photos"),
					Item: map[string]types.AttributeValue{
						"PK":            &types.AttributeValueMemberS{Value: user},
						"SK":            &types.AttributeValueMemberS{Value: friend},
						"followedUser":  &types.AttributeValueMemberS{Value: followed_user},
						"followingUser": &types.AttributeValueMemberS{Value: following_user},
						"timestamp":     &types.AttributeValueMemberS{Value: now},
					},
					ConditionExpression:                 aws.String("attribute_not_exists(SK)"),
					ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
				},
			},
			{
				Update: &types.Update{
					TableName: aws.String("quick-photos"),
					Key: map[string]types.AttributeValue{
						"PK": &types.AttributeValueMemberS{Value: user},
						"SK": &types.AttributeValueMemberS{Value: user_metadata},
					},
					UpdateExpression: aws.String("SET followers = followers + :i"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":i": &types.AttributeValueMemberN{Value: "1"},
					},
					ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
				},
			},
			{
				Update: &types.Update{
					TableName: aws.String("quick-photos"),
					Key: map[string]types.AttributeValue{
						"PK": &types.AttributeValueMemberS{Value: friend_user},
						"SK": &types.AttributeValueMemberS{Value: friend_metadata},
					},
					UpdateExpression: aws.String("SET following = following + :i"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":i": &types.AttributeValueMemberN{Value: "1"},
					},
					ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
				},
			},
		},
	}

	_, err := client.TransactWriteItems(context.TODO(), input)
	if err != nil {
		fmt.Println(err)
		return false
	}

	fmt.Println(fmt.Sprintf("User %s is now following user %s", following_user, followed_user))
	return true
}
