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
	REACTING_USER   = "kennedyheather"
	REACTION_TYPE   = "sunglasses"
	PHOTO_USER      = "ppierce"
	PHOTO_TIMESTAMP = "2019-04-14T08:09:34"
)

func main() {
	var ctx = context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("ap-northeast-1"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	client := dynamodb.NewFromConfig(cfg)

	add_reaction_to_photo(client, REACTING_USER, REACTION_TYPE, PHOTO_USER, PHOTO_TIMESTAMP)
}

func add_reaction_to_photo(client *dynamodb.Client, reacting_user, reaction_type, photo_user, photo_timestamp string) bool {
	reaction := fmt.Sprintf("REACTION#%s#%s", reacting_user, reaction_type)
	photo := fmt.Sprintf("PHOTO#%s#%s", photo_user, photo_timestamp)
	user := fmt.Sprintf("USER#%s", photo_user)
	input := &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Put: &types.Put{
					TableName: aws.String("quick-photos"),
					Item: map[string]types.AttributeValue{
						"PK":           &types.AttributeValueMemberS{Value: reaction},
						"SK":           &types.AttributeValueMemberS{Value: photo},
						"reactingUser": &types.AttributeValueMemberS{Value: reacting_user},
						"reactionType": &types.AttributeValueMemberS{Value: reaction_type},
						"photo":        &types.AttributeValueMemberS{Value: photo},
						"timestamp":    &types.AttributeValueMemberS{Value: time.Now().Format(time.RFC3339)},
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
						"SK": &types.AttributeValueMemberS{Value: photo},
					},
					UpdateExpression: aws.String("SET reactions.#t = reactions.#t + :i"),
					ExpressionAttributeNames: map[string]string{
						"#t": reaction_type,
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":i": &types.AttributeValueMemberN{Value: "1"},
					},
					ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
				}},
		},
	}

	_, err := client.TransactWriteItems(context.TODO(), input)
	if err != nil {
		fmt.Println(err)
		return false
	}

	fmt.Println(fmt.Sprintf("Added %s reaction from %s", reaction_type, reacting_user))
	return true
}
