package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
)

func main() {

	client := getDynamoDbClient()

	printAllTable(client)

	tableName := "Movies"
	createNewTable(tableName, client)

	insertItem(tableName, client)

	getItem(tableName, client)

	printAllTable(client)
	fmt.Println("end")

}

func getItem(tableName string, client *dynamodb.Client) {
	queryInput := &dynamodb.GetItemInput{
		TableName: &tableName,
		Key: map[string]types.AttributeValue{
			"ID": &types.AttributeValueMemberS{Value: "xaslkdfjalks"},
		}}
	getItemResult, err := client.GetItem(context.TODO(), queryInput)

	if err != nil {
		log.Fatalf("Fail to get item, %v", err)
	}

	fmt.Println(getItemResult.Item)
}

func insertItem(tableName string, client *dynamodb.Client) {
	itemInput := dynamodb.PutItemInput{
		TableName: &tableName,
		Item: map[string]types.AttributeValue{
			"ID":  &types.AttributeValueMemberS{Value: "xaslkdfjalks"},
			"URL": &types.AttributeValueMemberS{Value: "https://google.com"},
		},
	}

	client.PutItem(context.TODO(), &itemInput)
}

func createNewTable(tableName string, client *dynamodb.Client) {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("ID"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("ID"),
				KeyType:       types.KeyTypeHash,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
		TableName:   aws.String(tableName),
	}

	_, err := client.CreateTable(context.TODO(), input)
	var rie *types.ResourceInUseException
	if errors.As(err, &rie) {
		return
	}
	if err != nil {
		log.Fatalf("Got error calling CreateTable: %s", err)
	}

	fmt.Println("Created the table", tableName)
}

func printAllTable(client *dynamodb.Client) {
	result, err := client.ListTables(context.TODO(), &dynamodb.ListTablesInput{})
	if err != nil {
		log.Fatalf("Got error calling CreateTable: %s", err)
	}
	fmt.Println(result)
	fmt.Println("===========================")
}

func getDynamoDbClient() *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion("us-west-2"),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL: "http://localhost:8000",
			}, nil
		})),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	svc := dynamodb.NewFromConfig(cfg)
	return svc
}
